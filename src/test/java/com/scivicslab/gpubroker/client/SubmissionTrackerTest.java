package com.scivicslab.gpubroker.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pure-POJO tests: {@link SubmissionTracker} is exercised directly, not through an
 * {@code ActorRef} -- matches {@code JobQueuePriorityTest}'s convention. {@link FakeJobResultPoller}
 * plays the same role {@code LatchAiServiceClient} plays for {@code AiServiceEndpointWorker}.
 */
@DisplayName("SubmissionTracker — per-queueName admission and completion-driven release")
class SubmissionTrackerTest {

    @Test
    void requestSlot_isImmediatelyGranted_belowTarget() {
        SubmissionTracker tracker = new SubmissionTracker(2, new FakeJobResultPoller());

        assertTrue(tracker.requestSlot("q1").isDone());
        assertTrue(tracker.requestSlot("q1").isDone());
    }

    @Test
    void requestSlot_parksTheCaller_atTarget() {
        SubmissionTracker tracker = new SubmissionTracker(1, new FakeJobResultPoller());

        assertTrue(tracker.requestSlot("q1").isDone());
        assertFalse(tracker.requestSlot("q1").isDone());
    }

    @Test
    void pollOutstanding_releasesTheWaitingSlot_oncePendingBecomesDone() {
        FakeJobResultPoller poller = new FakeJobResultPoller();
        SubmissionTracker tracker = new SubmissionTracker(1, poller);

        tracker.requestSlot("q1").join();
        CompletableFuture<Void> secondSlot = tracker.requestSlot("q1");
        assertFalse(secondSlot.isDone());

        tracker.recordSubmitted("q1", "job-1", null);
        tracker.pollOutstanding();
        assertFalse(secondSlot.isDone(), "still PENDING -- should not release yet");

        poller.complete("job-1");
        tracker.pollOutstanding();
        assertTrue(secondSlot.isDone(), "job-1 reached DONE -- the waiting submit should be admitted");
    }

    @Test
    void pollOutstanding_invokesTheCompletionCallback() {
        FakeJobResultPoller poller = new FakeJobResultPoller();
        SubmissionTracker tracker = new SubmissionTracker(5, poller);
        List<JobResult.Status> seen = new ArrayList<>();

        tracker.requestSlot("q1").join();
        tracker.recordSubmitted("q1", "job-1", result -> seen.add(result.status()));

        poller.complete("job-1");
        tracker.pollOutstanding();

        assertEquals(List.of(JobResult.Status.DONE), seen);
    }

    @Test
    void isDrained_falseUntilEveryOutstandingJobCompletes() {
        FakeJobResultPoller poller = new FakeJobResultPoller();
        SubmissionTracker tracker = new SubmissionTracker(5, poller);

        assertTrue(tracker.isDrained());

        tracker.requestSlot("q1").join();
        tracker.recordSubmitted("q1", "job-1", null);
        assertFalse(tracker.isDrained());

        poller.complete("job-1");
        tracker.pollOutstanding();
        assertTrue(tracker.isDrained());
    }

    @Test
    void releaseSlot_withoutAnyJobRecorded_admitsTheParkedWaiter() {
        SubmissionTracker tracker = new SubmissionTracker(1, new FakeJobResultPoller());

        tracker.requestSlot("q1").join();                       // reserved, then the POST fails
        CompletableFuture<Void> secondSlot = tracker.requestSlot("q1");
        assertFalse(secondSlot.isDone());

        tracker.releaseSlot("q1");                              // what submit() does on a failed POST
        assertTrue(secondSlot.isDone(), "the failed submission's slot must go to the waiter");
    }

    @Test
    void releaseSlot_withoutAnyWaiter_lowersTheCountSoTheNextRequestIsGranted() {
        SubmissionTracker tracker = new SubmissionTracker(1, new FakeJobResultPoller());

        tracker.requestSlot("q1").join();
        tracker.releaseSlot("q1");

        assertTrue(tracker.requestSlot("q1").isDone(), "count is back to 0, so the slot is granted at once");
    }

    /** Deterministic {@link JobResultPoller} stub: a job stays PENDING until explicitly {@link #complete}d. */
    private static final class FakeJobResultPoller implements JobResultPoller {

        private final Map<String, Boolean> done = new HashMap<>();

        void complete(String jobId) {
            done.put(jobId, true);
        }

        @Override
        public JobResult getJobResult(String queueName, String jobId) {
            if (done.getOrDefault(jobId, false)) {
                return new JobResult(JobResult.Status.DONE, new byte[0], "text/plain", null, null);
            }
            return JobResult.pending();
        }
    }

    /** The broker lost every job it was holding (it restarted without them): it answers 404 for all. */
    private static final class LostJobPoller implements JobResultPoller {
        boolean unreachable;

        @Override
        public JobResult getJobResult(String queueName, String jobId) {
            if (unreachable) {
                throw new GpuBrokerClientException("connection refused", new java.io.IOException());
            }
            return JobResult.unknown();
        }
    }

    @Test
    void pollOutstanding_keepsAJobTheBrokerAnswers404For_untilTheGracePeriodPasses() {
        LostJobPoller poller = new LostJobPoller();
        java.time.Instant[] now = {java.time.Instant.parse("2026-10-25T00:00:00Z")};
        SubmissionTracker tracker = new SubmissionTracker(1, poller, java.time.Duration.ofSeconds(60), () -> now[0]);
        tracker.requestSlot("q1");
        java.util.List<JobResult> reported = new java.util.ArrayList<>();
        tracker.recordSubmitted("q1", "job-1", reported::add);

        tracker.pollOutstanding();
        now[0] = now[0].plusSeconds(59);
        tracker.pollOutstanding();

        assertTrue(reported.isEmpty(), "within the grace period the job is still waited for");
        assertTrue(!tracker.isDrained());
    }

    @Test
    void pollOutstanding_reportsALostJobFailed_andGivesItsSlotBack() {
        LostJobPoller poller = new LostJobPoller();
        java.time.Instant[] now = {java.time.Instant.parse("2026-10-25T00:00:00Z")};
        SubmissionTracker tracker = new SubmissionTracker(1, poller, java.time.Duration.ofSeconds(60), () -> now[0]);
        tracker.requestSlot("q1");
        java.util.List<JobResult> reported = new java.util.ArrayList<>();
        tracker.recordSubmitted("q1", "job-1", reported::add);
        java.util.concurrent.CompletableFuture<Void> parked = tracker.requestSlot("q1");
        assertTrue(!parked.isDone(), "the only slot is held by the lost job");

        tracker.pollOutstanding();
        now[0] = now[0].plusSeconds(60);
        tracker.pollOutstanding();

        assertEquals(1, reported.size());
        assertEquals(JobResult.Status.FAILED, reported.get(0).status());
        assertTrue(reported.get(0).error().contains("job-1"));
        assertTrue(parked.isDone(), "the slot the lost job held now admits the parked caller");
        assertTrue(tracker.isDrained());
    }

    @Test
    void pollOutstanding_aBrokerThatIsUnreachableIsNotALostJob() {
        LostJobPoller poller = new LostJobPoller();
        poller.unreachable = true;
        java.time.Instant[] now = {java.time.Instant.parse("2026-10-25T00:00:00Z")};
        SubmissionTracker tracker = new SubmissionTracker(1, poller, java.time.Duration.ofSeconds(60), () -> now[0]);
        tracker.requestSlot("q1");
        java.util.List<JobResult> reported = new java.util.ArrayList<>();
        tracker.recordSubmitted("q1", "job-1", reported::add);

        now[0] = now[0].plusSeconds(3600);
        tracker.pollOutstanding();   // must not throw, and must not fail the job

        assertTrue(reported.isEmpty());
        assertTrue(!tracker.isDrained());
    }

    @Test
    void pollOutstanding_theGraceClockRestartsWhenTheBrokerKnowsTheJobAgain() {
        java.util.concurrent.atomic.AtomicBoolean known = new java.util.concurrent.atomic.AtomicBoolean(false);
        JobResultPoller flaky = (queue, id) -> known.get() ? JobResult.pending() : JobResult.unknown();
        java.time.Instant[] now = {java.time.Instant.parse("2026-10-25T00:00:00Z")};
        SubmissionTracker tracker = new SubmissionTracker(1, flaky, java.time.Duration.ofSeconds(60), () -> now[0]);
        tracker.requestSlot("q1");
        java.util.List<JobResult> reported = new java.util.ArrayList<>();
        tracker.recordSubmitted("q1", "job-1", reported::add);

        tracker.pollOutstanding();            // 404 at t=0
        now[0] = now[0].plusSeconds(50);
        known.set(true);
        tracker.pollOutstanding();            // known again at t=50: PENDING
        known.set(false);
        now[0] = now[0].plusSeconds(50);
        tracker.pollOutstanding();            // 404 again at t=100: the clock starts over

        assertTrue(reported.isEmpty());
    }
}
