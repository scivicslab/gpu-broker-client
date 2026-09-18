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
}
