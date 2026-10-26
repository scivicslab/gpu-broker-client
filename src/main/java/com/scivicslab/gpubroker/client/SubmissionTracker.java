package com.scivicslab.gpubroker.client;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Counts each {@code queueName}'s not-yet-completed jobs and hands out admission, wrapped as a
 * single POJO-actor so that {@link #requestSlot}, {@link #recordSubmitted}, and {@link
 * #pollOutstanding} are serialized through one mailbox -- no {@code Semaphore}, no
 * {@code AtomicInteger}, no shared mutable collection touched from more than one thread. See
 * {@code FlowControlSdkDesign_260820_oo01}.
 *
 * <p>Holds no {@code ActorRef}/{@code ActorSystem} of its own -- unlike {@code JobQueue}, it
 * never needs to reach another actor by name; the only outside call it makes is the plain,
 * stateless {@link JobResultPoller} passed in at construction.
 */
final class SubmissionTracker {

    private final int targetPerQueue;
    private final JobResultPoller poller;

    private final Map<String, Integer> inFlight = new HashMap<>();
    private final Map<String, Deque<CompletableFuture<Void>>> waiting = new HashMap<>();
    private final Map<String, Map<String, Consumer<JobResult>>> outstanding = new HashMap<>();

    /** How long the broker may answer 404 for a job before the job is reported {@link JobResult.Status#FAILED}. */
    static final Duration DEFAULT_LOST_AFTER = Duration.ofMinutes(2);

    private final Duration lostAfter;
    private final Supplier<Instant> clock;
    /** When each job was first answered 404; cleared the moment the broker knows the job again. */
    private final Map<String, Instant> firstMissing = new HashMap<>();

    SubmissionTracker(int targetPerQueue, JobResultPoller poller) {
        this(targetPerQueue, poller, DEFAULT_LOST_AFTER, Instant::now);
    }

    SubmissionTracker(int targetPerQueue, JobResultPoller poller, Duration lostAfter, Supplier<Instant> clock) {
        this.targetPerQueue = targetPerQueue;
        this.poller = poller;
        this.lostAfter = lostAfter;
        this.clock = clock;
    }

    /**
     * Reserves a slot for {@code queueName} if it is still under {@link #targetPerQueue}, or
     * parks the caller behind a not-yet-completed future otherwise. Never blocks -- the
     * returned future is what the caller (on its own thread, outside this actor) blocks on.
     */
    CompletableFuture<Void> requestSlot(String queueName) {
        int current = inFlight.getOrDefault(queueName, 0);
        if (current < targetPerQueue) {
            inFlight.put(queueName, current + 1);
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        waiting.computeIfAbsent(queueName, k -> new ArrayDeque<>()).addLast(future);
        return future;
    }

    /** Registers a just-submitted {@code jobId} for {@link #pollOutstanding} to track. */
    void recordSubmitted(String queueName, String jobId, Consumer<JobResult> onComplete) {
        outstanding.computeIfAbsent(queueName, k -> new LinkedHashMap<>()).put(jobId, onComplete);
    }

    /**
     * Scheduled periodically (see {@link GpuBrokerClient}'s constructor) against this actor, so
     * it runs serialized with {@link #requestSlot}/{@link #recordSubmitted} like every other
     * message -- the same shape as {@code JobQueue.reconcileIdleEndpoints}. Polls every still-
     * outstanding job once; each one that has reached {@code DONE}/{@code FAILED} is reported to
     * its completion callback (if any) and its slot is released.
     */
    void pollOutstanding() {
        for (Map.Entry<String, Map<String, Consumer<JobResult>>> queueEntry : outstanding.entrySet()) {
            String queueName = queueEntry.getKey();
            Iterator<Map.Entry<String, Consumer<JobResult>>> jobs = queueEntry.getValue().entrySet().iterator();
            while (jobs.hasNext()) {
                Map.Entry<String, Consumer<JobResult>> job = jobs.next();
                JobResult result;
                try {
                    result = poller.getJobResult(queueName, job.getKey());
                } catch (GpuBrokerClientException e) {
                    // The broker is unreachable (restarting): keep the job and ask again next time.
                    continue;
                }
                if (result.status() == JobResult.Status.UNKNOWN) {
                    Instant since = firstMissing.computeIfAbsent(job.getKey(), id -> clock.get());
                    if (Duration.between(since, clock.get()).compareTo(lostAfter) < 0) {
                        continue;
                    }
                    result = new JobResult(JobResult.Status.FAILED, null, null,
                            "the broker no longer has job " + job.getKey() + " (HTTP 404 for "
                                    + lostAfter.toSeconds() + " seconds or more)", null);
                }
                if (result.status() == JobResult.Status.PENDING) {
                    firstMissing.remove(job.getKey());
                    continue;
                }
                firstMissing.remove(job.getKey());
                if (job.getValue() != null) {
                    job.getValue().accept(result);
                }
                jobs.remove();
                releaseSlot(queueName);
            }
        }
    }

    /**
     * Hands the freed slot directly to the oldest waiter, if any; otherwise lowers the count.
     * Called from {@link #pollOutstanding} when a job completes, and from {@link GpuBrokerClient#submit}
     * when the POST after {@link #requestSlot} fails and the reserved slot has no job to complete it.
     */
    void releaseSlot(String queueName) {
        Deque<CompletableFuture<Void>> queueWaiting = waiting.get(queueName);
        if (queueWaiting != null && !queueWaiting.isEmpty()) {
            queueWaiting.pollFirst().complete(null);
            return;
        }
        inFlight.merge(queueName, -1, Integer::sum);
    }

    /** Whether every {@code queueName} has zero outstanding jobs and zero waiters. See {@link GpuBrokerClient#close}. */
    boolean isDrained() {
        return outstanding.values().stream().allMatch(Map::isEmpty)
                && waiting.values().stream().allMatch(Deque::isEmpty);
    }
}
