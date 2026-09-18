package com.scivicslab.gpubroker.client;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.scivicslab.pojoactor.core.ActorRef;
import com.scivicslab.pojoactor.core.ActorSystem;
import com.scivicslab.pojoactor.core.scheduler.Scheduler;

/**
 * Flow-controlled client for {@code quarkus-gpu-broker}'s async submit-then-poll path
 * ({@code POST /jobs/{queueName}}). One instance corresponds to one {@code submitterId}. {@link
 * #submit} blocks the caller while its {@code queueName} already has {@code targetInFlightPerQueue}
 * jobs outstanding, and unblocks as soon as one of them completes -- see
 * {@code FlowControlSdkDesign_260820_oo01}.
 *
 * <p>Owns a private {@link ActorSystem} and a {@link SubmissionTracker} actor that hold all
 * flow-control state; nothing in this class touches a {@code Semaphore} or any other raw
 * concurrency primitive directly.
 *
 * <p>Because the tracking is in-process, the calling program must stay alive from the first
 * {@link #submit} until {@link #close} returns -- see {@code FlowControlSdkDesign_260820_oo01},
 * "投入元プログラム自身が、投入が終わるまで生き続ける必要がある".
 */
public final class GpuBrokerClient implements AutoCloseable {

    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);
    private static final Duration DRAIN_POLL_INTERVAL = Duration.ofMillis(200);

    private final GpuBrokerHttp http;
    private final ActorSystem system;
    private final ActorRef<SubmissionTracker> tracker;
    private final Scheduler scheduler;
    private volatile boolean closed = false;

    /**
     * @param baseUrl                 e.g. {@code "http://192.168.5.9:28003"}
     * @param submitterId              sent as {@code X-Submitter-Id} on every request
     * @param targetInFlightPerQueue   how many not-yet-completed jobs {@link #submit} allows per
     *                                 {@code queueName} before it starts blocking; keep this at
     *                                 or below the server's own admission limit for this
     *                                 submitter (see {@code BackgroundJobAdmissionControl_260820_oo01})
     */
    public GpuBrokerClient(String baseUrl, String submitterId, int targetInFlightPerQueue) {
        this.http = new GpuBrokerHttp(baseUrl, submitterId);
        this.system = new ActorSystem("gpu-broker-client-" + submitterId);
        this.tracker = system.actorOf("submission-tracker", new SubmissionTracker(targetInFlightPerQueue, http));
        this.scheduler = new Scheduler();
        scheduler.scheduleWithFixedDelay("poll", tracker, SubmissionTracker::pollOutstanding,
                POLL_INTERVAL.toSeconds(), POLL_INTERVAL.toSeconds(), TimeUnit.SECONDS);
    }

    /** Submits without a completion callback -- fire-and-forget beyond the flow-control wait. */
    public JobHandle submit(String queueName, byte[] body, String contentType, Priority priority) {
        return submit(queueName, body, contentType, priority, null);
    }

    /**
     * Blocks the calling thread until {@code queueName} has room (see the class Javadoc), then
     * submits. {@code onComplete}, if given, runs on {@link SubmissionTracker}'s polling thread
     * once the job reaches {@code DONE}/{@code FAILED} -- keep it quick and non-blocking, the
     * same way any actor message handler should be.
     */
    public JobHandle submit(String queueName, byte[] body, String contentType, Priority priority,
                             Consumer<JobResult> onComplete) {
        if (closed) {
            throw new IllegalStateException("GpuBrokerClient is closed");
        }
        tracker.ask(t -> t.requestSlot(queueName)).join().join();
        String jobId;
        try {
            jobId = http.postJob(queueName, body, contentType, priority);
        } catch (RuntimeException e) {
            // No job exists for pollOutstanding to complete, so nothing else would ever free the
            // slot reserved above; give it back here or targetPerQueue failures park every later submit.
            tracker.tell(t -> t.releaseSlot(queueName));
            throw e;
        }
        tracker.tell(t -> t.recordSubmitted(queueName, jobId, onComplete));
        return new JobHandle(jobId, queueName);
    }

    /**
     * Stops accepting new {@link #submit} calls, then blocks until every outstanding job has
     * reached {@code DONE}/{@code FAILED}, then shuts down the poller and the actor system.
     * Idempotent -- a second call is a no-op, rather than blocking forever asking a
     * {@link #system} that {@link ActorSystem#terminate} already shut down.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        while (!tracker.ask(SubmissionTracker::isDrained).join()) {
            sleepDrainInterval();
        }
        scheduler.close();
        system.terminate();
    }

    private static void sleepDrainInterval() {
        try {
            Thread.sleep(DRAIN_POLL_INTERVAL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
