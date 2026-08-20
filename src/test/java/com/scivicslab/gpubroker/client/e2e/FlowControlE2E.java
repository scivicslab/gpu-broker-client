package com.scivicslab.gpubroker.client.e2e;

import java.util.concurrent.atomic.AtomicInteger;

import com.scivicslab.gpubroker.client.GpuBrokerClient;
import com.scivicslab.gpubroker.client.Priority;

/**
 * Exercises {@link GpuBrokerClient} against an already-running {@code quarkus-gpu-broker}
 * (default {@code http://localhost:28003}) -- plain {@code main()}-driven, not JUnit, per
 * {@code TestingStandard_260404_oo01} §3. Submits deliberately-invalid PDFs to {@code marker-ocr}
 * so each job fails fast (server-side {@code FAILED}), which is enough to exercise the flow
 * control loop without needing a real document.
 *
 * <p>Confirms: the first {@code target} submits return immediately, the next one blocks until a
 * slot frees up, and {@link GpuBrokerClient#close} returns once every job has drained.
 */
class FlowControlE2E {

    private static final String BASE_URL = System.getProperty("e2e.base.url", "http://localhost:28003");
    private static final String QUEUE_NAME = "marker-ocr";
    private static final int TARGET = 2;
    private static final int TOTAL_JOBS = 5;

    public static void main(String[] args) throws Exception {
        new FlowControlE2E().run();
    }

    void run() throws Exception {
        System.out.println("--- FlowControlE2E ---");
        AtomicInteger completed = new AtomicInteger();

        // Not try-with-resources: close() is called explicitly below to measure its duration,
        // and a plain variable avoids the implicit second close() try-with-resources would add.
        GpuBrokerClient client = new GpuBrokerClient(BASE_URL, "gpu-broker-client-e2e", TARGET);

        long firstBatchStart = System.nanoTime();
        for (int i = 0; i < TARGET; i++) {
            client.submit(QUEUE_NAME, ("not-a-real-pdf-" + i).getBytes(), "application/octet-stream",
                    Priority.BACKGROUND, result -> completed.incrementAndGet());
        }
        long firstBatchMillis = elapsedMillis(firstBatchStart);
        require(firstBatchMillis < 5_000,
                "first " + TARGET + " submits (below target) should return near-instantly, took " + firstBatchMillis + "ms");
        System.out.println("first " + TARGET + " submits returned in " + firstBatchMillis + "ms (no blocking expected)");

        long blockingStart = System.nanoTime();
        for (int i = TARGET; i < TOTAL_JOBS; i++) {
            client.submit(QUEUE_NAME, ("not-a-real-pdf-" + i).getBytes(), "application/octet-stream",
                    Priority.BACKGROUND, result -> completed.incrementAndGet());
        }
        long blockingMillis = elapsedMillis(blockingStart);
        require(blockingMillis >= 1_000,
                "submits beyond target=" + TARGET + " should block for at least one poll cycle, took only " + blockingMillis + "ms");
        System.out.println("remaining " + (TOTAL_JOBS - TARGET) + " submits blocked for " + blockingMillis
                + "ms before being admitted (flow control confirmed)");

        long closeStart = System.nanoTime();
        client.close();
        long closeMillis = elapsedMillis(closeStart);
        require(closeMillis < 60_000, "close() should drain and return, took " + closeMillis + "ms");
        System.out.println("close() drained all outstanding jobs in " + closeMillis + "ms");

        require(completed.get() == TOTAL_JOBS,
                "expected all " + TOTAL_JOBS + " completion callbacks to have fired by close(), got " + completed.get());
        System.out.println("all " + TOTAL_JOBS + " completion callbacks fired");
        System.out.println("FlowControlE2E: PASSED");
    }

    private static long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
