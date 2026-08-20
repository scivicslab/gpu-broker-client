package com.scivicslab.gpubroker.client;

/**
 * Fetches one job's current result. An interface so {@link SubmissionTracker#pollOutstanding}
 * can be unit-tested with a deterministic stub, without a real {@code quarkus-gpu-broker}.
 */
interface JobResultPoller {

    JobResult getJobResult(String queueName, String jobId);
}
