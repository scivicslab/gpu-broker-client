package com.scivicslab.gpubroker.client;

import java.time.Instant;

/**
 * This SDK's own parsing of the {@code GET /jobs/{queueName}/{jobId}} response. Deliberately
 * not the same class as {@code quarkus-gpu-broker}'s internal {@code JobResult} record --
 * the two are decoupled on purpose, sharing only the JSON field names as their contract. See
 * {@code FlowControlSdkDesign_260820_oo01}, "なぜ JobResult 型を共有しないか".
 */
public record JobResult(Status status, byte[] body, String contentType, String error, Instant createdAt) {

    public enum Status {
        PENDING, DONE, FAILED,
        /**
         * The broker answered 404: it has no such job. Produced by the poller and consumed by
         * {@code SubmissionTracker}, which waits a grace period and then reports the job as
         * {@link #FAILED}; never passed to a completion callback.
         */
        UNKNOWN
    }

    static JobResult pending() {
        return new JobResult(Status.PENDING, null, null, null, null);
    }

    static JobResult unknown() {
        return new JobResult(Status.UNKNOWN, null, null, null, null);
    }
}
