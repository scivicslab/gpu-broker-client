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
        PENDING, DONE, FAILED
    }

    static JobResult pending() {
        return new JobResult(Status.PENDING, null, null, null, null);
    }
}
