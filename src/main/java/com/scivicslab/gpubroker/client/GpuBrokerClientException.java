package com.scivicslab.gpubroker.client;

/** Wraps any failure talking to {@code quarkus-gpu-broker} over HTTP (I/O failure, unexpected status). */
public class GpuBrokerClientException extends RuntimeException {

    public GpuBrokerClientException(String message) {
        super(message);
    }

    public GpuBrokerClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
