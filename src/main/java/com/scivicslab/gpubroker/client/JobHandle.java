package com.scivicslab.gpubroker.client;

/** What {@link GpuBrokerClient#submit} returns: enough to look the job up later via {@code GET /jobs/{queueName}/{jobId}}. */
public record JobHandle(String jobId, String queueName) {
}
