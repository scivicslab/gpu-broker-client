package com.scivicslab.gpubroker.client;

/**
 * Derives the {@code queueName} a caller must pass to {@link GpuBrokerClient#submit} for a given
 * model, matching how {@code quarkus-gpu-broker} itself names the queue it discovers. Kept here
 * (not left for each consumer to reimplement) so this naming rule has one place to change.
 */
public final class QueueNames {

    private QueueNames() {
    }

    /**
     * The queue name for a vLLM OpenAI-compatible chat model, e.g. {@code google/gemma-4-26B-A4B-it}
     * -&gt; {@code vllm-google-gemma-4-26B-A4B-it}. Must match {@code VllmChatProbe.deriveQueueName}
     * in {@code quarkus-gpu-broker} exactly -- a model id containing {@code /} is not a safe single
     * {@code /queue/{queueName}} path segment as-is, so anything outside the URL path-segment-safe
     * unreserved set is replaced with {@code -}.
     */
    public static String vllmChat(String modelId) {
        return "vllm-" + modelId.replaceAll("[^A-Za-z0-9._-]", "-");
    }
}
