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
     * The queue name for an OpenAI-compatible chat model, e.g. {@code google/gemma-4-26B-A4B-it}
     * -&gt; {@code chat-google-gemma-4-26B-A4B-it}. Must match {@code ChatQueueName.of} in {@code
     * quarkus-gpu-broker} exactly -- a model id containing {@code /} is not a safe single
     * {@code /queue/{queueName}} path segment as-is, so anything outside the URL path-segment-safe
     * unreserved set is replaced with {@code -}.
     *
     * <p>The prefix names the kind of service, not the server behind it: vLLM, TensorFold and
     * Strata all answer this API and all land here.</p>
     */
    public static String chat(String modelId) {
        return "chat-" + modelId.replaceAll("[^A-Za-z0-9._-]", "-");
    }

    /**
     * The former name of {@link #chat}, from when the prefix was {@code vllm-}. It now returns the
     * same {@code chat-} name: a caller that has not been rebuilt still reaches the right queue
     * because the broker resolves the old prefix, and one that recompiles against this version
     * reaches it directly.
     *
     * @deprecated call {@link #chat} instead.
     */
    @Deprecated(since = "0.2.0", forRemoval = true)
    public static String vllmChat(String modelId) {
        return chat(modelId);
    }
}
