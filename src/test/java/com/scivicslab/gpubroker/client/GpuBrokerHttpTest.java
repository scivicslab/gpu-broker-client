package com.scivicslab.gpubroker.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Pure parsing tests for {@link GpuBrokerHttp#parseTotalSlots}: no HTTP call is made. */
@DisplayName("GpuBrokerHttp — reading totalSlots out of GET /queues")
class GpuBrokerHttpTest {

    private static final String QUEUES = "["
            + "{\"name\":\"vllm-qwen3.8-flash-next\",\"activeSlots\":0,\"idleSlots\":32,\"totalSlots\":32},"
            + "{\"name\":\"whisper-transcript\",\"activeSlots\":1,\"idleSlots\":1,\"totalSlots\":2,"
            + "\"endpoints\":[{\"address\":\"192.168.5.13:8003\"},{\"address\":\"192.168.5.18:8003\"}]}"
            + "]";

    private final GpuBrokerHttp http = new GpuBrokerHttp("http://127.0.0.1:1", "test-submitter");

    @Test
    void parseTotalSlots_knownQueue_returnsItsTotalSlots() {
        assertEquals(2, http.parseTotalSlots(QUEUES, "whisper-transcript"));
        assertEquals(32, http.parseTotalSlots(QUEUES, "vllm-qwen3.8-flash-next"));
    }

    @Test
    void parseTotalSlots_unknownQueue_returnsZero() {
        assertEquals(0, http.parseTotalSlots(QUEUES, "marker-ocr"));
        assertEquals(0, http.parseTotalSlots("[]", "whisper-transcript"));
    }

    @Test
    void parseTotalSlots_malformedBody_throwsClientException() {
        assertThrows(GpuBrokerClientException.class, () -> http.parseTotalSlots("not json", "whisper-transcript"));
    }
}
