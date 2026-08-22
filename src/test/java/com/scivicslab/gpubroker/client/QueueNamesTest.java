package com.scivicslab.gpubroker.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class QueueNamesTest {

    @Test
    void vllmChat_sanitizesSlashesAndKeepsRestAsIs() {
        assertEquals("vllm-google-gemma-4-26B-A4B-it", QueueNames.vllmChat("google/gemma-4-26B-A4B-it"));
    }

    @Test
    void vllmChat_modelWithNoSpecialChars_isUnchangedApartFromPrefix() {
        assertEquals("vllm-Qwen2.5-14B-Instruct-AWQ", QueueNames.vllmChat("Qwen2.5-14B-Instruct-AWQ"));
    }
}
