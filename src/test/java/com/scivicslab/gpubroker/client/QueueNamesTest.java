package com.scivicslab.gpubroker.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class QueueNamesTest {

    @Test
    void chat_sanitizesSlashesAndKeepsRestAsIs() {
        assertEquals("chat-google-gemma-4-26B-A4B-it", QueueNames.chat("google/gemma-4-26B-A4B-it"));
    }

    @Test
    void chat_modelWithNoSpecialChars_isUnchangedApartFromPrefix() {
        assertEquals("chat-Qwen2.5-14B-Instruct-AWQ", QueueNames.chat("Qwen2.5-14B-Instruct-AWQ"));
    }

    @SuppressWarnings("deprecation")
    @Test
    void theFormerNameGivesTheSameQueue() {
        assertEquals(QueueNames.chat("google/gemma-4-26B-A4B-it"),
                QueueNames.vllmChat("google/gemma-4-26B-A4B-it"));
    }
}
