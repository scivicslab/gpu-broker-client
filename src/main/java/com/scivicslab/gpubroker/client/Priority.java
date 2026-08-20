package com.scivicslab.gpubroker.client;

/** Mirrors {@code quarkus-gpu-broker}'s {@code X-Job-Priority} header values. */
public enum Priority {

    FOREGROUND("foreground"),
    BACKGROUND("background");

    private final String headerValue;

    Priority(String headerValue) {
        this.headerValue = headerValue;
    }

    String headerValue() {
        return headerValue;
    }
}
