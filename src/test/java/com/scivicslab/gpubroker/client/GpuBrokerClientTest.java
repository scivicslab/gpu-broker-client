package com.scivicslab.gpubroker.client;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * No real HTTP call is made here -- with nothing ever submitted, {@code close()} only asks the
 * (empty, therefore already-drained) {@code SubmissionTracker}, so a fake {@code baseUrl} is fine.
 */
@DisplayName("GpuBrokerClient — submit failure and close() lifecycle")
class GpuBrokerClientTest {

    /**
     * A failed POST (here: connection refused on a closed port) must hand its reserved slot back.
     * With a target of 1, a leaked slot would park the second submit forever instead of letting it
     * fail the same way.
     */
    @Test
    void submit_whenThePostFails_releasesTheSlotSoTheNextSubmitIsNotParked() {
        GpuBrokerClient client = new GpuBrokerClient("http://127.0.0.1:1", "test-submitter", 1);
        byte[] body = "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
            for (int i = 0; i < 3; i++) {
                assertThrows(GpuBrokerClientException.class,
                        () -> client.submit("q1", body, "application/json", Priority.BACKGROUND));
            }
        }, "every submit must fail promptly; a parked submit means the failed one leaked its slot");
        client.close();
    }

    @Test
    void close_isIdempotent_asASecondCallDoesNotHang() {
        GpuBrokerClient client = new GpuBrokerClient("http://localhost:0", "test-submitter", 10);

        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            client.close();
            client.close();
        }, "a second close() must return promptly, not re-ask a terminated ActorSystem");
    }
}
