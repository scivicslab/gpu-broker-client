package com.scivicslab.gpubroker.client;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * No real HTTP call is made here -- with nothing ever submitted, {@code close()} only asks the
 * (empty, therefore already-drained) {@code SubmissionTracker}, so a fake {@code baseUrl} is fine.
 */
@DisplayName("GpuBrokerClient — close() lifecycle")
class GpuBrokerClientTest {

    @Test
    void close_isIdempotent_asASecondCallDoesNotHang() {
        GpuBrokerClient client = new GpuBrokerClient("http://localhost:0", "test-submitter", 10);

        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            client.close();
            client.close();
        }, "a second close() must return promptly, not re-ask a terminated ActorSystem");
    }
}
