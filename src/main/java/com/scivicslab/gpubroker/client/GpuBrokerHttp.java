package com.scivicslab.gpubroker.client;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The only class in this package that speaks HTTP to {@code quarkus-gpu-broker}'s async path
 * ({@code POST /jobs/{queueName}}, {@code GET /jobs/{queueName}/{jobId}}). Stateless beyond its
 * {@code baseUrl}/{@code submitterId} -- safe to call from {@link GpuBrokerClient#submit} (the
 * caller's own thread) and from {@link SubmissionTracker#pollOutstanding} (that actor's own
 * mailbox thread) alike.
 */
final class GpuBrokerHttp implements JobResultPoller {

    private final String baseUrl;
    private final String submitterId;
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    GpuBrokerHttp(String baseUrl, String submitterId) {
        this.baseUrl = baseUrl;
        this.submitterId = submitterId;
    }

    /** Returns the new {@code jobId}. Throws {@link GpuBrokerClientException} on anything but {@code 202}. */
    String postJob(String queueName, byte[] body, String contentType, Priority priority) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/jobs/" + queueName))
                .header("Content-Type", contentType)
                .header("X-Job-Priority", priority.headerValue())
                .header("X-Submitter-Id", submitterId)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        HttpResponse<String> response = send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 429) {
            throw new GpuBrokerClientException(
                    "gpu-broker rejected submission to " + queueName + " (429: over the server's admission limit for " + submitterId + ")");
        }
        if (response.statusCode() != 202) {
            throw new GpuBrokerClientException(
                    "unexpected status " + response.statusCode() + " from POST /jobs/" + queueName);
        }
        return response.body();
    }

    /** Returns {@link JobResult#pending()} for a {@code 404} (job not found yet). */
    @Override
    public JobResult getJobResult(String queueName, String jobId) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/jobs/" + queueName + "/" + jobId))
                .GET()
                .build();
        HttpResponse<byte[]> response = send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() == 404) {
            return JobResult.pending();
        }
        if (response.statusCode() != 200) {
            throw new GpuBrokerClientException(
                    "unexpected status " + response.statusCode() + " from GET /jobs/" + queueName + "/" + jobId);
        }
        return parseJobResult(response.body());
    }

    private JobResult parseJobResult(byte[] responseBody) {
        try {
            JsonNode node = objectMapper.readTree(responseBody);
            JobResult.Status status = JobResult.Status.valueOf(node.path("status").asText());
            JsonNode bodyNode = node.path("body");
            byte[] body = bodyNode.isMissingNode() || bodyNode.isNull() ? null : bodyNode.binaryValue();
            String contentType = node.path("contentType").isMissingNode() ? null : node.path("contentType").asText(null);
            String error = node.path("error").isMissingNode() ? null : node.path("error").asText(null);
            JsonNode createdAtNode = node.path("createdAt");
            Instant createdAt = createdAtNode.isMissingNode() || createdAtNode.isNull()
                    ? null : Instant.parse(createdAtNode.asText());
            return new JobResult(status, body, contentType, error, createdAt);
        } catch (IOException e) {
            throw new GpuBrokerClientException("malformed JobResult JSON", e);
        }
    }

    private <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> bodyHandler) {
        try {
            return httpClient.send(request, bodyHandler);
        } catch (IOException e) {
            throw new GpuBrokerClientException(request.method() + " " + request.uri() + " failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GpuBrokerClientException(request.method() + " " + request.uri() + " interrupted", e);
        }
    }
}
