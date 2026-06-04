/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

import org.apache.hc.core5.http.ParseException;

import java.io.IOException;
import java.util.Map;

/**
 * REST integration test verifying that native memory errors return HTTP 429 (Too Many Requests)
 * rather than HTTP 500. Exercises the full path: Rust error → FFM boundary → NativeErrorConverter
 * → Flight transport → coordinator reconstruction → REST response.
 *
 * <p>Run with:
 * {@code ./gradlew :sandbox:qa:analytics-engine-rest:integTest --tests "*.MemoryGuardRestIT" -Dsandbox.enabled=true}
 */
public class MemoryGuardRestIT extends AnalyticsRestTestCase {

    private static final String INDEX = "mem_guard_rest_it";

    public void testPoolLimitExhausted_returns429() throws Exception {
        createIndex();
        ingestData();

        setPoolLimit(1L);
        try {
            ResponseException ex = expectThrows(
                ResponseException.class,
                () -> executePpl("source = " + INDEX + " | stats count() by url")
            );
            int statusCode = ex.getResponse().getStatusLine().getStatusCode();
            assertEquals(
                "Expected HTTP 429 for pool-limit exhaustion, got " + statusCode + ": " + ex.getMessage(),
                429,
                statusCode
            );
        } finally {
            resetPoolLimit();
        }
    }

    public void testPoolLimitExhausted_responseContainsCircuitBreakingException() throws Exception {
        createIndex();
        ingestData();

        setPoolLimit(1L);
        try {
            ResponseException ex = expectThrows(
                ResponseException.class,
                () -> executePpl("source = " + INDEX + " | stats count() by url")
            );
            String body = entityAsString(ex.getResponse());
            assertTrue(
                "Response body should mention circuit_breaking_exception, got: " + body,
                body.contains("circuit_breaking_exception")
            );
        } finally {
            resetPoolLimit();
        }
    }

    public void testNormalQuery_succeedsAfterPoolReset() throws Exception {
        createIndex();
        ingestData();

        setPoolLimit(1L);
        try {
            expectThrows(
                ResponseException.class,
                () -> executePpl("source = " + INDEX + " | stats count() by url")
            );
        } finally {
            resetPoolLimit();
        }

        Map<String, Object> result = executePpl("source = " + INDEX + " | stats count() by url");
        assertNotNull("Query should succeed after pool limit reset", result.get("datarows"));
    }

    // ── Cluster settings helpers ───────────────────────────────────────────

    private void setPoolLimit(long bytes) throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"transient\":{\"datafusion.memory_pool_limit_bytes\": " + bytes + "}}");
        Response response = client().performRequest(req);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void resetPoolLimit() throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"transient\":{\"datafusion.memory_pool_limit_bytes\": null}}");
        client().performRequest(req);
    }

    private static String entityAsString(Response response) throws IOException, ParseException {
        return org.apache.hc.core5.http.io.entity.EntityUtils.toString(response.getEntity());
    }

    // ── Index setup ────────────────────────────────────────────────────────

    private void createIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"user_id\": { \"type\": \"long\" },"
            + "    \"url\": { \"type\": \"keyword\" },"
            + "    \"count\": { \"type\": \"integer\" }"
            + "  }"
            + "}"
            + "}";

        Request req = new Request("PUT", "/" + INDEX);
        req.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(req), "Create index");
        assertEquals(true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void ingestData() throws Exception {
        StringBuilder ndjson = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            ndjson.append("{\"index\": {}}\n");
            ndjson.append("{\"user_id\": ").append(i)
                .append(", \"url\": \"https://example.com/page-").append(i % 20).append("\"")
                .append(", \"count\": ").append(i % 50)
                .append("}\n");
        }

        Request req = new Request("POST", "/" + INDEX + "/_bulk");
        req.setJsonEntity(ndjson.toString());
        req.addParameter("refresh", "true");
        req.setOptions(req.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> response = assertOkAndParse(client().performRequest(req), "Bulk index");
        assertEquals("Bulk indexing should have no errors", false, response.get("errors"));
    }
}
