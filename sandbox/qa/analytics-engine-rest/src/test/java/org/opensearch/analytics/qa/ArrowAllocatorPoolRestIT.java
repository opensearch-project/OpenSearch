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
 * REST integration test verifying that Arrow Java allocator pool exhaustion returns HTTP 429
 * (Too Many Requests) rather than HTTP 500. Exercises the full path: shard-side Arrow
 * BufferAllocator refuses growth → {@code FlightServerChannel.classifyError} maps to
 * {@code CallStatus.RESOURCE_EXHAUSTED} → Flight transports across RPC → coordinator-side
 * {@code DefaultPlanExecutor.translateArrowOom} translates the {@code RESOURCE_EXHAUSTED}
 * {@code StreamException} to {@code CircuitBreakingException} → REST returns 429.
 *
 * <p>This is the IT counterpart to {@link MemoryGuardRestIT}, which exercises the parallel
 * DataFusion Rust pool path. Both budget-exhaustion paths must surface as 429 so OpenSearch
 * core's {@code FailAwareWeightedRouting} skips replica retries — preventing retry storms.
 *
 * <p>Run with:
 * {@code ./gradlew :sandbox:qa:analytics-engine-rest:integTest --tests "*.ArrowAllocatorPoolRestIT" -Dsandbox.enabled=true}
 */
public class ArrowAllocatorPoolRestIT extends AnalyticsRestTestCase {

    private static final String INDEX = "arrow_pool_rest_it";

    /**
     * Aggregation that materializes enough Arrow IPC buffer state per partial-result batch to
     * exceed the deliberately-tightened pool cap (256 KB). {@code dc()} lowers to
     * {@code approx_distinct}, whose per-group HyperLogLog register state is a few KB per group;
     * combined with the many distinct {@code url} values, the partial-aggregate IPC batches grow
     * beyond the cap during result import. A simple {@code count() by url} produces tiny output
     * that fits trivially in 256 KB and does not exercise the failure path.
     */
    private static final String AGG_QUERY = "source = " + INDEX + " | stats dc(token) as cnt by url";

    /**
     * Three shards is the minimum that reliably forces partial-aggregate results to cross the
     * Arrow Flight RPC boundary from shards back to the coordinator. The shard-side
     * {@code ArrayImporter} fails mid-stream when the per-batch Arrow buffers exceed the
     * 256 KB pool cap. This is the path our dev repro and AOS clusters both hit.
     *
     * <p>Pre-patch: HTTP 500 with {@code "type": "StreamException"}.
     * <p>Post-patch: HTTP 429 with {@code "type": "CircuitBreakingException"}.
     */
    public void testQueryPoolExhausted_returns429() throws Exception {
        createIndex(3);
        ingestData();

        setQueryPoolLimit(262144, 131072);
        try {
            ResponseException ex = expectThrows(ResponseException.class, () -> executePpl(AGG_QUERY));
            int statusCode = ex.getResponse().getStatusLine().getStatusCode();
            assertEquals(
                "Expected HTTP 429 for Arrow query pool exhaustion, got " + statusCode + ": " + ex.getMessage(),
                429,
                statusCode
            );
        } finally {
            resetQueryPoolLimit();
        }
    }

    /**
     * Pins the response shape: customers/SDKs key off the {@code type} field. The translation
     * must produce {@code CircuitBreakingException} so retry policies treat it the same way as
     * the existing DataFusion pool-budget path ({@link MemoryGuardRestIT}).
     */
    public void testQueryPoolExhausted_responseContainsCircuitBreakingException() throws Exception {
        createIndex(3);
        ingestData();

        setQueryPoolLimit(262144, 131072);
        try {
            ResponseException ex = expectThrows(ResponseException.class, () -> executePpl(AGG_QUERY));
            String body = entityAsString(ex.getResponse());
            assertTrue(
                "Response body should mention CircuitBreakingException (type field), got: " + body,
                body.contains("CircuitBreakingException")
            );
            // Original Arrow OOM message must survive across the Flight RPC + translation layers.
            assertTrue(
                "Response body should preserve the original Arrow allocator message, got: " + body,
                body.contains("Unable to allocate buffer")
            );
        } finally {
            resetQueryPoolLimit();
        }
    }

    /**
     * Regression guard: the rejection must be transient. After clearing the pool override the
     * cluster must serve the same query normally — proves we did not leak any allocator state
     * or leave the breaker in a tripped condition that requires a restart.
     */
    public void testNormalQuery_succeedsAfterPoolReset() throws Exception {
        createIndex(3);
        ingestData();

        setQueryPoolLimit(262144, 131072);
        try {
            expectThrows(ResponseException.class, () -> executePpl(AGG_QUERY));
        } finally {
            resetQueryPoolLimit();
        }

        Map<String, Object> result = executePpl(AGG_QUERY);
        assertNotNull("Query should succeed after pool limit reset", result.get("datarows"));
    }

    // ── Cluster settings helpers ───────────────────────────────────────────

    private void setQueryPoolLimit(long max, long min) throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity(
            "{\"transient\":{"
                + "\"native.allocator.pool.query.max\":" + max + ","
                + "\"native.allocator.pool.query.min\":" + min
                + "}}"
        );
        Response response = client().performRequest(req);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void resetQueryPoolLimit() throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity(
            "{\"transient\":{"
                + "\"native.allocator.pool.query.max\":null,"
                + "\"native.allocator.pool.query.min\":null"
                + "}}"
        );
        client().performRequest(req);
    }

    private static String entityAsString(Response response) throws IOException, ParseException {
        return org.apache.hc.core5.http.io.entity.EntityUtils.toString(response.getEntity());
    }

    // ── Index setup ────────────────────────────────────────────────────────

    private void createIndex() throws Exception {
        createIndex(1);
    }

    private void createIndex(int numberOfShards) throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": " + numberOfShards + ","
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"user_id\": { \"type\": \"long\" },"
            + "    \"url\":     { \"type\": \"keyword\" },"
            + "    \"token\":   { \"type\": \"keyword\" }"
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

    /**
     * Ingest enough rows × cardinality that the result of
     * {@code stats dc(token) by url} produces partial-aggregate Arrow batches that exceed
     * 256 KB during IPC import. With {@code DOCS=5000} and {@code URL_CARDINALITY=500}, each
     * group's HLL register state plus the keyword group-key bytes push the batch comfortably
     * past the test pool cap.
     */
    private void ingestData() throws Exception {
        final int DOCS = 5000;
        final int URL_CARDINALITY = 500;

        // Bulk in chunks of 1000 to stay under the OpenSearch bulk request size limit.
        final int CHUNK = 1000;
        for (int chunkStart = 0; chunkStart < DOCS; chunkStart += CHUNK) {
            StringBuilder ndjson = new StringBuilder();
            int end = Math.min(chunkStart + CHUNK, DOCS);
            for (int i = chunkStart; i < end; i++) {
                ndjson.append("{\"index\": {}}\n");
                ndjson.append("{\"user_id\": ").append(i)
                    .append(", \"url\": \"https://example.com/page-").append(i % URL_CARDINALITY).append("\"")
                    .append(", \"token\": \"tok-").append(i).append("-").append(i * 31).append("\"")
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
}
