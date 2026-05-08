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

import java.util.List;
import java.util.Map;

/**
 * Streaming variant of {@link CoordinatorReduceIT}: same 2-shard parquet-backed index and
 * deterministic dataset, but with Arrow Flight RPC streaming enabled. Exercises the
 * shard-fragment → Flight → DatafusionReduceSink.feed handoff that previously failed with
 * "A buffer can only be associated between two allocators that share the same root" on
 * multi-shard queries.
 *
 * <p>Requires a dedicated cluster configuration with the stream transport feature flag enabled
 * (configured via the {@code integTestStreaming} task in build.gradle).
 */
public class StreamingCoordinatorReduceIT extends AnalyticsRestTestCase {

    private static final String INDEX = "coord_reduce_streaming_e2e";
    private static final int NUM_SHARDS = 2;
    private static final int DOCS_PER_SHARD = 10;
    private static final int VALUE = 7;

    /**
     * {@code source = T} on a 2-shard parquet-backed index with streaming enabled exercises the
     * coordinator reduce sink's cross-plugin VectorSchemaRoot handoff.
     */
    public void testBaselineScanAcrossShards() throws Exception {
        createParquetBackedIndex();
        indexDeterministicDocs();

        Map<String, Object> result = executePPL("source = " + INDEX);

        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) result.get("columns");
        assertNotNull("columns must not be null", columns);
        assertTrue("columns must contain 'value', got " + columns, columns.contains("value"));

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("rows must not be null", rows);

        int expectedRows = NUM_SHARDS * DOCS_PER_SHARD;
        assertEquals("all docs across shards must be returned", expectedRows, rows.size());

        int idx = columns.indexOf("value");
        for (List<Object> row : rows) {
            Object cell = row.get(idx);
            assertNotNull("value cell must not be null", cell);
            assertEquals("every doc has value=" + VALUE, (long) VALUE, ((Number) cell).longValue());
        }
    }

    private void createParquetBackedIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": " + NUM_SHARDS + ","
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"value\": { \"type\": \"integer\" }"
            + "  }"
            + "}"
            + "}";

        Request createIndex = new Request("PUT", "/" + INDEX);
        createIndex.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(createIndex), "Create index");
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void indexDeterministicDocs() throws Exception {
        int total = NUM_SHARDS * DOCS_PER_SHARD;
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < total; i++) {
            bulk.append("{\"index\": {\"_id\": \"").append(i).append("\"}}\n");
            bulk.append("{\"value\": ").append(VALUE).append("}\n");
        }

        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }

    private Map<String, Object> executePPL(String ppl) throws Exception {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + ppl + "\"}");
        Response response = client().performRequest(request);
        return entityAsMap(response);
    }
}
