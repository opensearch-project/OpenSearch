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
 * End-to-end smoke test for the streaming coordinator-reduce path:
 *
 * <pre>
 *   PPL → planner → multi-shard SHARD_FRAGMENT dispatch → DataFusion shard scan
 *       → ExchangeSink.feed → DatafusionReduceSink (Substrait SUM via convertFinalAggFragment)
 *       → drain → downstream → assembled PPLResponse
 * </pre>
 *
 * <p>Builds a parquet-backed composite index with two shards, indexes a small
 * deterministic dataset, then runs a {@code stats sum(value) as total} aggregate.
 * The total is a function of the indexed values × shard count; any drift in
 * shard fan-out, sink wiring, or final-agg merge will show up as a mismatch.
 *
 * <p>Requires a 2-node cluster (configured in build.gradle) so that shards
 * are distributed across nodes, exercising the coordinator-reduce path.
 */
public class CoordinatorReduceIT extends AnalyticsRestTestCase {

    private static final String INDEX = "coord_reduce_e2e";
    private static final int NUM_SHARDS = 2;
    private static final int DOCS_PER_SHARD = 10;
    private static final int VALUE = 7;

    /**
     * {@code source = T | stats sum(value) as total} on a 2-shard parquet-backed index
     * → coordinator-reduce path runs the final SUM via DatafusionReduceSink
     * and returns the deterministic total.
     */
    public void testScalarSumAcrossShards() throws Exception {
        createParquetBackedIndex();
        indexDeterministicDocs();

        Map<String, Object> result = executePPL("source = " + INDEX + " | stats sum(value) as total");

        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) result.get("columns");
        assertNotNull("columns must not be null", columns);
        assertTrue("columns must contain 'total', got " + columns, columns.contains("total"));

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("rows must not be null", rows);
        assertEquals("scalar agg must return exactly 1 row", 1, rows.size());

        int idx = columns.indexOf("total");
        Object cell = rows.get(0).get(idx);
        assertNotNull("SUM(value) cell must not be null — coordinator-reduce returned no value", cell);
        long actual = ((Number) cell).longValue();
        long expected = (long) VALUE * NUM_SHARDS * DOCS_PER_SHARD;
        assertEquals(
            "SUM(value) across " + NUM_SHARDS + " shards × " + DOCS_PER_SHARD + " docs × value=" + VALUE + " = " + expected,
            expected,
            actual
        );
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
