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
import java.util.function.IntUnaryOperator;

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

    /**
     * {@code stats avg(value) as a} — primitive decomposition. PARTIAL emits
     * {@code [count:Int64, sum:Float64]}; FINAL reduces each with SUM and a Project wraps
     * {@code finalExpression = sum/count}. Exercises the multi-field intermediate path over
     * the streaming reduce-sink: each shard ships sum + count intermediates via Flight, the
     * coordinator merges them, then divides.
     *
     * <p>Uses varied per-doc values (value = doc index) so the AVG is non-trivial — a
     * per-shard pass-through (e.g. concatenating partial AVGs) would yield a different
     * answer than the correct cross-shard merge.
     */
    public void testAvgAcrossShards() throws Exception {
        createParquetBackedIndex();
        int total = NUM_SHARDS * DOCS_PER_SHARD;
        indexValuedDocs(i -> i);

        // Expected: AVG(0, 1, ..., total-1) = (total - 1) / 2.0
        double expected = (total - 1) / 2.0;

        Map<String, Object> result = executePPL("source = " + INDEX + " | stats avg(value) as a");
        List<List<Object>> rows = scalarRows(result, "a");

        double actual = ((Number) rows.get(0).get(0)).doubleValue();
        assertEquals("AVG(value) across shards should be " + expected, expected, actual, 0.001);
    }

    /**
     * {@code stats dc(value) as dc} — engine-native HLL merge. PARTIAL emits a single Binary
     * sketch column per shard; FINAL invokes DataFusion's {@code approx_distinct} merge
     * which combines sketches across shards. Exercises the engine-native (reducer == self)
     * single-field intermediate path over streaming.
     *
     * <p>Tolerance is 10% — HLL is approximate; with 20 distinct values the error margin
     * easily covers the variance.
     */
    public void testDistinctCountAcrossShards() throws Exception {
        createParquetBackedIndex();
        int total = NUM_SHARDS * DOCS_PER_SHARD;
        indexValuedDocs(i -> i); // all distinct

        Map<String, Object> result = executePPL("source = " + INDEX + " | stats dc(value) as dc");
        List<List<Object>> rows = scalarRows(result, "dc");

        long actual = ((Number) rows.get(0).get(0)).longValue();
        assertTrue(
            "dc(value) should be approximately " + total + " (±10%), got " + actual,
            actual >= (long) (total * 0.9) && actual <= (long) (total * 1.1)
        );
    }

    /**
     * {@code stats stddev_pop(value) as s} — multi-field statistical aggregate. Reduced by
     * {@link org.opensearch.analytics.planner.rules.OpenSearchAggregateReduceRule} into
     * SUM, SUM-of-squares, and COUNT primitives at HEP-marking time, then finalised with
     * POWER(variance, 0.5).
     *
     * <p>Expected: population stddev of (0..19) = sqrt(33.25) ≈ 5.766.
     */
    public void testStddevPopAcrossShards() throws Exception {
        createParquetBackedIndex();
        int total = NUM_SHARDS * DOCS_PER_SHARD;
        indexValuedDocs(i -> i);

        double mean = (total - 1) / 2.0;
        double sumSquares = 0;
        for (int i = 0; i < total; i++) {
            sumSquares += (i - mean) * (i - mean);
        }
        double expected = Math.sqrt(sumSquares / total);

        Map<String, Object> result = executePPL("source = " + INDEX + " | stats stddev_pop(value) as s");
        List<List<Object>> rows = scalarRows(result, "s");

        double actual = ((Number) rows.get(0).get(0)).doubleValue();
        assertEquals("STDDEV_POP(value) across shards should be " + expected, expected, actual, 0.001);
    }

    /**
     * {@code stats stddev_samp(value) as s} — sample standard deviation. Reduced to
     * {@code sqrt(SUM((x - mean)^2) / (N - 1))}. Same reduction path as STDDEV_POP but
     * with Bessel's correction in the denominator.
     *
     * <p>Expected: sample stddev of (0..19) = sqrt(sum((i - mean)^2) / (N - 1)) = sqrt(35) ≈ 5.916.
     */
    public void testStddevSampAcrossShards() throws Exception {
        createParquetBackedIndex();
        int total = NUM_SHARDS * DOCS_PER_SHARD;
        indexValuedDocs(i -> i);

        double mean = (total - 1) / 2.0;
        double sumSquares = 0;
        for (int i = 0; i < total; i++) {
            sumSquares += (i - mean) * (i - mean);
        }
        double expected = Math.sqrt(sumSquares / (total - 1));

        Map<String, Object> result = executePPL("source = " + INDEX + " | stats stddev_samp(value) as s");
        List<List<Object>> rows = scalarRows(result, "s");

        double actual = ((Number) rows.get(0).get(0)).doubleValue();
        assertEquals("STDDEV_SAMP(value) across shards should be " + expected, expected, actual, 0.001);
    }

    /**
     * {@code stats var_pop(value) as v} — population variance. Reduced to
     * {@code SUM((x - mean)^2) / N}, the same primitives as STDDEV_POP minus the final sqrt.
     *
     * <p>Expected: population variance of (0..19) = 33.25.
     */
    public void testVarPopAcrossShards() throws Exception {
        createParquetBackedIndex();
        int total = NUM_SHARDS * DOCS_PER_SHARD;
        indexValuedDocs(i -> i);

        double mean = (total - 1) / 2.0;
        double sumSquares = 0;
        for (int i = 0; i < total; i++) {
            sumSquares += (i - mean) * (i - mean);
        }
        double expected = sumSquares / total;

        Map<String, Object> result = executePPL("source = " + INDEX + " | stats var_pop(value) as v");
        List<List<Object>> rows = scalarRows(result, "v");

        double actual = ((Number) rows.get(0).get(0)).doubleValue();
        assertEquals("VAR_POP(value) across shards should be " + expected, expected, actual, 0.001);
    }

    /**
     * {@code stats var_samp(value) as v} — sample variance. Reduced to
     * {@code SUM((x - mean)^2) / (N - 1)}.
     *
     * <p>Expected: sample variance of (0..19) = 35.0.
     */
    public void testVarSampAcrossShards() throws Exception {
        createParquetBackedIndex();
        int total = NUM_SHARDS * DOCS_PER_SHARD;
        indexValuedDocs(i -> i);

        double mean = (total - 1) / 2.0;
        double sumSquares = 0;
        for (int i = 0; i < total; i++) {
            sumSquares += (i - mean) * (i - mean);
        }
        double expected = sumSquares / (total - 1);

        Map<String, Object> result = executePPL("source = " + INDEX + " | stats var_samp(value) as v");
        List<List<Object>> rows = scalarRows(result, "v");

        double actual = ((Number) rows.get(0).get(0)).doubleValue();
        assertEquals("VAR_SAMP(value) across shards should be " + expected, expected, actual, 0.001);
    }

    /** Indexes {@code NUM_SHARDS * DOCS_PER_SHARD} docs with values produced by {@code valueFn}. */
    private void indexValuedDocs(IntUnaryOperator valueFn) throws Exception {
        int total = NUM_SHARDS * DOCS_PER_SHARD;
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < total; i++) {
            bulk.append("{\"index\": {\"_id\": \"").append(i).append("\"}}\n");
            bulk.append("{\"value\": ").append(valueFn.applyAsInt(i)).append("}\n");
        }

        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }

    /** Local copy of {@code CoordinatorReduceIT.scalarRows} (the original is package-private). */
    private static List<List<Object>> scalarRows(Map<String, Object> result, String columnName) {
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) result.get("columns");
        assertNotNull("columns must not be null", columns);
        assertTrue("columns must contain '" + columnName + "', got " + columns, columns.contains(columnName));

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("rows must not be null", rows);
        assertEquals("scalar agg must return exactly 1 row", 1, rows.size());

        Object cell = rows.get(0).get(columns.indexOf(columnName));
        assertNotNull("cell for '" + columnName + "' must not be null — coordinator-reduce returned no value", cell);
        return rows;
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
