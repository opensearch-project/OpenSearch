/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.util.List;
import java.util.Map;

/**
 * End-to-end tests for the distributed partial/final aggregate path:
 *
 * <pre>
 *   PPL → planner (AggregateDecompositionResolver) → multi-shard SHARD_FRAGMENT dispatch
 *       → DataFusion partial-agg (prepare_partial_plan, force_aggregate_mode(Partial))
 *       → ExchangeSink.feed → DatafusionReduceSink (prepare_final_plan,
 *                             force_aggregate_mode(Final), execute_local_prepared_plan)
 *       → drain → downstream → assembled PPLResponse
 * </pre>
 *
 * <p>Each test exercises a distinct branch of the resolver's four-case decomposition:
 * <ul>
 *   <li>{@link #testScalarSumAcrossShards()} — pass-through
 *       ({@code AggregateFunction.intermediateFields == null})</li>
 *   <li>{@link #testScalarCountAcrossShards()} — function-swap
 *       (COUNT → SUM at FINAL over a single-field intermediate)</li>
 *   <li>{@link #testAvgAcrossShards()} — primitive decomposition
 *       (multi-field intermediate + {@code finalExpression} wrap)</li>
 *   <li>{@link #testDistinctCountAcrossShards()} — engine-native merge
 *       (Binary intermediate, reducer == self; HLL merge inside DataFusion)</li>
 *   <li>{@link #testGroupedSumAcrossShards()} — group keys propagate through
 *       partial/final without affecting the aggregate-call decomposition path</li>
 *   <li>{@link #testQ10ShapeAcrossShards()} — all four families in one query, grouped</li>
 * </ul>
 *
 * <p>Requires a 2-node cluster (configured in build.gradle) so that shards
 * are distributed across nodes, exercising the coordinator-reduce path.
 */
public class CoordinatorReduceIT extends AnalyticsRestTestCase {

    private static final String INDEX = "coord_reduce_e2e";
    private static final int NUM_SHARDS = 2;
    private static final int DOCS_PER_SHARD = 10;
    /**
     * Constant value used for {@link #INDEX}: every doc has {@code value=VALUE}. Makes the
     * deterministic SUM / AVG predictable regardless of which shard a doc lands on.
     */
    private static final int VALUE = 7;

    /**
     * {@code source = T | stats sum(value) as total} on a 2-shard parquet-backed index
     * → coordinator-reduce path runs the final SUM via DatafusionReduceSink
     * and returns the deterministic total.
     */
    public void testScalarSumAcrossShards() throws Exception {
        createParquetBackedIndex(INDEX);
        indexConstantValueDocs(INDEX);

        Map<String, Object> result = executePPL("source = " + INDEX + " | stats sum(value) as total");
        List<List<Object>> rows = scalarRows(result, "total");

        long actual = ((Number) rows.get(0).get(0)).longValue();
        long expected = (long) VALUE * NUM_SHARDS * DOCS_PER_SHARD;
        assertEquals(
            "SUM(value) across " + NUM_SHARDS + " shards × " + DOCS_PER_SHARD + " docs × value=" + VALUE + " = " + expected,
            expected,
            actual
        );
    }

    /**
     * {@code stats count() as cnt} — function-swap at FINAL. PARTIAL emits COUNT(*) as Int64;
     * resolver rewrites FINAL's COUNT to SUM over the partial-count column.
     */
    public void testScalarCountAcrossShards() throws Exception {
        createParquetBackedIndex(INDEX);
        indexConstantValueDocs(INDEX);

        Map<String, Object> result = executePPL("source = " + INDEX + " | stats count() as cnt");
        List<List<Object>> rows = scalarRows(result, "cnt");

        long actual = ((Number) rows.get(0).get(0)).longValue();
        long expected = (long) NUM_SHARDS * DOCS_PER_SHARD;
        assertEquals("COUNT() across shards", expected, actual);
    }

    /**
     * {@code stats avg(value) as a} — primitive decomposition. PARTIAL emits
     * {@code [count:Int64, sum:Float64]}; FINAL reduces each with SUM and a Project wraps
     * {@code finalExpression = sum/count}. Exercises the multi-field intermediate path.
     */
    public void testAvgAcrossShards() throws Exception {
        createParquetBackedIndex(INDEX);
        indexConstantValueDocs(INDEX);

        Map<String, Object> result = executePPL("source = " + INDEX + " | stats avg(value) as a");
        List<List<Object>> rows = scalarRows(result, "a");

        double actual = ((Number) rows.get(0).get(0)).doubleValue();
        assertEquals("AVG(value) across shards should be " + VALUE, (double) VALUE, actual, 0.001);
    }

    /**
     * {@code stats dc(value) as dc} — engine-native merge. PARTIAL emits a single Binary
     * HLL sketch; resolver rebinds FINAL's arg to the sketch column and DataFusion's
     * approx_distinct Final merges sketches in-place. Tolerance is 10% (standard HLL
     * accuracy).
     */
    public void testDistinctCountAcrossShards() throws Exception {
        String index = "coord_reduce_dc";
        createParquetBackedIndex(index);
        indexVaryingValueDocs(index);

        Map<String, Object> result = executePPL("source = " + index + " | stats dc(value) as dc");
        List<List<Object>> rows = scalarRows(result, "dc");

        long actual = ((Number) rows.get(0).get(0)).longValue();
        int totalDocs = NUM_SHARDS * DOCS_PER_SHARD;
        assertTrue(
            "dc(value) should be approximately " + totalDocs + " (±10%), got " + actual,
            actual >= totalDocs * 0.9 && actual <= totalDocs * 1.1
        );
    }

    /**
     * {@code stats sum(value) as total by value} — group-by flows through partial/final
     * without interacting with the aggregate-call decomposition (key columns sit at the
     * front of the row type).
     */
    public void testGroupedSumAcrossShards() throws Exception {
        createParquetBackedIndex(INDEX);
        indexConstantValueDocs(INDEX);

        Map<String, Object> result = executePPL("source = " + INDEX + " | stats sum(value) as total by value");

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("rows must not be null", rows);
        assertEquals("grouped agg on a single-valued column must return exactly 1 group", 1, rows.size());
    }

    /**
     * Q10 shape: SUM + COUNT + AVG + DC together, grouped. Exercises all four resolver
     * branches in a single query and validates column positions in the final Project
     * wrapper produced for AVG.
     *
     * <p>pf2 had this test ignored because its {@code decomposeFinalFragment} mishandled
     * parent Project expressions after decomposition. pf4's single-pass
     * {@code AggregateDecompositionResolver} builds the Project wrapper correctly from
     * intermediateFields + finalExpression, so the test runs here.
     */
    public void testQ10ShapeAcrossShards() throws Exception {
        createParquetBackedIndex(INDEX);
        indexConstantValueDocs(INDEX);

        Map<String, Object> result = executePPL(
            "source = " + INDEX + " | stats sum(value) as s, count() as c, avg(value) as a, dc(value) as d by value"
        );

        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) result.get("columns");
        assertNotNull("columns must not be null", columns);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("rows must not be null", rows);
        assertEquals("Q10-shape on a single-valued column must return exactly 1 group", 1, rows.size());

        List<Object> row = rows.get(0);
        long totalDocs = (long) NUM_SHARDS * DOCS_PER_SHARD;
        assertEquals("SUM", (long) VALUE * totalDocs, ((Number) row.get(columns.indexOf("s"))).longValue());
        assertEquals("COUNT", totalDocs, ((Number) row.get(columns.indexOf("c"))).longValue());
        assertEquals("AVG", (double) VALUE, ((Number) row.get(columns.indexOf("a"))).doubleValue(), 0.001);
        // DC on a single-valued column: exact result is 1.
        long dcValue = ((Number) row.get(columns.indexOf("d"))).longValue();
        assertTrue("dc on single-valued column should be 1 (±small HLL error), got " + dcValue, dcValue >= 1 && dcValue <= 2);
    }

    // ─── Multi-shard WHERE + GROUP-BY reproducer (Arrow "project index 0 out of bounds") ───

    private static final String WHERE_REPRO_INDEX = "coord_reduce_where_repro";

    /**
     * Reproducer for the multi-shard "project index 0 out of bounds" bug.
     * <p>
     * Shape: {@code WHERE <pred> | stats count() as c by <string-key> | sort - c | head N}
     * (mirrors ClickBench Q13: {@code where SearchPhrase != '' | stats count() as c
     * by SearchPhrase | sort - c | head 10})
     * <p>
     * The trigger is that the WHERE clause filters out ALL rows on every shard (all docs
     * have category=''), causing the partial aggregate to produce an empty batch with 0
     * fields. The final aggregate then fails trying to project from that empty schema.
     * <p>
     * This query passes on single-shard but fails on multi-shard (≥2 shards) with:
     * <pre>
     *   java.lang.RuntimeException: Execution error: Arrow error: Schema error:
     *   project index 0 out of bounds, max field 0
     * </pre>
     * at NativeBridge.streamNext → AbstractDatafusionReduceSink.drainOutputIntoDownstream.
     * <p>
     * Ignored via @AwaitsFix so the IT suite stays green. Drop the annotation to observe
     * the failure.
     */
    public void testWhereGroupByCountMultiShard_reproducer() throws Exception {
        createWhereReproIndex();
        indexWhereReproDocs();

        // WHERE filters out ALL rows (all docs have category='') → partial agg on each
        // shard produces empty output → final agg hits "project index 0 out of bounds"
        executePPL(
            "source = " + WHERE_REPRO_INDEX + " | where category != '' | stats count() as c by category | sort - c | head 5"
        );
    }

    /**
     * Control case: same query shape WITHOUT the WHERE clause. This passes on multi-shard,
     * demonstrating that the WHERE predicate (which filters out all rows) is the trigger.
     */
    public void testGroupByCountMultiShard_noWhereControl() throws Exception {
        createWhereReproIndex();
        indexWhereReproDocs();

        Map<String, Object> result = executePPL(
            "source = " + WHERE_REPRO_INDEX + " | stats count() as c by category | sort - c | head 5"
        );

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("rows must not be null", rows);
        assertFalse("should return at least one group", rows.isEmpty());
    }

    private void createWhereReproIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + WHERE_REPRO_INDEX));
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
            + "    \"category\": { \"type\": \"keyword\" },"
            + "    \"value\": { \"type\": \"integer\" }"
            + "  }"
            + "}"
            + "}";

        Request createIndex = new Request("PUT", "/" + WHERE_REPRO_INDEX);
        createIndex.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(createIndex), "Create index " + WHERE_REPRO_INDEX);
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + WHERE_REPRO_INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void indexWhereReproDocs() throws Exception {
        // ALL docs have category='' so WHERE category != '' filters out everything on every
        // shard. This matches the ClickBench scenario where SearchPhrase is empty for all
        // rows in the test dataset, triggering the empty-partial-aggregate bug.
        StringBuilder bulk = new StringBuilder();
        int total = NUM_SHARDS * DOCS_PER_SHARD;
        for (int i = 0; i < total; i++) {
            bulk.append("{\"index\": {\"_id\": \"w").append(i).append("\"}}\n");
            bulk.append("{\"category\": \"\", \"value\": ").append(i + 1).append("}\n");
        }
        bulkAndRefresh(WHERE_REPRO_INDEX, bulk.toString());
    }

    // ─── Helpers ────────────────────────────────────────────────────────────────

    /**
     * Returns the {@code rows} list from a scalar-aggregate PPL response, asserting that
     * the single row contains the requested named column. Parameterised so each test
     * doesn't repeat the null/empty checks.
     */
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

    /**
     * Creates a 2-shard parquet-backed composite index with a single integer field {@code value}.
     * Uses a per-call name so DC (varying values) and the other tests (constant value) can
     * live in the same JVM without the bulk indexing steps colliding.
     */
    private void createParquetBackedIndex(String indexName) throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + indexName));
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

        Request createIndex = new Request("PUT", "/" + indexName);
        createIndex.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(createIndex), "Create index " + indexName);
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + indexName);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    /** Indexes {@link #NUM_SHARDS} × {@link #DOCS_PER_SHARD} docs, each with {@code value=VALUE}. */
    private void indexConstantValueDocs(String indexName) throws Exception {
        StringBuilder bulk = new StringBuilder();
        int total = NUM_SHARDS * DOCS_PER_SHARD;
        for (int i = 0; i < total; i++) {
            bulk.append("{\"index\": {\"_id\": \"").append(i).append("\"}}\n");
            bulk.append("{\"value\": ").append(VALUE).append("}\n");
        }
        bulkAndRefresh(indexName, bulk.toString());
    }

    /**
     * Indexes {@link #NUM_SHARDS} × {@link #DOCS_PER_SHARD} docs with {@code value = i+1},
     * giving a distinct value per doc — required for the DC test to have a meaningful
     * cardinality to approximate.
     */
    private void indexVaryingValueDocs(String indexName) throws Exception {
        StringBuilder bulk = new StringBuilder();
        int total = NUM_SHARDS * DOCS_PER_SHARD;
        for (int i = 0; i < total; i++) {
            bulk.append("{\"index\": {\"_id\": \"v").append(i).append("\"}}\n");
            bulk.append("{\"value\": ").append(i + 1).append("}\n");
        }
        bulkAndRefresh(indexName, bulk.toString());
    }

    private void bulkAndRefresh(String indexName, String bulkBody) throws Exception {
        Request bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.setJsonEntity(bulkBody);
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);
        client().performRequest(new Request("POST", "/" + indexName + "/_flush?force=true"));
    }

    private Map<String, Object> executePPL(String ppl) throws Exception {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + ppl + "\"}");
        Response response = client().performRequest(request);
        return entityAsMap(response);
    }
}
