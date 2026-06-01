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
 * End-to-end tests for the distributed partial/final aggregate path:
 *
 * <pre>
 *   PPL → planner (AggregateDecompositionResolver) → multi-shard SHARD_FRAGMENT dispatch
 *       → shard-side partial aggregate → ExchangeSink.feed → coordinator reduce
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
 *       (Binary intermediate, reducer == self; HLL merge inside the backend)</li>
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

        Map<String, Object> result = executePpl("source = " + INDEX + " | stats sum(value) as total");
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

        Map<String, Object> result = executePpl("source = " + INDEX + " | stats count() as cnt");
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

        Map<String, Object> result = executePpl("source = " + INDEX + " | stats avg(value) as a");
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

        Map<String, Object> result = executePpl("source = " + index + " | stats dc(value) as dc");
        List<List<Object>> rows = scalarRows(result, "dc");

        long actual = ((Number) rows.get(0).get(0)).longValue();
        int totalDocs = NUM_SHARDS * DOCS_PER_SHARD;
        assertTrue(
            "dc(value) should be approximately " + totalDocs + " (±10%), got " + actual,
            actual >= totalDocs * 0.9 && actual <= totalDocs * 1.1
        );
    }

    /**
     * {@code stats percentile_approx(value, 50) as p} — t-digest approximate median.
     * STATE_EXPANDING, so the split rule gathers to coordinator + single-stage. Maps to
     * DataFusion's {@code approx_percentile_cont} via {@link
     * org.opensearch.be.datafusion.PplAggregateCallRewriter}.
     */
    public void testPercentileApproxAcrossShards() throws Exception {
        String index = "coord_reduce_percentile_approx";
        createParquetBackedIndex(index);
        indexVaryingValueDocs(index);

        Map<String, Object> result = executePpl("source = " + index + " | stats percentile_approx(value, 50) as p");
        List<List<Object>> rows = scalarRows(result, "p");

        Object cell = rows.get(0).get(0);
        assertNotNull("cell for 'p' must not be null — coordinator-reduce returned no value", cell);
        double actual = ((Number) cell).doubleValue();
        int totalDocs = NUM_SHARDS * DOCS_PER_SHARD;
        double expected = (totalDocs + 1) / 2.0;
        assertTrue(
            "percentile_approx(value, 50) should be approximately " + expected + " (±2.0), got " + actual,
            Math.abs(actual - expected) <= 2.0
        );
    }

    /** Single-shard {@code take(value, 3)} — bounded array of up to 3 values. */
    public void testTakeSingleShard() throws Exception {
        String index = "coord_reduce_take_single";
        createSingleShardParquetBackedIndex(index);
        indexSequentialValueDocsSingleShard(index);

        Map<String, Object> result = executePpl("source = " + index + " | stats take(value, 3) as t");

        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(result);
        assertNotNull("schema must not be null", columns);
        assertTrue("columns must contain 't', got " + columns, columns.contains("t"));

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("rows must not be null", rows);
        assertEquals("scalar agg must return exactly 1 row", 1, rows.size());

        Object cell = rows.get(0).get(columns.indexOf("t"));
        assertNotNull("cell for 't' must not be null — coordinator-reduce returned no value", cell);
        assertTrue("take() must return a List, got " + cell.getClass(), cell instanceof List);

        @SuppressWarnings("unchecked")
        List<Object> taken = (List<Object>) cell;
        assertEquals("take(value, 3) must return exactly 3 elements", 3, taken.size());
        for (Object v : taken) {
            assertNotNull("take(value, 3) elements must not be null", v);
            int iv = ((Number) v).intValue();
            assertTrue("take(value, 3) element " + iv + " must be in {1.." + DOCS_PER_SHARD + "}", iv >= 1 && iv <= DOCS_PER_SHARD);
        }
    }

    /** Cross-shard {@code take(value, 5)} — coordinator unions per-shard arrays and truncates to 5. */
    public void testTakeAcrossShards() throws Exception {
        String index = "coord_reduce_take_multi";
        createParquetBackedIndex(index);
        indexVaryingValueDocs(index);

        Map<String, Object> result = executePpl("source = " + index + " | stats take(value, 5) as t");

        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(result);
        assertNotNull("schema must not be null", columns);
        assertTrue("columns must contain 't', got " + columns, columns.contains("t"));

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("rows must not be null", rows);
        assertEquals("scalar agg must return exactly 1 row", 1, rows.size());

        Object cell = rows.get(0).get(columns.indexOf("t"));
        assertNotNull("cell for 't' must not be null — coordinator-reduce returned no value", cell);
        assertTrue("take() must return a List, got " + cell.getClass(), cell instanceof List);

        @SuppressWarnings("unchecked")
        List<Object> taken = (List<Object>) cell;
        assertEquals("take(value, 5) must return exactly 5 elements", 5, taken.size());

        int totalDocs = NUM_SHARDS * DOCS_PER_SHARD;
        java.util.Set<Integer> seen = new java.util.HashSet<>();
        for (Object v : taken) {
            assertNotNull("take(value, 5) elements must not be null", v);
            int iv = ((Number) v).intValue();
            assertTrue("take(value, 5) element " + iv + " must be in {1.." + totalDocs + "}", iv >= 1 && iv <= totalDocs);
            assertTrue("take(value, 5) elements must be distinct, duplicate=" + iv, seen.add(iv));
        }
    }

    /** Single-shard {@code first(value)} — arrival order depends on parquet read; assert membership only. */
    public void testFirstSingleShard() throws Exception {
        String index = "coord_reduce_first_single";
        createSingleShardParquetBackedIndex(index);
        indexSequentialValueDocsSingleShard(index);

        Map<String, Object> result = executePpl("source = " + index + " | stats first(value) as f");
        List<List<Object>> rows = scalarRows(result, "f");

        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(result);
        Object cell = rows.get(0).get(columns.indexOf("f"));
        int actual = ((Number) cell).intValue();
        assertTrue("first(value) must be in {1.." + DOCS_PER_SHARD + "}, got " + actual, actual >= 1 && actual <= DOCS_PER_SHARD);
    }

    /** Cross-shard {@code first(value)} — arrival order non-deterministic; assert membership only. */
    public void testFirstAcrossShards() throws Exception {
        String index = "coord_reduce_first_multi";
        createParquetBackedIndex(index);
        indexVaryingValueDocs(index);

        Map<String, Object> result = executePpl("source = " + index + " | stats first(value) as f");
        List<List<Object>> rows = scalarRows(result, "f");

        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(result);
        Object cell = rows.get(0).get(columns.indexOf("f"));
        int actual = ((Number) cell).intValue();
        int totalDocs = NUM_SHARDS * DOCS_PER_SHARD;
        assertTrue("first(value) must be in {1.." + totalDocs + "}, got " + actual, actual >= 1 && actual <= totalDocs);
    }

    /** Single-shard {@code last(value)} — arrival order depends on parquet read; assert membership only. */
    public void testLastSingleShard() throws Exception {
        String index = "coord_reduce_last_single";
        createSingleShardParquetBackedIndex(index);
        indexSequentialValueDocsSingleShard(index);

        Map<String, Object> result = executePpl("source = " + index + " | stats last(value) as l");
        List<List<Object>> rows = scalarRows(result, "l");

        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(result);
        Object cell = rows.get(0).get(columns.indexOf("l"));
        int actual = ((Number) cell).intValue();
        assertTrue("last(value) must be in {1.." + DOCS_PER_SHARD + "}, got " + actual, actual >= 1 && actual <= DOCS_PER_SHARD);
    }

    /** Cross-shard {@code last(value)} — arrival order non-deterministic; assert membership only. */
    public void testLastAcrossShards() throws Exception {
        String index = "coord_reduce_last_multi";
        createParquetBackedIndex(index);
        indexVaryingValueDocs(index);

        Map<String, Object> result = executePpl("source = " + index + " | stats last(value) as l");
        List<List<Object>> rows = scalarRows(result, "l");

        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(result);
        Object cell = rows.get(0).get(columns.indexOf("l"));
        int actual = ((Number) cell).intValue();
        int totalDocs = NUM_SHARDS * DOCS_PER_SHARD;
        assertTrue("last(value) must be in {1.." + totalDocs + "}, got " + actual, actual >= 1 && actual <= totalDocs);
    }

    /** Single-shard {@code list(value)} — preserves duplicates; result must contain every input. */
    public void testListSingleShard() throws Exception {
        String index = "coord_reduce_list_single";
        createSingleShardParquetBackedIndex(index);
        indexSequentialValueDocsSingleShard(index);

        Map<String, Object> result = executePpl("source = " + index + " | stats list(value) as l");

        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(result);
        assertNotNull("schema must not be null", columns);
        assertTrue("columns must contain 'l', got " + columns, columns.contains("l"));

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("rows must not be null", rows);
        assertEquals("scalar agg must return exactly 1 row", 1, rows.size());

        Object cell = rows.get(0).get(columns.indexOf("l"));
        assertNotNull("cell for 'l' must not be null — coordinator-reduce returned no value", cell);
        assertTrue("list() must return a List, got " + cell.getClass(), cell instanceof List);

        @SuppressWarnings("unchecked")
        List<Object> listed = (List<Object>) cell;
        assertEquals("list(value) must return exactly " + DOCS_PER_SHARD + " elements", DOCS_PER_SHARD, listed.size());

        java.util.Set<Integer> seen = new java.util.HashSet<>();
        for (Object v : listed) {
            assertNotNull("list(value) elements must not be null", v);
            seen.add(((Number) v).intValue());
        }
        java.util.Set<Integer> expected = new java.util.HashSet<>();
        for (int i = 1; i <= DOCS_PER_SHARD; i++) {
            expected.add(i);
        }
        assertEquals("list(value) must contain every integer in {1.." + DOCS_PER_SHARD + "}", expected, seen);
    }

    /** Cross-shard {@code list(value)} — coordinator concatenates per-shard lists via list_merge UDAF. */
    public void testListAcrossShards() throws Exception {
        String index = "coord_reduce_list_multi";
        createParquetBackedIndex(index);
        indexVaryingValueDocs(index);

        Map<String, Object> result = executePpl("source = " + index + " | stats list(value) as l");

        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(result);
        assertNotNull("schema must not be null", columns);
        assertTrue("columns must contain 'l', got " + columns, columns.contains("l"));

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("rows must not be null", rows);
        assertEquals("scalar agg must return exactly 1 row", 1, rows.size());

        Object cell = rows.get(0).get(columns.indexOf("l"));
        assertNotNull("cell for 'l' must not be null — coordinator-reduce returned no value", cell);
        assertTrue("list() must return a List, got " + cell.getClass(), cell instanceof List);

        int totalDocs = NUM_SHARDS * DOCS_PER_SHARD;
        @SuppressWarnings("unchecked")
        List<Object> listed = (List<Object>) cell;
        assertEquals("list(value) must return exactly " + totalDocs + " elements", totalDocs, listed.size());

        java.util.Set<Integer> seen = new java.util.HashSet<>();
        for (Object v : listed) {
            assertNotNull("list(value) elements must not be null", v);
            seen.add(((Number) v).intValue());
        }
        java.util.Set<Integer> expected = new java.util.HashSet<>();
        for (int i = 1; i <= totalDocs; i++) {
            expected.add(i);
        }
        assertEquals("list(value) must contain every integer in {1.." + totalDocs + "}", expected, seen);
    }

    /** Single-shard {@code values(value)} — de-duplicates; result must equal the distinct input set. */
    public void testValuesSingleShard() throws Exception {
        String index = "coord_reduce_values_single";
        createSingleShardParquetBackedIndex(index);
        indexDuplicateValueDocsSingleShard(index);

        Map<String, Object> result = executePpl("source = " + index + " | stats values(value) as v");

        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(result);
        assertNotNull("schema must not be null", columns);
        assertTrue("columns must contain 'v', got " + columns, columns.contains("v"));

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("rows must not be null", rows);
        assertEquals("scalar agg must return exactly 1 row", 1, rows.size());

        Object cell = rows.get(0).get(columns.indexOf("v"));
        assertNotNull("cell for 'v' must not be null — coordinator-reduce returned no value", cell);
        assertTrue("values() must return a List, got " + cell.getClass(), cell instanceof List);

        @SuppressWarnings("unchecked")
        List<Object> got = (List<Object>) cell;
        assertEquals("values(value) must return exactly 5 distinct elements", 5, got.size());

        java.util.Set<Integer> seen = new java.util.HashSet<>();
        for (Object v : got) {
            assertNotNull("values(value) elements must not be null", v);
            seen.add(((Number) v).intValue());
        }
        java.util.Set<Integer> expected = new java.util.HashSet<>();
        for (int i = 1; i <= 5; i++) {
            expected.add(i);
        }
        assertEquals("values(value) must contain exactly {1..5}", expected, seen);
    }

    /** Cross-shard {@code values(value)} — coordinator concatenates and re-deduplicates via list_merge_distinct UDAF. */
    public void testValuesAcrossShards() throws Exception {
        String index = "coord_reduce_values_multi";
        createParquetBackedIndex(index);
        indexDuplicateValueDocs(index);

        Map<String, Object> result = executePpl("source = " + index + " | stats values(value) as v");

        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(result);
        assertNotNull("schema must not be null", columns);
        assertTrue("columns must contain 'v', got " + columns, columns.contains("v"));

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("rows must not be null", rows);
        assertEquals("scalar agg must return exactly 1 row", 1, rows.size());

        Object cell = rows.get(0).get(columns.indexOf("v"));
        assertNotNull("cell for 'v' must not be null — coordinator-reduce returned no value", cell);
        assertTrue("values() must return a List, got " + cell.getClass(), cell instanceof List);

        @SuppressWarnings("unchecked")
        List<Object> got = (List<Object>) cell;
        assertEquals("values(value) must return exactly 10 distinct elements", 10, got.size());

        java.util.Set<Integer> seen = new java.util.HashSet<>();
        for (Object v : got) {
            assertNotNull("values(value) elements must not be null", v);
            seen.add(((Number) v).intValue());
        }
        java.util.Set<Integer> expected = new java.util.HashSet<>();
        for (int i = 1; i <= 10; i++) {
            expected.add(i);
        }
        assertEquals("values(value) must contain exactly {1..10}", expected, seen);
    }

    /**
     * {@code stats sum(value) as total by value} — group-by flows through partial/final
     * without interacting with the aggregate-call decomposition (key columns sit at the
     * front of the row type).
     */
    public void testGroupedSumAcrossShards() throws Exception {
        createParquetBackedIndex(INDEX);
        indexConstantValueDocs(INDEX);

        Map<String, Object> result = executePpl("source = " + INDEX + " | stats sum(value) as total by value");

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("rows must not be null", rows);
        assertEquals("grouped agg on a single-valued column must return exactly 1 group", 1, rows.size());
    }

    /**
     * Q10 shape: SUM + COUNT + AVG + DC together, grouped. Exercises all four resolver
     * branches in a single query and validates column positions in the final Project
     * wrapper produced for AVG. Covers the case where the aggregate decomposition has to
     * rewrite the parent Project's expressions to reference the rebuilt exchange columns.
     */
    public void testQ10ShapeAcrossShards() throws Exception {
        createParquetBackedIndex(INDEX);
        indexConstantValueDocs(INDEX);

        Map<String, Object> result = executePpl(
            "source = " + INDEX + " | stats sum(value) as s, count() as c, avg(value) as a, dc(value) as d by value"
        );

        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(result);
        assertNotNull("schema must not be null", columns);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
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

    // ─── Multi-shard GROUP BY on string columns ─────────────────────────────────

    private static final String STRING_GROUP_INDEX = "coord_reduce_string_group";

    /**
     * Multi-shard GROUP BY with a string key where WHERE filters every row on every shard.
     * Shape: {@code WHERE <pred> | stats count() as c by <string-key> | sort - c | head N}
     * (mirrors ClickBench Q13 {@code where SearchPhrase != '' | stats count() by
     * SearchPhrase}.)
     *
     * <p>All docs have {@code category=''} so {@code WHERE category != ''} filters
     * everything, causing each shard's partial aggregate to produce zero rows. The
     * coordinator's final aggregate must still report an empty result without erroring —
     * the wire-format has to carry the schema on an empty batch so downstream operators
     * have something to project from.
     */
    public void testGroupByCountMultiShard_allRowsFilteredByWhere() throws Exception {
        createStringGroupIndex();
        indexStringGroupDocs();

        executePpl(
            "source = " + STRING_GROUP_INDEX + " | where category != '' | stats count() as c by category | sort - c | head 5"
        );
    }

    /**
     * Control for {@link #testGroupByCountMultiShard_allRowsFilteredByWhere}: same query
     * shape without the WHERE clause. Every doc lands in the single {@code category=''}
     * group, so the shard's partial emits one non-empty batch and the final aggregate
     * returns a single row. Validates the non-empty path with the same data shape.
     */
    public void testGroupByCountMultiShard_noWhereClause() throws Exception {
        createStringGroupIndex();
        indexStringGroupDocs();

        Map<String, Object> result = executePpl(
            "source = " + STRING_GROUP_INDEX + " | stats count() as c by category | sort - c | head 5"
        );

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("rows must not be null", rows);
        assertFalse("should return at least one group", rows.isEmpty());
    }

    private void createStringGroupIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + STRING_GROUP_INDEX));
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

        Request createIndex = new Request("PUT", "/" + STRING_GROUP_INDEX);
        createIndex.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(createIndex), "Create index " + STRING_GROUP_INDEX);
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + STRING_GROUP_INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void indexStringGroupDocs() throws Exception {
        // All docs share category='' — makes "WHERE category != ''" filter every row on
        // every shard, exercising the empty-partial path.
        StringBuilder bulk = new StringBuilder();
        int total = NUM_SHARDS * DOCS_PER_SHARD;
        for (int i = 0; i < total; i++) {
            bulk.append("{\"index\": {\"_id\": \"w").append(i).append("\"}}\n");
            bulk.append("{\"category\": \"\", \"value\": ").append(i + 1).append("}\n");
        }
        bulkAndRefresh(STRING_GROUP_INDEX, bulk.toString());
    }

    // ─── Helpers ────────────────────────────────────────────────────────────────

    /**
     * Returns the {@code rows} list from a scalar-aggregate PPL response, asserting that
     * the single row contains the requested named column. Parameterised so each test
     * doesn't repeat the null/empty checks.
     */
    private static List<List<Object>> scalarRows(Map<String, Object> result, String columnName) {
        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(result);
        assertNotNull("schema must not be null", columns);
        assertTrue("columns must contain '" + columnName + "', got " + columns, columns.contains(columnName));

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
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
     * Creates a single-shard parquet-backed composite index with a single integer field
     * {@code value}. Mirrors {@link #createParquetBackedIndex(String)} but with
     * {@code number_of_shards=1} so tests can exercise the trivial pass-through merge path
     * without distributed coordination.
     */
    private void createSingleShardParquetBackedIndex(String indexName) throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + indexName));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
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

    /**
     * Indexes {@link #DOCS_PER_SHARD} docs with {@code value = i+1} into a single-shard
     * index — yielding values {@code 1..DOCS_PER_SHARD} (i.e. {@code 1..10}). Used by the
     * single-shard {@code take()} test.
     */
    private void indexSequentialValueDocsSingleShard(String indexName) throws Exception {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < DOCS_PER_SHARD; i++) {
            bulk.append("{\"index\": {\"_id\": \"s").append(i).append("\"}}\n");
            bulk.append("{\"value\": ").append(i + 1).append("}\n");
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

    /**
     * Indexes 10 docs into a single-shard index where {@code value} cycles through
     * {@code 1..5} twice — so 10 docs but only 5 distinct values. Used by
     * {@link #testValuesSingleShard()} to validate that {@code values()} de-duplicates.
     */
    private void indexDuplicateValueDocsSingleShard(String indexName) throws Exception {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < DOCS_PER_SHARD; i++) {
            bulk.append("{\"index\": {\"_id\": \"d").append(i).append("\"}}\n");
            bulk.append("{\"value\": ").append((i % 5) + 1).append("}\n");
        }
        bulkAndRefresh(indexName, bulk.toString());
    }

    /**
     * Indexes {@link #NUM_SHARDS} × {@link #DOCS_PER_SHARD} docs where {@code value}
     * cycles through {@code 1..10} twice — so 20 docs but only 10 distinct values. Used
     * by {@link #testValuesAcrossShards()} to validate that {@code values()}
     * de-duplicates across the partial/final boundary.
     */
    private void indexDuplicateValueDocs(String indexName) throws Exception {
        StringBuilder bulk = new StringBuilder();
        int total = NUM_SHARDS * DOCS_PER_SHARD;
        for (int i = 0; i < total; i++) {
            bulk.append("{\"index\": {\"_id\": \"d").append(i).append("\"}}\n");
            bulk.append("{\"value\": ").append((i % 10) + 1).append("}\n");
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

}
