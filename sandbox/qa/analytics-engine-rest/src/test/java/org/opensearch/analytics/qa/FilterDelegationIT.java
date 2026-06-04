/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.util.List;
import java.util.Map;

/**
 * E2E integration test for filter delegation: a MATCH predicate is delegated to Lucene
 * while DataFusion drives the scan + aggregation.
 *
 * <p>Exercises the full path: PPL → planner → ShardScanWithDelegationInstructionNode →
 * data node dispatch → Lucene FilterDelegationHandle → Rust indexed executor → results.
 *
 * <p><b>Not asserted in code:</b> whether Lucene was actually called to evaluate the
 * predicate. The result-correctness checks below pass whether DataFusion handled the
 * filter natively or delegated to Lucene. The only current way to confirm Lucene
 * delegation actually happened is to read cluster logs and look for the per-RG
 * "consulting peer" / "lazy provider initialized" lines emitted from the handle and
 * the Rust evaluator. Asserting it in code would need a mock test plugin that wraps
 * {@code FilterDelegationHandle} to count calls and exposes the count via REST — a
 * bigger surface than this IT warrants, so deferred.
 */
public class FilterDelegationIT extends AnalyticsRestTestCase {

    private static final String INDEX_NAME = "filter_delegation_e2e";

    public void testMatchFilterDelegationWithAggregate() throws Exception {
        createIndex();
        indexDocs();

        String ppl = "source = " + INDEX_NAME + " | where match(message, 'hello') | stats sum(value) as total";
        Map<String, Object> result = executePplViaShim(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("rows must not be null", rows);
        assertEquals("scalar agg must return exactly 1 row", 1, rows.size());

        // 10 docs with "hello world" and value=5 → total = 50
        Number total = (Number) rows.get(0).get(0);
        assertEquals("SUM(value) for MATCH(message, 'hello') docs", 50L, total.longValue());
    }

    /**
     * Performance-delegation path: an EQUALS predicate on a {@code keyword} field is
     * dual-viable (both DataFusion can evaluate against parquet and Lucene can evaluate
     * via the inverted index). The planner emits a
     * {@code delegation_possible(tag = 'hello', annotationId)} marker; DataFusion
     * evaluates the predicate natively for page-stat pruning, and consults Lucene only
     * when its own pruning isn't selective enough for an RG.
     *
     * <p>Result-correctness check: 10 docs with tag='hello' → SUM(value) = 50.
     *
     * <p>TODO(scf-prod): runtime assertion that Lucene was actually consulted is
     * currently performed by inspecting cluster log output for the {@code [scf]
     * createProvider/createCollector/collectDocs} lines emitted by
     * {@code LuceneFilterDelegationHandle}. Replace with a proper counter check
     * (test plugin + REST endpoint or DF metrics surfaced through PPL response)
     * once that infrastructure exists. Without it, this test passes even if
     * Lucene is never consulted.
     */
    public void testEqualsFilterPerformanceDelegationWithAggregate() throws Exception {
        createIndex();
        indexDocs();

        String ppl = "source = " + INDEX_NAME + " | where tag = 'hello' | stats sum(value) as total";
        Map<String, Object> result = executePplViaShim(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("rows must not be null", rows);
        assertEquals("scalar agg must return exactly 1 row", 1, rows.size());

        // 10 docs with tag='hello' → total = 50
        Number total = (Number) rows.get(0).get(0);
        assertEquals("SUM(value) for tag='hello' docs (dual-viable performance delegation)", 50L, total.longValue());
    }

    /**
     * Disjunctive shape: two dual-viable EQUALS predicates sitting under OR. Without a
     * gate, the planner emits {@code delegation_possible(...)} markers for both leaves,
     * the Rust filter classifier sends the tree to the bitmap-tree evaluator, and that
     * evaluator hits {@code unimplemented!()} arms for {@code BoolNode::DelegationPossible}
     * — the data node panics on the FFM bridge and the query fails.
     *
     * <p>Both predicates use EQUALS so we exercise the existing
     * {@code EqualsSerializer} (no other comparison serializers are wired today).
     * The two leaves are on <em>different</em> fields ({@code tag} keyword and
     * {@code value} integer) — same-field OR collapses into a Calcite Sarg
     * (single SEARCH predicate, no real OR in the marked tree), which would
     * defeat the purpose of this test.
     *
     * <p>Correctness: 10 docs have {@code tag='hello'} (and value=5), 10 have
     * {@code value=3}. The OR matches all 20 docs. SUM(value) = 10*5 + 10*3 = 80.
     */
    public void testEqualsFilterUnderOr_DoesNotPanic() throws Exception {
        createIndex();
        indexDocs();

        String ppl = "source = " + INDEX_NAME + " | where tag = 'hello' or value = 3 | stats sum(value) as total";
        Map<String, Object> result = executePplViaShim(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("rows must not be null", rows);
        assertEquals("scalar agg must return exactly 1 row", 1, rows.size());

        Number total = (Number) rows.get(0).get(0);
        assertEquals("SUM(value) for tag='hello' OR tag='goodbye' (all 20 docs)", 80L, total.longValue());
    }

    /**
     * Regression: OR(match, range-on-numeric) where the numeric field sits at a high
     * schema index. The bitmap-tree evaluator's general expr.evaluate(batch) path
     * referenced columns by full-schema index, but the projected batch had a different
     * layout — DataFusion panicked with "PhysicalExpr Column references column ... at
     * index N but input schema only has 1 columns". Fix: remap_expr_to_batch rewrites
     * indices to match the projected batch before evaluating.
     */
    public void testOrRangePredicateOnNumeric_DoesNotPanic() throws Exception {
        createIndex();
        indexDocs();

        // 10 docs tag='hello' value=5 + 10 docs tag='goodbye' value=3.
        // match(message,'hello') OR value > 4 → 10 hello docs ∪ 10 value=5 docs (same set) = 10.
        String ppl = "source = " + INDEX_NAME + " | where match(message, 'hello') or value > 4 | stats count() as c";
        Map<String, Object> result = executePplViaShim(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals(10L, ((Number) rows.get(0).get(0)).longValue());
    }

    /**
     * Regression: NOT(match) on a delegated predicate was misclassified as CONJUNCTIVE
     * by FilterTreeShapeDeriver because hasMixed required a driving-backend sibling.
     * SingleCollector can't invert a collector bitmap, so the query crashed. Fix:
     * a delegated leaf under OR/NOT triggers INTERLEAVED regardless of siblings.
     */
    public void testNotMatch_RoutesToTreeEvaluator() throws Exception {
        createIndex();
        indexDocs();

        // NOT(match(message,'hello')) → 10 goodbye docs.
        String ppl = "source = " + INDEX_NAME + " | where not match(message, 'hello') | stats count() as c";
        Map<String, Object> result = executePplViaShim(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull(rows);
        assertEquals(10L, ((Number) rows.get(0).get(0)).longValue());
    }

    /**
     * Regression: COUNT(DISTINCT col) is converted by Calcite into a stacked Filter
     * (auto-injected IS NOT NULL on top of the user's WHERE). FilterTreeShapeDeriver
     * only inspected the outermost OpenSearchFilter via findNode and missed the inner
     * filter's delegated predicate, producing tree_shape=NO_DELEGATION while
     * delegationBytes recorded 1 delegated leaf — the data node errored with
     * "execute_indexed_query called with no index_filter(...) in plan". Fix:
     * CoreRules.FILTER_MERGE collapses adjacent filters before derivation.
     */
    public void testCountDistinctWithMatchFilter_StackedFilterMerged() throws Exception {
        createIndex();
        indexDocs();

        // dc(value) over docs matching match(message,'hello') → all values are 5 → 1 distinct.
        String ppl = "source = " + INDEX_NAME + " | where match(message, 'hello') | stats dc(value) as d";
        Map<String, Object> result = executePplViaShim(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull(rows);
        assertEquals(1L, ((Number) rows.get(0).get(0)).longValue());
    }

    /**
     * Regression: AND(match, keyword=, integer=) routed via SingleCollector with
     * performance peers. The integer field was marked Lucene-indexable in
     * FieldStorageResolver and routed to peer consultation, but Lucene's secondary
     * format (composite-parquet primary) only indexes text/keyword/match_only_text —
     * the integer query produced a null Scorer → empty bitset → wrong zero count
     * after AND-intersection. Fix: STANDARD_TYPES in LuceneAnalyticsBackendPlugin
     * only declares Lucene-indexable types.
     */
    public void testAndMatchKeywordInteger_NoEmptyPeerIntersection() throws Exception {
        createIndex();
        indexDocs();

        // 10 docs match all three: match(message,'hello') AND tag='hello' AND value=5.
        String ppl = "source = " + INDEX_NAME
            + " | where match(message, 'hello') and tag = 'hello' and value = 5 | stats count() as c";
        Map<String, Object> result = executePplViaShim(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull(rows);
        assertEquals(10L, ((Number) rows.get(0).get(0)).longValue());
    }

    /**
     * Regression: AND with sum() aggregation — non-count aggregations also need
     * correctly filtered batches. Pre-fix this returned null because peer
     * consultation zeroed out candidates.
     */
    public void testAndMatchKeywordInteger_SumAggregation() throws Exception {
        createIndex();
        indexDocs();

        // sum(value) over the 10 matching docs (value=5 each) → 50.
        String ppl = "source = " + INDEX_NAME
            + " | where match(message, 'hello') and tag = 'hello' and value = 5 | stats sum(value) as s";
        Map<String, Object> result = executePplViaShim(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull(rows);
        assertEquals(50L, ((Number) rows.get(0).get(0)).longValue());
    }

    // ---- Combining delegation tests ----

    /**
     * match(message,'hello') AND tag='hello' AND value=5
     * correctness (match) + performance (tag=) + native (value=) under AND.
     * Both delegations combined separately; result = 50.
     */
    public void testAndCorrectnessAndPerfAndNative() throws Exception {
        createIndex();
        indexDocs();
        String ppl = "source = " + INDEX_NAME + " | where match(message, 'hello') and tag = 'hello' and value = 5 | stats count() as cnt";
        Map<String, Object> result = executePplViaShim(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertEquals(10L, ((Number) rows.get(0).get(0)).longValue());
    }

    /**
     * tag='hello' AND value=5 (no correctness delegation, only performance + native under AND).
     * Performance-delegated tag= combined; value= stays native. Result = 10.
     */
    public void testAndOnlyPerfAndNative() throws Exception {
        createIndex();
        indexDocs();
        String ppl = "source = " + INDEX_NAME + " | where tag = 'hello' and value = 5 | stats count() as cnt";
        Map<String, Object> result = executePplViaShim(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertEquals(10L, ((Number) rows.get(0).get(0)).longValue());
    }

    /**
     * match(message,'hello') OR tag='hello' AND value=5
     * Precedence: match OR (tag= AND value=). Correctness under OR, perf under AND.
     * Performance-delegated under OR stays native (not combined with correctness).
     * Result: 10 (match 'hello') + 0 extra (tag='hello' AND value=5 is subset) = 10.
     */
    public void testOrCorrectnessWithPerfAndNative() throws Exception {
        createIndex();
        indexDocs();
        String ppl = "source = " + INDEX_NAME + " | where match(message, 'hello') or tag = 'hello' and value = 5 | stats count() as cnt";
        Map<String, Object> result = executePplViaShim(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertEquals(10L, ((Number) rows.get(0).get(0)).longValue());
    }

    /**
     * (match(message,'hello') AND tag='hello') OR (tag='goodbye' AND value=3)
     * OR of two AND arms: left has correctness+perf, right has perf+native.
     * Result: 10 (left arm) + 10 (right arm) = 20 (but overlap → 10 + 10 = 20 distinct docs).
     */
    public void testOrOfTwoAndArms() throws Exception {
        createIndex();
        indexDocs();
        String ppl = "source = " + INDEX_NAME
            + " | where (match(message, 'hello') and tag = 'hello') or (tag = 'goodbye' and value = 3) | stats count() as cnt";
        Map<String, Object> result = executePplViaShim(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertEquals(20L, ((Number) rows.get(0).get(0)).longValue());
    }

    /**
     * OR(MATCH on text, EQUALS on keyword), oracle = 10. Both arms are Lucene-delegatable.
     * Under {@code prefer=true} Lucene drives end-to-end (combiner skipped, no tree_shape).
     * Under {@code prefer=false} the combiner runs: {@code fuse=false} keeps
     * {@code OR(delegated_predicate, delegation_possible)} as INTERLEAVED; {@code fuse=true}
     * collapses to a single {@code delegated_predicate} as CONJUNCTIVE.
     */
    public void testOrCorrectnessAndPerf_fuseDualViable() throws Exception {
        createIndex();
        indexDocs();

        String ppl = "source = " + INDEX_NAME + " | where match(message, 'hello') or tag = 'hello' | stats count() as cnt";
        Map<MatrixKey, ShardStage> expected = Map.of(
            new MatrixKey(true, false), new ShardStage("lucene", null),
            new MatrixKey(true, true), new ShardStage("lucene", null),
            new MatrixKey(false, false), new ShardStage("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"),
            new MatrixKey(false, true), new ShardStage("datafusion", "CONJUNCTIVE")
        );
        runFuseMatrix(ppl, 10L, expected);
    }

    /**
     * OR(EQUALS on keyword, EQUALS on integer), oracle = 20. The integer arm isn't
     * Lucene-filterable, so the planner picks DataFusion in every cell, and the OR has a
     * non-delegatable sibling which keeps the shape INTERLEAVED in both fuse modes.
     */
    public void testOrTwoPerf_fuseDualViable() throws Exception {
        createIndex();
        indexDocs();

        String ppl = "source = " + INDEX_NAME + " | where tag = 'hello' or value = 3 | stats count() as cnt";
        ShardStage interleavedDf = new ShardStage("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION");
        Map<MatrixKey, ShardStage> expected = Map.of(
            new MatrixKey(true, false), interleavedDf,
            new MatrixKey(true, true), interleavedDf,
            new MatrixKey(false, false), interleavedDf,
            new MatrixKey(false, true), interleavedDf
        );
        runFuseMatrix(ppl, 20L, expected);
    }

    /** Cluster-setting combination: ({@code prefer_metadata_driver}, {@code fuse_dual_viable}). */
    private record MatrixKey(boolean prefer, boolean fuse) {}

    /** Asserted SHARD_FRAGMENT profile fields. {@code treeShape == null} means the field
     *  must be absent (Lucene-as-driver has no delegation instruction). */
    private record ShardStage(String chosenBackend, String treeShape) {}

    private void runFuseMatrix(String ppl, long oracle, Map<MatrixKey, ShardStage> expected) throws Exception {
        try {
            for (Map.Entry<MatrixKey, ShardStage> entry : expected.entrySet()) {
                MatrixKey key = entry.getKey();
                ShardStage want = entry.getValue();
                setPreferMetadataDriver(key.prefer());
                setFuseDualViable(key.fuse());

                String label = "prefer=" + key.prefer() + ",fuse=" + key.fuse();
                assertEquals(label + " — count", oracle, executeCount(ppl));
                Map<String, Object> stage = shardFragmentStage(ppl);
                assertEquals(label + " — chosen_backend", want.chosenBackend(), stage.get("chosen_backend"));
                assertEquals(label + " — tree_shape", want.treeShape(), stage.get("tree_shape"));
            }
        } finally {
            setFuseDualViable(false);
            setPreferMetadataDriver(true);
        }
    }

    private long executeCount(String ppl) throws Exception {
        Map<String, Object> result = executePplViaShim(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        return ((Number) rows.get(0).get(0)).longValue();
    }

    private Map<String, Object> shardFragmentStage(String ppl) throws Exception {
        Request request = new Request("POST", "/_analytics/ppl/_explain");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Map<String, Object> explain = assertOkAndParse(client().performRequest(request), "EXPLAIN: " + ppl);
        @SuppressWarnings("unchecked")
        Map<String, Object> profile = (Map<String, Object>) explain.get("profile");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");
        for (Map<String, Object> stage : stages) {
            if ("SHARD_FRAGMENT".equals(stage.get("execution_type"))) return stage;
        }
        throw new AssertionError("No SHARD_FRAGMENT stage in profile: " + stages);
    }

    private void setFuseDualViable(boolean value) throws Exception {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"analytics.delegation.fuse_dual_viable\": " + value + "}}");
        client().performRequest(req);
    }

    private void setPreferMetadataDriver(boolean value) throws Exception {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"analytics.planner.prefer_metadata_driver\": " + value + "}}");
        client().performRequest(req);
    }

    private void createIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX_NAME));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"message\": { \"type\": \"text\" },"
            + "    \"tag\": { \"type\": \"keyword\" },"
            + "    \"value\": { \"type\": \"integer\" }"
            + "  }"
            + "}"
            + "}";

        Request createIndex = new Request("PUT", "/" + INDEX_NAME);
        createIndex.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(createIndex), "Create index");
        assertEquals(true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX_NAME);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void indexDocs() throws Exception {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            bulk.append("{\"index\": {}}\n");
            bulk.append("{\"message\": \"hello world\", \"tag\": \"hello\", \"value\": 5}\n");
        }
        for (int i = 0; i < 10; i++) {
            bulk.append("{\"index\": {}}\n");
            bulk.append("{\"message\": \"goodbye world\", \"tag\": \"goodbye\", \"value\": 3}\n");
        }

        Request bulkRequest = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        // Flush to ensure parquet files are written
        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_flush?force=true"));
    }

}
