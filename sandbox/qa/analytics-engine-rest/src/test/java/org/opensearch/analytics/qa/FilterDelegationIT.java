/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

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
@LuceneTestCase.AwaitsFix(bugUrl = "")
public class FilterDelegationIT extends AnalyticsRestTestCase {

    private static final String INDEX_NAME = "filter_delegation_e2e";

    public void testMatchFilterDelegationWithAggregate() throws Exception {
        createIndex();
        indexDocs();

        String ppl = "source = " + INDEX_NAME + " | where match(message, 'hello') | stats sum(value) as total";
        Map<String, Object> result = executePPL(ppl);

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
        Map<String, Object> result = executePPL(ppl);

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
        Map<String, Object> result = executePPL(ppl);

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
        Map<String, Object> result = executePPL(ppl);

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
        Map<String, Object> result = executePPL(ppl);

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
        Map<String, Object> result = executePPL(ppl);

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
        Map<String, Object> result = executePPL(ppl);

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
        Map<String, Object> result = executePPL(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull(rows);
        assertEquals(50L, ((Number) rows.get(0).get(0)).longValue());
    }

    /**
     * Exercises the cross-query caching path for delegated MATCH predicates.
     * Runs the same MATCH query twice. The node-level IndicesQueryCache should
     * cache the delegated query's DocIdSet after the first execution; the second
     * execution should hit the cache (same segment, same query, same reader).
     *
     * <p>Correctness is verified by result equality. Cache hit cannot be asserted
     * from REST without a dedicated stats endpoint, but correct results on both
     * calls confirm the cached Weight/DocIdSet is still valid.
     */
    public void testMatchDelegation_cacheHitOnRepeat() throws Exception {
        createIndex();
        indexDocs();

        String ppl = "source = " + INDEX_NAME + " | where match(message, 'hello') | stats count() as c";

        // First execution — populates cache (miss)
        Map<String, Object> result1 = executePPL(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows1 = (List<List<Object>>) result1.get("rows");
        assertNotNull(rows1);
        assertEquals(1, rows1.size());
        assertEquals(10L, ((Number) rows1.get(0).get(0)).longValue());

        // Second execution — should hit cache (same reader, same segment, same query)
        Map<String, Object> result2 = executePPL(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows2 = (List<List<Object>>) result2.get("rows");
        assertNotNull(rows2);
        assertEquals(1, rows2.size());
        assertEquals(10L, ((Number) rows2.get(0).get(0)).longValue());
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

    private Map<String, Object> executePPL(String ppl) throws Exception {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + ppl + "\"}");
        Response response = client().performRequest(request);
        return entityAsMap(response);
    }
}
