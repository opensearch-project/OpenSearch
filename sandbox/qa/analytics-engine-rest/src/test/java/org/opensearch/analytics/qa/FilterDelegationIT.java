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
