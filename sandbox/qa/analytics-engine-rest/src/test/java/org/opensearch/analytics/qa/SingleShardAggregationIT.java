/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Single-shard aggregation correctness. On a 1-shard index the planner keeps the aggregate
 * as SINGLE (no PARTIAL/FINAL split). Verifies group-by results are identical across all
 * combinations of: partitions (1, N), TopK (off, on), scan type (vanilla, delegation).
 */
public class SingleShardAggregationIT extends AnalyticsRestTestCase {

    private static final Dataset CLICKBENCH_1SHARD = new Dataset("clickbench", "clickbench_1shard");
    private static volatile boolean provisioned = false;

    private static final String IDX = CLICKBENCH_1SHARD.indexName;

    // Vanilla DF scan — numeric filters only
    private static final String VANILLA_QUERY = "source=" + IDX
        + " | where GoodEvent = 1 AND Age > 0 | stats count() as cnt by OS | where cnt > 5";
    private static final String VANILLA_TOPK_QUERY = "source=" + IDX
        + " | where GoodEvent = 1 AND Age > 0 | stats count() as cnt by OS | sort - cnt, OS | head 5";

    // Delegation scan — keyword equality delegated to indexed table provider
    private static final String DELEGATION_QUERY = "source=" + IDX
        + " | where Title = 'Search Results' AND GoodEvent = 1 | stats count() as cnt by RegionID | where cnt > 1";
    private static final String DELEGATION_TOPK_QUERY = "source=" + IDX
        + " | where Title = 'Search Results' AND GoodEvent = 1 | stats count() as cnt by RegionID"
        + " | sort - cnt, RegionID | head 5";

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned == false) {
            DatasetProvisioner.provision(client(), CLICKBENCH_1SHARD, 1);
            provisioned = true;
        }
    }

    // ── slice=1, topk off ───────────────────────────────────────────────────────

    public void testSlice1_topkOff_vanilla() throws Exception {
        runCheck("none", 0, 0.0, VANILLA_QUERY);
    }

    public void testSlice1_topkOff_delegation() throws Exception {
        runCheck("none", 0, 0.0, DELEGATION_QUERY);
    }

    // ── slice=1, topk on ────────────────────────────────────────────────────────

    public void testSlice1_topkOn_vanilla() throws Exception {
        runCheck("none", 0, 2.0, VANILLA_TOPK_QUERY);
    }

    public void testSlice1_topkOn_delegation() throws Exception {
        runCheck("none", 0, 2.0, DELEGATION_TOPK_QUERY);
    }

    // ── slice=N, topk off ───────────────────────────────────────────────────────

    public void testSliceN_topkOff_vanilla() throws Exception {
        runCheck("all", 4, 0.0, VANILLA_QUERY);
    }

    public void testSliceN_topkOff_delegation() throws Exception {
        runCheck("all", 4, 0.0, DELEGATION_QUERY);
    }

    // ── slice=N, topk on ────────────────────────────────────────────────────────

    public void testSliceN_topkOn_vanilla() throws Exception {
        runCheck("all", 4, 2.0, VANILLA_TOPK_QUERY);
    }

    public void testSliceN_topkOn_delegation() throws Exception {
        runCheck("all", 4, 2.0, DELEGATION_TOPK_QUERY);
    }

    // ── Plan shape: no coordinator reduce ───────────────────────────────────────

    public void testPlanShape_noCoordinatorReduce_vanilla() throws Exception {
        assertNoCoordinatorReduce(VANILLA_QUERY);
    }

    public void testPlanShape_noCoordinatorReduce_delegation() throws Exception {
        assertNoCoordinatorReduce(DELEGATION_QUERY);
    }

    // ── Additional aggregate shapes ─────────────────────────────────────────────

    public void testSliceN_multiAgg_vanilla() throws Exception {
        runCheck("all", 4, 0.0, "source=" + IDX
            + " | where GoodEvent = 1 AND Income > 0"
            + " | stats avg(Age) as avg_age, sum(Income) as total, count() as cnt by OS"
            + " | where avg_age > 20 AND cnt > 5");
    }

    public void testSliceN_multiGroupKey_vanilla() throws Exception {
        runCheck("all", 4, 0.0, "source=" + IDX
            + " | where GoodEvent = 1"
            + " | stats count() as cnt, sum(Income) as total by OS, Age"
            + " | where cnt > 1");
    }

    // ── Core test logic ─────────────────────────────────────────────────────────

    private void runCheck(String mode, int sliceCount, double oversampling, String ppl) throws Exception {
        // Baseline: concurrent off, topk off
        setConcurrentSearchMode("none", 0);
        setOversampling(0.0);
        Map<String, Object> baseline = executePpl(ppl);

        // Test: apply settings
        setConcurrentSearchMode(mode, sliceCount);
        setOversampling(oversampling);
        Map<String, Object> test = executePpl(ppl);

        // Reset
        setConcurrentSearchMode("none", 0);
        setOversampling(0.0);

        String diff = compareResults(baseline, test, ppl);
        if (diff != null) {
            fail("Single-shard mismatch [mode=" + mode + ", slice=" + sliceCount
                + ", topk=" + (oversampling > 0) + "]: " + diff);
        }
    }

    @SuppressWarnings("unchecked")
    private void assertNoCoordinatorReduce(String ppl) throws IOException {
        setConcurrentSearchMode("all", 4);
        Request request = new Request("POST", "/_analytics/ppl/_explain");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Map<String, Object> result = assertOkAndParse(client().performRequest(request), "EXPLAIN: " + ppl);

        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("profile present", profile);
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");
        assertNotNull("stages present", stages);

        boolean hasReduce = stages.stream()
            .anyMatch(s -> "COORDINATOR_REDUCE".equals(s.get("execution_type")));
        assertFalse("Single-shard must not have COORDINATOR_REDUCE stage", hasReduce);

        setConcurrentSearchMode("none", 0);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private String compareResults(Map<String, Object> expected, Map<String, Object> actual, String label) {
        List<List<Object>> expectedRows = (List<List<Object>>) expected.get("datarows");
        List<List<Object>> actualRows = (List<List<Object>>) actual.get("datarows");

        if (expectedRows == null && actualRows == null) return null;
        if (expectedRows == null || actualRows == null) {
            return String.format(Locale.ROOT, "%s: one side null", label);
        }
        if (expectedRows.size() != actualRows.size()) {
            return String.format(Locale.ROOT, "%s: row count — baseline=%d, test=%d",
                label, expectedRows.size(), actualRows.size());
        }

        List<String> e = expectedRows.stream().map(Object::toString).sorted()
            .collect(java.util.stream.Collectors.toList());
        List<String> a = actualRows.stream().map(Object::toString).sorted()
            .collect(java.util.stream.Collectors.toList());

        for (int i = 0; i < e.size(); i++) {
            if (!e.get(i).equals(a.get(i))) {
                return String.format(Locale.ROOT, "%s: row %d — baseline=%s, test=%s",
                    label, i, e.get(i), a.get(i));
            }
        }
        return null;
    }

    private void setConcurrentSearchMode(String mode, int sliceCount) throws IOException {
        String body = sliceCount > 0
            ? "{\"transient\":{\"search.concurrent_segment_search.mode\":\"" + mode
                + "\",\"search.concurrent.max_slice_count\":" + sliceCount + "}}"
            : "{\"transient\":{\"search.concurrent_segment_search.mode\":\"" + mode + "\"}}";
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity(body);
        client().performRequest(req);
    }

    private void setOversampling(double factor) throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"transient\":{\"analytics.shard_bucket_oversampling_factor\":" + factor + "}}");
        client().performRequest(req);
    }
}
