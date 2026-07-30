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
import java.util.List;
import java.util.Map;

/**
 * Verifies shard DF physical plans use only {@code mode=Partial} for aggregate stages.
 * Tests all combinations of slice count (1, N), TopK (on, off), and scan type
 * (vanilla, delegation with filter).
 */
public class PartialAggregateModeIT extends AnalyticsRestTestCase {

    private static volatile boolean provisioned = false;
    private static final Dataset CLICKBENCH = ClickBenchTestHelper.DATASET;
    private static final String VANILLA_QUERY = "source=" + CLICKBENCH.indexName + " | stats count() as c by RegionID | sort - c | head 10";
    private static final String DELEGATION_QUERY = "source=" + CLICKBENCH.indexName
        + " | where URL = 'https://opensearch.org' | stats count() as c by Title | sort - c | head 10";

    private void ensureProvisioned() throws Exception {
        if (provisioned == false) {
            DatasetProvisioner.provision(client(), CLICKBENCH, 2);
            provisioned = true;
        }
    }

    public void testShardPlan_slice1_topkOff_vanilla() throws Exception {
        runShardPlanCheck("none", 0, 0.0, VANILLA_QUERY);
    }

    public void testShardPlan_slice1_topkOn_vanilla() throws Exception {
        runShardPlanCheck("none", 0, 2.0, VANILLA_QUERY);
    }

    public void testShardPlan_sliceN_topkOff_vanilla() throws Exception {
        runShardPlanCheck("all", 4, 0.0, VANILLA_QUERY);
    }

    public void testShardPlan_sliceN_topkOn_vanilla() throws Exception {
        runShardPlanCheck("all", 4, 2.0, VANILLA_QUERY);
    }

    public void testShardPlan_slice1_topkOff_delegation() throws Exception {
        runShardPlanCheck("none", 0, 0.0, DELEGATION_QUERY);
    }

    public void testShardPlan_slice1_topkOn_delegation() throws Exception {
        runShardPlanCheck("none", 0, 2.0, DELEGATION_QUERY);
    }

    public void testShardPlan_sliceN_topkOff_delegation() throws Exception {
        runShardPlanCheck("all", 4, 0.0, DELEGATION_QUERY);
    }

    public void testShardPlan_sliceN_topkOn_delegation() throws Exception {
        runShardPlanCheck("all", 4, 2.0, DELEGATION_QUERY);
    }

    @SuppressWarnings("unchecked")
    private void runShardPlanCheck(String searchMode, int sliceCount, double oversampling, String ppl) throws Exception {
        ensureProvisioned();
        setConcurrentSearchMode(searchMode, sliceCount);
        setOversampling(oversampling);
        assertShardPlanPartialOnly(ppl);
    }

    @SuppressWarnings("unchecked")
    private void assertShardPlanPartialOnly(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl/_explain");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Map<String, Object> result = assertOkAndParse(client().performRequest(request), "EXPLAIN: " + ppl);
        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("profile present", profile);
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");

        Map<String, Object> shardStage = stages.stream()
            .filter(s -> "SHARD_FRAGMENT".equals(s.get("execution_type")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no SHARD_FRAGMENT stage"));

        List<Map<String, Object>> tasks = (List<Map<String, Object>>) shardStage.get("tasks");
        assertNotNull("shard tasks present", tasks);

        for (Map<String, Object> task : tasks) {
            Object plan = task.get("physical_plan");
            if (plan instanceof String s && s.isEmpty() == false) {
                assertFalse(
                    "shard plan must NOT contain mode=Final. Plan:\n" + s,
                    s.contains("mode=Final,") || s.contains("mode=Final ")
                );
                assertFalse(
                    "shard plan must NOT contain FinalPartitioned. Plan:\n" + s,
                    s.contains("FinalPartitioned")
                );
                assertTrue(
                    "shard plan must contain mode=Partial. Plan:\n" + s,
                    s.contains("mode=Partial")
                );
            }
        }

        stages.stream()
            .filter(s -> "COORDINATOR_REDUCE".equals(s.get("execution_type")))
            .findFirst()
            .ifPresent(reduceStage -> {
                String reducePlan = (String) reduceStage.get("physical_plan");
                if (reducePlan != null && reducePlan.isEmpty() == false) {
                    assertTrue(
                        "coordinator plan must contain Final or FinalPartitioned. Plan:\n" + reducePlan,
                        reducePlan.contains("mode=Final") || reducePlan.contains("FinalPartitioned")
                    );
                }
            });
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

    /** Regression guard: TopK with high-cardinality group keys must produce valid counts. */
    @SuppressWarnings("unchecked")
    public void testTopK_highCardinalityGroupBy_correctness() throws Exception {
        ensureProvisioned();
        setConcurrentSearchMode("all", 4);
        setOversampling(2.0);

        String idx = CLICKBENCH.indexName;

        Map<String, Object> total = executePpl("source=" + idx + " | stats count()");
        long totalCount = ((Number) ((List<List<Object>>) total.get("datarows")).get(0).get(0)).longValue();

        Map<String, Object> topk = executePpl(
            "source=" + idx + " | stats count() as cnt by RegionID | sort - cnt | head 5"
        );
        List<List<Object>> rows = (List<List<Object>>) topk.get("datarows");
        assertNotNull(rows);
        assertFalse("TopK must return rows", rows.isEmpty());

        long sumTopK = rows.stream().mapToLong(r -> ((Number) r.get(0)).longValue()).sum();
        assertTrue(
            "Sum of top-K counts (" + sumTopK + ") must not exceed total (" + totalCount + ")",
            sumTopK <= totalCount
        );

        for (List<Object> row : rows) {
            long cnt = ((Number) row.get(0)).longValue();
            assertTrue("Each group count must be > 0", cnt > 0);
        }

        setConcurrentSearchMode("none", 0);
        setOversampling(0.0);
    }
}
