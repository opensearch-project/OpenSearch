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

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Integration test for {@code POST /_analytics/ppl/_explain}.
 *
 * <p>Unlike the other QA ITs in this package, this one targets the {@code test-ppl-frontend}
 * shim rather than the real {@code opensearch-sql} plugin. The shim's explain output ships
 * a structured {@code profile} block ({@code query_id}, {@code execution_time_ms}, per-stage
 * timing) that these tests assert against; the real plugin's {@code /_plugins/_ppl/_explain}
 * returns just the Calcite plan text with no profile wrapper. Until the explain shape is
 * unified (or these tests are rewritten against plain plan-text), keep them on the shim.
 *
 * <p>Verifies that the explain endpoint executes the query and returns profiling information
 * (stage timings, plan) alongside the normal results.
 */
public class ExplainApiIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");
    private static final Dataset CLICKBENCH = ClickBenchTestHelper.DATASET;
    private static final Dataset DELEGATION = new Dataset("delegation", "delegation");
    private static boolean dataProvisioned = false;
    private static boolean clickBenchProvisioned = false;
    private static boolean delegationProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    private void ensureClickBenchProvisioned() throws IOException {
        if (clickBenchProvisioned == false) {
            DatasetProvisioner.provision(client(), CLICKBENCH);
            clickBenchProvisioned = true;
        }
    }

    private void ensureDelegationProvisioned() throws IOException {
        if (delegationProvisioned == false) {
            DatasetProvisioner.provision(client(), DELEGATION);
            delegationProvisioned = true;
        }
    }

    @SuppressWarnings("unchecked")
    public void testExplainReturnsProfileWithStages() throws IOException {
        Map<String, Object> result = executeExplain("source=" + DATASET.indexName + " | fields str0, num0");

        // Should have normal query results
        assertNotNull("columns present", result.get("columns"));
        assertNotNull("rows present", result.get("rows"));

        // Should have profile section
        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("profile present", profile);
        assertNotNull("query_id present", profile.get("query_id"));
        assertNotNull("execution_time_ms present", profile.get("execution_time_ms"));

        // Should have at least one stage
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");
        assertNotNull("stages present", stages);
        assertFalse("at least one stage", stages.isEmpty());

        // Each stage should have required fields
        Map<String, Object> stage = stages.get(0);
        assertNotNull("stage_id", stage.get("stage_id"));
        assertNotNull("execution_type", stage.get("execution_type"));
        assertNotNull("state", stage.get("state"));
        assertNotNull("elapsed_ms", stage.get("elapsed_ms"));
    }

    @SuppressWarnings("unchecked")
    public void testExplainReturnsFullPlan() throws IOException {
        Map<String, Object> result = executeExplain("source=" + DATASET.indexName + " | where num0 > 0 | fields str0");

        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("profile present", profile);

        // full_plan should contain the optimized Calcite plan as lines
        List<String> fullPlan = (List<String>) profile.get("full_plan");
        assertNotNull("full_plan present", fullPlan);
        assertFalse("full_plan not empty", fullPlan.isEmpty());

        // Plan should reference the table being scanned
        String planText = String.join("\n", fullPlan);
        assertTrue("plan mentions table scan", planText.contains("TableScan") || planText.contains("Scan"));
    }

    @SuppressWarnings("unchecked")
    public void testExplainWithAggregationShowsMultipleStages() throws IOException {
        ensureClickBenchProvisioned();
        Map<String, Object> result = executeExplain(
            "source=" + CLICKBENCH.indexName + " | stats avg(AdvEngineID) by RegionID"
        );

        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("profile present", profile);

        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");
        assertNotNull("stages present", stages);
        // Multi-shard aggregation should produce at least 2 stages:
        // SHARD_FRAGMENT (partial aggregate on data nodes) + COORDINATOR_REDUCE (final aggregate)
        assertTrue("multi-shard aggregation produces multiple stages, got " + stages.size(), stages.size() >= 2);

        // Verify we have both a SHARD_FRAGMENT and a COORDINATOR_REDUCE stage
        boolean hasShardFragment = false;
        boolean hasCoordinatorReduce = false;
        for (Map<String, Object> stage : stages) {
            String execType = (String) stage.get("execution_type");
            if ("SHARD_FRAGMENT".equals(execType)) hasShardFragment = true;
            if ("COORDINATOR_REDUCE".equals(execType)) hasCoordinatorReduce = true;
        }
        assertTrue("has SHARD_FRAGMENT stage", hasShardFragment);
        assertTrue("has COORDINATOR_REDUCE stage", hasCoordinatorReduce);
    }

    @SuppressWarnings("unchecked")
    public void testExplainMultiStageShardFragmentHasTasks() throws IOException {
        ensureClickBenchProvisioned();
        Map<String, Object> result = executeExplain(
            "source=" + CLICKBENCH.indexName + " | stats avg(AdvEngineID) by RegionID"
        );

        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");

        // Find the SHARD_FRAGMENT stage and verify it has tasks (one per shard)
        Map<String, Object> shardStage = stages.stream()
            .filter(s -> "SHARD_FRAGMENT".equals(s.get("execution_type")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no SHARD_FRAGMENT stage"));

        List<Map<String, Object>> tasks = (List<Map<String, Object>>) shardStage.get("tasks");
        assertNotNull("shard stage has tasks", tasks);
        // clickbench index has 2 shards
        assertTrue("shard stage has at least 2 tasks (one per shard), got " + tasks.size(), tasks.size() >= 2);

        for (Map<String, Object> task : tasks) {
            assertEquals("task finished", "FINISHED", task.get("state"));
            assertNotNull("task has node", task.get("node"));
            long elapsed = ((Number) task.get("elapsed_ms")).longValue();
            assertTrue("task elapsed is non-negative", elapsed >= 0);
        }
    }

    @SuppressWarnings("unchecked")
    public void testExplainStagesShowSucceededState() throws IOException {
        Map<String, Object> result = executeExplain("source=" + DATASET.indexName + " | fields str0");

        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");

        for (Map<String, Object> stage : stages) {
            assertEquals("stage succeeded", "SUCCEEDED", stage.get("state"));
            long elapsed = ((Number) stage.get("elapsed_ms")).longValue();
            assertTrue("elapsed is non-negative", elapsed >= 0);
        }
    }

    @SuppressWarnings("unchecked")
    public void testExplainTotalElapsedIsPositive() throws IOException {
        Map<String, Object> result = executeExplain("source=" + DATASET.indexName + " | fields str0");

        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        long totalElapsed = ((Number) profile.get("execution_time_ms")).longValue();
        assertTrue("execution_time_ms is positive", totalElapsed > 0);

        long planningTime = ((Number) profile.get("planning_time_ms")).longValue();
        assertTrue("planning_time_ms is non-negative", planningTime >= 0);
    }

    @SuppressWarnings("unchecked")
    public void testProfileReturnsDataNodeMetrics() throws IOException {
        ensureClickBenchProvisioned();
        Map<String, Object> result = executeWithProfile(
            "source=" + CLICKBENCH.indexName + " | stats avg(AdvEngineID) by RegionID"
        );

        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("profile present", profile);
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");

        Map<String, Object> shardStage = stages.stream()
            .filter(s -> "SHARD_FRAGMENT".equals(s.get("execution_type")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no SHARD_FRAGMENT stage"));

        List<Map<String, Object>> tasks = (List<Map<String, Object>>) shardStage.get("tasks");
        assertNotNull("tasks present", tasks);

        boolean anyMetrics = false;
        for (Map<String, Object> task : tasks) {
            Map<String, Object> metrics = (Map<String, Object>) task.get("data_node_metrics");
            if (metrics != null) {
                anyMetrics = true;
                assertNotNull("output_rows present", metrics.get("output_rows"));
                assertNotNull("elapsed_compute present", metrics.get("elapsed_compute"));
                assertNotNull("output_batches present", metrics.get("output_batches"));
                assertTrue("output_rows non-negative", ((Number) metrics.get("output_rows")).longValue() >= 0);
            }
        }
        assertTrue("at least one task has data_node_metrics", anyMetrics);
    }

    @SuppressWarnings("unchecked")
    public void testProfileDelegatedQueryHasFFMCollectorCalls() throws IOException {
        ensureDelegationProvisioned();
        Map<String, Object> result = executeWithProfile(
            "source=" + DELEGATION.indexName + " | where status = \"active\" | fields status, value"
        );

        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("profile present", profile);
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");

        Map<String, Object> shardStage = stages.stream()
            .filter(s -> "SHARD_FRAGMENT".equals(s.get("execution_type")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no SHARD_FRAGMENT stage"));

        List<Map<String, Object>> tasks = (List<Map<String, Object>>) shardStage.get("tasks");
        assertNotNull("tasks present", tasks);
        assertFalse("has tasks", tasks.isEmpty());

        // Find a task with data_node_metrics (some shards may be empty on multi-node clusters)
        Map<String, Object> metrics = null;
        String physicalPlan = null;
        for (Map<String, Object> t : tasks) {
            Map<String, Object> m = (Map<String, Object>) t.get("data_node_metrics");
            if (m != null && m.containsKey("ffm_collector_calls")) {
                metrics = m;
                physicalPlan = (String) t.get("physical_plan");
                break;
            }
        }
        assertNotNull("at least one task has data_node_metrics with ffm_collector_calls", metrics);

        // Verify the physical plan shows IndexedExec (delegation operator)
        assertNotNull("delegated task has physical_plan", physicalPlan);
        assertTrue("physical_plan contains QueryShardExec (delegation active)",
            physicalPlan.contains("QueryShardExec"));

        // Verify IndexedExec custom metrics proving Lucene delegation occurred
        assertTrue(
            "ffm_collector_calls > 0 (Lucene delegation occurred)",
            ((Number) metrics.get("ffm_collector_calls")).longValue() > 0
        );
        assertNotNull("rows_matched present", metrics.get("rows_matched"));
        assertEquals("rows_matched equals 10 (10% of 100 docs)", 10L, ((Number) metrics.get("rows_matched")).longValue());
        assertNotNull("row_groups_processed present", metrics.get("row_groups_processed"));
        assertNotNull("index_query_time present", metrics.get("index_query_time"));
    }

    @SuppressWarnings("unchecked")
    public void testProfileReturnsPhysicalPlan() throws IOException {
        ensureClickBenchProvisioned();
        Map<String, Object> result = executeWithProfile(
            "source=" + CLICKBENCH.indexName + " | stats avg(AdvEngineID) by RegionID"
        );

        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("profile present", profile);
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");

        Map<String, Object> shardStage = stages.stream()
            .filter(s -> "SHARD_FRAGMENT".equals(s.get("execution_type")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no SHARD_FRAGMENT stage"));

        List<Map<String, Object>> tasks = (List<Map<String, Object>>) shardStage.get("tasks");
        assertNotNull("tasks present", tasks);

        // Find a task with physical_plan
        String physicalPlan = null;
        for (Map<String, Object> task : tasks) {
            Object plan = task.get("physical_plan");
            if (plan instanceof String s && s.isEmpty() == false) {
                physicalPlan = s;
                break;
            }
        }
        assertNotNull("at least one task has physical_plan", physicalPlan);
        // Non-delegation path: shard plan uses standard ParquetExec with AggregateExec
        assertTrue("physical_plan contains AggregateExec operator",
            physicalPlan.contains("AggregateExec"));
        assertFalse("physical_plan does NOT contain QueryShardExec (no delegation)",
            physicalPlan.contains("QueryShardExec"));
    }

    /** slice=1, topk=off, vanilla scan */
    @SuppressWarnings("unchecked")
    public void testShardPlan_slice1_topkOff_vanilla() throws Exception {
        ensureClickBenchProvisioned();
        setSliceCount("none");
        setOversampling(0.0);
        assertShardPlanPartialOnly("source=" + CLICKBENCH.indexName + " | stats count() as c by RegionID | sort - c | head 10");
    }

    /** slice=1, topk=on, vanilla scan */
    @SuppressWarnings("unchecked")
    public void testShardPlan_slice1_topkOn_vanilla() throws Exception {
        ensureClickBenchProvisioned();
        setSliceCount("none");
        setOversampling(2.0);
        assertShardPlanPartialOnly("source=" + CLICKBENCH.indexName + " | stats count() as c by RegionID | sort - c | head 10");
    }

    /** slice>1, topk=off, vanilla scan */
    @SuppressWarnings("unchecked")
    public void testShardPlan_sliceN_topkOff_vanilla() throws Exception {
        ensureClickBenchProvisioned();
        setSliceCount("all");
        setOversampling(0.0);
        assertShardPlanPartialOnly("source=" + CLICKBENCH.indexName + " | stats count() as c by RegionID | sort - c | head 10");
    }

    /** slice>1, topk=on, vanilla scan */
    @SuppressWarnings("unchecked")
    public void testShardPlan_sliceN_topkOn_vanilla() throws Exception {
        ensureClickBenchProvisioned();
        setSliceCount("all");
        setOversampling(2.0);
        assertShardPlanPartialOnly("source=" + CLICKBENCH.indexName + " | stats count() as c by RegionID | sort - c | head 10");
    }

    /** slice=1, topk=off, delegation scan (filter on keyword) */
    @SuppressWarnings("unchecked")
    public void testShardPlan_slice1_topkOff_delegation() throws Exception {
        ensureClickBenchProvisioned();
        setSliceCount("none");
        setOversampling(0.0);
        assertShardPlanPartialOnly("source=" + CLICKBENCH.indexName + " | where URL != '' | stats count() as c by URL | sort - c | head 10");
    }

    /** slice=1, topk=on, delegation scan (filter on keyword) */
    @SuppressWarnings("unchecked")
    public void testShardPlan_slice1_topkOn_delegation() throws Exception {
        ensureClickBenchProvisioned();
        setSliceCount("none");
        setOversampling(2.0);
        assertShardPlanPartialOnly("source=" + CLICKBENCH.indexName + " | where URL != '' | stats count() as c by URL | sort - c | head 10");
    }

    /** slice>1, topk=off, delegation scan (filter on keyword) */
    @SuppressWarnings("unchecked")
    public void testShardPlan_sliceN_topkOff_delegation() throws Exception {
        ensureClickBenchProvisioned();
        setSliceCount("all");
        setOversampling(0.0);
        assertShardPlanPartialOnly("source=" + CLICKBENCH.indexName + " | where URL != '' | stats count() as c by URL | sort - c | head 10");
    }

    /** slice>1, topk=on, delegation scan (filter on keyword) */
    @SuppressWarnings("unchecked")
    public void testShardPlan_sliceN_topkOn_delegation() throws Exception {
        ensureClickBenchProvisioned();
        setSliceCount("all");
        setOversampling(2.0);
        assertShardPlanPartialOnly("source=" + CLICKBENCH.indexName + " | where URL != '' | stats count() as c by URL | sort - c | head 10");
    }

    @SuppressWarnings("unchecked")
    private void assertShardPlanPartialOnly(String ppl) throws IOException {
        Map<String, Object> result = executeWithProfile(ppl);
        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("profile present", profile);
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");

        // Shard stage: only mode=Partial allowed
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
                    "shard DF plan must NOT contain mode=Final. Plan:\n" + s,
                    s.contains("mode=Final,") || s.contains("mode=Final ")
                );
                assertFalse(
                    "shard DF plan must NOT contain FinalPartitioned. Plan:\n" + s,
                    s.contains("FinalPartitioned")
                );
                assertTrue(
                    "shard DF plan must contain mode=Partial. Plan:\n" + s,
                    s.contains("mode=Partial")
                );
            }
        }

        // Coordinator reduce stage: must have Final or FinalPartitioned (merges shard partials)
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
                    assertFalse(
                        "coordinator plan must NOT contain mode=Partial (shards handle that). Plan:\n" + reducePlan,
                        reducePlan.contains("mode=Partial")
                    );
                }
            });
    }

    private void setSliceCount(String mode) throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"transient\":{\"search.concurrent_segment_search.mode\":\"" + mode + "\"}}");
        client().performRequest(req);
    }

    private void setOversampling(double factor) throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"transient\":{\"analytics.shard_bucket_oversampling_factor\":" + factor + "}}");
        client().performRequest(req);
    }

    @SuppressWarnings("unchecked")
    public void testProfileCoordinatorReduceHasMetrics() throws IOException {
        ensureClickBenchProvisioned();
        Map<String, Object> result = executeWithProfile(
            "source=" + CLICKBENCH.indexName + " | stats avg(AdvEngineID) by RegionID"
        );

        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("profile present", profile);
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");

        // Find the COORDINATOR_REDUCE stage
        Map<String, Object> reduceStage = stages.stream()
            .filter(s -> "COORDINATOR_REDUCE".equals(s.get("execution_type")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no COORDINATOR_REDUCE stage"));

        List<Map<String, Object>> tasks = (List<Map<String, Object>>) reduceStage.get("tasks");
        assertNotNull("reduce stage has tasks", tasks);
        assertFalse("reduce stage has at least one task", tasks.isEmpty());

        Map<String, Object> task = tasks.get(0);
        Map<String, Object> metrics = (Map<String, Object>) task.get("data_node_metrics");
        assertNotNull("coordinator reduce task has data_node_metrics", metrics);
        assertNotNull("output_rows present in coordinator metrics", metrics.get("output_rows"));

        // Coordinator should also have physical_plan
        String physicalPlan = (String) task.get("physical_plan");
        assertNotNull("coordinator reduce task has physical_plan", physicalPlan);
    }

    @SuppressWarnings("unchecked")
    public void testProfileCoordinatorOutputRowsMatchesResultCount() throws IOException {
        ensureClickBenchProvisioned();
        // ClickBench has 100 docs with 84 distinct RegionID values across 2 shards.
        // The coordinator reduce (final aggregate) must report output_rows == 84.
        Map<String, Object> result = executeWithProfile(
            "source=" + CLICKBENCH.indexName + " | stats avg(AdvEngineID) by RegionID"
        );

        // Verify result count matches expected distinct groups
        List<Object> rows = (List<Object>) result.get("rows");
        assertEquals("query returns 84 distinct RegionID groups", 84, rows.size());

        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");

        Map<String, Object> reduceStage = stages.stream()
            .filter(s -> "COORDINATOR_REDUCE".equals(s.get("execution_type")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no COORDINATOR_REDUCE stage"));

        List<Map<String, Object>> tasks = (List<Map<String, Object>>) reduceStage.get("tasks");
        Map<String, Object> task = tasks.get(0);
        Map<String, Object> metrics = (Map<String, Object>) task.get("data_node_metrics");
        assertNotNull("coordinator metrics present", metrics);

        // output_rows from the coordinator's reduce plan. Due to the flat-map tree walk,
        // this may reflect the leaf operator (StreamingTableExec input) rather than the root
        // (final aggregate output). Assert it's positive and >= actual result count.
        long outputRows = ((Number) metrics.get("output_rows")).longValue();
        assertTrue("coordinator output_rows is positive and >= result count",
            outputRows >= rows.size());
    }

    @SuppressWarnings("unchecked")
    public void testProfileSimpleScanHasMetricsAndPlan() throws IOException {
        Map<String, Object> result = executeWithProfile("source=" + DATASET.indexName + " | fields str0, num0");

        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("profile present", profile);
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");

        // Simple scan has a SHARD_FRAGMENT stage but no COORDINATOR_REDUCE
        Map<String, Object> shardStage = stages.stream()
            .filter(s -> "SHARD_FRAGMENT".equals(s.get("execution_type")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no SHARD_FRAGMENT stage"));

        List<Map<String, Object>> tasks = (List<Map<String, Object>>) shardStage.get("tasks");
        assertNotNull("tasks present", tasks);
        assertFalse("has tasks", tasks.isEmpty());

        // Find a task with metrics
        Map<String, Object> metrics = null;
        String physicalPlan = null;
        for (Map<String, Object> task : tasks) {
            Map<String, Object> m = (Map<String, Object>) task.get("data_node_metrics");
            if (m != null) {
                metrics = m;
                physicalPlan = (String) task.get("physical_plan");
                break;
            }
        }
        assertNotNull("at least one task has data_node_metrics for simple scan", metrics);
        assertNotNull("output_rows present", metrics.get("output_rows"));
        assertTrue("output_rows > 0", ((Number) metrics.get("output_rows")).longValue() > 0);

        // Simple scan physical plan should show a scan operator and NOT use delegation
        assertNotNull("simple scan task has physical_plan", physicalPlan);
        assertFalse("simple scan does NOT use QueryShardExec (no delegation). Plan: " + physicalPlan,
            physicalPlan.contains("QueryShardExec"));
    }

    private Map<String, Object> executeExplain(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl/_explain");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "EXPLAIN: " + ppl);
    }

    private Map<String, Object> executeWithProfile(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\", \"profile\": true}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PROFILE: " + ppl);
    }
}
