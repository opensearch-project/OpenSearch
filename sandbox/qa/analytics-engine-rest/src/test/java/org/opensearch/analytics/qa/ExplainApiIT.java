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
 * Verifies that the explain endpoint executes the query and returns
 * profiling information (stage timings, plan) alongside the normal results.
 */
public class ExplainApiIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");
    private static final Dataset CLICKBENCH = ClickBenchTestHelper.DATASET;
    private static boolean dataProvisioned = false;
    private static boolean clickBenchProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
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

    @SuppressWarnings("unchecked")
    public void testExplainReturnsProfileWithStages() throws IOException {
        ensureDataProvisioned();
        Map<String, Object> result = executeExplain("source=" + DATASET.indexName + " | fields str0, num0");

        // Should have normal query results
        assertNotNull("columns present", result.get("columns"));
        assertNotNull("rows present", result.get("rows"));

        // Should have profile section
        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("profile present", profile);
        assertNotNull("query_id present", profile.get("query_id"));
        assertNotNull("total_elapsed_ms present", profile.get("total_elapsed_ms"));

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
        ensureDataProvisioned();
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
        ensureDataProvisioned();
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
        ensureDataProvisioned();
        Map<String, Object> result = executeExplain("source=" + DATASET.indexName + " | fields str0");

        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        long totalElapsed = ((Number) profile.get("total_elapsed_ms")).longValue();
        assertTrue("total_elapsed_ms is positive", totalElapsed > 0);
    }

    private Map<String, Object> executeExplain(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl/_explain");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "EXPLAIN: " + ppl);
    }
}
