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
 * Integration test verifying that LATE_MATERIALIZATION stages return {@code physical_plan}
 * and {@code data_node_metrics} when profiling is enabled.
 */
public class LateMaterializationProfileIT extends AnalyticsRestTestCase {

    private static final String INDEX = "lm_profile_test";
    private static boolean provisioned = false;

    private void ensureProvisioned() throws IOException {
        if (!provisioned) {
            createIndex();
            indexData();
            provisioned = true;
        }
    }

    /**
     * A sort + head + fields query where projected fields are NOT in the sort key triggers
     * late materialization.
     * With profile=true, the LM stage's tasks must include
     * {@code physical_plan} and {@code data_node_metrics}.
     */
    @SuppressWarnings("unchecked")
    public void testLateMaterializationProfileReturnsPhysicalPlan() throws IOException {
        ensureProvisioned();
        // sort by num0, but project str0 (not in sort) → triggers LM fetch phase.
        // head 20 requests every doc in the 20-doc index. Docs are indexed with auto-generated
        // IDs (default hash-based routing across the 2 shards), so fetching ALL of them makes
        // both-shards-participate virtually certain (P(all 20 land on one shard) ≈ 2 × 0.5^20
        // ≈ 0.0002%) — letting us assert an exact per-shard task count deterministically rather
        // than "at least one," which a smaller head N could satisfy from a single lucky shard.
        Map<String, Object> result = executeWithProfile(
            "source = " + INDEX + " | sort num0 | fields str0, num1 | head 20"
        );

        // Basic result sanity
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("rows present", rows);
        assertEquals("all 20 rows returned", 20, rows.size());

        // Profile must be present
        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("profile present", profile);

        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");
        assertNotNull("stages present", stages);

        // Find the LATE_MATERIALIZATION stage
        Map<String, Object> lmStage = null;
        for (Map<String, Object> stage : stages) {
            if ("LATE_MATERIALIZATION".equals(stage.get("execution_type"))) {
                lmStage = stage;
                break;
            }
        }
        assertNotNull("LATE_MATERIALIZATION stage must be present in profile", lmStage);
        assertEquals("SUCCEEDED", lmStage.get("state"));

        // LM stage must have exactly one task per shard — both of our 2 shards must have
        // contributed rows to the fetch, per the statistical argument above.
        List<Map<String, Object>> tasks = (List<Map<String, Object>>) lmStage.get("tasks");
        assertNotNull("LM stage must have tasks. Full LM stage: " + lmStage, tasks);
        assertEquals("LM stage should have one task per shard. Full LM stage: " + lmStage, 2, tasks.size());

        // The per-shard task labels must be distinct — proves addShardMetrics keyed each
        // shard separately rather than one overwriting another under the same key.
        java.util.Set<Object> nodeLabels = new java.util.HashSet<>();
        for (Map<String, Object> task : tasks) {
            nodeLabels.add(task.get("node"));
        }
        assertEquals("per-shard task labels must be distinct. Full LM stage: " + lmStage, 2, nodeLabels.size());

        // Each task must have data_node_metrics (from the fetch path)
        for (Map<String, Object> task : tasks) {
            assertNotNull("task state present", task.get("state"));
            assertEquals("FINISHED", task.get("state"));

            Map<String, Object> metrics = (Map<String, Object>) task.get("data_node_metrics");
            assertNotNull(
                "LM task must return data_node_metrics. Full LM stage: " + lmStage,
                metrics
            );

            // The metrics should contain at least some known Parquet/DataFusion fields
            assertTrue(
                "data_node_metrics should contain execution metrics, got: " + metrics.keySet(),
                metrics.containsKey("output_rows") || metrics.containsKey("elapsed_compute") || metrics.containsKey("bytes_scanned")
            );
        }
    }

    @SuppressWarnings("unchecked")
    public void testLateMaterializationProfileReturnsPhysicalPlanText() throws IOException {
        ensureProvisioned();
        Map<String, Object> result = executeWithProfile(
            "source = " + INDEX + " | sort num0 | fields str0, num1 | head 20"
        );

        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");

        // Find LM stage task with physical_plan
        for (Map<String, Object> stage : stages) {
            if ("LATE_MATERIALIZATION".equals(stage.get("execution_type"))) {
                List<Map<String, Object>> tasks = (List<Map<String, Object>>) stage.get("tasks");
                // head 20 (all docs) forces both of our 2 shards to contribute — see the
                // statistical argument in testLateMaterializationProfileReturnsPhysicalPlan.
                assertEquals("LM stage should have one task per shard. Full LM stage: " + stage, 2, tasks.size());

                // Each per-shard task must have a physical_plan referencing Parquet
                for (Map<String, Object> task : tasks) {
                    String physicalPlan = (String) task.get("physical_plan");
                    assertNotNull("LM task must return physical_plan string. Full LM stage: " + stage, physicalPlan);
                    assertTrue(
                        "physical_plan should reference Parquet scan, got: " + physicalPlan,
                        physicalPlan.contains("parquet") || physicalPlan.contains("Parquet")
                    );
                }
                return;
            }
        }
        fail("No LATE_MATERIALIZATION stage found in profile");
    }

    /**
     * Exercises the OFF branch of the code we changed in
     * {@code AnalyticsSearchService#drainFetchByRowIds}: when {@code profile} is omitted (or
     * false), the LM fetch path must NOT collect data_node_metrics, and the response must not
     * include a {@code "profile"} object at all — only that the query still returns correct
     * results through the same LM code path.
     */
    @SuppressWarnings("unchecked")
    public void testLateMaterializationWithoutProfileOmitsProfileData() throws IOException {
        ensureProvisioned();
        Map<String, Object> result = executeWithoutProfile(
            "source = " + INDEX + " | sort num0 | fields str0, num1 | head 20"
        );

        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("rows present", rows);
        assertEquals("all 20 rows returned", 20, rows.size());

        assertNull("profile must be absent when profiling is not requested", result.get("profile"));
    }

    /**
     * Opposite extreme from {@link #testLateMaterializationProfileReturnsPhysicalPlan}'s
     * head-20 case: a filter matching exactly one row guarantees at most one shard
     * participates in the fetch, deterministically exercising the single-entry branch of
     * the per-shard {@code shardMetrics} map (as opposed to the two-entry case above).
     * num0 = i * 1.5 for i in [0, 20), so num0 = 0 matches only i = 0.
     */
    @SuppressWarnings("unchecked")
    public void testLateMaterializationSingleShardProfile() throws IOException {
        ensureProvisioned();
        Map<String, Object> result = executeWithProfile(
            "source = " + INDEX + " | where num0 = 0 | sort num0 | fields str0, num1 | head 20"
        );

        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("rows present", rows);
        assertEquals("exactly 1 row matches the filter", 1, rows.size());

        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("profile present", profile);
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");

        Map<String, Object> lmStage = null;
        for (Map<String, Object> stage : stages) {
            if ("LATE_MATERIALIZATION".equals(stage.get("execution_type"))) {
                lmStage = stage;
                break;
            }
        }
        assertNotNull("LATE_MATERIALIZATION stage must be present in profile", lmStage);
        assertEquals("SUCCEEDED", lmStage.get("state"));

        List<Map<String, Object>> tasks = (List<Map<String, Object>>) lmStage.get("tasks");
        assertNotNull("LM stage must have tasks. Full LM stage: " + lmStage, tasks);
        assertEquals("exactly 1 shard should participate when only 1 row matches. Full LM stage: " + lmStage, 1, tasks.size());

        Map<String, Object> task = tasks.get(0);
        assertEquals("FINISHED", task.get("state"));

        Map<String, Object> metrics = (Map<String, Object>) task.get("data_node_metrics");
        assertNotNull("LM task must return data_node_metrics. Full LM stage: " + lmStage, metrics);

        String physicalPlan = (String) task.get("physical_plan");
        assertNotNull("LM task must return physical_plan string. Full LM stage: " + lmStage, physicalPlan);
        assertTrue(
            "physical_plan should reference Parquet scan, got: " + physicalPlan,
            physicalPlan.contains("parquet") || physicalPlan.contains("Parquet")
        );
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────────

    private void createIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 2,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"num0\": { \"type\": \"double\" },"
            + "    \"num1\": { \"type\": \"double\" },"
            + "    \"str0\": { \"type\": \"keyword\" }"
            + "  }"
            + "}"
            + "}";

        Request req = new Request("PUT", "/" + INDEX);
        req.setJsonEntity(body);
        client().performRequest(req);
    }

    private void indexData() throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"num0\":").append(i * 1.5).append(",\"num1\":").append(i * 2.0)
                .append(",\"str0\":\"val_").append(i).append("\"}\n");
        }
        Request req = new Request("POST", "/" + INDEX + "/_bulk");
        req.addParameter("refresh", "true");
        req.setJsonEntity(bulk.toString());
        Response resp = client().performRequest(req);
        assertEquals(200, resp.getStatusLine().getStatusCode());
    }

    private Map<String, Object> executeWithProfile(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\", \"profile\": true}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PROFILE: " + ppl);
    }

    /** Same endpoint as {@link #executeWithProfile}, but omits the profile flag entirely —
     *  matches how a normal (non-profiled) query is actually sent. */
    private Map<String, Object> executeWithoutProfile(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "NO-PROFILE: " + ppl);
    }
}
