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
 * and {@code data_node_metrics} when profiling is enabled. Addresses
 * <a href="https://github.com/opensearch-project/OpenSearch/issues/22601">#22601</a>.
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
     * late materialization. With profile=true, the LM stage's tasks must include
     * {@code physical_plan} and {@code data_node_metrics}.
     */
    @SuppressWarnings("unchecked")
    public void testLateMaterializationProfileReturnsPhysicalPlan() throws IOException {
        ensureProvisioned();
        // sort by num0, but project str0 (not in sort) → triggers LM fetch phase.
        // Must have multi-shard index (2+) for LM rewriter to fire.
        Map<String, Object> result = executeWithProfile(
            "source = " + INDEX + " | sort num0 | fields str0, num1 | head 5"
        );

        // Basic result sanity
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("rows present", rows);
        assertTrue("at least 1 row returned", rows.size() >= 1);

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

        // LM stage must have tasks
        List<Map<String, Object>> tasks = (List<Map<String, Object>>) lmStage.get("tasks");
        assertNotNull("LM stage must have tasks", tasks);
        assertFalse("LM stage must have at least one task", tasks.isEmpty());

        // The task must have data_node_metrics (from the fetch path)
        Map<String, Object> task = tasks.get(0);
        assertNotNull("task state present", task.get("state"));
        assertEquals("FINISHED", task.get("state"));

        Map<String, Object> metrics = (Map<String, Object>) task.get("data_node_metrics");
        assertNotNull(
            "LM task must return data_node_metrics (physical_plan + execution metrics from fetch path)",
            metrics
        );

        // The metrics should contain at least some known Parquet/DataFusion fields
        // (bytes_scanned, elapsed_compute, output_rows are standard DataFusion metrics)
        assertTrue(
            "data_node_metrics should contain execution metrics, got: " + metrics.keySet(),
            metrics.containsKey("output_rows") || metrics.containsKey("elapsed_compute") || metrics.containsKey("bytes_scanned")
        );
    }

    @SuppressWarnings("unchecked")
    public void testLateMaterializationProfileReturnsPhysicalPlanText() throws IOException {
        ensureProvisioned();
        Map<String, Object> result = executeWithProfile(
            "source = " + INDEX + " | sort num0 | fields str0, num1 | head 5"
        );

        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");

        // Find LM stage task with physical_plan
        for (Map<String, Object> stage : stages) {
            if ("LATE_MATERIALIZATION".equals(stage.get("execution_type"))) {
                List<Map<String, Object>> tasks = (List<Map<String, Object>>) stage.get("tasks");
                assertFalse("LM must have tasks", tasks.isEmpty());
                Map<String, Object> task = tasks.get(0);

                String physicalPlan = (String) task.get("physical_plan");
                assertNotNull("LM task must return physical_plan string", physicalPlan);
                // The fetch plan should contain a DataSourceExec with parquet file type
                assertTrue(
                    "physical_plan should reference Parquet scan, got: " + physicalPlan,
                    physicalPlan.contains("parquet") || physicalPlan.contains("Parquet")
                );
                return;
            }
        }
        fail("No LATE_MATERIALIZATION stage found in profile");
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
}
