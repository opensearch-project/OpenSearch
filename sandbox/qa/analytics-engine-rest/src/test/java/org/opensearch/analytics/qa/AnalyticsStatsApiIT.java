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
import java.util.Map;

/**
 * Integration test for {@code GET /_plugins/_analytics/stats}. Fires a few
 * PPL queries via {@code POST /_analytics/ppl} and verifies the stats endpoint
 * returns a cluster-wide response with one entry per node, each carrying the
 * full analytics rollup.
 *
 * <p>The endpoint mirrors {@code _nodes/stats}'s shape:
 * <pre>
 * {
 *   "_nodes": {...},
 *   "cluster_name": "...",
 *   "nodes": {
 *     "&lt;node-id&gt;": { "analytics": { "queries": {...}, "stages_by_type": {...}, "fragments": {...} } },
 *     ...
 *   }
 * }
 * </pre>
 */
public class AnalyticsStatsApiIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");
    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    @SuppressWarnings("unchecked")
    public void testStatsEndpointReflectsExecutedQueries() throws IOException {
        ensureDataProvisioned();

        // Fire a mix of query shapes so the per-stage-type buckets see varied work and the
        // histograms have enough samples for percentile spread. The REST client round-robins
        // the coordinator across nodes, so different queries land on different coordinators.
        for (int i = 0; i < 30; i++) {
            executePpl("source=" + DATASET.indexName + " | fields str0");
            executePpl("source=" + DATASET.indexName + " | where num0 > 0 | fields str0, num0");
            executePpl("source=" + DATASET.indexName + " | stats avg(num0)");
        }

        Map<String, Object> body = fetchStats();
        Map<String, Object> nodesHeader = (Map<String, Object>) body.get("_nodes");
        assertNotNull("_nodes header present", nodesHeader);
        assertNotNull("cluster_name present", body.get("cluster_name"));

        Map<String, Object> nodes = (Map<String, Object>) body.get("nodes");
        assertNotNull("nodes block present", nodes);
        assertFalse("at least one node in response", nodes.isEmpty());

        // At least one node must have recorded work — every query was coordinated by some
        // node in this cluster. Walk all nodes, validate each one's shape, and track which
        // saw the most activity.
        boolean sawAnyQuery = false;
        boolean sawAnyStage = false;
        boolean sawAnyFragment = false;
        for (Map.Entry<String, Object> nodeEntry : nodes.entrySet()) {
            String nodeId = nodeEntry.getKey();
            Map<String, Object> nodeBody = (Map<String, Object>) nodeEntry.getValue();
            Map<String, Object> analytics = (Map<String, Object>) nodeBody.get("analytics");
            assertNotNull(nodeId + ".analytics present", analytics);

            // queries bucket carries only the latency distributions — query counters
            // (total/succeeded/failed) live in _nodes/stats, not here.
            Map<String, Object> queries = (Map<String, Object>) analytics.get("queries");
            assertNotNull(nodeId + ".queries present", queries);
            Map<String, Object> elapsed = (Map<String, Object>) queries.get("elapsed_ms");
            assertLatencyStatsShape(nodeId + ".queries.elapsed_ms", elapsed);
            assertLatencyStatsShape(nodeId + ".queries.planning_ms", (Map<String, Object>) queries.get("planning_ms"));
            if (((Number) elapsed.get("count")).longValue() > 0) {
                sawAnyQuery = true;
            }

            Map<String, Object> stagesByType = (Map<String, Object>) analytics.get("stages_by_type");
            assertNotNull(nodeId + ".stages_by_type present", stagesByType);
            for (Map.Entry<String, Object> stageEntry : stagesByType.entrySet()) {
                Map<String, Object> bucket = (Map<String, Object>) stageEntry.getValue();
                String label = nodeId + ".stages_by_type." + stageEntry.getKey();
                assertNotNull(label + ".started", bucket.get("started"));
                assertNotNull(label + ".succeeded", bucket.get("succeeded"));
                assertLatencyStatsShape(label + ".elapsed_ms", (Map<String, Object>) bucket.get("elapsed_ms"));
                if (((Number) bucket.get("started")).longValue() > 0) {
                    sawAnyStage = true;
                }
            }

            Map<String, Object> fragments = (Map<String, Object>) analytics.get("fragments");
            assertNotNull(nodeId + ".fragments present", fragments);
            assertNotNull(nodeId + ".fragments.total", fragments.get("total"));
            Map<String, Object> fragElapsed = (Map<String, Object>) fragments.get("elapsed_ms");
            assertLatencyStatsShape(nodeId + ".fragments.elapsed_ms", fragElapsed);
            long fragTotal = ((Number) fragments.get("total")).longValue();
            long fragSucceeded = ((Number) fragments.get("succeeded")).longValue();
            if (fragTotal > 0) {
                sawAnyFragment = true;
                assertTrue(
                    nodeId + ".fragments.succeeded > 0 when total > 0, total=" + fragTotal + " succeeded=" + fragSucceeded,
                    fragSucceeded > 0
                );
                long fragSum = ((Number) fragElapsed.get("sum_ms")).longValue();
                assertTrue(nodeId + ".fragments.elapsed_ms.sum_ms > 0 when fragments succeeded", fragSum > 0);
            }
        }

        assertTrue("at least one node recorded a query", sawAnyQuery);
        assertTrue("at least one node recorded a stage", sawAnyStage);
        assertTrue("at least one node recorded a fragment", sawAnyFragment);
    }

    @SuppressWarnings("unchecked")
    public void testStatsEndpointScopedToSingleNode() throws IOException {
        ensureDataProvisioned();

        // Pick an arbitrary node id from a cluster-wide call, then re-query scoped to that id.
        Map<String, Object> all = fetchStats();
        Map<String, Object> nodes = (Map<String, Object>) all.get("nodes");
        assertNotNull("nodes block present", nodes);
        assertFalse("at least one node in cluster", nodes.isEmpty());
        String nodeId = nodes.keySet().iterator().next();

        Request request = new Request("GET", "/_plugins/_analytics/" + nodeId + "/stats");
        Map<String, Object> body = assertOkAndParse(client().performRequest(request), "STATS GET (single node)");

        Map<String, Object> nodesHeader = (Map<String, Object>) body.get("_nodes");
        assertNotNull("_nodes header present", nodesHeader);
        assertEquals("path scope contacts exactly one node", 1, ((Number) nodesHeader.get("total")).intValue());

        Map<String, Object> scopedNodes = (Map<String, Object>) body.get("nodes");
        assertEquals("response carries exactly one node entry", 1, scopedNodes.size());
        assertTrue(nodeId + " is the only entry", scopedNodes.containsKey(nodeId));
        assertNotNull(nodeId + ".analytics present", ((Map<String, Object>) scopedNodes.get(nodeId)).get("analytics"));
    }

    private static void assertLatencyStatsShape(String label, Map<String, Object> latency) {
        assertNotNull(label + " object present", latency);
        for (String field : new String[] { "count", "sum_ms" }) {
            assertNotNull(label + "." + field, latency.get(field));
            long v = ((Number) latency.get(field)).longValue();
            assertTrue(label + "." + field + " non-negative, got " + v, v >= 0);
        }
    }

    private Map<String, Object> fetchStats() throws IOException {
        Request request = new Request("GET", "/_plugins/_analytics/stats");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "STATS GET");
    }

}
