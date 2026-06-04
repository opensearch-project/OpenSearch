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
 * reflects the activity in its query / stage / fragment counters.
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

        // Fire a handful of queries. The test cluster has multiple nodes and the REST client
        // round-robins, so a single GET /_plugins/_analytics/stats only sees one node's slice
        // of the activity. We poll until at least one query is reflected somewhere — proving
        // the listener fired and the rollup serialized — without depending on which node
        // happened to coordinate.
        // Mix of queries — a few shapes so the per-stage-type buckets see varied work
        // and the histograms have enough samples for percentile spread.
        for (int i = 0; i < 30; i++) {
            executePpl("source=" + DATASET.indexName + " | fields str0");
            executePpl("source=" + DATASET.indexName + " | where num0 > 0 | fields str0, num0");
            executePpl("source=" + DATASET.indexName + " | stats avg(num0)");
        }

        // Hit every node so the snapshot reflects both coordinator-side (queries, stages)
        // and data-node-side (fragments) activity. Pick whichever node returned the most.
        Map<String, Object> stats = fetchBusiestNodeStats();
        Map<String, Object> analytics = (Map<String, Object>) stats.get("analytics");
        assertNotNull("analytics present", analytics);

        // queries bucket carries only the latency distributions — query counters
        // (total/succeeded/failed) live in _nodes/stats, not here.
        Map<String, Object> queries = (Map<String, Object>) analytics.get("queries");
        assertNotNull("queries present", queries);
        Map<String, Object> elapsed = (Map<String, Object>) queries.get("elapsed_ms");
        assertLatencyStatsShape("queries.elapsed_ms", elapsed);
        Map<String, Object> planning = (Map<String, Object>) queries.get("planning_ms");
        assertLatencyStatsShape("queries.planning_ms", planning);
        // At least one query should have been recorded somewhere on the busiest node.
        long elapsedCount = ((Number) elapsed.get("count")).longValue();
        assertTrue("queries.elapsed_ms.count >= 1, got " + elapsedCount, elapsedCount >= 1);

        Map<String, Object> stagesByType = (Map<String, Object>) analytics.get("stages_by_type");
        assertNotNull("stages_by_type present", stagesByType);
        assertFalse("at least one stage type recorded", stagesByType.isEmpty());
        for (Map.Entry<String, Object> entry : stagesByType.entrySet()) {
            Map<String, Object> bucket = (Map<String, Object>) entry.getValue();
            assertNotNull(entry.getKey() + ".started", bucket.get("started"));
            assertNotNull(entry.getKey() + ".succeeded", bucket.get("succeeded"));
            assertLatencyStatsShape(entry.getKey() + ".elapsed_ms", (Map<String, Object>) bucket.get("elapsed_ms"));
            long started = ((Number) bucket.get("started")).longValue();
            assertTrue(entry.getKey() + ".started > 0", started > 0);
        }

        Map<String, Object> fragments = (Map<String, Object>) analytics.get("fragments");
        assertNotNull("fragments present", fragments);
        assertNotNull("fragments.total", fragments.get("total"));
        Map<String, Object> fragElapsed = (Map<String, Object>) fragments.get("elapsed_ms");
        assertLatencyStatsShape("fragments.elapsed_ms", fragElapsed);
        long fragTotal = ((Number) fragments.get("total")).longValue();
        long fragSucceeded = ((Number) fragments.get("succeeded")).longValue();
        if (fragTotal > 0) {
            // If this node ran any fragments, at least some should have completed and reported success.
            assertTrue("fragments.succeeded > 0 when total > 0, total=" + fragTotal + " succeeded=" + fragSucceeded, fragSucceeded > 0);
            long fragSum = ((Number) fragElapsed.get("sum_ms")).longValue();
            assertTrue("fragments.elapsed_ms.sum_ms > 0 when fragments succeeded", fragSum > 0);
        }
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

    /**
     * Per-node round-robin makes a single GET land on whichever node the client picks.
     * For the example/visualisation test we want the busiest snapshot — the node that
     * did the most work — so percentile spread and all three buckets show meaningful
     * data.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> fetchBusiestNodeStats() throws IOException {
        Map<String, Object> best = null;
        long bestSignal = -1;
        for (int i = 0; i < 12; i++) {
            Map<String, Object> stats = fetchStats();
            Map<String, Object> analytics = (Map<String, Object>) stats.get("analytics");
            Map<String, Object> elapsed = (Map<String, Object>) ((Map<String, Object>) analytics.get("queries")).get("elapsed_ms");
            Map<String, Object> fragments = (Map<String, Object>) analytics.get("fragments");
            long signal = ((Number) elapsed.get("count")).longValue() + ((Number) fragments.get("total")).longValue();
            if (signal > bestSignal) {
                bestSignal = signal;
                best = stats;
            }
        }
        return best;
    }

}
