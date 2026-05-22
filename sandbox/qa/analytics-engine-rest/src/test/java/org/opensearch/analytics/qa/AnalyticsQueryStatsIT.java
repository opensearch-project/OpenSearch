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

import java.util.Map;

/**
 * Integration test validating that analytics query stats are reported through
 * the {@code GET /_nodes/stats/plugin_stats} API after executing queries.
 */
public class AnalyticsQueryStatsIT extends AnalyticsRestTestCase {

    private static final String INDEX = "query_stats_test";

    public void testStatsAppearAfterQueryExecution() throws Exception {
        createIndex();
        indexData();

        // Execute multiple queries to generate stats
        executePPL("source = " + INDEX + " | stats avg(score) by name");
        executePPL("source = " + INDEX + " | where score > 80");
        executePPL("source = " + INDEX + " | fields name, score");

        // Fetch plugin stats
        Map<String, Object> stats = getAnalyticsQueryStats();

        // Verify structure
        assertNotNull("analytics_query_stats must be present", stats);

        @SuppressWarnings("unchecked")
        Map<String, Object> queryCounts = (Map<String, Object>) stats.get("query_counts");
        assertNotNull("query_counts must be present", queryCounts);
        assertTrue("total queries must be >= 1", ((Number) queryCounts.get("total")).longValue() >= 1);
        assertTrue("failed queries must be 0", ((Number) queryCounts.get("failed")).longValue() == 0);

        @SuppressWarnings("unchecked")
        Map<String, Object> planningTime = (Map<String, Object>) stats.get("planning_time_ms");
        assertNotNull("planning_time_ms must be present", planningTime);
        assertNotNull("planning_time_ms.p50 must be present", planningTime.get("p50"));
        assertNotNull("planning_time_ms.p95 must be present", planningTime.get("p95"));
        assertNotNull("planning_time_ms.p99 must be present", planningTime.get("p99"));

        @SuppressWarnings("unchecked")
        Map<String, Object> executionTime = (Map<String, Object>) stats.get("execution_time_ms");
        assertNotNull("execution_time_ms must be present", executionTime);
        assertNotNull("execution_time_ms.p50 must be present", executionTime.get("p50"));

        @SuppressWarnings("unchecked")
        Map<String, Object> coordinatorRows = (Map<String, Object>) stats.get("coordinator_rows");
        assertNotNull("coordinator_rows must be present", coordinatorRows);

        @SuppressWarnings("unchecked")
        Map<String, Object> stages = (Map<String, Object>) stats.get("stages");
        assertNotNull("stages must be present", stages);

        @SuppressWarnings("unchecked")
        Map<String, Object> shardFragment = (Map<String, Object>) stages.get("shard_fragment");
        assertNotNull("stages.shard_fragment must be present", shardFragment);
        assertTrue("shard_fragment count must be >= 1", ((Number) shardFragment.get("count")).longValue() >= 1);

        @SuppressWarnings("unchecked")
        Map<String, Object> coordinatorReduce = (Map<String, Object>) stages.get("coordinator_reduce");
        assertNotNull("stages.coordinator_reduce must be present", coordinatorReduce);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getAnalyticsQueryStats() throws Exception {
        Request request = new Request("GET", "/_nodes/stats/plugin_stats");
        Response response = client().performRequest(request);
        Map<String, Object> body = entityAsMap(response);

        Map<String, Object> nodes = (Map<String, Object>) body.get("nodes");
        assertNotNull("nodes must be present in _nodes/stats response", nodes);
        assertFalse("nodes must not be empty", nodes.isEmpty());

        // Find the node that has analytics_query_stats (the coordinator node)
        for (Object nodeValue : nodes.values()) {
            Map<String, Object> nodeStats = (Map<String, Object>) nodeValue;
            Map<String, Object> stats = (Map<String, Object>) nodeStats.get("analytics_query_stats");
            if (stats != null) {
                Map<String, Object> counts = (Map<String, Object>) stats.get("query_counts");
                if (counts != null && ((Number) counts.get("total")).longValue() > 0) {
                    return stats;
                }
            }
        }
        // Fall back to first node's stats (may be empty)
        Map<String, Object> nodeStats = (Map<String, Object>) nodes.values().iterator().next();
        return (Map<String, Object>) nodeStats.get("analytics_query_stats");
    }

    private void executePPL(String ppl) throws Exception {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + ppl + "\"}");
        Response response = client().performRequest(request);
        assertEquals("PPL query must succeed", 200, response.getStatusLine().getStatusCode());
    }

    private void createIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity("{"
            + "\"settings\": {"
            + "  \"index.number_of_shards\": 2,"
            + "  \"index.number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"name\": {\"type\": \"keyword\"},"
            + "    \"score\": {\"type\": \"double\"}"
            + "  }"
            + "}"
            + "}");
        client().performRequest(create);
    }

    private void indexData() throws Exception {
        Request bulk = new Request("POST", "/" + INDEX + "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(
            "{\"index\":{}}\n{\"name\":\"alice\",\"score\":95.5}\n"
                + "{\"index\":{}}\n{\"name\":\"bob\",\"score\":87.3}\n"
                + "{\"index\":{}}\n{\"name\":\"carol\",\"score\":91.0}\n"
        );
        client().performRequest(bulk);

        // Flush to commit parquet files to disk
        Request flush = new Request("POST", "/" + INDEX + "/_flush");
        flush.addParameter("force", "true");
        client().performRequest(flush);

        // Wait for index health
        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "yellow");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }
}
