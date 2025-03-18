/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */


//package org.opensearch.action.pagination;
//
//import org.junit.jupiter.api.Test;
//import org.opensearch.test.OpenSearchTestCase;
//import org.opensearch.wlm.stats.QueryGroupStats;
//import org.opensearch.wlm.stats.QueryGroupStats.QueryGroupStatsHolder;
//import org.opensearch.wlm.stats.WlmStats;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import static org.junit.jupiter.api.Assertions.*;
//
///**
// * Unit tests for {@link WlmPaginationStrategy} with mocked data.
// */
//public class WlmPaginationStrategyTests extends OpenSearchTestCase {
//
//    /**
//     * Tests sorting by node ID in ascending order.
//     */
//    @Test
//    public void testSortingByNodeIdAsc() {
//        List<Map<String, Object>> mockData = createMockWlmStatsData();
//        WlmPaginationStrategy strategy = new WlmPaginationStrategy(10, null, "node_id", "asc", mockData);
//
//        List<WlmStats> paginatedStats = strategy.getRequestedEntities();
//        assertNotNull(paginatedStats);
//        assertFalse(paginatedStats.isEmpty());
//
//        String previousNodeId = "";
//        for (Map<String, Object> stat : paginatedStats) {
//            String currentNodeId = (String) stat.get("node_id");
//            assertTrue(currentNodeId.compareTo(previousNodeId) >= 0);
//            previousNodeId = currentNodeId;
//        }
//    }
//
//    /**
//     * Tests sorting by query group in ascending order.
//     */
//    @Test
//    public void testSortingByQueryGroupAsc() {
//        List<Map<String, Object>> mockData = createMockWlmStatsData();
//        WlmPaginationStrategy strategy = new WlmPaginationStrategy(10, null, "query_group", "asc", mockData);
//
//        List<WlmStats> paginatedStats = strategy.getRequestedEntities();
//        assertNotNull(paginatedStats);
//        assertFalse(paginatedStats.isEmpty());
//
//        String previousQueryGroup = "";
//        for (Map<String, Object> stat : paginatedStats) {
//            String currentQueryGroup = ((Map<String, QueryGroupStatsHolder>) stat.get("query_groups")).keySet().iterator().next();
//            assertTrue(currentQueryGroup.compareTo(previousQueryGroup) >= 0);
//            previousQueryGroup = currentQueryGroup;
//        }
//    }
//
//    /**
//     * Tests pagination with next token functionality.
//     */
//    @Test
//    public void testPaginationWithNextToken() {
//        List<Map<String, Object>> mockData = createMockWlmStatsData();
//        WlmPaginationStrategy strategy = new WlmPaginationStrategy(2, null, "node_id", "asc", mockData);
//
//        List<WlmStats> firstPage = strategy.getRequestedEntities();
//        assertEquals(2, firstPage.size());
//
//        PageToken nextToken = strategy.getResponseToken();
//        assertNotNull(nextToken);
//        assertNotNull(nextToken.getNextToken());
//
//        WlmPaginationStrategy nextPageStrategy = new WlmPaginationStrategy(2, nextToken.getNextToken(), "node_id", "asc", mockData);
//        List<WlmStats> secondPage = nextPageStrategy.getRequestedEntities();
//        assertEquals(2, secondPage.size());
//
//        assertNotEquals(firstPage, secondPage);
//    }
//
//    /**
//     * Tests that an empty response does not break pagination.
//     */
//    @Test
//    public void testEmptyWlmStatsResponse() {
//        List<Map<String, Object>> emptyResponse = new ArrayList<>();
//        WlmPaginationStrategy strategy = new WlmPaginationStrategy(5, null, "node_id", "asc", emptyResponse);
//
//        List<WlmStats> paginatedStats = strategy.getRequestedEntities();
//        assertNotNull(paginatedStats);
//        assertTrue(paginatedStats.isEmpty());
//        assertNull(strategy.getResponseToken());
//    }
//
//    /**
//     * Creates mock WLM stats data in JSON-like format.
//     */
//    private Map<String, Object> createMockWlmStatsData() {
//        Map<String, Object> mockData = new LinkedHashMap<>();
//
//        // Add metadata fields
//        Map<String, Object> nodesMeta = new LinkedHashMap<>();
//        nodesMeta.put("total", 1);
//        nodesMeta.put("successful", 1);
//        nodesMeta.put("failed", 0);
//        mockData.put("_nodes", nodesMeta);
//        mockData.put("cluster_name", "XXXXXXYYYYYYYY");
//
//        // Create a mock node ID
//        String nodeId = "A3L9EfBIQf2anrrUhh_goA";
//        Map<String, Object> nodeData = new LinkedHashMap<>();
//
//        // Query groups inside the node
//        Map<String, Object> queryGroups = new LinkedHashMap<>();
//        queryGroups.put("16YGxFlPRdqIO7K4EACJlw", createMockQueryGroup(33570, 0.03319935314357281, 0.002306486276211217));
//        queryGroups.put("DEFAULT_QUERY_GROUP", createMockQueryGroup(42572, 0.0, 0.0));
//
//        nodeData.put("query_groups", queryGroups);
//        mockData.put(nodeId, nodeData);
//
//        return mockData;
//    }
//
//    /**
//     * Helper function to create a mock query group entry.
//     */
//    private Map<String, Object> createMockQueryGroup(int completions, double cpuUsage, double memoryUsage) {
//        Map<String, Object> queryGroup = new LinkedHashMap<>();
//        queryGroup.put("total_completions", completions);
//        queryGroup.put("total_rejections", 0);
//        queryGroup.put("total_cancellations", 0);
//
//        // CPU usage
//        Map<String, Object> cpuStats = new LinkedHashMap<>();
//        cpuStats.put("current_usage", cpuUsage);
//        cpuStats.put("cancellations", 0);
//        cpuStats.put("rejections", 0);
//        queryGroup.put("cpu", cpuStats);
//
//        // Memory usage
//        Map<String, Object> memoryStats = new LinkedHashMap<>();
//        memoryStats.put("current_usage", memoryUsage);
//        memoryStats.put("cancellations", 0);
//        memoryStats.put("rejections", 0);
//        queryGroup.put("memory", memoryStats);
//
//        return queryGroup;
//    }
//}
