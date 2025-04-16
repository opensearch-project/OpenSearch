/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */


package org.opensearch.action.pagination;

import org.junit.Test;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.admin.cluster.wlm.WlmStatsResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.wlm.stats.WlmStats;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.wlm.stats.WorkloadGroupStats;


import java.util.Collections;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class WlmPaginationStrategyTests {

    private WlmStatsResponse mockResponse(int count) {
        List<WlmStats> stats = generateRandomWlmStats(count);
        return new WlmStatsResponse(ClusterName.DEFAULT, stats, Collections.emptyList());
    }

    public List<WlmStats> generateRandomWlmStats(int count) {
        List<WlmStats> statsList = new ArrayList<>();

        for (int i = 1; i <= count; i++) {
            DiscoveryNode mockNode = DiscoveryNodeMock.createDummyNode(i);

            WorkloadGroupStats.WorkloadGroupStatsHolder statsHolder = new WorkloadGroupStats.WorkloadGroupStatsHolder();
            Map<String, WorkloadGroupStats.WorkloadGroupStatsHolder> queryStats = new HashMap<>();
            queryStats.put("query-group-" + i, statsHolder);

            WlmStats wlmStats = new WlmStats(mockNode, new WorkloadGroupStats(queryStats));
            statsList.add(wlmStats);
        }
        return statsList;
    }

//    @Test
//    public void testSortingByNodeId() {
//        WlmPaginationStrategy strategy = new WlmPaginationStrategy(5, null, "node_id", "asc", mockResponse(10));
//        List<WlmStats> stats = strategy.getPaginatedStats();
//
//        assertNotNull(stats);
//        assertFalse(stats.isEmpty());
//        assertTrue("Sorting should be in ascending order by node_id",
//            stats.get(0).getNode().getId().compareTo(stats.get(1).getNode().getId()) <= 0);
//    }
//
//    @Test
//    public void testSortingByQueryGroup() {
//        WlmPaginationStrategy strategy = new WlmPaginationStrategy(5, null, mockResponse(10));
//        List<WlmStats> stats = strategy.getPaginatedStats();
//
//        assertNotNull(stats);
//        assertFalse(stats.isEmpty());
//        assertTrue("Sorting should be in ascending order by query_group",
//            stats.get(0).getQueryGroupStats().getStats().keySet().iterator().next()
//                .compareTo(stats.get(1).getQueryGroupStats().getStats().keySet().iterator().next()) <= 0);
//    }
//
//    @Test
//    public void testPaginationWithFullPageSize() {
//        WlmPaginationStrategy strategy = new WlmPaginationStrategy(5, null, "node_id", "asc", mockResponse(10));
//        List<WlmStats> page1 = strategy.getPaginatedStats();
//        assertEquals(5, page1.size());
//
//        PageToken nextToken = strategy.getNextToken();
//        assertNotNull("Next token should be present for the next page", nextToken);
//    }
//
//    @Test
//    public void testPaginationWithLessThanFullPageSize() {
//        WlmPaginationStrategy strategy = new WlmPaginationStrategy(5, null, "node_id", "asc", mockResponse(3));
//        List<WlmStats> page1 = strategy.getPaginatedStats();
//        assertEquals(3, page1.size());
//
//        PageToken nextToken = strategy.getNextToken();
//        assertNull("Next token should be null since there is no next page", nextToken);
//    }
//
//    @Test
//    public void testPaginationWithEmptyList() {
//        WlmPaginationStrategy strategy = new WlmPaginationStrategy(5, null, "node_id", "asc", mockResponse(0));
//        List<WlmStats> page1 = strategy.getPaginatedStats();
//        assertTrue(page1.isEmpty());
//
//        PageToken nextToken = strategy.getNextToken();
//        assertNull("Next token should be null since there is no data", nextToken);
//    }
//
//    @Test(expected = OpenSearchParseException.class)
//    public void testInvalidTokenHandling() {
//        new WlmPaginationStrategy(5, "invalid-token", "node_id", "asc", mockResponse(10));
//    }
//
//    @Test
//    public void testInvalidSortByDefaultsToNodeId() {
//        WlmPaginationStrategy strategy = new WlmPaginationStrategy(5, null, "invalid_sort", "asc", mockResponse(10));
//        assertNotNull(strategy.getPaginatedStats());
//    }
//
//    @Test
//    public void testNullSortOrderDefaultsToAscending() {
//        WlmPaginationStrategy strategy = new WlmPaginationStrategy(5, null, "node_id", null, mockResponse(10));
//        List<WlmStats> stats = strategy.getPaginatedStats();
//
//        assertTrue("Sorting should default to ascending order",
//            stats.get(0).getNode().getId().compareTo(stats.get(1).getNode().getId()) <= 0);
//    }
//
//    @Test
//    public void testSortingByNodeIdDescending() {
//        WlmPaginationStrategy strategy = new WlmPaginationStrategy(5, null, "node_id", "desc", mockResponse(10));
//        List<WlmStats> stats = strategy.getPaginatedStats();
//
//        assertNotNull(stats);
//        assertFalse(stats.isEmpty());
//        assertTrue("Sorting should be in descending order by node_id",
//            stats.get(0).getNode().getId().compareTo(stats.get(1).getNode().getId()) >= 0);
//    }
//
//    @Test
//    public void testSortingByQueryGroupDescending() {
//        WlmPaginationStrategy strategy = new WlmPaginationStrategy(5, null, "query_group", "desc", mockResponse(10));
//        List<WlmStats> stats = strategy.getPaginatedStats();
//
//        assertNotNull(stats);
//        assertFalse(stats.isEmpty());
//        assertTrue("Sorting should be in descending order by query_group",
//            stats.get(0).getQueryGroupStats().getStats().keySet().iterator().next()
//                .compareTo(stats.get(1).getQueryGroupStats().getStats().keySet().iterator().next()) >= 0);
//    }
//
//
//    @Test
//    public void testTokenAtLastEntryReturnsEmptyResults() {
//        List<WlmStats> stats = generateRandomWlmStats(5);
//        WlmPaginationStrategy strategy = new WlmPaginationStrategy(
//            5,
//            WlmPaginationStrategy.WlmStrategyToken.generateEncryptedToken(
//                stats.get(4).getQueryGroupStats().getStats().keySet().iterator().next()
//            ),
//            "node_id",
//            "asc",
//            new WlmStatsResponse(ClusterName.DEFAULT, stats, Collections.emptyList())
//        );
//
//        assertTrue(strategy.getPaginatedStats().isEmpty());
//        assertNull(strategy.getNextToken());
//    }
//
//    @Test
//    public void testPaginationWithNextToken() {
//        WlmPaginationStrategy strategy = new WlmPaginationStrategy(5, null, "node_id", "asc", mockResponse(10));
//        List<WlmStats> firstPage = strategy.getPaginatedStats();
//        PageToken nextToken = strategy.getNextToken();
//
//        assertNotNull("Next token should be present", nextToken);
//        assertEquals(5, firstPage.size());
//
//        WlmPaginationStrategy secondStrategy = new WlmPaginationStrategy(5, nextToken.getNextToken(), "node_id", "asc", mockResponse(10));
//        List<WlmStats> secondPage = secondStrategy.getPaginatedStats();
//
//        assertNotNull(secondPage);
//        assertEquals(5, secondPage.size());
//    }
//
//    @Test(expected = OpenSearchParseException.class)
//    public void testOutdatedNextToken() {
//        List<WlmStats> stats = generateRandomWlmStats(5);
//        String outdatedToken = WlmPaginationStrategy.WlmStrategyToken.generateEncryptedToken("non-existent-query-group");
//
//        new WlmPaginationStrategy(5, outdatedToken, "node_id", "asc", new WlmStatsResponse(ClusterName.DEFAULT, stats, Collections.emptyList()));
//    }
//
//    @Test
//    public void testSortingWithEmptyQueryGroups() {
//        List<WlmStats> stats = generateRandomWlmStats(5);
//
//        for (int i = 0; i < stats.size(); i += 2) {
//            stats.get(i).getQueryGroupStats().getStats().clear();
//        }
//
//        WlmPaginationStrategy strategy = new WlmPaginationStrategy(5, null, "query_group", "asc",
//            new WlmStatsResponse(ClusterName.DEFAULT, stats, Collections.emptyList()));
//
//        List<WlmStats> sortedStats = strategy.getPaginatedStats();
//        assertNotNull(sortedStats);
//        assertFalse(sortedStats.isEmpty());
//    }
}
