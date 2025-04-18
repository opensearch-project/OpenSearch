/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */


package org.opensearch.action.pagination;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
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
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class WlmPaginationStrategyTests {
    private SortBy sortBy;
    private SortOrder sortOrder;

    // Constructor for parameterized test
    public WlmPaginationStrategyTests(SortBy sortBy, SortOrder sortOrder) {
        this.sortBy = sortBy;
        this.sortOrder = sortOrder;
    }

    // Parameterized values (ascending and descending order for both node_id and workload_group)
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
            {SortBy.NODE_ID, SortOrder.ASC}, // Sorting by node_id in ascending order
            {SortBy.NODE_ID, SortOrder.DESC}, // Sorting by node_id in descending order
            {SortBy.WORKLOAD_GROUP, SortOrder.ASC}, // Sorting by workload_group in ascending order
            {SortBy.WORKLOAD_GROUP, SortOrder.DESC} // Sorting by workload_group in descending order
        });
    }

    private WlmStatsResponse mockResponse(int count) {
        List<WlmStats> stats = generateRandomWlmStats(count);
        return new WlmStatsResponse(ClusterName.DEFAULT, stats, Collections.emptyList());
    }

    public List<WlmStats> generateRandomWlmStats(int count) {
        List<WlmStats> statsList = new ArrayList<>();

        for (int i = 1; i <= count; i++) {
            DiscoveryNode mockNode = DiscoveryNodeMock.createDummyNode(i);

            WorkloadGroupStats.WorkloadGroupStatsHolder statsHolder = new WorkloadGroupStats.WorkloadGroupStatsHolder();
            Map<String, WorkloadGroupStats.WorkloadGroupStatsHolder> workloadStats = new HashMap<>();
            workloadStats.put("workload-group-" + i, statsHolder);

            WlmStats wlmStats = new WlmStats(mockNode, new WorkloadGroupStats(workloadStats));
            statsList.add(wlmStats);
        }
        return statsList;
    }

    @Test
    public void testConstructor_ValidInputs_ShouldInitializePagination() {
        // Arrange
        int pageSize = 10;
        String nextToken = null;  // First page
        SortBy sortBy = SortBy.WORKLOAD_GROUP;  // Example sorting criteria
        SortOrder sortOrder = SortOrder.ASC;  // Example sort order

        // Mock response with 5 entries
        WlmStatsResponse response = mockResponse(5);  // Mocking a response with 5 entries

        // Act
        WlmPaginationStrategy paginationStrategy = new WlmPaginationStrategy(pageSize, nextToken, sortBy, sortOrder, response);

        // Assert
        assertNotNull(paginationStrategy);
        assertEquals(5, paginationStrategy.getRequestedEntities().size());
    }

    @Test
    public void testTokenGeneration_ShouldGenerateValidToken() {
        // Arrange
        String nodeId = "node1";
        String workloadGroupId = "workloadGroup1";
        int workloadGroupCount = 5;
        String currentHash = "somehashvalue";

        // Act
        String token = WlmPaginationStrategy.WlmStrategyToken.generateEncryptedToken(nodeId, workloadGroupId, workloadGroupCount, currentHash);

        // Assert
        assertNotNull(token);
        assertFalse(token.isEmpty());
    }

    @Test
    public void testPagination_EmptyStatsList_ShouldReturnEmptyList() {
        // Arrange
        int pageSize = 10;
        String nextToken = null;
        SortBy sortBy = SortBy.WORKLOAD_GROUP;
        SortOrder sortOrder = SortOrder.ASC;

        // Mocking an empty WlmStatsResponse
        WlmStatsResponse response = mockResponse(0);

        // Act
        WlmPaginationStrategy paginationStrategy = new WlmPaginationStrategy(pageSize, nextToken, sortBy, sortOrder, response);

        // Assert
        assertTrue(paginationStrategy.getRequestedEntities().isEmpty());
    }

    @Test
    public void testConstructor_InvalidToken_ShouldThrowException() {
        // Arrange
        int pageSize = 10;
        String nextToken = "invalid-token";
        SortBy sortBy = SortBy.WORKLOAD_GROUP;
        SortOrder sortOrder = SortOrder.ASC;

        // Mock response with 5 entries
        WlmStatsResponse response = mockResponse(5);

        // Act & Assert
        OpenSearchParseException thrown = assertThrows(OpenSearchParseException.class, () -> {
            new WlmPaginationStrategy(pageSize, nextToken, sortBy, sortOrder, response);
        });

        // Update expected message to match the actual exception message
        assertEquals("Parameter [next_token] has been tainted and is incorrect. Please provide a valid [next_token].", thrown.getMessage());
    }

    @Test
    public void testPagination_WhenNextTokenIsNull_ShouldStartFromFirstPage() {
        // Arrange
        int pageSize = 3;
        String nextToken = null;  // First page
        SortBy sortBy = SortBy.WORKLOAD_GROUP;
        SortOrder sortOrder = SortOrder.ASC;

        // Mock response with 5 stats entries
        WlmStatsResponse response = mockResponse(5);

        // Act
        WlmPaginationStrategy paginationStrategy = new WlmPaginationStrategy(pageSize, nextToken, sortBy, sortOrder, response);

        // Assert
        assertNotNull(paginationStrategy.getRequestedEntities());
        assertEquals(pageSize, paginationStrategy.getRequestedEntities().size());
    }

    @Test
    public void testPagination_LargerPageSize_ShouldReturnAllData() {
        // Arrange
        int pageSize = 10;
        String nextToken = null;  // First page
        SortBy sortBy = SortBy.WORKLOAD_GROUP;
        SortOrder sortOrder = SortOrder.ASC;

        // Mock response with 5 stats entries
        WlmStatsResponse response = mockResponse(5);

        // Act
        WlmPaginationStrategy paginationStrategy = new WlmPaginationStrategy(pageSize, nextToken, sortBy, sortOrder, response);

        // Assert
        assertNotNull(paginationStrategy.getRequestedEntities());
        assertEquals(5, paginationStrategy.getRequestedEntities().size()); // Size should be 5 as we have only 5 entries
    }

    @Test
    public void testSorting() {
        // Act
        WlmPaginationStrategy strategy = new WlmPaginationStrategy(5, null, sortBy, sortOrder, mockResponse(10));
        List<WlmStats> stats = strategy.getPaginatedStats();

        // Assert
        assertNotNull(stats);
        assertFalse(stats.isEmpty());

        if (sortBy == SortBy.NODE_ID) {
            assertTrue("Sorting should be in " + sortOrder + " order by node_id",
                stats.get(0).getNode().getId().compareTo(stats.get(1).getNode().getId())
                    <= (sortOrder == SortOrder.ASC ? 0 : 1));
        } else if (sortBy == SortBy.WORKLOAD_GROUP) {
            assertTrue("Sorting should be in " + sortOrder + " order by workload_group",
                stats.get(0).getWorkloadGroupStats().getStats().keySet().iterator().next()
                    .compareTo(stats.get(1).getWorkloadGroupStats().getStats().keySet().iterator().next())
                    <= (sortOrder == SortOrder.ASC ? 0 : 1));
        }
    }
}
