/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.pagination;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.admin.cluster.wlm.WlmStatsResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.stats.SortBy;
import org.opensearch.wlm.stats.SortOrder;
import org.opensearch.wlm.stats.WlmStats;
import org.opensearch.wlm.stats.WorkloadGroupStats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WlmPaginationStrategyTests extends OpenSearchTestCase {
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

    public void testValid() {
        int pageSize = 10;
        String nextToken = null;
        SortBy sortBy = SortBy.WORKLOAD_GROUP;
        SortOrder sortOrder = SortOrder.ASC;

        WlmStatsResponse response = mockResponse(5);  // Mocking a response with 5 entries

        WlmPaginationStrategy paginationStrategy = new WlmPaginationStrategy(pageSize, nextToken, sortBy, sortOrder, response);

        assertNotNull(paginationStrategy);
        assertEquals(5, paginationStrategy.getRequestedEntities().size());
    }

    public void testToken() {
        String nodeId = "node1";
        String workloadGroupId = "workloadGroup1";
        int workloadGroupCount = 5;
        String currentHash = "somehashvalue";
        String sortOrder = SortOrder.ASC.name();
        String sortBy = SortBy.WORKLOAD_GROUP.name();

        String token = WlmPaginationStrategy.WlmStrategyToken.generateEncryptedToken(
            nodeId,
            workloadGroupId,
            workloadGroupCount,
            currentHash,
            sortOrder,
            sortBy
        );

        assertNotNull(token);
        assertFalse(token.isEmpty());
    }

    public void testEmpty() {
        int pageSize = 10;
        String nextToken = null;
        SortBy sortBy = SortBy.WORKLOAD_GROUP;
        SortOrder sortOrder = SortOrder.ASC;

        WlmStatsResponse response = mockResponse(0);

        WlmPaginationStrategy paginationStrategy = new WlmPaginationStrategy(pageSize, nextToken, sortBy, sortOrder, response);

        assertTrue(paginationStrategy.getRequestedEntities().isEmpty());
    }

    public void testInvalid() {
        int pageSize = 10;
        String nextToken = "invalid-token";
        SortBy sortBy = SortBy.WORKLOAD_GROUP;
        SortOrder sortOrder = SortOrder.ASC;

        WlmStatsResponse response = mockResponse(5);

        OpenSearchParseException thrown = assertThrows(OpenSearchParseException.class, () -> {
            new WlmPaginationStrategy(pageSize, nextToken, sortBy, sortOrder, response);
        });

        assertEquals("Parameter [next_token] has been tainted and is incorrect. Please provide a valid [next_token].", thrown.getMessage());
    }

    public void testStart() {
        int pageSize = 3;
        String nextToken = null;
        SortBy sortBy = SortBy.WORKLOAD_GROUP;
        SortOrder sortOrder = SortOrder.ASC;

        WlmStatsResponse response = mockResponse(5);

        WlmPaginationStrategy paginationStrategy = new WlmPaginationStrategy(pageSize, nextToken, sortBy, sortOrder, response);

        assertNotNull(paginationStrategy.getRequestedEntities());
        assertEquals(pageSize, paginationStrategy.getRequestedEntities().size());
    }

    public void testPageLimit() {
        int pageSize = 10;
        String nextToken = null;
        SortBy sortBy = SortBy.WORKLOAD_GROUP;
        SortOrder sortOrder = SortOrder.ASC;

        WlmStatsResponse response = mockResponse(5);

        WlmPaginationStrategy paginationStrategy = new WlmPaginationStrategy(pageSize, nextToken, sortBy, sortOrder, response);

        assertNotNull(paginationStrategy.getRequestedEntities());
        assertEquals(5, paginationStrategy.getRequestedEntities().size());
    }

    public void testSorting() {
        List<WlmStats> statsList = mockResponse(10).getNodes();

        for (SortBy sortBy : SortBy.values()) {
            for (SortOrder sortOrder : SortOrder.values()) {
                WlmPaginationStrategy strategy = new WlmPaginationStrategy(
                    5,
                    null,
                    sortBy,
                    sortOrder,
                    new WlmStatsResponse(ClusterName.DEFAULT, statsList, Collections.emptyList())
                );
                List<WlmStats> stats = strategy.getRequestedEntities();

                assertNotNull(stats);
                assertFalse(stats.isEmpty());

                if (sortBy == SortBy.NODE_ID) {
                    assertTrue(
                        "Sorting should be in " + sortOrder + " order by node_id",
                        stats.get(0).getNode().getId().compareTo(stats.get(1).getNode().getId()) <= (sortOrder == SortOrder.ASC ? 0 : 1)
                    );
                } else if (sortBy == SortBy.WORKLOAD_GROUP) {
                    assertTrue(
                        "Sorting should be in " + sortOrder + " order by workload_group",
                        stats.get(0)
                            .getWorkloadGroupStats()
                            .getStats()
                            .keySet()
                            .iterator()
                            .next()
                            .compareTo(
                                stats.get(1).getWorkloadGroupStats().getStats().keySet().iterator().next()
                            ) <= (sortOrder == SortOrder.ASC ? 0 : 1)
                    );
                }
            }
        }
    }
}
