/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingPool;
import org.opensearch.cluster.routing.ShardRouting;

import java.util.HashMap;
import java.util.Map;

public class RemoteShardsRebalanceShardsTests extends RemoteShardsBalancerBaseTestCase {

    /**
     * Test remote shard allocation and balancing for standard new cluster setup.
     * <p>
     * Post rebalance primaries should be balanced across all the nodes.
     */
    public void testShardAllocationAndRebalance() {
        final int localOnlyNodes = 20;
        final int remoteCapableNodes = 40;
        final int halfRemoteCapableNodes = remoteCapableNodes / 2;
        final int localIndices = 40;
        final int remoteIndices = 80;
        ClusterState clusterState = createInitialCluster(localOnlyNodes, remoteCapableNodes, localIndices, remoteIndices);
        final StringBuilder excludeNodes = new StringBuilder();
        for (int i = 0; i < halfRemoteCapableNodes; i++) {
            excludeNodes.append(getNodeId(i, true));
            if (i != (remoteCapableNodes / 2 - 1)) {
                excludeNodes.append(", ");
            }
        }
        AllocationService service = this.createRemoteCapableAllocationService(excludeNodes.toString());
        clusterState = allocateShardsAndBalance(clusterState, service);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        RoutingAllocation allocation = getRoutingAllocation(clusterState, routingNodes);

        Map<String, Integer> nodePrimariesCounter = getShardCounterPerNodeForRemoteCapablePool(clusterState, allocation, true);
        Map<String, Integer> nodeReplicaCounter = getShardCounterPerNodeForRemoteCapablePool(clusterState, allocation, false);
        int avgPrimariesPerNode = getTotalShardCountAcrossNodes(nodePrimariesCounter) / remoteCapableNodes;

        // Primary and replica are balanced after first allocating unassigned
        for (RoutingNode node : routingNodes) {
            if (RoutingPool.REMOTE_CAPABLE.equals(RoutingPool.getNodePool(node))) {
                if (Integer.parseInt(node.nodeId().split("-")[4]) < halfRemoteCapableNodes) {
                    assertEquals(0, (int) nodePrimariesCounter.getOrDefault(node.nodeId(), 0));
                } else {
                    assertEquals(avgPrimariesPerNode * 2, (int) nodePrimariesCounter.get(node.nodeId()));
                }
                assertTrue(nodeReplicaCounter.getOrDefault(node.nodeId(), 0) >= 0);
            }
        }

        // Remove exclude constraint and rebalance
        service = this.createRemoteCapableAllocationService();
        clusterState = allocateShardsAndBalance(clusterState, service);
        routingNodes = clusterState.getRoutingNodes();
        allocation = getRoutingAllocation(clusterState, routingNodes);
        nodePrimariesCounter = getShardCounterPerNodeForRemoteCapablePool(clusterState, allocation, true);
        nodeReplicaCounter = getShardCounterPerNodeForRemoteCapablePool(clusterState, allocation, false);
        for (RoutingNode node : routingNodes) {
            if (RoutingPool.REMOTE_CAPABLE.equals(RoutingPool.getNodePool(node))) {
                assertEquals(avgPrimariesPerNode, (int) nodePrimariesCounter.get(node.nodeId()));
                assertTrue(nodeReplicaCounter.getOrDefault(node.nodeId(), 0) >= 0);
            }
        }
    }

    private Map<String, Integer> getShardCounterPerNodeForRemoteCapablePool(
        ClusterState clusterState,
        RoutingAllocation allocation,
        boolean primary
    ) {
        final Map<String, Integer> nodePrimariesCounter = new HashMap<>();
        for (ShardRouting shard : clusterState.getRoutingTable().allShards()) {
            if (RoutingPool.REMOTE_CAPABLE.equals(RoutingPool.getShardPool(shard, allocation)) && shard.primary() == primary) {
                nodePrimariesCounter.compute(shard.currentNodeId(), (k, v) -> (v == null) ? 1 : v + 1);
            }
        }
        return nodePrimariesCounter;
    }

    private int getTotalShardCountAcrossNodes(final Map<String, Integer> nodePrimariesCounter) {
        int totalShardCount = 0;
        for (int value : nodePrimariesCounter.values()) {
            totalShardCount += value;
        }
        return totalShardCount;
    }

    /**
     * Asserts that the expected value is within the variance range.
     * <p>
     * Being used to assert the average number of shards per node.
     * Variance is required in case of non-absolute mean values;
     * for example, total number of remote capable nodes in a cluster.
     */
    private void assertInRange(int actual, int expectedMean, int variance) {
        assertTrue(actual >= expectedMean - variance);
        assertTrue(actual <= expectedMean + variance);
    }
}
