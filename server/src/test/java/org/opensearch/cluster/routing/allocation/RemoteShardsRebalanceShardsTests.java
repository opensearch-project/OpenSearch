/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import com.carrotsearch.hppc.ObjectIntHashMap;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingPool;
import org.opensearch.cluster.routing.ShardRouting;

public class RemoteShardsRebalanceShardsTests extends RemoteShardsBalancerBaseTestCase {

    /**
     * Test remote shard allocation and balancing for standard new cluster setup.
     *
     * Post rebalance primaries should be balanced across all the nodes.
     */
    public void testShardAllocationAndRebalance() {
        int localOnlyNodes = 20;
        int remoteCapableNodes = 40;
        int localIndices = 40;
        int remoteIndices = 80;
        ClusterState clusterState = createInitialCluster(localOnlyNodes, remoteCapableNodes, localIndices, remoteIndices);
        AllocationService service = this.createRemoteCapableAllocationService();
        clusterState = allocateShardsAndBalance(clusterState, service);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        RoutingAllocation allocation = getRoutingAllocation(clusterState, routingNodes);

        ObjectIntHashMap<String> nodePrimariesCounter = getShardCounterPerNodeForRemoteCapablePool(clusterState, allocation, true);
        ObjectIntHashMap<String> nodeReplicaCounter = getShardCounterPerNodeForRemoteCapablePool(clusterState, allocation, false);
        int avgPrimariesPerNode = getTotalShardCountAcrossNodes(nodePrimariesCounter) / remoteCapableNodes;

        // Primary and replica are balanced post first reroute
        for (RoutingNode node : routingNodes) {
            if (RoutingPool.REMOTE_CAPABLE.equals(RoutingPool.getNodePool(node))) {
                assertInRange(nodePrimariesCounter.get(node.nodeId()), avgPrimariesPerNode, remoteCapableNodes - 1);
                assertTrue(nodeReplicaCounter.get(node.nodeId()) >= 0);
            }
        }
    }

    private ObjectIntHashMap<String> getShardCounterPerNodeForRemoteCapablePool(
        ClusterState clusterState,
        RoutingAllocation allocation,
        boolean primary
    ) {
        ObjectIntHashMap<String> nodePrimariesCounter = new ObjectIntHashMap<>();
        for (ShardRouting shard : clusterState.getRoutingTable().allShards()) {
            if (RoutingPool.REMOTE_CAPABLE.equals(RoutingPool.getShardPool(shard, allocation)) && shard.primary() == primary) {
                nodePrimariesCounter.putOrAdd(shard.currentNodeId(), 1, 1);
            }
        }
        return nodePrimariesCounter;
    }

    private int getTotalShardCountAcrossNodes(ObjectIntHashMap<String> nodePrimariesCounter) {
        int totalShardCount = 0;
        for (int value : nodePrimariesCounter.values) {
            totalShardCount += value;
        }
        return totalShardCount;
    }

    /**
     * Asserts that the expected value is within the variance range.
     *
     * Being used to assert the average number of shards per node.
     * Variance is required in case of non-absolute mean values;
     * for example, total number of remote capable nodes in a cluster.
     */
    private void assertInRange(int actual, int expectedMean, int variance) {
        assertTrue(actual >= expectedMean - variance);
        assertTrue(actual <= expectedMean + variance);
    }
}
