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
import org.opensearch.cluster.routing.allocation.allocator.RemoteShardsBalancer;

import java.util.HashMap;
import java.util.Map;

public class RemoteShardsAllocateUnassignedTests extends RemoteShardsBalancerBaseTestCase {

    /**
     * Test Remote Shards Balancer initialization.
     */
    public void testInit() {
        int localOnlyNodes = 7;
        int remoteCapableNodes = 3;
        int localIndices = 10;
        int remoteIndices = 15;
        ClusterState clusterState = createInitialCluster(localOnlyNodes, remoteCapableNodes, localIndices, remoteIndices);
        RoutingNodes routingNodes = new RoutingNodes(clusterState, false);
        RoutingAllocation allocation = getRoutingAllocation(clusterState, routingNodes);

        RemoteShardsBalancer remoteShardsBalancer = new RemoteShardsBalancer(logger, allocation);
        Map<String, RemoteShardsBalancer.UnassignedIndexShards> unassignedShardMap = remoteShardsBalancer.groupUnassignedShardsByIndex();

        assertEquals(remoteIndices, unassignedShardMap.size());
        for (String index : unassignedShardMap.keySet()) {
            assertTrue(index.startsWith(REMOTE_IDX_PREFIX));
            RemoteShardsBalancer.UnassignedIndexShards indexShards = unassignedShardMap.get(index);
            assertEquals(5, indexShards.getPrimaries().size());
            for (ShardRouting shard : indexShards.getPrimaries()) {
                assertTrue(shard.primary());
                assertEquals(shard.getIndexName(), index);
            }
            assertEquals(5, indexShards.getReplicas().size());
            for (ShardRouting shard : indexShards.getReplicas()) {
                assertFalse(shard.primary());
                assertEquals(shard.getIndexName(), index);
            }
        }
    }

    /**
     * Test remote unassigned shard allocation for standard new cluster setup.
     */
    public void testPrimaryAllocation() {
        int localOnlyNodes = 7;
        int remoteCapableNodes = 3;
        int localIndices = 10;
        int remoteIndices = 13;
        ClusterState clusterState = createInitialCluster(localOnlyNodes, remoteCapableNodes, localIndices, remoteIndices);
        AllocationService service = this.createRemoteCapableAllocationService();
        clusterState = allocateShardsAndBalance(clusterState, service);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        RoutingAllocation allocation = getRoutingAllocation(clusterState, routingNodes);

        assertEquals(0, routingNodes.unassigned().size());

        final Map<String, Integer> nodePrimariesCounter = new HashMap<>();
        for (ShardRouting shard : clusterState.getRoutingTable().allShards()) {
            assertFalse(shard.unassigned());
            RoutingNode node = routingNodes.node(shard.currentNodeId());
            RoutingPool nodePool = RoutingPool.getNodePool(node);
            RoutingPool shardPool = RoutingPool.getShardPool(shard, allocation);
            if (RoutingPool.REMOTE_CAPABLE.equals(shardPool)) {
                assertEquals(nodePool, shardPool);
            }
            if (RoutingPool.getNodePool(node) == RoutingPool.REMOTE_CAPABLE
                && RoutingPool.REMOTE_CAPABLE.equals(RoutingPool.getShardPool(shard, allocation))
                && shard.primary()) {
                nodePrimariesCounter.compute(node.nodeId(), (k, v) -> (v == null) ? 1 : v + 1);
            }
        }
        final int indexShardLimit = (int) Math.ceil(totalPrimaries(remoteIndices) / (float) remoteCapableNodes);
        for (int primaries : nodePrimariesCounter.values()) {
            assertTrue(primaries <= indexShardLimit);
        }
    }

    /**
     * Test remote unassigned shard allocation when remote capable nodes fail to come up.
     */
    public void testAllocationRemoteCapableNodesUnavailable() {
        int localOnlyNodes = 7;
        int remoteCapableNodes = 0;
        int localIndices = 10;
        int remoteIndices = 13;
        ClusterState clusterState = createInitialCluster(localOnlyNodes, remoteCapableNodes, localIndices, remoteIndices);
        AllocationService service = this.createRemoteCapableAllocationService();
        clusterState = allocateShardsAndBalance(clusterState, service);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        RoutingAllocation allocation = getRoutingAllocation(clusterState, routingNodes);

        assertEquals(totalShards(remoteIndices), routingNodes.unassigned().size());

        for (ShardRouting shard : clusterState.getRoutingTable().allShards()) {
            if (RoutingPool.getShardPool(shard, allocation) == RoutingPool.REMOTE_CAPABLE) {
                assertTrue(shard.unassigned());
            } else {
                assertFalse(shard.unassigned());
                RoutingNode node = routingNodes.node(shard.currentNodeId());
                assertEquals(RoutingPool.LOCAL_ONLY, RoutingPool.getNodePool(node));
            }
        }
        for (RoutingNode node : routingNodes) {
            if (RoutingPool.getNodePool(node) == RoutingPool.REMOTE_CAPABLE) {
                assertEquals(0, node.size());
            }
        }
    }
}
