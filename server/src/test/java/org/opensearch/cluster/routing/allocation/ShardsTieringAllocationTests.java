/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingPool;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.index.IndexModule;

import static org.opensearch.cluster.routing.RoutingPool.LOCAL_ONLY;
import static org.opensearch.cluster.routing.RoutingPool.REMOTE_CAPABLE;
import static org.opensearch.cluster.routing.RoutingPool.getIndexPool;
import static org.opensearch.common.util.FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG;
import static org.opensearch.index.IndexModule.IS_WARM_INDEX_SETTING;

public class ShardsTieringAllocationTests extends TieringAllocationBaseTestCase {

    @LockFeatureFlag(WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)
    public void testShardsInLocalPool() {
        int localOnlyNodes = 5;
        int remoteCapableNodes = 3;
        int localIndices = 5;
        int remoteIndices = 0;
        ClusterState clusterState = createInitialCluster(localOnlyNodes, remoteCapableNodes, localIndices, remoteIndices);
        AllocationService service = this.createRemoteCapableAllocationService();
        // assign shards to respective nodes
        clusterState = allocateShardsAndBalance(clusterState, service);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        RoutingAllocation allocation = getRoutingAllocation(clusterState, routingNodes);
        assertEquals(0, routingNodes.unassigned().size());

        for (ShardRouting shard : clusterState.getRoutingTable().allShards()) {
            assertFalse(shard.unassigned());
            RoutingPool shardPool = RoutingPool.getShardPool(shard, allocation);
            assertEquals(LOCAL_ONLY, shardPool);
        }
    }

    @LockFeatureFlag(WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)
    public void testShardsInRemotePool() {
        int localOnlyNodes = 7;
        int remoteCapableNodes = 3;
        int localIndices = 0;
        int remoteIndices = 13;
        ClusterState clusterState = createInitialCluster(localOnlyNodes, remoteCapableNodes, localIndices, remoteIndices);
        AllocationService service = this.createRemoteCapableAllocationService();
        // assign shards to respective nodes
        clusterState = allocateShardsAndBalance(clusterState, service);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        RoutingAllocation allocation = getRoutingAllocation(clusterState, routingNodes);
        assertEquals(0, routingNodes.unassigned().size());

        for (ShardRouting shard : clusterState.getRoutingTable().allShards()) {
            assertFalse(shard.unassigned());
            RoutingPool shardPool = RoutingPool.getShardPool(shard, allocation);
            assertEquals(REMOTE_CAPABLE, shardPool);
        }
    }

    @LockFeatureFlag(WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)
    public void testShardsWithTiering() {
        int localOnlyNodes = 15;
        int remoteCapableNodes = 13;
        int localIndices = 10;
        int remoteIndices = 0;
        ClusterState clusterState = createInitialCluster(localOnlyNodes, remoteCapableNodes, localIndices, remoteIndices);
        AllocationService service = this.createRemoteCapableAllocationService();
        // assign shards to respective nodes
        clusterState = allocateShardsAndBalance(clusterState, service);
        // put indices in the hot to warm tiering state
        clusterState = updateIndexMetadataForTiering(
            clusterState,
            localIndices,
            remoteIndices,
            IndexModule.TieringState.HOT_TO_WARM.name(),
            true
        );
        // trigger shard relocation
        clusterState = allocateShardsAndBalance(clusterState, service);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        RoutingAllocation allocation = getRoutingAllocation(clusterState, routingNodes);
        assertEquals(0, routingNodes.unassigned().size());

        for (ShardRouting shard : clusterState.getRoutingTable().allShards()) {
            assertFalse(shard.unassigned());
            RoutingNode node = routingNodes.node(shard.currentNodeId());
            RoutingPool nodePool = RoutingPool.getNodePool(node);
            RoutingPool shardPool = RoutingPool.getShardPool(shard, allocation);
            assertEquals(RoutingPool.REMOTE_CAPABLE, shardPool);
            assertEquals(nodePool, shardPool);
        }
    }

    @LockFeatureFlag(WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)
    public void testShardsWithWarmToHotTiering() throws Exception {
        int localOnlyNodes = 60;
        int remoteCapableNodes = 13;
        int localIndices = 0;
        int remoteIndices = 10; // 5 primary, 1 replica

        // Create a cluster with warm only roles (dedicated setup) and remote index is of type warm only.
        ClusterState clusterState = createInitialCluster(localOnlyNodes, remoteCapableNodes, true, localIndices, remoteIndices, true);
        AllocationService service = this.createRemoteCapableAllocationService();

        // assign shards to respective nodes
        clusterState = allocateShardsAndBalance(clusterState, service);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        RoutingAllocation allocation = getRoutingAllocation(clusterState, routingNodes);
        assertEquals(0, routingNodes.unassigned().size());

        for (ShardRouting shard : clusterState.getRoutingTable().allShards()) {
            assertTrue(shard.relocating() || shard.started());
            RoutingPool shardPool = RoutingPool.getShardPool(shard, allocation);
            RoutingNode node = routingNodes.node(shard.currentNodeId());
            RoutingPool nodePool = RoutingPool.getNodePool(node);
            assertEquals(REMOTE_CAPABLE, shardPool);
            assertEquals(REMOTE_CAPABLE, nodePool);
        }

        // put indices in the hot to warm tiering state
        clusterState = updateIndexMetadataForTiering(
            clusterState,
            localIndices,
            remoteIndices,
            IndexModule.TieringState.WARM_TO_HOT.name(),
            false
        );
        // trigger shard relocation
        clusterState = allocateShardsAndBalance(clusterState, service);
        routingNodes = clusterState.getRoutingNodes();
        allocation = getRoutingAllocation(clusterState, routingNodes);
        assertEquals(0, routingNodes.unassigned().size());

        for (ShardRouting shard : clusterState.getRoutingTable().allShards()) {
            assertBusy(() -> { assertFalse(shard.unassigned()); });
            RoutingNode node = routingNodes.node(shard.currentNodeId());
            RoutingPool nodePool = RoutingPool.getNodePool(node);
            RoutingPool shardPool = RoutingPool.getShardPool(shard, allocation);
            assertEquals(LOCAL_ONLY, shardPool);
            assertEquals(nodePool, shardPool);
        }
    }

    @LockFeatureFlag(WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)
    public void testShardPoolForPartialIndices() {
        String index = "test-index";
        IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .settings(settings(Version.CURRENT).put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true))
            .numberOfShards(PRIMARIES)
            .numberOfReplicas(REPLICAS)
            .build();
        RoutingPool indexPool = getIndexPool(indexMetadata);
        assertEquals(REMOTE_CAPABLE, indexPool);
    }

    @LockFeatureFlag(WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)
    public void testShardPoolForFullIndices() {
        String index = "test-index";
        IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .settings(settings(Version.CURRENT).put(IS_WARM_INDEX_SETTING.getKey(), false))
            .numberOfShards(PRIMARIES)
            .numberOfReplicas(REPLICAS)
            .build();
        RoutingPool indexPool = getIndexPool(indexMetadata);
        assertEquals(LOCAL_ONLY, indexPool);
    }
}
