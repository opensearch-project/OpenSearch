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
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
import org.opensearch.test.FeatureFlagSetter;
import org.junit.Before;

import static org.opensearch.cluster.routing.RoutingPool.LOCAL_ONLY;
import static org.opensearch.cluster.routing.RoutingPool.REMOTE_CAPABLE;
import static org.opensearch.cluster.routing.RoutingPool.getIndexPool;
import static org.opensearch.index.IndexModule.INDEX_STORE_LOCALITY_SETTING;

public class ShardsTieringAllocationTests extends TieringAllocationBaseTestCase {

    @Before
    public void setup() {
        FeatureFlagSetter.set(FeatureFlags.TIERED_REMOTE_INDEX);
    }

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
            IndexModule.TieringState.HOT_TO_WARM.name(),
            IndexModule.DataLocalityType.PARTIAL.name()
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

    public void testShardPoolForPartialIndices() {
        String index = "test-index";
        IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .settings(settings(Version.CURRENT).put(INDEX_STORE_LOCALITY_SETTING.getKey(), IndexModule.DataLocalityType.PARTIAL.name()))
            .numberOfShards(PRIMARIES)
            .numberOfReplicas(REPLICAS)
            .build();
        RoutingPool indexPool = getIndexPool(indexMetadata);
        assertEquals(REMOTE_CAPABLE, indexPool);
    }

    public void testShardPoolForFullIndices() {
        String index = "test-index";
        IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .settings(settings(Version.CURRENT).put(INDEX_STORE_LOCALITY_SETTING.getKey(), IndexModule.DataLocalityType.FULL.name()))
            .numberOfShards(PRIMARIES)
            .numberOfReplicas(REPLICAS)
            .build();
        RoutingPool indexPool = getIndexPool(indexMetadata);
        assertEquals(LOCAL_ONLY, indexPool);
    }
}
