/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.Version;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.AllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.IGNORE_THROTTLE_FOR_REMOTE_RESTORE;

public class DecideAllocateUnassignedTests extends OpenSearchAllocationTestCase {
    public void testAllocateUnassignedRemoteRestore_IgnoreThrottle() {
        final String[] indices = { "idx1" };
        // Create a cluster state with 1 indices, each with 1 started primary shard, and only
        // one node initially so that all primary shards get allocated to the same node.
        //
        // When we add 1 more 1 index with 1 started primary shard and 1 more node , if the new node throttles the recovery
        // shard should get assigned on the older node if IgnoreThrottle is set to true
        ClusterState clusterState = ClusterStateCreationUtils.state(1, indices, 1);
        clusterState = addNodesToClusterState(clusterState, 1);
        clusterState = addRestoringIndexToClusterState(clusterState, "idx2");
        List<AllocationDecider> allocationDeciders = getAllocationDecidersThrottleOnNode1();
        RoutingAllocation routingAllocation = newRoutingAllocation(new AllocationDeciders(allocationDeciders), clusterState);
        // allocate and get the node that is now relocating
        Settings build = Settings.builder().put(IGNORE_THROTTLE_FOR_REMOTE_RESTORE.getKey(), true).build();
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(build);
        allocator.allocate(routingAllocation);
        assertEquals(routingAllocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), "node_0");
        assertEquals(routingAllocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).getIndexName(), "idx2");
        assertFalse(routingAllocation.routingNodes().hasUnassignedPrimaries());
    }

    public void testAllocateUnassignedRemoteRestore() {
        final String[] indices = { "idx1" };
        // Create a cluster state with 1 indices, each with 1 started primary shard, and only
        // one node initially so that all primary shards get allocated to the same node.
        //
        // When we add 1 more 1 index with 1 started primary shard and 1 more node , if the new node throttles the recovery
        // shard should remain unassigned if IgnoreThrottle is set to false
        ClusterState clusterState = ClusterStateCreationUtils.state(1, indices, 1);
        clusterState = addNodesToClusterState(clusterState, 1);
        clusterState = addRestoringIndexToClusterState(clusterState, "idx2");
        List<AllocationDecider> allocationDeciders = getAllocationDecidersThrottleOnNode1();
        RoutingAllocation routingAllocation = newRoutingAllocation(new AllocationDeciders(allocationDeciders), clusterState);
        // allocate and get the node that is now relocating
        Settings build = Settings.builder().put(IGNORE_THROTTLE_FOR_REMOTE_RESTORE.getKey(), false).build();
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(build);
        allocator.allocate(routingAllocation);
        assertEquals(routingAllocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), 0);
        assertTrue(routingAllocation.routingNodes().hasUnassignedPrimaries());
    }

    private static List<AllocationDecider> getAllocationDecidersThrottleOnNode1() {
        // Allocation Deciders to throttle on `node_1`
        final Set<String> throttleNodes = new HashSet<>();
        throttleNodes.add("node_1");
        AllocationDecider allocationDecider = new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                if (throttleNodes.contains(node.nodeId())) {
                    return Decision.THROTTLE;
                }
                return Decision.YES;
            }
        };
        List<AllocationDecider> allocationDeciders = Arrays.asList(allocationDecider);
        return allocationDeciders;
    }

    private ClusterState addNodesToClusterState(ClusterState clusterState, int nodeId) {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(clusterState.nodes());
        DiscoveryNode discoveryNode = newNode("node_" + nodeId);
        nodesBuilder.add(discoveryNode);
        return ClusterState.builder(clusterState).nodes(nodesBuilder).build();
    }

    private ClusterState addRestoringIndexToClusterState(ClusterState clusterState, String index) {
        final int primaryTerm = 1 + randomInt(200);
        final ShardId shardId = new ShardId(index, "_na_", 0);

        IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .settings(
                Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_CREATION_DATE, System.currentTimeMillis())
            )
            .primaryTerm(0, primaryTerm)
            .build();

        IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
        UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.EXISTING_INDEX_RESTORED, null);
        indexShardRoutingBuilder.addShard(
            TestShardRouting.newShardRoutingRemoteRestore(index, shardId, null, null, true, ShardRoutingState.UNASSIGNED, unassignedInfo)
        );
        final IndexShardRoutingTable indexShardRoutingTable = indexShardRoutingBuilder.build();

        IndexMetadata.Builder indexMetadataBuilder = new IndexMetadata.Builder(indexMetadata);
        indexMetadataBuilder.putInSyncAllocationIds(
            0,
            indexShardRoutingTable.activeShards()
                .stream()
                .map(ShardRouting::allocationId)
                .map(AllocationId::getId)
                .collect(Collectors.toSet())
        );
        ClusterState.Builder state = ClusterState.builder(clusterState);
        state.metadata(Metadata.builder(clusterState.metadata()).put(indexMetadataBuilder.build(), false).generateClusterUuidIfNeeded());
        state.routingTable(
            RoutingTable.builder(clusterState.routingTable())
                .add(IndexRoutingTable.builder(indexMetadata.getIndex()).addIndexShard(indexShardRoutingTable))
                .build()
        );
        return state.build();
    }

}
