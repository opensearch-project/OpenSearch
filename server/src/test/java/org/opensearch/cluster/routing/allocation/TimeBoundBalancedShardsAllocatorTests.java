/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.common.settings.Settings;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;

public class TimeBoundBalancedShardsAllocatorTests extends OpenSearchAllocationTestCase {

    private final DiscoveryNode node1 = newNode("node1", "node1", Collections.singletonMap("zone", "1a"));
    private final DiscoveryNode node2 = newNode("node2", "node2", Collections.singletonMap("zone", "1b"));
    private final DiscoveryNode node3 = newNode("node3", "node3", Collections.singletonMap("zone", "1c"));

    public void testAllUnassignedShardsAllocatedWhenNoTimeOut() {
        int numberOfIndices = 2;
        int numberOfShards = 5;
        int numberOfReplicas = 1;
        int totalPrimaryCount = numberOfIndices * numberOfShards;
        int totalShardCount = numberOfIndices * (numberOfShards * (numberOfReplicas + 1));
        Settings.Builder settings = Settings.builder();
        // passing total shard count for timed out latch such that no shard times out
        BalancedShardsAllocator allocator = new TestBalancedShardsAllocator(settings.build(), new CountDownLatch(totalShardCount));
        Metadata metadata = buildMetadata(Metadata.builder(), numberOfIndices, numberOfShards, numberOfReplicas);
        RoutingTable routingTable = buildRoutingTable(metadata);
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        RoutingAllocation allocation = new RoutingAllocation(
            yesAllocationDeciders(),
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            null,
            System.nanoTime()
        );
        allocator.allocate(allocation);
        List<ShardRouting> initializingShards = allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
        int node1Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node1.getId());
        int node2Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node2.getId());
        int node3Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node3.getId());
        assertEquals(totalShardCount, initializingShards.size());
        assertEquals(0, allocation.routingNodes().unassigned().ignored().size());
        assertEquals(totalPrimaryCount, node1Recoveries + node2Recoveries + node3Recoveries);
    }

    public void testAllUnassignedShardsIgnoredWhenTimedOut() {
        int numberOfIndices = 2;
        int numberOfShards = 5;
        int numberOfReplicas = 1;
        int totalShardCount = numberOfIndices * (numberOfShards * (numberOfReplicas + 1));
        Settings.Builder settings = Settings.builder();
        // passing 0 for timed out latch such that all shard times out
        BalancedShardsAllocator allocator = new TestBalancedShardsAllocator(settings.build(), new CountDownLatch(0));
        Metadata metadata = buildMetadata(Metadata.builder(), numberOfIndices, numberOfShards, numberOfReplicas);
        RoutingTable routingTable = buildRoutingTable(metadata);
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        RoutingAllocation allocation = new RoutingAllocation(
            yesAllocationDeciders(),
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            null,
            System.nanoTime()
        );
        allocator.allocate(allocation);
        List<ShardRouting> initializingShards = allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
        int node1Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node1.getId());
        int node2Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node2.getId());
        int node3Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node3.getId());
        assertEquals(0, initializingShards.size());
        assertEquals(totalShardCount, allocation.routingNodes().unassigned().ignored().size());
        assertEquals(0, node1Recoveries + node2Recoveries + node3Recoveries);
    }

    public void testAllocatePartialPrimaryShardsUntilTimedOut() {
        int numberOfIndices = 2;
        int numberOfShards = 5;
        int numberOfReplicas = 1;
        int totalShardCount = numberOfIndices * (numberOfShards * (numberOfReplicas + 1));
        Settings.Builder settings = Settings.builder();
        int shardsToAllocate = randomIntBetween(1, numberOfShards * numberOfIndices);
        // passing shards to allocate for timed out latch such that only few primary shards are allocated in this reroute round
        BalancedShardsAllocator allocator = new TestBalancedShardsAllocator(settings.build(), new CountDownLatch(shardsToAllocate));
        Metadata metadata = buildMetadata(Metadata.builder(), numberOfIndices, numberOfShards, numberOfReplicas);
        RoutingTable routingTable = buildRoutingTable(metadata);
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        RoutingAllocation allocation = new RoutingAllocation(
            yesAllocationDeciders(),
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            null,
            System.nanoTime()
        );
        allocator.allocate(allocation);
        List<ShardRouting> initializingShards = allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
        int node1Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node1.getId());
        int node2Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node2.getId());
        int node3Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node3.getId());
        assertEquals(shardsToAllocate, initializingShards.size());
        assertEquals(totalShardCount - shardsToAllocate, allocation.routingNodes().unassigned().ignored().size());
        assertEquals(shardsToAllocate, node1Recoveries + node2Recoveries + node3Recoveries);
    }

    public void testAllocateAllPrimaryShardsAndPartialReplicaShardsUntilTimedOut() {
        int numberOfIndices = 2;
        int numberOfShards = 5;
        int numberOfReplicas = 1;
        int totalShardCount = numberOfIndices * (numberOfShards * (numberOfReplicas + 1));
        Settings.Builder settings = Settings.builder();
        int shardsToAllocate = randomIntBetween(numberOfShards * numberOfIndices, totalShardCount);
        // passing shards to allocate for timed out latch such that all primary shards and few replica shards are allocated in this reroute
        // round
        BalancedShardsAllocator allocator = new TestBalancedShardsAllocator(settings.build(), new CountDownLatch(shardsToAllocate));
        Metadata metadata = buildMetadata(Metadata.builder(), numberOfIndices, numberOfShards, numberOfReplicas);
        RoutingTable routingTable = buildRoutingTable(metadata);
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        RoutingAllocation allocation = new RoutingAllocation(
            yesAllocationDeciders(),
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            null,
            System.nanoTime()
        );
        allocator.allocate(allocation);
        List<ShardRouting> initializingShards = allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
        int node1Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node1.getId());
        int node2Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node2.getId());
        int node3Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node3.getId());
        assertEquals(shardsToAllocate, initializingShards.size());
        assertEquals(totalShardCount - shardsToAllocate, allocation.routingNodes().unassigned().ignored().size());
        assertEquals(numberOfShards * numberOfIndices, node1Recoveries + node2Recoveries + node3Recoveries);
    }

    public void testAllShardsMoveWhenExcludedAndTimeoutNotBreached() {
        int numberOfIndices = 3;
        int numberOfShards = 5;
        int numberOfReplicas = 1;
        int totalShardCount = numberOfIndices * (numberOfShards * (numberOfReplicas + 1));
        Metadata metadata = buildMetadata(Metadata.builder(), numberOfIndices, numberOfShards, numberOfReplicas);
        RoutingTable routingTable = buildRoutingTable(metadata);
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        MockAllocationService allocationService = createAllocationService();
        state = applyStartedShardsUntilNoChange(state, allocationService);
        // check all shards allocated
        assertEquals(0, state.getRoutingNodes().shardsWithState(INITIALIZING).size());
        assertEquals(totalShardCount, state.getRoutingNodes().shardsWithState(STARTED).size());
        int node1ShardCount = state.getRoutingNodes().node("node1").size();
        Settings settings = Settings.builder().put("cluster.routing.allocation.exclude.zone", "1a").build();
        int shardsToMove = 10 + 1000; // such that time out is never breached
        BalancedShardsAllocator allocator = new TestBalancedShardsAllocator(settings, new CountDownLatch(shardsToMove));
        RoutingAllocation allocation = new RoutingAllocation(
            allocationDecidersForExcludeAPI(settings),
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            null,
            System.nanoTime()
        );
        allocator.allocate(allocation);
        List<ShardRouting> relocatingShards = allocation.routingNodes().shardsWithState(ShardRoutingState.RELOCATING);
        assertEquals(node1ShardCount, relocatingShards.size());
    }

    public void testNoShardsMoveWhenExcludedAndTimeoutBreached() {
        int numberOfIndices = 3;
        int numberOfShards = 5;
        int numberOfReplicas = 1;
        int totalShardCount = numberOfIndices * (numberOfShards * (numberOfReplicas + 1));
        Metadata metadata = buildMetadata(Metadata.builder(), numberOfIndices, numberOfShards, numberOfReplicas);
        RoutingTable routingTable = buildRoutingTable(metadata);
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        MockAllocationService allocationService = createAllocationService();
        state = applyStartedShardsUntilNoChange(state, allocationService);
        // check all shards allocated
        assertEquals(0, state.getRoutingNodes().shardsWithState(INITIALIZING).size());
        assertEquals(totalShardCount, state.getRoutingNodes().shardsWithState(STARTED).size());
        Settings settings = Settings.builder().put("cluster.routing.allocation.exclude.zone", "1a").build();
        int shardsToMove = 0; // such that time out is never breached
        BalancedShardsAllocator allocator = new TestBalancedShardsAllocator(settings, new CountDownLatch(shardsToMove));
        RoutingAllocation allocation = new RoutingAllocation(
            allocationDecidersForExcludeAPI(settings),
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            null,
            System.nanoTime()
        );
        allocator.allocate(allocation);
        List<ShardRouting> relocatingShards = allocation.routingNodes().shardsWithState(ShardRoutingState.RELOCATING);
        assertEquals(0, relocatingShards.size());
    }

    public void testPartialShardsMoveWhenExcludedAndTimeoutBreached() {
        int numberOfIndices = 3;
        int numberOfShards = 5;
        int numberOfReplicas = 1;
        int totalShardCount = numberOfIndices * (numberOfShards * (numberOfReplicas + 1));
        Metadata metadata = buildMetadata(Metadata.builder(), numberOfIndices, numberOfShards, numberOfReplicas);
        RoutingTable routingTable = buildRoutingTable(metadata);
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        MockAllocationService allocationService = createAllocationService();
        state = applyStartedShardsUntilNoChange(state, allocationService);
        // check all shards allocated
        assertEquals(0, state.getRoutingNodes().shardsWithState(INITIALIZING).size());
        assertEquals(totalShardCount, state.getRoutingNodes().shardsWithState(STARTED).size());
        Settings settings = Settings.builder().put("cluster.routing.allocation.exclude.zone", "1a").build();
        // since for moves, it creates an iterator over shards which interleaves between nodes, hence
        // for shardsToMove=6, it will have 2 shards from node1, node2, node3 each attempting to move with only
        // shards from node1 can actually move. Hence, total moves that will be executed is 2 (6/3).
        int shardsToMove = 6; // such that time out is never breached
        BalancedShardsAllocator allocator = new TestBalancedShardsAllocator(settings, new CountDownLatch(shardsToMove));
        RoutingAllocation allocation = new RoutingAllocation(
            allocationDecidersForExcludeAPI(settings),
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            null,
            System.nanoTime()
        );
        allocator.allocate(allocation);
        List<ShardRouting> relocatingShards = allocation.routingNodes().shardsWithState(ShardRoutingState.RELOCATING);
        assertEquals(shardsToMove / 3, relocatingShards.size());
    }

    public void testClusterRebalancedWhenNotTimedOut() {
        int numberOfIndices = 1;
        int numberOfShards = 15;
        int numberOfReplicas = 1;
        int totalShardCount = numberOfIndices * (numberOfShards * (numberOfReplicas + 1));
        Metadata metadata = buildMetadata(Metadata.builder(), numberOfIndices, numberOfShards, numberOfReplicas);
        RoutingTable routingTable = buildRoutingTable(metadata);
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        MockAllocationService allocationService = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.exclude.zone", "1a").build()
        ); // such that no shards are allocated to node1
        state = applyStartedShardsUntilNoChange(state, allocationService);
        int node1ShardCount = state.getRoutingNodes().node("node1").size();
        // check all shards allocated
        assertEquals(0, state.getRoutingNodes().shardsWithState(INITIALIZING).size());
        assertEquals(totalShardCount, state.getRoutingNodes().shardsWithState(STARTED).size());
        assertEquals(0, node1ShardCount);
        Settings newSettings = Settings.builder().put("cluster.routing.allocation.exclude.zone", "").build();
        int shardsToMove = 1000; // such that time out is never breached
        BalancedShardsAllocator allocator = new TestBalancedShardsAllocator(newSettings, new CountDownLatch(shardsToMove));
        RoutingAllocation allocation = new RoutingAllocation(
            allocationDecidersForExcludeAPI(newSettings),
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            null,
            System.nanoTime()
        );
        allocator.allocate(allocation);
        List<ShardRouting> relocatingShards = allocation.routingNodes().shardsWithState(ShardRoutingState.RELOCATING);
        assertEquals(totalShardCount / 3, relocatingShards.size());
    }

    private RoutingTable buildRoutingTable(Metadata metadata) {
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (Map.Entry<String, IndexMetadata> entry : metadata.getIndices().entrySet()) {
            routingTableBuilder.addAsNew(entry.getValue());
        }
        return routingTableBuilder.build();
    }

    private Metadata buildMetadata(Metadata.Builder mb, int numberOfIndices, int numberOfShards, int numberOfReplicas) {
        for (int i = 0; i < numberOfIndices; i++) {
            mb.put(
                IndexMetadata.builder("test_" + i)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            );
        }

        return mb.build();
    }

    static class TestBalancedShardsAllocator extends BalancedShardsAllocator {
        private final CountDownLatch timedOutLatch;

        public TestBalancedShardsAllocator(Settings settings, CountDownLatch timedOutLatch) {
            super(settings);
            this.timedOutLatch = timedOutLatch;
        }

        @Override
        protected boolean allocatorTimedOut(long currentTime) {
            if (timedOutLatch.getCount() == 0) {
                return true;
            }
            timedOutLatch.countDown();
            return false;
        }
    }
}
