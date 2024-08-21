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
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class TimeBoundBalancedShardsAllocatorTests extends OpenSearchAllocationTestCase {

    private final DiscoveryNode node1 = newNode("node1");
    private final DiscoveryNode node2 = newNode("node2");
    private final DiscoveryNode node3 = newNode("node3");

    public void testAllUnassignedShardsAllocatedWhenNoTimeOut() {
        int numberOfIndices = 2;
        int numberOfShards = 5;
        int numberOfReplicas = 1;
        int totalPrimaryCount = numberOfIndices * numberOfShards;
        int totalShardCount = numberOfIndices * (numberOfShards * (numberOfReplicas + 1));
        Settings.Builder settings = Settings.builder();
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        // passing total shard count for timed out latch such that no shard times out
        BalancedShardsAllocator allocator = new TestBalancedShardsAllocator(
            settings.build(),
            clusterSettings,
            new CountDownLatch(totalShardCount)
        );
        RoutingAllocation allocation = buildRoutingAllocation(yesAllocationDeciders(), numberOfIndices, numberOfShards, numberOfReplicas);
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
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        // passing 0 for timed out latch such that all shard times out
        BalancedShardsAllocator allocator = new TestBalancedShardsAllocator(settings.build(), clusterSettings, new CountDownLatch(0));
        RoutingAllocation allocation = buildRoutingAllocation(yesAllocationDeciders(), numberOfIndices, numberOfShards, numberOfReplicas);
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
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        int shardsToAllocate = randomIntBetween(1, numberOfShards * numberOfIndices);
        // passing shards to allocate for timed out latch such that only few primary shards are allocated in this reroute round
        BalancedShardsAllocator allocator = new TestBalancedShardsAllocator(
            settings.build(),
            clusterSettings,
            new CountDownLatch(shardsToAllocate)
        );
        RoutingAllocation allocation = buildRoutingAllocation(yesAllocationDeciders(), numberOfIndices, numberOfShards, numberOfReplicas);
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
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        int shardsToAllocate = randomIntBetween(numberOfShards * numberOfIndices, totalShardCount);
        // passing shards to allocate for timed out latch such that all primary shards and few replica shards are allocated in this reroute
        // round
        BalancedShardsAllocator allocator = new TestBalancedShardsAllocator(
            settings.build(),
            clusterSettings,
            new CountDownLatch(shardsToAllocate)
        );
        RoutingAllocation allocation = buildRoutingAllocation(yesAllocationDeciders(), numberOfIndices, numberOfShards, numberOfReplicas);
        allocator.allocate(allocation);
        List<ShardRouting> initializingShards = allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
        int node1Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node1.getId());
        int node2Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node2.getId());
        int node3Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node3.getId());
        assertEquals(shardsToAllocate, initializingShards.size());
        assertEquals(totalShardCount - shardsToAllocate, allocation.routingNodes().unassigned().ignored().size());
        assertEquals(numberOfShards * numberOfIndices, node1Recoveries + node2Recoveries + node3Recoveries);
    }

    private RoutingAllocation buildRoutingAllocation(
        AllocationDeciders deciders,
        int numberOfIndices,
        int numberOfShards,
        int numberOfReplicas
    ) {
        Metadata metadata = buildMetadata(Metadata.builder(), numberOfIndices, numberOfShards, numberOfReplicas);
        RoutingTable routingTable = buildRoutingTable(metadata);
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        return new RoutingAllocation(deciders, new RoutingNodes(state, false), state, ClusterInfo.EMPTY, null, System.nanoTime());
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

        public TestBalancedShardsAllocator(Settings settings, ClusterSettings clusterSettings, CountDownLatch timedOutLatch) {
            super(settings, clusterSettings);
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
