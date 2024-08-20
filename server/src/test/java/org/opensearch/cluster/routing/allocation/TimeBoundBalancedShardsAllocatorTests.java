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
import java.util.concurrent.atomic.AtomicBoolean;

public class TimeBoundBalancedShardsAllocatorTests extends OpenSearchAllocationTestCase {

    private final DiscoveryNode node1 = newNode("node1");
    private final DiscoveryNode node2 = newNode("node2");
    private final DiscoveryNode node3 = newNode("node3");

    public void testAllUnassignedShardsAllocatedWhenNoTimeOut() {
        Settings.Builder settings = Settings.builder();
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        BalancedShardsAllocator allocator = new TestBalancedShardsAllocator(settings.build(), clusterSettings, false);
        int numberOfIndices = 2;
        int numberOfShards = 5;
        int numberOfReplicas = 1;
        RoutingAllocation allocation = buildRoutingAllocation(yesAllocationDeciders(), numberOfIndices, numberOfShards, numberOfReplicas);
        allocator.allocate(allocation);
        List<ShardRouting> initializingShards = allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
        int node1Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node1.getId());
        int node2Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node2.getId());
        int node3Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node3.getId());
        assertEquals(numberOfIndices * (numberOfShards * (numberOfReplicas + 1)), initializingShards.size());
        assertEquals(0, allocation.routingNodes().unassigned().ignored().size());
        assertEquals(numberOfIndices * numberOfShards, node1Recoveries + node2Recoveries + node3Recoveries);
    }

    public void testAllUnassignedShardsIgnoredWhenTimedOut() {
        Settings.Builder settings = Settings.builder();
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        BalancedShardsAllocator allocator = new TestBalancedShardsAllocator(settings.build(), clusterSettings, true);
        int numberOfIndices = 2;
        int numberOfShards = 5;
        int numberOfReplicas = 1;
        RoutingAllocation allocation = buildRoutingAllocation(yesAllocationDeciders(), numberOfIndices, numberOfShards, numberOfReplicas);
        allocator.allocate(allocation);
        List<ShardRouting> initializingShards = allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
        int node1Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node1.getId());
        int node2Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node2.getId());
        int node3Recoveries = allocation.routingNodes().getInitialPrimariesIncomingRecoveries(node3.getId());
        assertEquals(0, initializingShards.size());
        assertEquals(numberOfIndices * (numberOfShards * (numberOfReplicas + 1)), allocation.routingNodes().unassigned().ignored().size());
        assertEquals(0, node1Recoveries + node2Recoveries + node3Recoveries);
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
        private final AtomicBoolean timedOut;

        public TestBalancedShardsAllocator(Settings settings, ClusterSettings clusterSettings, boolean timedOut) {
            super(settings, clusterSettings);
            this.timedOut = new AtomicBoolean(timedOut);
        }

        @Override
        protected boolean allocatorTimedOut(long currentTime) {
            return timedOut.get();
        }
    }
}
