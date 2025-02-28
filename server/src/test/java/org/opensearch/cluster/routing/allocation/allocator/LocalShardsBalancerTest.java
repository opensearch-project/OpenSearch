/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.allocator;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.AllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LocalShardsBalancerTest extends OpenSearchAllocationTestCase {

    private final DiscoveryNode node1 = newNode("node1", "node1", Collections.singletonMap("zone", "1a"));
    private final DiscoveryNode node2 = newNode("node2", "node2", Collections.singletonMap("zone", "1b"));
    private final DiscoveryNode node3 = newNode("node3", "node3", Collections.singletonMap("zone", "1c"));
    private final DiscoveryNode node4 = newNode("node4", "node4", Collections.singletonMap("zone", "1a"));
    private final DiscoveryNode node5 = newNode("node5", "node5", Collections.singletonMap("zone", "1b"));
    private final DiscoveryNode node6 = newNode("node6", "node6", Collections.singletonMap("zone", "1c"));

    public void testAllocateUnassignedWhenAllShardsCanBeAllocated() {
        int numberOfIndices = 2;
        int numberOfShards = 1;
        int numberOfReplicas = 2;
        int numberOfSearchReplicas = 3;

        Metadata metadata = buildMetadata(Metadata.builder(), numberOfIndices, numberOfShards, numberOfReplicas, numberOfSearchReplicas);
        RoutingTable routingTable = buildRoutingTable(metadata);
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3).add(node4).add(node5).add(node6))
            .build();

        RoutingAllocation allocation = new RoutingAllocation(
            yesAllocationDeciders(),
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            null,
            System.nanoTime()
        );

        BalancedShardsAllocator.WeightFunction weightFunction = mock(BalancedShardsAllocator.WeightFunction.class);
        when(weightFunction.weightWithAllocationConstraints(any(), any(), any())).thenReturn(0.5F);

        final ShardsBalancer localShardsBalancer = new LocalShardsBalancer(
            logger,
            allocation,
            null,
            weightFunction,
            0,
            false,
            false,
            false,
            null
        );

        localShardsBalancer.allocateUnassigned();

        List<ShardRouting> initializingShards = allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
        assertEquals(12, initializingShards.size());

        List<ShardRouting> unassignedShards = allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED);
        assertEquals(0, unassignedShards.size());
    }

    public void testAllocateUnassignedWhenSearchShardsCannotBeAllocated() {
        int numberOfIndices = 2;
        int numberOfShards = 1;
        int numberOfReplicas = 2;
        int numberOfSearchReplicas = 3;

        Metadata metadata = buildMetadata(Metadata.builder(), numberOfIndices, numberOfShards, numberOfReplicas, numberOfSearchReplicas);
        RoutingTable routingTable = buildRoutingTable(metadata);
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3).add(node4).add(node5).add(node6))
            .build();

        RoutingAllocation allocation = new RoutingAllocation(
            provideAllocationDecidersWithNoDecisionForSearchReplica(),
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            null,
            System.nanoTime()
        );

        BalancedShardsAllocator.WeightFunction weightFunction = mock(BalancedShardsAllocator.WeightFunction.class);
        when(weightFunction.weightWithAllocationConstraints(any(), any(), any())).thenReturn(0.5F);

        final ShardsBalancer localShardsBalancer = new LocalShardsBalancer(
            logger,
            allocation,
            null,
            weightFunction,
            0,
            false,
            false,
            false,
            null
        );

        localShardsBalancer.allocateUnassigned();

        List<ShardRouting> initializingShards = allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
        assertEquals(6, initializingShards.size());

        List<ShardRouting> unassignedShards = allocation.routingNodes().unassigned().ignored();
        assertEquals(6, unassignedShards.size());
    }

    public void testAllocateUnassignedWhenRegularReplicaShardsCannotBeAllocated() {
        int numberOfIndices = 2;
        int numberOfShards = 1;
        int numberOfReplicas = 2;
        int numberOfSearchReplicas = 3;

        Metadata metadata = buildMetadata(Metadata.builder(), numberOfIndices, numberOfShards, numberOfReplicas, numberOfSearchReplicas);
        RoutingTable routingTable = buildRoutingTable(metadata);
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3).add(node4).add(node5).add(node6))
            .build();

        RoutingAllocation allocation = new RoutingAllocation(
            provideAllocationDecidersWithNoDecisionForRegularReplica(),
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            null,
            System.nanoTime()
        );

        BalancedShardsAllocator.WeightFunction weightFunction = mock(BalancedShardsAllocator.WeightFunction.class);
        when(weightFunction.weightWithAllocationConstraints(any(), any(), any())).thenReturn(0.5F);

        final ShardsBalancer localShardsBalancer = new LocalShardsBalancer(
            logger,
            allocation,
            null,
            weightFunction,
            0,
            false,
            false,
            false,
            null
        );

        localShardsBalancer.allocateUnassigned();

        List<ShardRouting> initializingShards = allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
        assertEquals(8, initializingShards.size());

        List<ShardRouting> unassignedShards = allocation.routingNodes().unassigned().ignored();
        assertEquals(4, unassignedShards.size());
    }

    private RoutingTable buildRoutingTable(Metadata metadata) {
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (Map.Entry<String, IndexMetadata> entry : metadata.getIndices().entrySet()) {
            routingTableBuilder.addAsNew(entry.getValue());
        }
        return routingTableBuilder.build();
    }

    private Metadata buildMetadata(
        Metadata.Builder mb,
        int numberOfIndices,
        int numberOfShards,
        int numberOfReplicas,
        int numberOfSearchReplicas
    ) {
        for (int i = 0; i < numberOfIndices; i++) {
            mb.put(
                IndexMetadata.builder("test_" + i)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
                    .numberOfSearchReplicas(numberOfSearchReplicas)
            );
        }

        return mb.build();
    }

    private AllocationDeciders provideAllocationDecidersWithNoDecisionForSearchReplica() {
        return new AllocationDeciders(Arrays.asList(new TestAllocateDecision((shardRouting -> {
            if (shardRouting.isSearchOnly()) {
                return Decision.NO;
            } else {
                return Decision.YES;
            }
        })), new SameShardAllocationDecider(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)))
        );
    }

    private AllocationDeciders provideAllocationDecidersWithNoDecisionForRegularReplica() {
        return new AllocationDeciders(Arrays.asList(new TestAllocateDecision((shardRouting -> {
            if (!shardRouting.isSearchOnly() && !shardRouting.primary()) {
                return Decision.NO;
            } else {
                return Decision.YES;
            }
        })), new SameShardAllocationDecider(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)))
        );
    }

    public static class TestAllocateDecision extends AllocationDecider {

        private final Function<ShardRouting, Decision> decider;

        public TestAllocateDecision(Function<ShardRouting, Decision> decider) {
            this.decider = decider;
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return decider.apply(shardRouting);
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
            return decider.apply(shardRouting);
        }
    }
}
