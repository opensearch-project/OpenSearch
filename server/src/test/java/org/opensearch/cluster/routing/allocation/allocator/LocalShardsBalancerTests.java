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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LocalShardsBalancerTests extends OpenSearchAllocationTestCase {

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

    public void testAvgPrimaryShardsPerNodeRefreshesWhenFilterChanges() {
        int numberOfShards = 6;
        Metadata metadata = buildMetadata(Metadata.builder(), 1, numberOfShards, 1, 0);
        RoutingTable routingTable = buildRoutingTable(metadata);
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3).add(node4).add(node5).add(node6))
            .build();

        // ---- Round 1: exclude {node4,node5,node6} -> 3 eligible -> 6 / 3 = 2.0
        RoutingAllocation allocation1 = new RoutingAllocation(
            new AllocationDeciders(
                Collections.singletonList(new StaticClusterFilterDecider(new HashSet<>(Arrays.asList("node4", "node5", "node6"))))
            ),
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            null,
            System.nanoTime()
        );
        LocalShardsBalancer balancer1 = new LocalShardsBalancer(
            logger,
            allocation1,
            null,
            mock(BalancedShardsAllocator.WeightFunction.class),
            0,
            false,
            false,
            true,
            false,
            null
        );
        assertEquals("round1 per-index", 2.0f, balancer1.avgPrimaryShardsPerNode("test_0"), 0.0001f);
        assertEquals("round1 cluster-level", 2.0f, balancer1.avgPrimaryShardsPerNode(), 0.0001f);

        // ---- Round 2: filter shrinks to {node6} -> 5 eligible -> 6 / 5 = 1.2
        RoutingAllocation allocation2 = new RoutingAllocation(
            new AllocationDeciders(
                Collections.singletonList(new StaticClusterFilterDecider(new HashSet<>(Collections.singletonList("node6"))))
            ),
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            null,
            System.nanoTime()
        );
        LocalShardsBalancer balancer2 = new LocalShardsBalancer(
            logger,
            allocation2,
            null,
            mock(BalancedShardsAllocator.WeightFunction.class),
            0,
            false,
            false,
            true,
            false,
            null
        );
        assertEquals("round2 per-index", 6.0f / 5.0f, balancer2.avgPrimaryShardsPerNode("test_0"), 0.0001f);
        assertEquals("round2 cluster-level", 6.0f / 5.0f, balancer2.avgPrimaryShardsPerNode(), 0.0001f);

        // ---- Round 3: filter cleared -> 6 eligible -> 6 / 6 = 1.0
        RoutingAllocation allocation3 = new RoutingAllocation(
            new AllocationDeciders(Collections.singletonList(new StaticClusterFilterDecider(Collections.emptySet()))),
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            null,
            System.nanoTime()
        );
        LocalShardsBalancer balancer3 = new LocalShardsBalancer(
            logger,
            allocation3,
            null,
            mock(BalancedShardsAllocator.WeightFunction.class),
            0,
            false,
            false,
            true,
            false,
            null
        );
        assertEquals("round3 per-index", 1.0f, balancer3.avgPrimaryShardsPerNode("test_0"), 0.0001f);
        assertEquals("round3 cluster-level", 1.0f, balancer3.avgPrimaryShardsPerNode(), 0.0001f);
    }

    public static class StaticClusterFilterDecider extends AllocationDecider {
        private final Set<String> excluded;

        public StaticClusterFilterDecider(Set<String> excluded) {
            this.excluded = excluded;
        }

        @Override
        public Decision canAllocateAnyShardToNode(RoutingNode node, RoutingAllocation allocation) {
            return excluded.contains(node.nodeId()) ? Decision.NO : Decision.ALWAYS;
        }
    }

}
