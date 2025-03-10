/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.EmptyClusterInfoService;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingHelper;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.command.AllocationCommands;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.snapshots.EmptySnapshotsInfoService;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING;
import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;

public class SearchReplicaAllocationDeciderTests extends OpenSearchAllocationTestCase {

    public void testSearchReplicaRoutingDedicatedIncludes() {
        Set<Setting<?>> settings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), settings);
        SearchReplicaAllocationDecider filterAllocationDecider = new SearchReplicaAllocationDecider();
        AllocationDeciders allocationDeciders = new AllocationDeciders(
            Arrays.asList(
                filterAllocationDecider,
                new SameShardAllocationDecider(Settings.EMPTY, clusterSettings),
                new ReplicaAfterPrimaryActiveAllocationDecider()
            )
        );
        AllocationService service = new AllocationService(
            allocationDeciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );
        ClusterState state = FilterAllocationDeciderTests.createInitialClusterState(service, Settings.EMPTY, Settings.EMPTY);
        RoutingTable routingTable = state.routingTable();
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state, null, null, 0);
        allocation.debugDecision(true);

        ShardRouting searchReplica = ShardRouting.newUnassigned(
            routingTable.index("sourceIndex").shard(0).shardId(),
            false,
            true,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.NODE_LEFT, "")
        );

        ShardRouting regularReplica = ShardRouting.newUnassigned(
            routingTable.index("sourceIndex").shard(0).shardId(),
            false,
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );

        ShardRouting primary = ShardRouting.newUnassigned(
            routingTable.index("sourceIndex").shard(0).shardId(),
            true,
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );

        // Tests for canRemain
        // Can allocate searchReplica on search node
        Decision.Single decision = (Decision.Single) filterAllocationDecider.canAllocate(
            searchReplica,
            state.getRoutingNodes().node("node2"),
            allocation
        );
        assertEquals(decision.toString(), Decision.Type.YES, decision.type());

        // Cannot allocate searchReplica on data node
        decision = (Decision.Single) filterAllocationDecider.canAllocate(searchReplica, state.getRoutingNodes().node("node1"), allocation);
        assertEquals(decision.toString(), Decision.Type.NO, decision.type());

        // Cannot allocate regularReplica on search node
        decision = (Decision.Single) filterAllocationDecider.canAllocate(regularReplica, state.getRoutingNodes().node("node2"), allocation);
        assertEquals(decision.toString(), Decision.Type.NO, decision.type());

        // Can allocate regularReplica on data node
        decision = (Decision.Single) filterAllocationDecider.canAllocate(regularReplica, state.getRoutingNodes().node("node1"), allocation);
        assertEquals(decision.toString(), Decision.Type.YES, decision.type());

        // Can allocate primary on data node
        decision = (Decision.Single) filterAllocationDecider.canAllocate(primary, state.getRoutingNodes().node("node1"), allocation);
        assertEquals(decision.toString(), Decision.Type.YES, decision.type());

        // Cannot allocate primary on search node
        decision = (Decision.Single) filterAllocationDecider.canAllocate(primary, state.getRoutingNodes().node("node2"), allocation);
        assertEquals(decision.toString(), Decision.Type.NO, decision.type());

        // Tests for canRemain
        decision = (Decision.Single) filterAllocationDecider.canAllocate(searchReplica, state.getRoutingNodes().node("node2"), allocation);
        assertEquals(decision.toString(), Decision.Type.YES, decision.type());

        decision = (Decision.Single) filterAllocationDecider.canAllocate(searchReplica, state.getRoutingNodes().node("node1"), allocation);
        assertEquals(decision.toString(), Decision.Type.NO, decision.type());

        decision = (Decision.Single) filterAllocationDecider.canAllocate(regularReplica, state.getRoutingNodes().node("node2"), allocation);
        assertEquals(decision.toString(), Decision.Type.NO, decision.type());

        decision = (Decision.Single) filterAllocationDecider.canAllocate(regularReplica, state.getRoutingNodes().node("node1"), allocation);
        assertEquals(decision.toString(), Decision.Type.YES, decision.type());

        decision = (Decision.Single) filterAllocationDecider.canAllocate(primary, state.getRoutingNodes().node("node1"), allocation);
        assertEquals(decision.toString(), Decision.Type.YES, decision.type());

        decision = (Decision.Single) filterAllocationDecider.canAllocate(primary, state.getRoutingNodes().node("node2"), allocation);
        assertEquals(decision.toString(), Decision.Type.NO, decision.type());
    }

    public void testSearchReplicaWithThrottlingDecider_PrimaryBasedReplication() {
        TestGatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        // throttle outgoing on primary
        AllocationService strategy = createAllocationService(Settings.EMPTY, gatewayAllocator);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .numberOfSearchReplicas(1)
            )
            .build();

        ClusterState clusterState = initializeClusterStateWithSingleIndexAndShard(newNode("node1"), metadata, gatewayAllocator);
        clusterState = strategy.reroute(clusterState, "reroute");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertEquals(2, clusterState.routingTable().shardsWithState(STARTED).size());
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 0);
        // start a third node, we will try and move the SR to this node
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        // remove the primary and reroute - this would throw an NPE for search replicas but *not* regular.
        // regular replicas would get promoted to primary before the CanMoveAway call.
        clusterState = strategy.disassociateDeadNodes(
            ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node1")).build(),
            true,
            "test"
        );

        // attempt to move the replica
        AllocationService.CommandsResult commandsResult = strategy.reroute(
            clusterState,
            new AllocationCommands(new MoveAllocationCommand("test", 0, "node2", "node3")),
            true,
            false
        );

        assertEquals(commandsResult.explanations().explanations().size(), 1);
        assertEquals(commandsResult.explanations().explanations().get(0).decisions().type(), Decision.Type.NO);
        boolean isCorrectNoDecision = false;
        for (Decision decision : commandsResult.explanations().explanations().get(0).decisions().getDecisions()) {
            if (decision.label().equals(ThrottlingAllocationDecider.NAME)) {
                assertEquals("primary shard for this replica is not yet active", decision.getExplanation());
                assertEquals(Decision.Type.NO, decision.type());
                isCorrectNoDecision = true;
            }
        }
        assertTrue(isCorrectNoDecision);
    }

    public void testSearchReplicaWithThrottlingDeciderWithoutPrimary_RemoteStoreEnabled() {
        TestGatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        AllocationService strategy = createAllocationService(Settings.EMPTY, gatewayAllocator);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .numberOfSearchReplicas(1)
            )
            .build();

        ClusterState clusterState = initializeClusterStateWithSingleIndexAndShard(newRemoteNode("node1"), metadata, gatewayAllocator);

        clusterState = strategy.reroute(clusterState, "reroute");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        DiscoveryNode node2 = newRemoteNode("node2");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(node2)).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertEquals(2, clusterState.routingTable().shardsWithState(STARTED).size());
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 0);
        // start a third node, we will try and move the SR to this node
        DiscoveryNode node3 = newRemoteNode("node3");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(node3)).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        // remove the primary and reroute - this would throw an NPE for search replicas but *not* regular.
        // regular replicas would get promoted to primary before the CanMoveAway call.
        clusterState = strategy.disassociateDeadNodes(
            ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node1")).build(),
            true,
            "test"
        );

        // attempt to move the replica
        AllocationService.CommandsResult commandsResult = strategy.reroute(
            clusterState,
            new AllocationCommands(new MoveAllocationCommand("test", 0, "node2", "node3")),
            true,
            false
        );

        assertEquals(commandsResult.explanations().explanations().size(), 1);
        assertEquals(commandsResult.explanations().explanations().get(0).decisions().type(), Decision.Type.NO);
        boolean foundYesMessage = false;
        for (Decision decision : commandsResult.explanations().explanations().get(0).decisions().getDecisions()) {
            if (decision.label().equals(ThrottlingAllocationDecider.NAME)) {
                assertEquals("Remote based search replica below incoming recovery limit: [0 < 2]", decision.getExplanation());
                assertEquals(Decision.Type.YES, decision.type());
                foundYesMessage = true;
            }
        }
        assertTrue(foundYesMessage);
    }

    private ClusterState initializeClusterStateWithSingleIndexAndShard(
        DiscoveryNode primaryNode,
        Metadata metadata,
        TestGatewayAllocator gatewayAllocator
    ) {
        Metadata.Builder metadataBuilder = new Metadata.Builder(metadata);
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        IndexMetadata indexMetadata = metadata.index("test");
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
        initializePrimaryAndMarkInSync(indexMetadata.getIndex(), indexMetadataBuilder, gatewayAllocator, primaryNode);
        routingTableBuilder.addAsRecovery(indexMetadata);
        metadataBuilder.put(indexMetadata, false);
        RoutingTable routingTable = routingTableBuilder.build();
        return ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(DiscoveryNodes.builder().add(primaryNode))
            .metadata(metadataBuilder.build())
            .routingTable(routingTable)
            .build();
    }

    private void initializePrimaryAndMarkInSync(
        Index index,
        IndexMetadata.Builder indexMetadata,
        TestGatewayAllocator gatewayAllocator,
        DiscoveryNode primaryNode
    ) {
        final ShardRouting unassigned = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")
        );
        ShardRouting started = ShardRoutingHelper.moveToStarted(ShardRoutingHelper.initialize(unassigned, primaryNode.getId()));
        indexMetadata.putInSyncAllocationIds(0, Collections.singleton(started.allocationId().getId()));
        gatewayAllocator.addKnownAllocation(started);
    }

    private static DiscoveryNode newRemoteNode(String name) {
        return newNode(
            name,
            name,
            Map.of(
                REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY,
                "cluster-repo",
                REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY,
                "segment-repo",
                REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY,
                "translog-repo"
            )
        );
    }
}
