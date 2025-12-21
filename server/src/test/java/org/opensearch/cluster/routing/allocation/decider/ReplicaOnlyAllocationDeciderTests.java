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
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.snapshots.EmptySnapshotsInfoService;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;

public class ReplicaOnlyAllocationDeciderTests extends OpenSearchAllocationTestCase {

    private ReplicaOnlyAllocationDecider decider;
    private AllocationDeciders allocationDeciders;
    private ClusterState clusterState;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        decider = new ReplicaOnlyAllocationDecider();

        Set<org.opensearch.common.settings.Setting<?>> settings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), settings);

        allocationDeciders = new AllocationDeciders(
            Arrays.asList(decider, new SameShardAllocationDecider(Settings.EMPTY, clusterSettings), new ReplicaAfterPrimaryActiveAllocationDecider())
        );

        AllocationService service = new AllocationService(
            allocationDeciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );

        // Create a cluster with 2 nodes: one regular data node, one replica-only node
        DiscoveryNode dataNode = newNode("data-node", Collections.singleton(DiscoveryNodeRole.DATA_ROLE));
        DiscoveryNode replicaOnlyNode = newNode("replica-only-node", Collections.singleton(DiscoveryNodeRole.REPLICA_ONLY_ROLE));

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("auto-expand-index")
                    .settings(
                        settings(Version.CURRENT).put(SETTING_AUTO_EXPAND_REPLICAS, "0-all")
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(1)
            )
            .put(IndexMetadata.builder("regular-index").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .nodes(org.opensearch.cluster.node.DiscoveryNodes.builder().add(dataNode).add(replicaOnlyNode))
            .build();

        clusterState = ClusterState.builder(clusterState)
            .routingTable(org.opensearch.cluster.routing.RoutingTable.builder().addAsNew(metadata.index("auto-expand-index")).addAsNew(metadata.index("regular-index")).build())
            .build();
    }

    public void testPrimaryCannotAllocateToReplicaOnlyNode() {
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState.getRoutingNodes(), clusterState, null, null, 0);
        allocation.debugDecision(true);

        ShardRouting primaryShard = ShardRouting.newUnassigned(
            clusterState.routingTable().index("auto-expand-index").shard(0).shardId(),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );

        RoutingNode replicaOnlyNode = clusterState.getRoutingNodes().node("replica-only-node");
        RoutingNode dataNode = clusterState.getRoutingNodes().node("data-node");

        // Primary cannot be allocated to replica-only node
        Decision decision = decider.canAllocate(primaryShard, replicaOnlyNode, allocation);
        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.toString(), decision.toString().contains("primary shard"));
        assertTrue(decision.toString(), decision.toString().contains("replica-only"));

        // Primary can be allocated to data node
        decision = decider.canAllocate(primaryShard, dataNode, allocation);
        assertEquals(Decision.Type.YES, decision.type());
    }

    public void testReplicaCanAllocateToReplicaOnlyNodeWithAutoExpandAll() {
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState.getRoutingNodes(), clusterState, null, null, 0);
        allocation.debugDecision(true);

        ShardRouting replicaShard = ShardRouting.newUnassigned(
            clusterState.routingTable().index("auto-expand-index").shard(0).shardId(),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );

        RoutingNode replicaOnlyNode = clusterState.getRoutingNodes().node("replica-only-node");

        // Replica from auto-expand 0-all index CAN be allocated to replica-only node
        Decision decision = decider.canAllocate(replicaShard, replicaOnlyNode, allocation);
        assertEquals(Decision.Type.YES, decision.type());
        assertTrue(decision.toString(), decision.toString().contains("auto-expand"));
    }

    public void testReplicaCannotAllocateToReplicaOnlyNodeWithoutAutoExpandAll() {
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState.getRoutingNodes(), clusterState, null, null, 0);
        allocation.debugDecision(true);

        ShardRouting replicaShard = ShardRouting.newUnassigned(
            clusterState.routingTable().index("regular-index").shard(0).shardId(),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );

        RoutingNode replicaOnlyNode = clusterState.getRoutingNodes().node("replica-only-node");
        RoutingNode dataNode = clusterState.getRoutingNodes().node("data-node");

        // Replica from regular index CANNOT be allocated to replica-only node
        Decision decision = decider.canAllocate(replicaShard, replicaOnlyNode, allocation);
        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.toString(), decision.toString().contains("does not have auto_expand_replicas"));

        // Replica from regular index CAN be allocated to data node
        decision = decider.canAllocate(replicaShard, dataNode, allocation);
        assertEquals(Decision.Type.YES, decision.type());
    }

    public void testForceAllocatePrimaryBlocked() {
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState.getRoutingNodes(), clusterState, null, null, 0);
        allocation.debugDecision(true);

        ShardRouting primaryShard = ShardRouting.newUnassigned(
            clusterState.routingTable().index("auto-expand-index").shard(0).shardId(),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );

        RoutingNode replicaOnlyNode = clusterState.getRoutingNodes().node("replica-only-node");
        RoutingNode dataNode = clusterState.getRoutingNodes().node("data-node");

        // Force allocation of primary is STILL blocked on replica-only node
        Decision decision = decider.canForceAllocatePrimary(primaryShard, replicaOnlyNode, allocation);
        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.toString(), decision.toString().contains("cannot be force allocated"));

        // Force allocation allowed on data node
        decision = decider.canForceAllocatePrimary(primaryShard, dataNode, allocation);
        assertEquals(Decision.Type.YES, decision.type());
    }

    public void testCanRemainFollowsSameLogic() {
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState.getRoutingNodes(), clusterState, null, null, 0);
        allocation.debugDecision(true);

        RoutingNode replicaOnlyNode = clusterState.getRoutingNodes().node("replica-only-node");
        RoutingNode dataNode = clusterState.getRoutingNodes().node("data-node");

        // Primary cannot remain on replica-only node
        ShardRouting primaryShard = ShardRouting.newUnassigned(
            clusterState.routingTable().index("auto-expand-index").shard(0).shardId(),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );
        Decision decision = decider.canRemain(primaryShard, replicaOnlyNode, allocation);
        assertEquals(Decision.Type.NO, decision.type());

        // Replica from auto-expand index CAN remain on replica-only node
        ShardRouting autoExpandReplica = ShardRouting.newUnassigned(
            clusterState.routingTable().index("auto-expand-index").shard(0).shardId(),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );
        decision = decider.canRemain(autoExpandReplica, replicaOnlyNode, allocation);
        assertEquals(Decision.Type.YES, decision.type());

        // Replica from regular index CANNOT remain on replica-only node
        ShardRouting regularReplica = ShardRouting.newUnassigned(
            clusterState.routingTable().index("regular-index").shard(0).shardId(),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );
        decision = decider.canRemain(regularReplica, replicaOnlyNode, allocation);
        assertEquals(Decision.Type.NO, decision.type());
    }

    public void testShouldAutoExpandToNodeForAutoExpandAllIndex() {
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState.getRoutingNodes(), clusterState, null, null, 0);
        allocation.debugDecision(true);

        IndexMetadata autoExpandIndex = clusterState.metadata().index("auto-expand-index");
        DiscoveryNode replicaOnlyNode = clusterState.nodes().get("replica-only-node");
        DiscoveryNode dataNode = clusterState.nodes().get("data-node");

        // Replica-only node SHOULD be included for auto-expand 0-all index
        Decision decision = decider.shouldAutoExpandToNode(autoExpandIndex, replicaOnlyNode, allocation);
        assertEquals(Decision.Type.YES, decision.type());
        assertTrue(decision.toString(), decision.toString().contains("eligible for auto-expand"));

        // Data node always included
        decision = decider.shouldAutoExpandToNode(autoExpandIndex, dataNode, allocation);
        assertEquals(Decision.Type.YES, decision.type());
    }

    public void testShouldAutoExpandToNodeForNonAutoExpandIndex() {
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState.getRoutingNodes(), clusterState, null, null, 0);
        allocation.debugDecision(true);

        IndexMetadata regularIndex = clusterState.metadata().index("regular-index");
        DiscoveryNode replicaOnlyNode = clusterState.nodes().get("replica-only-node");
        DiscoveryNode dataNode = clusterState.nodes().get("data-node");

        // Replica-only node should NOT be included for non-auto-expand index
        Decision decision = decider.shouldAutoExpandToNode(regularIndex, replicaOnlyNode, allocation);
        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.toString(), decision.toString().contains("without auto_expand_replicas: 0-all"));

        // Data node still included
        decision = decider.shouldAutoExpandToNode(regularIndex, dataNode, allocation);
        assertEquals(Decision.Type.YES, decision.type());
    }

    public void testRegularDataNodeAcceptsAllShards() {
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState.getRoutingNodes(), clusterState, null, null, 0);
        allocation.debugDecision(true);

        RoutingNode dataNode = clusterState.getRoutingNodes().node("data-node");

        // Regular data node can accept primaries
        ShardRouting primaryShard = ShardRouting.newUnassigned(
            clusterState.routingTable().index("auto-expand-index").shard(0).shardId(),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );
        Decision decision = decider.canAllocate(primaryShard, dataNode, allocation);
        assertEquals(Decision.Type.YES, decision.type());

        // Regular data node can accept replicas from auto-expand index
        ShardRouting autoExpandReplica = ShardRouting.newUnassigned(
            clusterState.routingTable().index("auto-expand-index").shard(0).shardId(),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );
        decision = decider.canAllocate(autoExpandReplica, dataNode, allocation);
        assertEquals(Decision.Type.YES, decision.type());

        // Regular data node can accept replicas from regular index
        ShardRouting regularReplica = ShardRouting.newUnassigned(
            clusterState.routingTable().index("regular-index").shard(0).shardId(),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );
        decision = decider.canAllocate(regularReplica, dataNode, allocation);
        assertEquals(Decision.Type.YES, decision.type());
    }

    public void testAutoExpandWithDifferentSettings() {
        // Test auto-expand with different settings (0-5, 1-all, etc.)
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("auto-expand-0-5")
                    .settings(settings(Version.CURRENT).put(SETTING_AUTO_EXPAND_REPLICAS, "0-5"))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
            )
            .put(
                IndexMetadata.builder("auto-expand-1-all")
                    .settings(settings(Version.CURRENT).put(SETTING_AUTO_EXPAND_REPLICAS, "1-all"))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
            )
            .build();

        ClusterState testState = ClusterState.builder(clusterState).metadata(metadata).build();
        testState = ClusterState.builder(testState)
            .routingTable(
                org.opensearch.cluster.routing.RoutingTable.builder()
                    .addAsNew(metadata.index("auto-expand-0-5"))
                    .addAsNew(metadata.index("auto-expand-1-all"))
                    .build()
            )
            .build();

        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, testState.getRoutingNodes(), testState, null, null, 0);
        RoutingNode replicaOnlyNode = testState.getRoutingNodes().node("replica-only-node");

        // auto-expand 0-5: NOT 0-all, should be blocked
        ShardRouting replica05 = ShardRouting.newUnassigned(
            testState.routingTable().index("auto-expand-0-5").shard(0).shardId(),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );
        Decision decision = decider.canAllocate(replica05, replicaOnlyNode, allocation);
        assertEquals(Decision.Type.NO, decision.type());

        // auto-expand 1-all: NOT 0-all, should be blocked
        ShardRouting replica1all = ShardRouting.newUnassigned(
            testState.routingTable().index("auto-expand-1-all").shard(0).shardId(),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );
        decision = decider.canAllocate(replica1all, replicaOnlyNode, allocation);
        assertEquals(Decision.Type.NO, decision.type());
    }
}
