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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.RemoteShardsBalancerBaseTestCase;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;

import java.util.Collections;
import java.util.stream.Collectors;

public class TargetPoolAllocationDeciderTests extends RemoteShardsBalancerBaseTestCase {
    public void testTargetPoolHybridAllocationDecisions() {
        ClusterState clusterState = createInitialCluster(3, 3, 2, 2);
        AllocationService service = this.createRemoteCapableAllocationService();
        clusterState = allocateShardsAndBalance(clusterState, service);

        // Add an unassigned primary shard for force allocation checks
        Metadata metadata = Metadata.builder(clusterState.metadata())
            .put(IndexMetadata.builder("test_local_unassigned").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder(clusterState.routingTable())
            .addAsNew(metadata.index("test_local_unassigned"))
            .build();
        clusterState = ClusterState.builder(clusterState).metadata(metadata).routingTable(routingTable).build();

        // Add remote index unassigned primary
        clusterState = createRemoteIndex(clusterState, "test_remote_unassigned");

        RoutingNodes defaultRoutingNodes = clusterState.getRoutingNodes();
        RoutingAllocation globalAllocation = getRoutingAllocation(clusterState, defaultRoutingNodes);

        ShardRouting localShard = clusterState.routingTable()
            .allShards(getIndexName(0, false))
            .stream()
            .filter(ShardRouting::primary)
            .collect(Collectors.toList())
            .get(0);
        ShardRouting remoteShard = clusterState.routingTable()
            .allShards(getIndexName(0, true))
            .stream()
            .filter(ShardRouting::primary)
            .collect(Collectors.toList())
            .get(0);
        ShardRouting unassignedLocalShard = clusterState.routingTable()
            .allShards("test_local_unassigned")
            .stream()
            .filter(ShardRouting::primary)
            .collect(Collectors.toList())
            .get(0);
        ShardRouting unassignedRemoteShard = clusterState.routingTable()
            .allShards("test_remote_unassigned")
            .stream()
            .filter(ShardRouting::primary)
            .collect(Collectors.toList())
            .get(0);
        IndexMetadata localIdx = globalAllocation.metadata().getIndexSafe(localShard.index());
        IndexMetadata remoteIdx = globalAllocation.metadata().getIndexSafe(remoteShard.index());
        String localNodeId = LOCAL_NODE_PREFIX;
        for (RoutingNode routingNode : globalAllocation.routingNodes()) {
            if (routingNode.nodeId().startsWith(LOCAL_NODE_PREFIX)) {
                localNodeId = routingNode.nodeId();
                break;
            }
        }
        String remoteNodeId = remoteShard.currentNodeId();
        RoutingNode localOnlyNode = defaultRoutingNodes.node(localNodeId);
        RoutingNode remoteCapableNode = defaultRoutingNodes.node(remoteNodeId);

        AllocationDeciders deciders = new AllocationDeciders(Collections.singletonList(new TargetPoolAllocationDecider()));

        // Incompatible Pools
        assertEquals(Decision.NO.type(), deciders.canAllocate(remoteShard, localOnlyNode, globalAllocation).type());
        assertEquals(Decision.NO.type(), deciders.canAllocate(remoteIdx, localOnlyNode, globalAllocation).type());
        assertEquals(Decision.NO.type(), deciders.canForceAllocatePrimary(unassignedRemoteShard, localOnlyNode, globalAllocation).type());

        // Compatible Pools
        assertEquals(Decision.YES.type(), deciders.canAllocate(localShard, remoteCapableNode, globalAllocation).type());
        assertEquals(Decision.YES.type(), deciders.canAllocate(localIdx, remoteCapableNode, globalAllocation).type());
        assertEquals(Decision.YES.type(), deciders.canAllocate(remoteShard, remoteCapableNode, globalAllocation).type());
        assertEquals(Decision.YES.type(), deciders.canAllocate(remoteIdx, remoteCapableNode, globalAllocation).type());
        assertEquals(Decision.YES.type(), deciders.canAllocate(localShard, localOnlyNode, globalAllocation).type());
        assertEquals(Decision.YES.type(), deciders.canAllocate(localIdx, localOnlyNode, globalAllocation).type());
        assertEquals(
            Decision.YES.type(),
            deciders.canForceAllocatePrimary(unassignedRemoteShard, remoteCapableNode, globalAllocation).type()
        );
        assertEquals(Decision.YES.type(), deciders.canForceAllocatePrimary(unassignedLocalShard, localOnlyNode, globalAllocation).type());
        assertEquals(
            Decision.YES.type(),
            deciders.canForceAllocatePrimary(unassignedLocalShard, remoteCapableNode, globalAllocation).type()
        );

        // Verify only remote nodes are used for auto expand replica decision for remote index
        assertEquals(Decision.YES.type(), deciders.shouldAutoExpandToNode(localIdx, remoteCapableNode.node(), globalAllocation).type());
        assertEquals(Decision.NO.type(), deciders.shouldAutoExpandToNode(remoteIdx, localOnlyNode.node(), globalAllocation).type());
        assertEquals(Decision.YES.type(), deciders.shouldAutoExpandToNode(localIdx, localOnlyNode.node(), globalAllocation).type());
        assertEquals(Decision.YES.type(), deciders.shouldAutoExpandToNode(remoteIdx, remoteCapableNode.node(), globalAllocation).type());
    }

    public void testTargetPoolDedicatedSearchNodeAllocationDecisions() {
        ClusterState clusterState = createInitialCluster(3, 3, true, 2, 2);
        AllocationService service = this.createRemoteCapableAllocationService();
        clusterState = allocateShardsAndBalance(clusterState, service);

        // Add an unassigned primary shard for force allocation checks
        Metadata metadata = Metadata.builder(clusterState.metadata())
            .put(IndexMetadata.builder("test_local_unassigned").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder(clusterState.routingTable())
            .addAsNew(metadata.index("test_local_unassigned"))
            .build();
        clusterState = ClusterState.builder(clusterState).metadata(metadata).routingTable(routingTable).build();

        // Add remote index unassigned primary
        clusterState = createRemoteIndex(clusterState, "test_remote_unassigned");

        RoutingNodes defaultRoutingNodes = clusterState.getRoutingNodes();
        RoutingAllocation globalAllocation = getRoutingAllocation(clusterState, defaultRoutingNodes);

        ShardRouting localShard = clusterState.routingTable()
            .allShards(getIndexName(0, false))
            .stream()
            .filter(ShardRouting::primary)
            .collect(Collectors.toList())
            .get(0);
        ShardRouting remoteShard = clusterState.routingTable()
            .allShards(getIndexName(0, true))
            .stream()
            .filter(ShardRouting::primary)
            .collect(Collectors.toList())
            .get(0);
        ShardRouting unassignedLocalShard = clusterState.routingTable()
            .allShards("test_local_unassigned")
            .stream()
            .filter(ShardRouting::primary)
            .collect(Collectors.toList())
            .get(0);
        ShardRouting unassignedRemoteShard = clusterState.routingTable()
            .allShards("test_remote_unassigned")
            .stream()
            .filter(ShardRouting::primary)
            .collect(Collectors.toList())
            .get(0);
        IndexMetadata localIdx = globalAllocation.metadata().getIndexSafe(localShard.index());
        IndexMetadata remoteIdx = globalAllocation.metadata().getIndexSafe(remoteShard.index());
        String localNodeId = LOCAL_NODE_PREFIX;
        for (RoutingNode routingNode : globalAllocation.routingNodes()) {
            if (routingNode.nodeId().startsWith(LOCAL_NODE_PREFIX)) {
                localNodeId = routingNode.nodeId();
                break;
            }
        }
        String remoteNodeId = remoteShard.currentNodeId();
        RoutingNode localOnlyNode = defaultRoutingNodes.node(localNodeId);
        RoutingNode remoteCapableNode = defaultRoutingNodes.node(remoteNodeId);

        AllocationDeciders deciders = new AllocationDeciders(Collections.singletonList(new TargetPoolAllocationDecider()));

        // Incompatible Pools
        assertEquals(Decision.NO.type(), deciders.canAllocate(remoteShard, localOnlyNode, globalAllocation).type());
        assertEquals(Decision.NO.type(), deciders.canAllocate(remoteIdx, localOnlyNode, globalAllocation).type());
        assertEquals(Decision.NO.type(), deciders.canForceAllocatePrimary(unassignedRemoteShard, localOnlyNode, globalAllocation).type());
        // A dedicated warm node should not accept local shards and indices.
        assertEquals(Decision.NO.type(), deciders.canAllocate(localShard, remoteCapableNode, globalAllocation).type());
        assertEquals(Decision.NO.type(), deciders.canAllocate(localIdx, remoteCapableNode, globalAllocation).type());
        assertEquals(
            Decision.NO.type(),
            deciders.canForceAllocatePrimary(unassignedLocalShard, remoteCapableNode, globalAllocation).type()
        );

        // Compatible Pools
        assertEquals(Decision.YES.type(), deciders.canAllocate(remoteShard, remoteCapableNode, globalAllocation).type());
        assertEquals(Decision.YES.type(), deciders.canAllocate(remoteIdx, remoteCapableNode, globalAllocation).type());
        assertEquals(Decision.YES.type(), deciders.canAllocate(localShard, localOnlyNode, globalAllocation).type());
        assertEquals(Decision.YES.type(), deciders.canAllocate(localIdx, localOnlyNode, globalAllocation).type());
        assertEquals(
            Decision.YES.type(),
            deciders.canForceAllocatePrimary(unassignedRemoteShard, remoteCapableNode, globalAllocation).type()
        );
        assertEquals(Decision.YES.type(), deciders.canForceAllocatePrimary(unassignedLocalShard, localOnlyNode, globalAllocation).type());

        // Verify only compatible nodes are used for auto expand replica decision for remote index and local index
        assertEquals(Decision.NO.type(), deciders.shouldAutoExpandToNode(localIdx, remoteCapableNode.node(), globalAllocation).type());
        assertEquals(Decision.NO.type(), deciders.shouldAutoExpandToNode(remoteIdx, localOnlyNode.node(), globalAllocation).type());
        assertEquals(Decision.YES.type(), deciders.shouldAutoExpandToNode(localIdx, localOnlyNode.node(), globalAllocation).type());
        assertEquals(Decision.YES.type(), deciders.shouldAutoExpandToNode(remoteIdx, remoteCapableNode.node(), globalAllocation).type());
    }

    public void testDebugMessage() {
        ClusterState clusterState = createInitialCluster(3, 3, true, 2, 2);
        AllocationService service = this.createRemoteCapableAllocationService();
        clusterState = allocateShardsAndBalance(clusterState, service);

        // Add an unassigned primary shard for force allocation checks
        Metadata metadata = Metadata.builder(clusterState.metadata())
            .put(IndexMetadata.builder("test_local_unassigned").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder(clusterState.routingTable())
            .addAsNew(metadata.index("test_local_unassigned"))
            .build();
        clusterState = ClusterState.builder(clusterState).metadata(metadata).routingTable(routingTable).build();

        // Add remote index unassigned primary
        clusterState = createRemoteIndex(clusterState, "test_remote_unassigned");

        RoutingNodes defaultRoutingNodes = clusterState.getRoutingNodes();
        RoutingAllocation globalAllocation = getRoutingAllocation(clusterState, defaultRoutingNodes);
        globalAllocation.setDebugMode(RoutingAllocation.DebugMode.ON);

        ShardRouting localShard = clusterState.routingTable()
            .allShards(getIndexName(0, false))
            .stream()
            .filter(ShardRouting::primary)
            .collect(Collectors.toList())
            .get(0);
        ShardRouting remoteShard = clusterState.routingTable()
            .allShards(getIndexName(0, true))
            .stream()
            .filter(ShardRouting::primary)
            .collect(Collectors.toList())
            .get(0);
        ShardRouting unassignedLocalShard = clusterState.routingTable()
            .allShards("test_local_unassigned")
            .stream()
            .filter(ShardRouting::primary)
            .collect(Collectors.toList())
            .get(0);
        ShardRouting unassignedRemoteShard = clusterState.routingTable()
            .allShards("test_remote_unassigned")
            .stream()
            .filter(ShardRouting::primary)
            .collect(Collectors.toList())
            .get(0);
        IndexMetadata localIdx = globalAllocation.metadata().getIndexSafe(localShard.index());
        IndexMetadata remoteIdx = globalAllocation.metadata().getIndexSafe(remoteShard.index());
        String localNodeId = LOCAL_NODE_PREFIX;
        for (RoutingNode routingNode : globalAllocation.routingNodes()) {
            if (routingNode.nodeId().startsWith(LOCAL_NODE_PREFIX)) {
                localNodeId = routingNode.nodeId();
                break;
            }
        }
        String remoteNodeId = remoteShard.currentNodeId();
        RoutingNode localOnlyNode = defaultRoutingNodes.node(localNodeId);
        RoutingNode remoteCapableNode = defaultRoutingNodes.node(remoteNodeId);

        TargetPoolAllocationDecider targetPoolAllocationDecider = new TargetPoolAllocationDecider();
        Decision decision = targetPoolAllocationDecider.canAllocate(localShard, remoteCapableNode, globalAllocation);
        assertEquals(
            "Routing pools are incompatible. Shard pool: [LOCAL_ONLY], node pool: [REMOTE_CAPABLE] without [data] role",
            decision.getExplanation()
        );

        decision = targetPoolAllocationDecider.canAllocate(remoteShard, localOnlyNode, globalAllocation);
        assertEquals("Routing pools are incompatible. Shard pool: [REMOTE_CAPABLE], node pool: [LOCAL_ONLY]", decision.getExplanation());

        decision = targetPoolAllocationDecider.canAllocate(remoteShard, remoteCapableNode, globalAllocation);
        assertEquals("Routing pools are compatible. Shard pool: [REMOTE_CAPABLE], node pool: [REMOTE_CAPABLE]", decision.getExplanation());

        decision = targetPoolAllocationDecider.canAllocate(localIdx, remoteCapableNode, globalAllocation);
        assertEquals(
            "Routing pools are incompatible. Index pool: [LOCAL_ONLY], node pool: [REMOTE_CAPABLE] without [data] role",
            decision.getExplanation()
        );

        decision = targetPoolAllocationDecider.canAllocate(remoteIdx, localOnlyNode, globalAllocation);
        assertEquals("Routing pools are incompatible. Index pool: [REMOTE_CAPABLE], node pool: [LOCAL_ONLY]", decision.getExplanation());

        decision = targetPoolAllocationDecider.canAllocate(remoteIdx, remoteCapableNode, globalAllocation);
        assertEquals("Routing pools are compatible. Index pool: [REMOTE_CAPABLE], node pool: [REMOTE_CAPABLE]", decision.getExplanation());
    }
}
