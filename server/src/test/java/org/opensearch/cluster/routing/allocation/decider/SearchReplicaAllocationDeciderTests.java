/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.EmptyClusterInfoService;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.snapshots.EmptySnapshotsInfoService;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.opensearch.cluster.routing.allocation.decider.SearchReplicaAllocationDecider.SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING;

public class SearchReplicaAllocationDeciderTests extends OpenSearchAllocationTestCase {

    public void testSearchReplicaRoutingDedicatedIncludes() {
        // we aren't using a settingsModule here so we need to set feature flag gated setting
        Set<Setting<?>> settings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settings.add(SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), settings);
        Settings initialSettings = Settings.builder()
            .put("cluster.routing.allocation.search.replica.dedicated.include._id", "node1,node2")
            .build();

        SearchReplicaAllocationDecider filterAllocationDecider = new SearchReplicaAllocationDecider(initialSettings, clusterSettings);
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

        Decision.Single decision = (Decision.Single) filterAllocationDecider.canAllocate(
            searchReplica,
            state.getRoutingNodes().node("node2"),
            allocation
        );
        assertEquals(decision.toString(), Decision.Type.YES, decision.type());
        decision = (Decision.Single) filterAllocationDecider.canAllocate(searchReplica, state.getRoutingNodes().node("node1"), allocation);
        assertEquals(decision.toString(), Decision.Type.YES, decision.type());

        decision = (Decision.Single) filterAllocationDecider.canAllocate(regularReplica, state.getRoutingNodes().node("node2"), allocation);
        assertEquals(decision.toString(), Decision.Type.NO, decision.type());
        decision = (Decision.Single) filterAllocationDecider.canAllocate(regularReplica, state.getRoutingNodes().node("node1"), allocation);
        assertEquals(decision.toString(), Decision.Type.NO, decision.type());

        decision = (Decision.Single) filterAllocationDecider.canAllocate(primary, state.getRoutingNodes().node("node1"), allocation);
        assertEquals(decision.toString(), Decision.Type.NO, decision.type());
        decision = (Decision.Single) filterAllocationDecider.canAllocate(primary, state.getRoutingNodes().node("node2"), allocation);
        assertEquals(decision.toString(), Decision.Type.NO, decision.type());

        Settings updatedSettings = Settings.builder()
            .put("cluster.routing.allocation.search.replica.dedicated.include._id", "node2")
            .build();
        clusterSettings.applySettings(updatedSettings);

        decision = (Decision.Single) filterAllocationDecider.canAllocate(searchReplica, state.getRoutingNodes().node("node2"), allocation);
        assertEquals(decision.toString(), Decision.Type.YES, decision.type());
        decision = (Decision.Single) filterAllocationDecider.canAllocate(searchReplica, state.getRoutingNodes().node("node1"), allocation);
        assertEquals(decision.toString(), Decision.Type.NO, decision.type());
        decision = (Decision.Single) filterAllocationDecider.canRemain(searchReplica, state.getRoutingNodes().node("node1"), allocation);
        assertEquals(decision.toString(), Decision.Type.NO, decision.type());

        decision = (Decision.Single) filterAllocationDecider.canAllocate(regularReplica, state.getRoutingNodes().node("node2"), allocation);
        assertEquals(decision.toString(), Decision.Type.NO, decision.type());
        decision = (Decision.Single) filterAllocationDecider.canAllocate(regularReplica, state.getRoutingNodes().node("node1"), allocation);
        assertEquals(decision.toString(), Decision.Type.YES, decision.type());
        decision = (Decision.Single) filterAllocationDecider.canRemain(regularReplica, state.getRoutingNodes().node("node1"), allocation);
        assertEquals(decision.toString(), Decision.Type.YES, decision.type());

        decision = (Decision.Single) filterAllocationDecider.canAllocate(primary, state.getRoutingNodes().node("node1"), allocation);
        assertEquals(decision.toString(), Decision.Type.YES, decision.type());
        decision = (Decision.Single) filterAllocationDecider.canAllocate(primary, state.getRoutingNodes().node("node2"), allocation);
        assertEquals(decision.toString(), Decision.Type.NO, decision.type());
        decision = (Decision.Single) filterAllocationDecider.canRemain(primary, state.getRoutingNodes().node("node1"), allocation);
        assertEquals(decision.toString(), Decision.Type.YES, decision.type());
    }
}
