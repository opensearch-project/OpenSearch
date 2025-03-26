/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
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
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.Decision.Type;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.snapshots.EmptySnapshotsInfoService;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_RESIZE_SOURCE_NAME;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_RESIZE_SOURCE_UUID;
import static org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;
import static org.opensearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;

public class FilterAllocationDeciderTests extends OpenSearchAllocationTestCase {

    public void testFilterInitialRecovery() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        FilterAllocationDecider filterAllocationDecider = new FilterAllocationDecider(Settings.EMPTY, clusterSettings);
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
        ClusterState state = createInitialClusterState(
            service,
            Settings.builder().put("index.routing.allocation.initial_recovery._id", "node2").build()
        );
        RoutingTable routingTable = state.routingTable();

        // we can initially only allocate on node2
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), INITIALIZING);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).currentNodeId(), "node2");
        routingTable = service.applyFailedShard(state, routingTable.index("idx").shard(0).shards().get(0), randomBoolean()).routingTable();
        state = ClusterState.builder(state).routingTable(routingTable).build();
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), UNASSIGNED);
        assertNull(routingTable.index("idx").shard(0).shards().get(0).currentNodeId());

        // after failing the shard we are unassigned since the node is denylisted and we can't initialize on the other node
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state, null, null, 0);
        allocation.debugDecision(true);
        Decision.Single decision = (Decision.Single) filterAllocationDecider.canAllocate(
            routingTable.index("idx").shard(0).primaryShard(),
            state.getRoutingNodes().node("node2"),
            allocation
        );
        assertEquals(Type.YES, decision.type());
        assertEquals("node passes include/exclude/require filters", decision.getExplanation());
        ShardRouting primaryShard = routingTable.index("idx").shard(0).primaryShard();
        decision = (Decision.Single) filterAllocationDecider.canAllocate(
            routingTable.index("idx").shard(0).primaryShard(),
            state.getRoutingNodes().node("node1"),
            allocation
        );
        assertEquals(Type.NO, decision.type());
        if (primaryShard.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
            assertEquals(
                "initial allocation of the shrunken index is only allowed on nodes [_id:\"node2\"] that "
                    + "hold a copy of every shard in the index",
                decision.getExplanation()
            );
        } else {
            assertEquals("initial allocation of the index is only allowed on nodes [_id:\"node2\"]", decision.getExplanation());
        }

        state = service.reroute(state, "try allocate again");
        routingTable = state.routingTable();
        assertEquals(routingTable.index("idx").shard(0).primaryShard().state(), INITIALIZING);
        assertEquals(routingTable.index("idx").shard(0).primaryShard().currentNodeId(), "node2");

        state = startShardsAndReroute(service, state, routingTable.index("idx").shard(0).shardsWithState(INITIALIZING));
        routingTable = state.routingTable();

        // ok now we are started and can be allocated anywhere!! lets see...
        // first create another copy
        assertEquals(routingTable.index("idx").shard(0).replicaShards().get(0).state(), INITIALIZING);
        assertEquals(routingTable.index("idx").shard(0).replicaShards().get(0).currentNodeId(), "node1");
        state = startShardsAndReroute(service, state, routingTable.index("idx").shard(0).replicaShardsWithState(INITIALIZING));
        routingTable = state.routingTable();
        assertEquals(routingTable.index("idx").shard(0).replicaShards().get(0).state(), STARTED);
        assertEquals(routingTable.index("idx").shard(0).replicaShards().get(0).currentNodeId(), "node1");

        // now remove the node of the other copy and fail the current
        DiscoveryNode node1 = state.nodes().resolveNode("node1");
        state = service.disassociateDeadNodes(
            ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).remove("node1")).build(),
            true,
            "test"
        );
        state = service.applyFailedShard(state, routingTable.index("idx").shard(0).primaryShard(), randomBoolean());

        // now bring back node1 and see it's assigned
        state = service.reroute(ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).add(node1)).build(), "test");
        routingTable = state.routingTable();
        assertEquals(routingTable.index("idx").shard(0).primaryShard().state(), INITIALIZING);
        assertEquals(routingTable.index("idx").shard(0).primaryShard().currentNodeId(), "node1");

        allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state, null, null, 0);
        allocation.debugDecision(true);
        decision = (Decision.Single) filterAllocationDecider.canAllocate(
            routingTable.index("idx").shard(0).shards().get(0),
            state.getRoutingNodes().node("node2"),
            allocation
        );
        assertEquals(Type.YES, decision.type());
        assertEquals("node passes include/exclude/require filters", decision.getExplanation());
        decision = (Decision.Single) filterAllocationDecider.canAllocate(
            routingTable.index("idx").shard(0).shards().get(0),
            state.getRoutingNodes().node("node1"),
            allocation
        );
        assertEquals(Type.YES, decision.type());
        assertEquals("node passes include/exclude/require filters", decision.getExplanation());
    }

    public void testTierFilterIgnored() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        FilterAllocationDecider filterAllocationDecider = new FilterAllocationDecider(Settings.EMPTY, clusterSettings);
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
        ClusterState state = createInitialClusterState(
            service,
            Settings.builder()
                .put("index.routing.allocation.require._tier", "data_cold")
                .put("index.routing.allocation.include._tier", "data_cold")
                .put("index.routing.allocation.include._tier_preference", "data_cold")
                .put("index.routing.allocation.exclude._tier", "data_cold")
                .build(),
            Settings.builder()
                .put("cluster.routing.allocation.require._tier", "data_cold")
                .put("cluster.routing.allocation.include._tier", "data_cold")
                .put("cluster.routing.allocation.exclude._tier", "data_cold")
                .build()
        );
        RoutingTable routingTable = state.routingTable();
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state, null, null, 0);
        allocation.debugDecision(true);
        allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state, null, null, 0);
        allocation.debugDecision(true);
        Decision.Single decision = (Decision.Single) filterAllocationDecider.canAllocate(
            routingTable.index("idx").shard(0).shards().get(0),
            state.getRoutingNodes().node("node2"),
            allocation
        );
        assertEquals(decision.toString(), Type.YES, decision.type());
        assertEquals("node passes include/exclude/require filters", decision.getExplanation());
        decision = (Decision.Single) filterAllocationDecider.canAllocate(
            routingTable.index("idx").shard(0).shards().get(0),
            state.getRoutingNodes().node("node1"),
            allocation
        );
        assertEquals(Type.YES, decision.type());
        assertEquals("node passes include/exclude/require filters", decision.getExplanation());
    }

    private void filterSettingsUpdateHelper(
        final Settings initialSettings,
        final Type type1,
        final String explanation1,
        final Settings updatedSettings,
        final Type type2,
        final String explanation2
    ) {
        ClusterSettings clusterSettings = new ClusterSettings(initialSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        FilterAllocationDecider filterAllocationDecider = new FilterAllocationDecider(initialSettings, clusterSettings);
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
        ClusterState state = createInitialClusterState(service, Settings.EMPTY, Settings.EMPTY);
        RoutingTable routingTable = state.routingTable();
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state, null, null, 0);
        allocation.debugDecision(true);
        Decision.Single decision = (Decision.Single) filterAllocationDecider.canAllocate(
            routingTable.index("idx").shard(0).shards().get(0),
            state.getRoutingNodes().node("node2"),
            allocation
        );
        assertEquals(decision.toString(), type1, decision.type());
        assertEquals(explanation1, decision.getExplanation());

        clusterSettings.applySettings(updatedSettings);
        decision = (Decision.Single) filterAllocationDecider.canAllocate(
            routingTable.index("idx").shard(0).shards().get(0),
            state.getRoutingNodes().node("node2"),
            allocation
        );
        assertEquals(decision.toString(), type2, decision.type());
        assertEquals(explanation2, decision.getExplanation());
    }

    public void testFilterUpdateAddNew() {
        filterSettingsUpdateHelper(
            Settings.builder().put("cluster.routing.allocation.require.attr", "attr1").build(),
            Type.NO,
            "node does not match cluster setting [cluster.routing.allocation.require] filters [attr:\"attr1\"]",
            Settings.builder().put("cluster.routing.allocation.require.zone", "zone1").build(),
            Type.NO,
            "node does not match cluster setting [cluster.routing.allocation.require] filters [zone:\"zone1\",attr:\"attr1\"]"
        );
    }

    public void testFilterUpdateRemovePartial() {
        filterSettingsUpdateHelper(
            Settings.builder()
                .put("cluster.routing.allocation.require.attr", "attr1")
                .put("cluster.routing.allocation.require.zone", "zone1")
                .build(),
            Type.NO,
            "node does not match cluster setting [cluster.routing.allocation.require] filters [zone:\"zone1\",attr:\"attr1\"]",
            Settings.builder()
                .put("cluster.routing.allocation.require.attr", "")
                .put("cluster.routing.allocation.require._tier_preference", "hot")
                .build(),
            Type.NO,
            "node does not match cluster setting [cluster.routing.allocation.require] filters [zone:\"zone1\"]"
        );
    }

    public void testFilterUpdateRemoveAll() {
        filterSettingsUpdateHelper(
            Settings.builder()
                .put("cluster.routing.allocation.require.flag", "flag1")
                .put("cluster.routing.allocation.require.zone", "zone1")
                .build(),
            Type.NO,
            "node does not match cluster setting [cluster.routing.allocation.require] filters [flag:\"flag1\",zone:\"zone1\"]",
            Settings.builder()
                .put("cluster.routing.allocation.require.flag", "")
                .put("cluster.routing.allocation.require.zone", "")
                .build(),
            Type.YES,
            "node passes include/exclude/require filters"
        );
    }

    private ClusterState createInitialClusterState(AllocationService service, Settings indexSettings) {
        return createInitialClusterState(service, indexSettings, Settings.EMPTY);
    }

    static ClusterState createInitialClusterState(AllocationService service, Settings idxSettings, Settings clusterSettings) {
        Metadata.Builder metadata = Metadata.builder();
        metadata.persistentSettings(clusterSettings);
        final Settings.Builder indexSettings = settings(Version.CURRENT).put(idxSettings);
        final IndexMetadata sourceIndex;
        // put a fake closed source index
        sourceIndex = IndexMetadata.builder("sourceIndex")
            .settings(settings(Version.CURRENT))
            .numberOfShards(2)
            .numberOfReplicas(0)
            .putInSyncAllocationIds(0, Collections.singleton("aid0"))
            .putInSyncAllocationIds(1, Collections.singleton("aid1"))
            .build();
        metadata.put(sourceIndex, false);
        indexSettings.put(INDEX_RESIZE_SOURCE_UUID.getKey(), sourceIndex.getIndexUUID());
        indexSettings.put(INDEX_RESIZE_SOURCE_NAME.getKey(), sourceIndex.getIndex().getName());
        final IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder("idx")
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1);
        final IndexMetadata indexMetadata = indexMetadataBuilder.build();
        metadata.put(indexMetadata, false);
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsFromCloseToOpen(sourceIndex);
        routingTableBuilder.addAsNew(indexMetadata);

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newSearchNode("node2")))
            .build();
        return service.reroute(clusterState, "reroute");
    }

    public void testInvalidIPFilter() {
        String ipKey = randomFrom("_ip", "_host_ip", "_publish_ip");
        Setting<String> filterSetting = randomFrom(
            IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING,
            IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING,
            IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING
        );
        String invalidIP = randomFrom("192..168.1.1", "192.300.1.1");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            IndexScopedSettings indexScopedSettings = new IndexScopedSettings(Settings.EMPTY, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
            indexScopedSettings.updateDynamicSettings(
                Settings.builder().put(filterSetting.getKey() + ipKey, invalidIP).build(),
                Settings.builder().put(Settings.EMPTY),
                Settings.builder(),
                "test ip validation"
            );
        });
        assertEquals("invalid IP address [" + invalidIP + "] for [" + filterSetting.getKey() + ipKey + "]", e.getMessage());
    }

    public void testNull() {
        Setting<String> filterSetting = randomFrom(
            IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING,
            IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING,
            IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING
        );

        IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT).putNull(filterSetting.getKey() + "name"))
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();
    }

    public void testWildcardIPFilter() {
        String ipKey = randomFrom("_ip", "_host_ip", "_publish_ip");
        Setting<String> filterSetting = randomFrom(
            IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING,
            IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING,
            IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING
        );
        String wildcardIP = randomFrom("192.168.*", "192.*.1.1");
        IndexScopedSettings indexScopedSettings = new IndexScopedSettings(Settings.EMPTY, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        indexScopedSettings.updateDynamicSettings(
            Settings.builder().put(filterSetting.getKey() + ipKey, wildcardIP).build(),
            Settings.builder().put(Settings.EMPTY),
            Settings.builder(),
            "test ip validation"
        );
    }

    public void testMixedModeRemoteStoreAllocation() {
        // For mixed mode remote store direction cluster's existing indices replica creation ,
        // we don't consider filter allocation decider for replica of existing indices
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Settings initialSettings = Settings.builder()
            .put("cluster.routing.allocation.exclude._id", "node2")
            .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), RemoteStoreNodeService.CompatibilityMode.MIXED)
            .put(MIGRATION_DIRECTION_SETTING.getKey(), RemoteStoreNodeService.Direction.REMOTE_STORE)
            .build();

        FilterAllocationDecider filterAllocationDecider = new FilterAllocationDecider(initialSettings, clusterSettings);
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
        ClusterState state = createInitialClusterState(service, Settings.EMPTY, Settings.EMPTY);
        RoutingTable routingTable = state.routingTable();
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state, null, null, 0);
        allocation.debugDecision(true);
        ShardRouting sr = ShardRouting.newUnassigned(
            routingTable.index("sourceIndex").shard(0).shardId(),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.NODE_LEFT, "")
        );
        Decision.Single decision = (Decision.Single) filterAllocationDecider.canAllocate(
            sr,
            state.getRoutingNodes().node("node2"),
            allocation
        );
        assertEquals(decision.toString(), Type.YES, decision.type());

        sr = ShardRouting.newUnassigned(
            routingTable.index("sourceIndex").shard(0).shardId(),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );
        decision = (Decision.Single) filterAllocationDecider.canAllocate(sr, state.getRoutingNodes().node("node2"), allocation);
        assertEquals(decision.toString(), Type.NO, decision.type());
    }
}
