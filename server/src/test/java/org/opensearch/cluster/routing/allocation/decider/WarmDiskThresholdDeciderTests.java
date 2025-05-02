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
import org.opensearch.cluster.*;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.opensearch.index.store.remote.filecache.FileCacheStats;
import org.opensearch.snapshots.EmptySnapshotsInfoService;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.util.*;

import static org.hamcrest.Matchers.equalTo;

public class WarmDiskThresholdDeciderTests extends OpenSearchAllocationTestCase {

    WarmDiskThresholdDecider makeDecider(Settings settings) {
        return new WarmDiskThresholdDecider(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    /**
     * Tests that the WarmDiskThresholdDecider returns a YES decision when allocating a shard with available space.
     */
    public void testCanAllocateSufficientFreeSpace() {
        Settings diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "300b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "200b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "100b")
            .put(FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey(), 5.0)
            .build();

        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 2000L); // 2000 bytes
        shardSizes.put("[test][0][r]", 2000L);
        shardSizes.put("[test2][0][p]", 1000L); // 1000 bytes
        shardSizes.put("[test2][0][r]", 1000L);

        Map<String, FileCacheStats> fileCacheStatsMap = new HashMap<>();
        fileCacheStatsMap.put("node1", new FileCacheStats(0, 0, 1000, 0, 0, 0, 0));
        fileCacheStatsMap.put("node2", new FileCacheStats(0, 0, 1000, 0, 0, 0, 0));

        final Map<String, DiskUsage> usages = new HashMap<>();
        final ClusterInfo clusterInfo = new DiskThresholdDeciderTests.DevNullClusterInfo(usages, usages, shardSizes, fileCacheStatsMap);

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationDeciders deciders = new AllocationDeciders(
            new HashSet<>(Arrays.asList(new SameShardAllocationDecider(Settings.EMPTY, clusterSettings), makeDecider(diskSettings)))
        );

        ClusterInfoService cis = () -> {
            logger.info("--> calling fake getClusterInfo");
            return clusterInfo;
        };
        AllocationService strategy = new AllocationService(
            deciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            cis,
            EmptySnapshotsInfoService.INSTANCE
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(warmIndexSettings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(warmIndexSettings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).addAsNew(metadata.index("test2")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        Set<DiscoveryNodeRole> defaultWithWarmRole = new HashSet<>(CLUSTER_MANAGER_DATA_ROLES);
        defaultWithWarmRole.add(DiscoveryNodeRole.WARM_ROLE);

        logger.info("--> adding two warm nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1", defaultWithWarmRole)).add(newNode("node2", defaultWithWarmRole)))
            .build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logShardStates(clusterState);
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(2));
    }

    /**
     * Tests that the WarmDiskThresholdDecider returns a NO decision when allocating a shard with insufficient available space.
     */
    public void testCanAllocateInSufficientFreeSpace() {
        Settings diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), 0.7)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), 0.8)
            .put(FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey(), 5.0)
            .build();

        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 4000L); // 5000 bytes
        shardSizes.put("[test][0][r]", 4000L);
        shardSizes.put("[test2][0][p]", 1000L); // 1000 bytes
        shardSizes.put("[test2][0][r]", 1000L);

        Map<String, FileCacheStats> fileCacheStatsMap = new HashMap<>();
        fileCacheStatsMap.put("node1", new FileCacheStats(0, 0, 1000, 0, 0, 0, 0));
        fileCacheStatsMap.put("node2", new FileCacheStats(0, 0, 1000, 0, 0, 0, 0));

        final Map<String, DiskUsage> usages = new HashMap<>();
        final ClusterInfo clusterInfo = new DiskThresholdDeciderTests.DevNullClusterInfo(usages, usages, shardSizes, fileCacheStatsMap);

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationDeciders deciders = new AllocationDeciders(
            new HashSet<>(Arrays.asList(new SameShardAllocationDecider(Settings.EMPTY, clusterSettings), makeDecider(diskSettings)))
        );

        ClusterInfoService cis = () -> {
            logger.info("--> calling fake getClusterInfo");
            return clusterInfo;
        };
        AllocationService strategy = new AllocationService(
            deciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            cis,
            EmptySnapshotsInfoService.INSTANCE
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(warmIndexSettings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(warmIndexSettings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).addAsNew(metadata.index("test2")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        Set<DiscoveryNodeRole> defaultWithWarmRole = new HashSet<>(CLUSTER_MANAGER_DATA_ROLES);
        defaultWithWarmRole.add(DiscoveryNodeRole.WARM_ROLE);

        logger.info("--> adding two warm nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1", defaultWithWarmRole)).add(newNode("node2", defaultWithWarmRole)))
            .build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logShardStates(clusterState);
        // Assert that test indexes shards (P and R) are not allocated due to insufficient space
        for (ShardRouting shardRouting : clusterState.getRoutingNodes().unassigned()) {
            if (shardRouting.index().getName().equals("test")) {
                assertThat(shardRouting.state(), equalTo(ShardRoutingState.UNASSIGNED));
            }
        }
        assertThat(clusterState.getRoutingNodes().unassigned().size(), equalTo(2));
    }

    public void testCanRemainReturnsYes(){
        Settings diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "300b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "200b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "100b")
            .put(FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey(), 5.0)
            .build();

        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 2000L); // 2000 bytes
        shardSizes.put("[test][0][r]", 2000L);
        shardSizes.put("[test2][0][p]", 1000L); // 1000 bytes
        shardSizes.put("[test2][0][r]", 1000L);

        Map<String, FileCacheStats> fileCacheStatsMap = new HashMap<>();
        fileCacheStatsMap.put("node1", new FileCacheStats(0, 0, 1000, 0, 0, 0, 0));
        fileCacheStatsMap.put("node2", new FileCacheStats(0, 0, 1000, 0, 0, 0, 0));

        final Map<String, DiskUsage> usages = new HashMap<>();
        final ClusterInfo clusterInfo = new DiskThresholdDeciderTests.DevNullClusterInfo(usages, usages, shardSizes, fileCacheStatsMap);

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationDeciders deciders = new AllocationDeciders(
            new HashSet<>(Arrays.asList(new SameShardAllocationDecider(Settings.EMPTY, clusterSettings), makeDecider(diskSettings)))
        );

        ClusterInfoService cis = () -> {
            logger.info("--> calling fake getClusterInfo");
            return clusterInfo;
        };
        AllocationService strategy = new AllocationService(
            deciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            cis,
            EmptySnapshotsInfoService.INSTANCE
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(warmIndexSettings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(warmIndexSettings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).addAsNew(metadata.index("test2")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        Set<DiscoveryNodeRole> defaultWithWarmRole = new HashSet<>(CLUSTER_MANAGER_DATA_ROLES);
        defaultWithWarmRole.add(DiscoveryNodeRole.WARM_ROLE);

        logger.info("--> adding two warm nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1", defaultWithWarmRole)).add(newNode("node2", defaultWithWarmRole)))
            .build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logShardStates(clusterState);
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(2));
    }

    public void testCanRemainSufficientSpace(){
        Settings settings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "300b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "200b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "100b")
            .put(FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey(), 5.0)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WarmDiskThresholdDecider decider = new WarmDiskThresholdDecider(settings, clusterSettings);

        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 2000L); // 2000 bytes
        shardSizes.put("[test][0][r]", 2000L);
        shardSizes.put("[test2][0][p]", 1000L); // 1000 bytes
        shardSizes.put("[test2][0][r]", 1000L);

        Map<String, FileCacheStats> fileCacheStatsMap = new HashMap<>();
        fileCacheStatsMap.put("node1", new FileCacheStats(0, 0, 1000, 0, 0, 0, 0));
        fileCacheStatsMap.put("node2", new FileCacheStats(0, 0, 1000, 0, 0, 0, 0));

        final Map<String, DiskUsage> usages = new HashMap<>();
        final ClusterInfo clusterInfo = new DiskThresholdDeciderTests.DevNullClusterInfo(usages, usages, shardSizes, fileCacheStatsMap);

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(warmIndexSettings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(warmIndexSettings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        // Create a RoutingTable with shards 0 and 1 initialized on node1, and shard 2 unassigned
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        // [test][0][P] : STARTED on node1, [test][0][R] : STARTED on node2
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(metadata.index("test").getIndex());
        indexRoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test", 0, "node1", null, true, ShardRoutingState.STARTED));
        indexRoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test", 0, "node2", null, false, ShardRoutingState.STARTED));

        // [test2][0][P] : STARTED on node2, [test2][0][R] : STARTED on node1
        IndexRoutingTable.Builder indexRoutingTableBuilder2 = IndexRoutingTable.builder(metadata.index("test2").getIndex());
        indexRoutingTableBuilder2.addShard(TestShardRouting.newShardRouting("test2", 0, "node1", null, false, ShardRoutingState.STARTED));
        indexRoutingTableBuilder2.addShard(TestShardRouting.newShardRouting("test2", 0, "node2", null, true, ShardRoutingState.STARTED));

        routingTableBuilder.add(indexRoutingTableBuilder.build());
        routingTableBuilder.add(indexRoutingTableBuilder2.build());
        RoutingTable routingTable = routingTableBuilder.build();

        Set<DiscoveryNodeRole> defaultWithWarmRole = new HashSet<>(CLUSTER_MANAGER_DATA_ROLES);
        defaultWithWarmRole.add(DiscoveryNodeRole.WARM_ROLE);

        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(newNode("node1", defaultWithWarmRole)).add(newNode("node2", defaultWithWarmRole)))
            .build();

        RoutingAllocation allocation = new RoutingAllocation(null, clusterState.getRoutingNodes(), clusterState, clusterInfo, null, 0);
        allocation.debugDecision(true);

        ShardRouting shard1 = routingTable.index("test").shard(0).primaryShard();
        ShardRouting shard2 = routingTable.index("test2").shard(0).primaryShard();

        // Test canRemain decisions
        assertEquals(Decision.Type.YES, decider.canRemain(shard1, clusterState.getRoutingNodes().node("node1"), allocation).type());
        assertEquals(Decision.Type.YES, decider.canRemain(shard2, clusterState.getRoutingNodes().node("node2"), allocation).type());
    }

    public void testCanRemainInsufficientSpace(){
        Settings settings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "300b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "250b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "100b")
            .put(FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey(), 5.0)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WarmDiskThresholdDecider decider = new WarmDiskThresholdDecider(settings, clusterSettings);

        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 4000L); // 4000 bytes
        shardSizes.put("[test][0][r]", 4000L);
        shardSizes.put("[test2][0][p]", 1000L); // 1000 bytes
        shardSizes.put("[test2][0][r]", 1000L);

        Map<String, FileCacheStats> fileCacheStatsMap = new HashMap<>();
        fileCacheStatsMap.put("node1", new FileCacheStats(0, 0, 1000, 0, 0, 0, 0));
        fileCacheStatsMap.put("node2", new FileCacheStats(0, 0, 1000, 0, 0, 0, 0));

        final Map<String, DiskUsage> usages = new HashMap<>();
        final ClusterInfo clusterInfo = new DiskThresholdDeciderTests.DevNullClusterInfo(usages, usages, shardSizes, fileCacheStatsMap);

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(warmIndexSettings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(warmIndexSettings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        // Create a RoutingTable with shards 0 and 1 initialized on node1, and shard 2 unassigned
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        // [test][0][P]: STARTED on node1, [test][0][R]: STARTED on node2
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(metadata.index("test").getIndex());
        indexRoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test", 0, "node1", null, true, ShardRoutingState.STARTED));
        indexRoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test", 0, "node2", null, false, ShardRoutingState.STARTED));

        // [test2][0][P]: STARTED on node2, [test2][0][R]: STARTED on node1
        IndexRoutingTable.Builder indexRoutingTableBuilder2 = IndexRoutingTable.builder(metadata.index("test2").getIndex());
        indexRoutingTableBuilder2.addShard(TestShardRouting.newShardRouting("test2", 0, "node1", null, false, ShardRoutingState.STARTED));
        indexRoutingTableBuilder2.addShard(TestShardRouting.newShardRouting("test2", 0, "node2", null, true, ShardRoutingState.STARTED));

        routingTableBuilder.add(indexRoutingTableBuilder.build());
        routingTableBuilder.add(indexRoutingTableBuilder2.build());
        RoutingTable routingTable = routingTableBuilder.build();

        Set<DiscoveryNodeRole> defaultWithWarmRole = new HashSet<>(CLUSTER_MANAGER_DATA_ROLES);
        defaultWithWarmRole.add(DiscoveryNodeRole.WARM_ROLE);

        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(newNode("node1", defaultWithWarmRole)).add(newNode("node2", defaultWithWarmRole)))
            .build();

        RoutingAllocation allocation = new RoutingAllocation(null, clusterState.getRoutingNodes(), clusterState, clusterInfo, null, 0);
        allocation.debugDecision(true);

        ShardRouting shard1 = routingTable.index("test").shard(0).primaryShard();
        ShardRouting shard2 = routingTable.index("test2").shard(0).primaryShard();

        // Test canRemain decisions
        assertEquals(Decision.Type.NO, decider.canRemain(shard1, clusterState.getRoutingNodes().node("node1"), allocation).type()); // [test][0][P] can't remain on node1
        assertEquals(Decision.Type.YES, decider.canRemain(shard2, clusterState.getRoutingNodes().node("node2"), allocation).type()); // [test2][0][P] can remain on node2
    }

    public void logShardStates(ClusterState state) {
        RoutingNodes rn = state.getRoutingNodes();
        logger.info(
            "--> counts: total: {}, unassigned: {}, initializing: {}, relocating: {}, started: {}",
            rn.shards(shard -> true).size(),
            rn.shardsWithState(ShardRoutingState.UNASSIGNED).size(),
            rn.shardsWithState(ShardRoutingState.INITIALIZING).size(),
            rn.shardsWithState(ShardRoutingState.RELOCATING).size(),
            rn.shardsWithState(ShardRoutingState.STARTED).size()
        );
        logger.info(
            "--> unassigned: {}, initializing: {}, relocating: {}, started: {}",
            rn.shardsWithState(ShardRoutingState.UNASSIGNED),
            rn.shardsWithState(ShardRoutingState.INITIALIZING),
            rn.shardsWithState(ShardRoutingState.RELOCATING),
            rn.shardsWithState(ShardRoutingState.STARTED)
        );
    }
}
