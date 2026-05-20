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
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiskUsage;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats.FileCacheStatsType;
import org.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.opensearch.index.store.remote.filecache.FileCacheStats;
import org.opensearch.snapshots.EmptySnapshotsInfoService;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

        Map<String, AggregateFileCacheStats> fileCacheStatsMap = createFileCacheStatsMap(1000L, "node1", "node2");

        final Map<String, DiskUsage> usages = new HashMap<>();
        // With file cache of 1000 bytes and ratio of 5.0, total addressable space = 1000 * 5 = 5000 bytes
        usages.put("node1", createDiskUsage("node1", 5000, 5000));
        usages.put("node2", createDiskUsage("node2", 5000, 5000));
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
        shardSizes.put("[test][0][p]", 5500L); // 5500 bytes shard size and total addressable space - 5000 bytes
        shardSizes.put("[test][0][r]", 5500L);
        shardSizes.put("[test2][0][p]", 1000L); // 1000 bytes
        shardSizes.put("[test2][0][r]", 1000L);

        Map<String, AggregateFileCacheStats> fileCacheStatsMap = createFileCacheStatsMap(1000L, "node1", "node2");

        final Map<String, DiskUsage> usages = new HashMap<>();
        // With file cache of 1000 bytes and ratio of 5.0, total addressable space = 1000 * 5 = 5000 bytes
        // For 70% low watermark, we need at least 30% free = 1500 bytes free
        // Test shard is 5500 bytes, which exceeds the total addressable space
        usages.put("node1", createDiskUsage("node1", 5000, 5000));
        usages.put("node2", createDiskUsage("node2", 5000, 5000));
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
        // Assert that test index shards (P and R) are not allocated due to insufficient space
        for (ShardRouting shardRouting : clusterState.getRoutingNodes().unassigned()) {
            if (shardRouting.index().getName().equals("test")) {
                assertThat(shardRouting.state(), equalTo(ShardRoutingState.UNASSIGNED));
            }
        }
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(2));
    }

    public void testCanRemainSufficientSpace() {
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

        Map<String, AggregateFileCacheStats> fileCacheStatsMap = createFileCacheStatsMap(1000L, "node1", "node2");

        final Map<String, DiskUsage> usages = new HashMap<>();
        // With file cache of 1000 bytes and ratio of 5.0, total addressable space = 1000 * 5 = 5000 bytes
        usages.put("node1", createDiskUsage("node1", 5000, 3000));
        usages.put("node2", createDiskUsage("node2", 5000, 4000));
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

    public void testCanRemainInsufficientSpace() {
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
        shardSizes.put("[test][0][p]", 5500L); // Shard size more than total addressable space - 5000 bytes
        shardSizes.put("[test][0][r]", 5500L);
        shardSizes.put("[test2][0][p]", 1000L); // 1000 bytes
        shardSizes.put("[test2][0][r]", 1000L);

        Map<String, AggregateFileCacheStats> fileCacheStatsMap = createFileCacheStatsMap(1000L, "node1", "node2");

        final Map<String, DiskUsage> usages = new HashMap<>();
        // With file cache of 1000 bytes and ratio of 5.0, total addressable space = 1000 * 5 = 5000 bytes
        // For high watermark of 250b, we set free space < 250b to trigger NO decision
        usages.put("node1", createDiskUsage("node1", 5000, 0));
        usages.put("node2", createDiskUsage("node2", 5000, 4000));
        final ClusterInfo clusterInfo = new DiskThresholdDeciderTests.DevNullClusterInfo(usages, usages, shardSizes, fileCacheStatsMap);

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(warmIndexSettings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(warmIndexSettings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

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
        assertEquals(Decision.Type.NO, decider.canRemain(shard1, clusterState.getRoutingNodes().node("node1"), allocation).type());
        assertEquals(Decision.Type.YES, decider.canRemain(shard2, clusterState.getRoutingNodes().node("node2"), allocation).type());
    }

    public void testCanRemainSufficientSpaceAfterRelocation() {
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
        shardSizes.put("[test][0][p]", 3000L); // 3000 bytes
        shardSizes.put("[test][0][r]", 3000L);
        shardSizes.put("[test2][0][p]", 1000L); // 1000 bytes
        shardSizes.put("[test2][0][r]", 1000L);
        shardSizes.put("[test3][0][p]", 1500L);

        Map<String, AggregateFileCacheStats> fileCacheStatsMap = createFileCacheStatsMap(1000L, "node1", "node2");

        final Map<String, DiskUsage> usages = new HashMap<>();
        // With file cache of 1000 bytes and ratio of 5.0, total addressable space = 1000 * 5 = 5000 bytes
        // We need enough free space to handle relocation - with [test3][0][p] relocating away,
        // node1 will gain 1500b of free space
        usages.put("node1", createDiskUsage("node1", 5000, 500));
        usages.put("node2", createDiskUsage("node2", 5000, 4000));
        final ClusterInfo clusterInfo = new DiskThresholdDeciderTests.DevNullClusterInfo(usages, usages, shardSizes, fileCacheStatsMap);

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(warmIndexSettings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(warmIndexSettings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("test3").settings(warmIndexSettings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        // [test][0][P]: STARTED on node1, [test][0][R]: STARTED on node2
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(metadata.index("test").getIndex());
        indexRoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test", 0, "node1", null, true, ShardRoutingState.STARTED));
        indexRoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test", 0, "node2", null, false, ShardRoutingState.STARTED));

        // [test2][0][P]: STARTED on node2, [test2][0][R]: STARTED on node1
        IndexRoutingTable.Builder indexRoutingTableBuilder2 = IndexRoutingTable.builder(metadata.index("test2").getIndex());
        indexRoutingTableBuilder2.addShard(TestShardRouting.newShardRouting("test2", 0, "node1", null, false, ShardRoutingState.STARTED));
        indexRoutingTableBuilder2.addShard(TestShardRouting.newShardRouting("test2", 0, "node2", null, true, ShardRoutingState.STARTED));

        // [test3][0][P]: RELOCATING from node1
        IndexRoutingTable.Builder indexRoutingTableBuilder3 = IndexRoutingTable.builder(metadata.index("test3").getIndex());
        indexRoutingTableBuilder3.addShard(
            TestShardRouting.newShardRouting("test3", 0, "node1", "node2", true, ShardRoutingState.RELOCATING)
        );

        routingTableBuilder.add(indexRoutingTableBuilder.build());
        routingTableBuilder.add(indexRoutingTableBuilder2.build());
        routingTableBuilder.add(indexRoutingTableBuilder3.build());
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

        // Test canRemain decisions with [test3][0][p] relocating from node1
        assertEquals(Decision.Type.YES, decider.canRemain(shard1, clusterState.getRoutingNodes().node("node1"), allocation).type());
        assertEquals(Decision.Type.YES, decider.canRemain(shard2, clusterState.getRoutingNodes().node("node2"), allocation).type());
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

    /**
     * Creates a standard FileCacheStats map for testing with warm nodes.
     * @param fileCacheSize the size of the file cache
     * @param nodes the node IDs to create stats for
     * @return a map of node ID to AggregateFileCacheStats
     */
    private Map<String, AggregateFileCacheStats> createFileCacheStatsMap(long fileCacheSize, String... nodes) {
        Map<String, AggregateFileCacheStats> fileCacheStatsMap = new HashMap<>();
        for (String node : nodes) {
            fileCacheStatsMap.put(
                node,
                new AggregateFileCacheStats(
                    randomNonNegativeInt(),
                    new FileCacheStats(0, fileCacheSize, 0, 0, 0, 0, 0, 0, FileCacheStatsType.OVER_ALL_STATS),
                    new FileCacheStats(0, fileCacheSize, 0, 0, 0, 0, 0, 0, FileCacheStatsType.FULL_FILE_STATS),
                    new FileCacheStats(0, fileCacheSize, 0, 0, 0, 0, 0, 0, FileCacheStatsType.BLOCK_FILE_STATS),
                    new FileCacheStats(0, fileCacheSize, 0, 0, 0, 0, 0, 0, FileCacheStatsType.PINNED_FILE_STATS)
                )
            );
        }
        return fileCacheStatsMap;
    }

    /**
     * Creates a DiskUsage with the specified free bytes and total bytes.
     */
    private DiskUsage createDiskUsage(String nodeId, long totalBytes, long freeBytes) {
        return new DiskUsage(nodeId, nodeId, "/dev/null", totalBytes, freeBytes);
    }

}
