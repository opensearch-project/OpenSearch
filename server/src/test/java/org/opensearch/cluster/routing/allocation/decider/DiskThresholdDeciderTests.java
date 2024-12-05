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
import org.opensearch.cluster.RestoreInProgress;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.opensearch.cluster.routing.UnassignedInfo.Reason;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.command.AllocationCommand;
import org.opensearch.cluster.routing.allocation.command.AllocationCommands;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.remote.filecache.FileCacheStats;
import org.opensearch.repositories.IndexId;
import org.opensearch.snapshots.EmptySnapshotsInfoService;
import org.opensearch.snapshots.InternalSnapshotsInfoService.SnapshotShard;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotShardSizeInfo;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.opensearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;
import static org.opensearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.opensearch.index.store.remote.filecache.FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

public class DiskThresholdDeciderTests extends OpenSearchAllocationTestCase {

    DiskThresholdDecider makeDecider(Settings settings) {
        return new DiskThresholdDecider(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    public void testDiskThreshold() {
        Settings diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), 0.7)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), 0.8)
            .build();

        final Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node1", new DiskUsage("node1", "node1", "/dev/null", 100, 10)); // 90% used
        usages.put("node2", new DiskUsage("node2", "node2", "/dev/null", 100, 35)); // 65% used
        usages.put("node3", new DiskUsage("node3", "node3", "/dev/null", 100, 60)); // 40% used
        usages.put("node4", new DiskUsage("node4", "node4", "/dev/null", 100, 80)); // 20% used

        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 10L); // 10 bytes
        shardSizes.put("[test][0][r]", 10L);
        final ClusterInfo clusterInfo = new DevNullClusterInfo(usages, usages, shardSizes);

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
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        final RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("--> adding two nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");
        logShardStates(clusterState);

        // Primary shard should be initializing, replica should not
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        // Assert that we're able to start the primary
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(1));
        // Assert that node1 didn't get any shards because its disk usage is too high
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        // Assert that the replica couldn't be started since node1 doesn't have enough space
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(1));

        logger.info("--> adding node3");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logShardStates(clusterState);
        // Assert that the replica is initialized now that node3 is available with enough space
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        // Assert that the replica couldn't be started since node1 doesn't have enough space
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> changing decider settings");

        // Set the low threshold to 60 instead of 70
        // Set the high threshold to 70 instead of 80
        // node2 now should not have new shards allocated to it, but shards can remain
        diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "60%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), 0.7)
            .build();

        deciders = new AllocationDeciders(
            new HashSet<>(Arrays.asList(new SameShardAllocationDecider(Settings.EMPTY, clusterSettings), makeDecider(diskSettings)))
        );

        strategy = new AllocationService(
            deciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            cis,
            EmptySnapshotsInfoService.INSTANCE
        );

        clusterState = strategy.reroute(clusterState, "reroute");
        logShardStates(clusterState);

        // Shards remain started
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> changing settings again");

        // Set the low threshold to 50 instead of 60
        // Set the high threshold to 60 instead of 70
        // node2 now should not have new shards allocated to it, and shards cannot remain
        diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), 0.5)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), 0.6)
            .build();

        deciders = new AllocationDeciders(
            new HashSet<>(Arrays.asList(new SameShardAllocationDecider(Settings.EMPTY, clusterSettings), makeDecider(diskSettings)))
        );

        strategy = new AllocationService(
            deciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            cis,
            EmptySnapshotsInfoService.INSTANCE
        );

        clusterState = strategy.reroute(clusterState, "reroute");

        logShardStates(clusterState);
        // Shards remain started
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
        // Shard hasn't been moved off of node2 yet because there's nowhere for it to go
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> adding node4");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logShardStates(clusterState);
        // Shards remain started
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> apply INITIALIZING shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
        // Node4 is available now, so the shard is moved off of node2
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node4").size(), equalTo(1));
    }

    public void testDiskThresholdForRemoteShards() {
        Settings diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), 0.7)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), 0.8)
            .build();

        Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node1", new DiskUsage("node1", "node1", "/dev/null", 100, 10)); // 90% used
        usages.put("node2", new DiskUsage("node2", "node2", "/dev/null", 100, 35)); // 65% used
        usages.put("node3", new DiskUsage("node3", "node3", "/dev/null", 100, 60)); // 40% used

        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 10L); // 10 bytes
        shardSizes.put("[test][0][r]", 10L);

        Map<String, FileCacheStats> fileCacheStatsMap = new HashMap<>();
        fileCacheStatsMap.put("node1", new FileCacheStats(0, 0, 1000, 0, 0, 0, 0));
        fileCacheStatsMap.put("node2", new FileCacheStats(0, 0, 1000, 0, 0, 0, 0));
        fileCacheStatsMap.put("node3", new FileCacheStats(0, 0, 1000, 0, 0, 0, 0));
        final ClusterInfo clusterInfo = new DevNullClusterInfo(usages, usages, shardSizes, fileCacheStatsMap);

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
            .put(IndexMetadata.builder("test").settings(remoteIndexSettings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        final RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        Set<DiscoveryNodeRole> defaultWithSearchRole = new HashSet<>(CLUSTER_MANAGER_DATA_ROLES);
        defaultWithSearchRole.add(DiscoveryNodeRole.SEARCH_ROLE);

        logger.info("--> adding two nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1", defaultWithSearchRole)).add(newNode("node2", defaultWithSearchRole)))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");
        logShardStates(clusterState);

        // Primary shard should be initializing, replica should not
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        // Assert that we're able to start the primary
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));

        logger.info("--> adding node3");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logShardStates(clusterState);
        // Assert that the replica is initialized now that node3 is available with enough space
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(0));

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        // Assert that the replica couldn't be started since node1 doesn't have enough space
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));
    }

    public void testFileCacheRemoteShardsDecisions() {
        Settings diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "60%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "70%")
            .put(DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey(), 5)
            .build();

        // We have an index with 2 primary shards each taking 40 bytes. Each node has 100 bytes available
        final Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node1", new DiskUsage("node1", "n1", "/dev/null", 100, 20)); // 80% used
        usages.put("node2", new DiskUsage("node2", "n2", "/dev/null", 100, 100)); // 0% used

        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 40L);
        shardSizes.put("[test][1][p]", 40L);
        shardSizes.put("[foo][0][p]", 10L);

        // First node has filecache size as 0, second has 1000, greater than the shard sizes.
        Map<String, FileCacheStats> fileCacheStatsMap = new HashMap<>();
        fileCacheStatsMap.put("node1", new FileCacheStats(0, 0, 0, 0, 0, 0, 0));
        fileCacheStatsMap.put("node2", new FileCacheStats(0, 0, 1000, 0, 0, 0, 0));

        final ClusterInfo clusterInfo = new DevNullClusterInfo(usages, usages, shardSizes, fileCacheStatsMap);

        Set<DiscoveryNodeRole> defaultWithSearchRole = new HashSet<>(CLUSTER_MANAGER_DATA_ROLES);
        defaultWithSearchRole.add(DiscoveryNodeRole.SEARCH_ROLE);

        DiskThresholdDecider diskThresholdDecider = makeDecider(diskSettings);
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(remoteIndexSettings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        DiscoveryNode discoveryNode1 = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            defaultWithSearchRole,
            Version.CURRENT
        );
        DiscoveryNode discoveryNode2 = new DiscoveryNode(
            "node2",
            buildNewFakeTransportAddress(),
            emptyMap(),
            defaultWithSearchRole,
            Version.CURRENT
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(discoveryNode1).add(discoveryNode2).build();

        ClusterState baseClusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .nodes(discoveryNodes)
            .build();

        // Two shards consuming each 80% of disk space while 70% is allowed, so shard 0 isn't allowed here
        ShardRouting firstRouting = TestShardRouting.newShardRouting("test", 0, "node1", null, true, ShardRoutingState.STARTED);
        ShardRouting secondRouting = TestShardRouting.newShardRouting("test", 1, "node1", null, true, ShardRoutingState.STARTED);
        RoutingNode firstRoutingNode = new RoutingNode("node1", discoveryNode1, firstRouting, secondRouting);
        RoutingNode secondRoutingNode = new RoutingNode("node2", discoveryNode2);

        RoutingTable.Builder builder = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(firstRouting.index())
                    .addIndexShard(new IndexShardRoutingTable.Builder(firstRouting.shardId()).addShard(firstRouting).build())
                    .addIndexShard(new IndexShardRoutingTable.Builder(secondRouting.shardId()).addShard(secondRouting).build())
            );
        ClusterState clusterState = ClusterState.builder(baseClusterState).routingTable(builder.build()).build();
        RoutingAllocation routingAllocation = new RoutingAllocation(
            null,
            new RoutingNodes(clusterState),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        routingAllocation.debugDecision(true);
        Decision decision = diskThresholdDecider.canRemain(firstRouting, firstRoutingNode, routingAllocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));

        decision = diskThresholdDecider.canAllocate(firstRouting, firstRoutingNode, routingAllocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));

        assertThat(
            ((Decision.Single) decision).getExplanation(),
            containsString("file cache limit reached - remote shard size will exceed configured safeguard ratio")
        );

        decision = diskThresholdDecider.canAllocate(firstRouting, secondRoutingNode, routingAllocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
    }

    public void testDiskThresholdWithAbsoluteSizes() {
        Settings diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "30b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "9b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "5b")
            .build();

        final Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node1", new DiskUsage("node1", "n1", "/dev/null", 100, 10)); // 90% used
        usages.put("node2", new DiskUsage("node2", "n2", "/dev/null", 100, 10)); // 90% used
        usages.put("node3", new DiskUsage("node3", "n3", "/dev/null", 100, 60)); // 40% used
        usages.put("node4", new DiskUsage("node4", "n4", "/dev/null", 100, 80)); // 20% used
        usages.put("node5", new DiskUsage("node5", "n5", "/dev/null", 100, 85)); // 15% used

        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 10L); // 10 bytes
        shardSizes.put("[test][0][r]", 10L);
        final ClusterInfo clusterInfo = new DevNullClusterInfo(usages, usages, shardSizes);

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
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("--> adding node1 and node2 node");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        clusterState = strategy.reroute(clusterState, "reroute");
        logShardStates(clusterState);

        // Primary should initialize, even though both nodes are over the limit initialize
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        // below checks are unnecessary as the primary shard is always assigned to node2 as BSA always picks up that node
        // first as both node1 and node2 have equal weight as both of them contain zero shards.
        String nodeWithPrimary, nodeWithoutPrimary;
        if (clusterState.getRoutingNodes().node("node1").size() == 1) {
            nodeWithPrimary = "node1";
            nodeWithoutPrimary = "node2";
        } else {
            nodeWithPrimary = "node2";
            nodeWithoutPrimary = "node1";
        }
        logger.info("--> nodeWithPrimary: {}", nodeWithPrimary);
        logger.info("--> nodeWithoutPrimary: {}", nodeWithoutPrimary);

        // Make node without the primary now habitable to replicas
        usages.put(nodeWithoutPrimary, new DiskUsage(nodeWithoutPrimary, "", "/dev/null", 100, 35)); // 65% used
        final ClusterInfo clusterInfo2 = new DevNullClusterInfo(usages, usages, shardSizes);
        cis = () -> {
            logger.info("--> calling fake getClusterInfo");
            return clusterInfo2;
        };
        strategy = new AllocationService(
            deciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            cis,
            EmptySnapshotsInfoService.INSTANCE
        );

        clusterState = strategy.reroute(clusterState, "reroute");
        logShardStates(clusterState);

        // Now the replica should be able to initialize
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        // Assert that we're able to start the primary and replica, since they were both initializing
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));
        // Assert that node1 got a single shard (the primary), even though its disk usage is too high
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        // Assert that node2 got a single shard (a replica)
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));

        // Assert that one replica is still unassigned
        // assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));

        logger.info("--> adding node3");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logShardStates(clusterState);
        // Assert that the replica is initialized now that node3 is available with enough space
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        // Assert that all replicas could be started
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(3));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> changing decider settings");

        // Set the low threshold to 60 instead of 70
        // Set the high threshold to 70 instead of 80
        // node2 now should not have new shards allocated to it, but shards can remain
        diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "40b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "30b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "20b")
            .build();

        deciders = new AllocationDeciders(
            new HashSet<>(Arrays.asList(new SameShardAllocationDecider(Settings.EMPTY, clusterSettings), makeDecider(diskSettings)))
        );

        strategy = new AllocationService(
            deciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            cis,
            EmptySnapshotsInfoService.INSTANCE
        );

        clusterState = strategy.reroute(clusterState, "reroute");
        logShardStates(clusterState);

        // Shards remain started
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(3));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> changing settings again");

        // Set the low threshold to 50 instead of 60
        // Set the high threshold to 60 instead of 70
        // node2 now should not have new shards allocated to it, and shards cannot remain
        diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "50b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "40b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "30b")
            .build();

        deciders = new AllocationDeciders(
            new HashSet<>(Arrays.asList(new SameShardAllocationDecider(Settings.EMPTY, clusterSettings), makeDecider(diskSettings)))
        );

        strategy = new AllocationService(
            deciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            cis,
            EmptySnapshotsInfoService.INSTANCE
        );

        clusterState = strategy.reroute(clusterState, "reroute");

        logShardStates(clusterState);
        // Shards remain started
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(3));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        // Shard hasn't been moved off of node2 yet because there's nowhere for it to go
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> adding node4");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logShardStates(clusterState);
        // Shards remain started
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
        // One shard is relocating off of node1
        assertThat(clusterState.getRoutingNodes().shardsWithState(RELOCATING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> apply INITIALIZING shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        // primary shard already has been relocated away - this is a wrong expectation as we don't really move
        // primary first unless explicitly set by setting. This is caught with PR
        // https://github.com/opensearch-project/OpenSearch/pull/15239/
        // as it randomises nodes to check for potential moves
        // assertThat(clusterState.getRoutingNodes().node(nodeWithPrimary).size(), equalTo(0));
        // assertThat(clusterState.getRoutingNodes().node(nodeWithoutPrimary).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node4").size(), equalTo(1));

        logger.info("--> adding node5");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node5"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logShardStates(clusterState);
        // Shards remain started on node3 and node4
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
        // One shard is relocating off of node2 now
        assertThat(clusterState.getRoutingNodes().shardsWithState(RELOCATING).size(), equalTo(1));
        // Initializing on node5
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> apply INITIALIZING shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> final cluster state:");
        logShardStates(clusterState);
        // Node1 still has no shards because it has no space for them
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
        // Node5 is available now, so the shard is moved off of node2
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node4").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node5").size(), equalTo(1));
    }

    public void testDiskThresholdWithShardSizes() {
        Settings diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), 0.7)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "71%")
            .build();

        final Map<String, DiskUsage> usages = Map.of(
            "node1",
            new DiskUsage("node1", "n1", "/dev/null", 100, 31), // 69% used
            "node2",
            new DiskUsage("node2", "n2", "/dev/null", 100, 1)
        );  // 99% used

        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 10L); // 10 bytes
        final ClusterInfo clusterInfo = new DevNullClusterInfo(usages, usages, shardSizes);

        AllocationDeciders deciders = new AllocationDeciders(
            new HashSet<>(
                Arrays.asList(
                    new SameShardAllocationDecider(
                        Settings.EMPTY,
                        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                    ),
                    makeDecider(diskSettings)
                )
            )
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
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
        logger.info("--> adding node1");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")) // node2 is added because DiskThresholdDecider
                                                                                     // automatically ignore single-node clusters
            )
            .build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logger.info("--> start the shards (primaries)");
        routingTable = startInitializingShardsAndReroute(strategy, clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logShardStates(clusterState);

        // Shard can't be allocated to node1 (or node2) because it would cause too much usage
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));
        // No shards are started, no nodes have enough disk for allocation
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(0));
    }

    public void testUnknownDiskUsage() {
        Settings diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), 0.7)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), 0.85)
            .build();

        final Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node2", new DiskUsage("node2", "node2", "/dev/null", 100, 50)); // 50% used
        usages.put("node3", new DiskUsage("node3", "node3", "/dev/null", 100, 0));  // 100% used

        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 10L); // 10 bytes
        shardSizes.put("[test][0][r]", 10L); // 10 bytes
        final ClusterInfo clusterInfo = new DevNullClusterInfo(usages, usages, shardSizes);

        AllocationDeciders deciders = new AllocationDeciders(
            new HashSet<>(
                Arrays.asList(
                    new SameShardAllocationDecider(
                        Settings.EMPTY,
                        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                    ),
                    makeDecider(diskSettings)
                )
            )
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
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
        logger.info("--> adding node1");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node3")) // node3 is added because DiskThresholdDecider
                                                                                     // automatically ignore single-node clusters
            )
            .build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        // Shard can be allocated to node1, even though it only has 25% free,
        // because it's a primary that's never been allocated before
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        routingTable = startInitializingShardsAndReroute(strategy, clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logShardStates(clusterState);

        // A single shard is started on node1, even though it normally would not
        // be allowed, because it's a primary that hasn't been allocated, and node1
        // is still below the high watermark (unlike node3)
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
    }

    public void testFreeDiskPercentageAfterShardAssigned() {
        DiskThresholdDecider decider = makeDecider(Settings.EMPTY);

        Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node2", new DiskUsage("node2", "n2", "/dev/null", 100, 50)); // 50% used
        usages.put("node3", new DiskUsage("node3", "n3", "/dev/null", 100, 0));  // 100% used

        Double after = decider.freeDiskPercentageAfterShardAssigned(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("node2", "n2", "/dev/null", 100, 30), 0L),
            11L
        );
        assertThat(after, equalTo(19.0));
    }

    public void testShardRelocationsTakenIntoAccount() {
        Settings diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), 0.7)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), 0.8)
            .build();

        final Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node1", new DiskUsage("node1", "n1", "/dev/null", 100, 40)); // 60% used
        usages.put("node2", new DiskUsage("node2", "n2", "/dev/null", 100, 40)); // 60% used
        usages.put("node3", new DiskUsage("node3", "n3", "/dev/null", 100, 40)); // 60% used

        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 14L); // 14 bytes
        shardSizes.put("[test][0][r]", 14L);
        shardSizes.put("[test2][0][p]", 1L); // 1 bytes
        shardSizes.put("[test2][0][r]", 1L);
        final ClusterInfo clusterInfo = new DevNullClusterInfo(usages, usages, shardSizes);

        DiskThresholdDecider decider = makeDecider(diskSettings);
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationDeciders deciders = new AllocationDeciders(
            new HashSet<>(
                Arrays.asList(
                    new SameShardAllocationDecider(Settings.EMPTY, clusterSettings),
                    new EnableAllocationDecider(
                        Settings.builder().put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none").build(),
                        clusterSettings
                    ),
                    decider
                )
            )
        );

        final AtomicReference<ClusterInfo> clusterInfoReference = new AtomicReference<>(clusterInfo);
        final ClusterInfoService cis = clusterInfoReference::get;

        AllocationService strategy = new AllocationService(
            deciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            cis,
            EmptySnapshotsInfoService.INSTANCE
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test"))
            .addAsNew(metadata.index("test2"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("--> adding two nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");
        logShardStates(clusterState);

        // shards should be initializing
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(4));

        logger.info("--> start the shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        // Assert that we're able to start the primary and replicas
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> adding node3");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();

        {
            AllocationCommand moveAllocationCommand = new MoveAllocationCommand("test", 0, "node2", "node3");
            AllocationCommands cmds = new AllocationCommands(moveAllocationCommand);

            clusterState = strategy.reroute(clusterState, cmds, false, false).getClusterState();
            logShardStates(clusterState);
        }

        final Map<String, DiskUsage> overfullUsages = new HashMap<>();
        overfullUsages.put("node1", new DiskUsage("node1", "n1", "/dev/null", 100, 40)); // 60% used
        overfullUsages.put("node2", new DiskUsage("node2", "n2", "/dev/null", 100, 40)); // 60% used
        overfullUsages.put("node3", new DiskUsage("node3", "n3", "/dev/null", 100, 0));  // 100% used

        final Map<String, Long> largerShardSizes = new HashMap<>();
        largerShardSizes.put("[test][0][p]", 14L);
        largerShardSizes.put("[test][0][r]", 14L);
        largerShardSizes.put("[test2][0][p]", 2L);
        largerShardSizes.put("[test2][0][r]", 2L);

        final ClusterInfo overfullClusterInfo = new DevNullClusterInfo(overfullUsages, overfullUsages, largerShardSizes);

        {
            AllocationCommand moveAllocationCommand = new MoveAllocationCommand("test2", 0, "node2", "node3");
            AllocationCommands cmds = new AllocationCommands(moveAllocationCommand);

            final ClusterState clusterStateThatRejectsCommands = clusterState;

            assertThat(
                expectThrows(IllegalArgumentException.class, () -> strategy.reroute(clusterStateThatRejectsCommands, cmds, false, false))
                    .getMessage(),
                containsString(
                    "the node is above the low watermark cluster setting "
                        + "[cluster.routing.allocation.disk.watermark.low=0.7], using more disk space than the maximum "
                        + "allowed [70.0%], actual free: [26.0%]"
                )
            );

            clusterInfoReference.set(overfullClusterInfo);

            assertThat(
                expectThrows(IllegalArgumentException.class, () -> strategy.reroute(clusterStateThatRejectsCommands, cmds, false, false))
                    .getMessage(),
                containsString("the node has fewer free bytes remaining than the total size of all incoming shards")
            );

            clusterInfoReference.set(clusterInfo);
        }

        {
            AllocationCommand moveAllocationCommand = new MoveAllocationCommand("test2", 0, "node2", "node3");
            AllocationCommands cmds = new AllocationCommands(moveAllocationCommand);

            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
            clusterState = strategy.reroute(clusterState, cmds, false, false).getClusterState();
            logShardStates(clusterState);

            clusterInfoReference.set(overfullClusterInfo);

            strategy.reroute(clusterState, "foo"); // ensure reroute doesn't fail even though there is negative free space
        }

        {
            clusterInfoReference.set(overfullClusterInfo);
            clusterState = applyStartedShardsUntilNoChange(clusterState, strategy);
            final List<ShardRouting> startedShardsWithOverfullDisk = clusterState.getRoutingNodes().shardsWithState(STARTED);
            assertThat(startedShardsWithOverfullDisk.size(), equalTo(4));
            for (ShardRouting shardRouting : startedShardsWithOverfullDisk) {
                // no shards on node3 since it has no free space
                assertThat(shardRouting.toString(), shardRouting.currentNodeId(), oneOf("node1", "node2"));
            }

            // reset free space on node 3 and reserve space on node1
            clusterInfoReference.set(
                new DevNullClusterInfo(
                    usages,
                    usages,
                    shardSizes,
                    Map.of(
                        new ClusterInfo.NodeAndPath("node1", "/dev/null"),
                        new ClusterInfo.ReservedSpace.Builder().add(new ShardId("", "", 0), between(51, 200)).build()
                    ),
                    Map.of()
                )
            );
            clusterState = applyStartedShardsUntilNoChange(clusterState, strategy);
            final List<ShardRouting> startedShardsWithReservedSpace = clusterState.getRoutingNodes().shardsWithState(STARTED);
            assertThat(startedShardsWithReservedSpace.size(), equalTo(4));
            for (ShardRouting shardRouting : startedShardsWithReservedSpace) {
                // no shards on node1 since all its free space is reserved
                assertThat(shardRouting.toString(), shardRouting.currentNodeId(), oneOf("node2", "node3"));
            }
        }
    }

    public void testCanRemainWithShardRelocatingAway() {
        Settings diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "60%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "70%")
            .build();

        // We have an index with 2 primary shards each taking 40 bytes. Each node has 100 bytes available
        final Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node1", new DiskUsage("node1", "n1", "/dev/null", 100, 20)); // 80% used
        usages.put("node2", new DiskUsage("node2", "n2", "/dev/null", 100, 100)); // 0% used

        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 40L);
        shardSizes.put("[test][1][p]", 40L);
        shardSizes.put("[foo][0][p]", 10L);

        final ClusterInfo clusterInfo = new DevNullClusterInfo(usages, usages, shardSizes);

        DiskThresholdDecider diskThresholdDecider = makeDecider(diskSettings);
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0))
            .put(IndexMetadata.builder("foo").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).addAsNew(metadata.index("foo")).build();

        DiscoveryNode discoveryNode1 = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            CLUSTER_MANAGER_DATA_ROLES,
            Version.CURRENT
        );
        DiscoveryNode discoveryNode2 = new DiscoveryNode(
            "node2",
            buildNewFakeTransportAddress(),
            emptyMap(),
            CLUSTER_MANAGER_DATA_ROLES,
            Version.CURRENT
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(discoveryNode1).add(discoveryNode2).build();

        ClusterState baseClusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .nodes(discoveryNodes)
            .build();

        // Two shards consuming each 80% of disk space while 70% is allowed, so shard 0 isn't allowed here
        ShardRouting firstRouting = TestShardRouting.newShardRouting("test", 0, "node1", null, true, ShardRoutingState.STARTED);
        ShardRouting secondRouting = TestShardRouting.newShardRouting("test", 1, "node1", null, true, ShardRoutingState.STARTED);
        RoutingNode firstRoutingNode = new RoutingNode("node1", discoveryNode1, firstRouting, secondRouting);
        RoutingTable.Builder builder = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(firstRouting.index())
                    .addIndexShard(new IndexShardRoutingTable.Builder(firstRouting.shardId()).addShard(firstRouting).build())
                    .addIndexShard(new IndexShardRoutingTable.Builder(secondRouting.shardId()).addShard(secondRouting).build())
            );
        ClusterState clusterState = ClusterState.builder(baseClusterState).routingTable(builder.build()).build();
        RoutingAllocation routingAllocation = new RoutingAllocation(
            null,
            new RoutingNodes(clusterState),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        routingAllocation.debugDecision(true);
        Decision decision = diskThresholdDecider.canRemain(firstRouting, firstRoutingNode, routingAllocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(
            ((Decision.Single) decision).getExplanation(),
            containsString(
                "the shard cannot remain on this node because it is above the high watermark cluster setting "
                    + "[cluster.routing.allocation.disk.watermark.high=70%] and there is less than the required [30.0%] free disk on node, "
                    + "actual free: [20.0%]"
            )
        );

        // Two shards consuming each 80% of disk space while 70% is allowed, but one is relocating, so shard 0 can stay
        firstRouting = TestShardRouting.newShardRouting("test", 0, "node1", null, true, ShardRoutingState.STARTED);
        secondRouting = TestShardRouting.newShardRouting("test", 1, "node1", "node2", true, ShardRoutingState.RELOCATING);
        ShardRouting fooRouting = TestShardRouting.newShardRouting("foo", 0, null, true, ShardRoutingState.UNASSIGNED);
        firstRoutingNode = new RoutingNode("node1", discoveryNode1, firstRouting, secondRouting);
        builder = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(firstRouting.index())
                    .addIndexShard(new IndexShardRoutingTable.Builder(firstRouting.shardId()).addShard(firstRouting).build())
                    .addIndexShard(new IndexShardRoutingTable.Builder(secondRouting.shardId()).addShard(secondRouting).build())
            );
        clusterState = ClusterState.builder(baseClusterState).routingTable(builder.build()).build();
        routingAllocation = new RoutingAllocation(null, new RoutingNodes(clusterState), clusterState, clusterInfo, null, System.nanoTime());
        routingAllocation.debugDecision(true);
        decision = diskThresholdDecider.canRemain(firstRouting, firstRoutingNode, routingAllocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertEquals(
            "there is enough disk on this node for the shard to remain, free: [60b]",
            ((Decision.Single) decision).getExplanation()
        );
        decision = diskThresholdDecider.canAllocate(fooRouting, firstRoutingNode, routingAllocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        if (fooRouting.recoverySource().getType() == RecoverySource.Type.EMPTY_STORE) {
            assertThat(
                ((Decision.Single) decision).getExplanation(),
                containsString(
                    "the node is above the high watermark cluster setting [cluster.routing.allocation.disk.watermark.high=70%], using "
                        + "more disk space than the maximum allowed [70.0%], actual free: [20.0%]"
                )
            );
        } else {
            assertThat(
                ((Decision.Single) decision).getExplanation(),
                containsString(
                    "the node is above the low watermark cluster setting [cluster.routing.allocation.disk.watermark.low=60%], using more "
                        + "disk space than the maximum allowed [60.0%], actual free: [20.0%]"
                )
            );
        }

        // Creating AllocationService instance and the services it depends on...
        ClusterInfoService cis = () -> {
            logger.info("--> calling fake getClusterInfo");
            return clusterInfo;
        };
        AllocationDeciders deciders = new AllocationDeciders(
            new HashSet<>(
                Arrays.asList(
                    new SameShardAllocationDecider(
                        Settings.EMPTY,
                        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                    ),
                    diskThresholdDecider
                )
            )
        );
        AllocationService strategy = new AllocationService(
            deciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            cis,
            EmptySnapshotsInfoService.INSTANCE
        );
        // Ensure that the reroute call doesn't alter the routing table, since the first primary is relocating away
        // and therefor we will have sufficient disk space on node1.
        ClusterState result = strategy.reroute(clusterState, "reroute");
        assertThat(result, equalTo(clusterState));
        assertThat(result.routingTable().index("test").getShards().get(0).primaryShard().state(), equalTo(STARTED));
        assertThat(result.routingTable().index("test").getShards().get(0).primaryShard().currentNodeId(), equalTo("node1"));
        assertThat(result.routingTable().index("test").getShards().get(0).primaryShard().relocatingNodeId(), nullValue());
        assertThat(result.routingTable().index("test").getShards().get(1).primaryShard().state(), equalTo(RELOCATING));
        assertThat(result.routingTable().index("test").getShards().get(1).primaryShard().currentNodeId(), equalTo("node1"));
        assertThat(result.routingTable().index("test").getShards().get(1).primaryShard().relocatingNodeId(), equalTo("node2"));
    }

    public void testForSingleDataNode() {
        // remove test in 9.0
        Settings diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "60%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "70%")
            .build();

        final Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node1", new DiskUsage("node1", "n1", "/dev/null", 100, 100)); // 0% used
        usages.put("node2", new DiskUsage("node2", "n2", "/dev/null", 100, 20));  // 80% used
        usages.put("node3", new DiskUsage("node3", "n3", "/dev/null", 100, 100)); // 0% used

        // We have an index with 1 primary shards each taking 40 bytes. Each node has 100 bytes available
        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 40L);
        shardSizes.put("[test][1][p]", 40L);
        final ClusterInfo clusterInfo = new DevNullClusterInfo(usages, usages, shardSizes);

        DiskThresholdDecider diskThresholdDecider = makeDecider(diskSettings);
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        logger.info("--> adding one cluster-manager node, one data node");
        DiscoveryNode discoveryNode1 = new DiscoveryNode(
            "",
            "node1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        DiscoveryNode discoveryNode2 = new DiscoveryNode(
            "",
            "node2",
            buildNewFakeTransportAddress(),
            emptyMap(),
            singleton(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(discoveryNode1).add(discoveryNode2).build();
        ClusterState baseClusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .nodes(discoveryNodes)
            .build();

        // Two shards consumes 80% of disk space in data node, but we have only one data node, shards should remain.
        ShardRouting firstRouting = TestShardRouting.newShardRouting("test", 0, "node2", null, true, ShardRoutingState.STARTED);
        ShardRouting secondRouting = TestShardRouting.newShardRouting("test", 1, "node2", null, true, ShardRoutingState.STARTED);
        RoutingNode firstRoutingNode = new RoutingNode("node2", discoveryNode2, firstRouting, secondRouting);

        RoutingTable.Builder builder = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(firstRouting.index())
                    .addIndexShard(new IndexShardRoutingTable.Builder(firstRouting.shardId()).addShard(firstRouting).build())
                    .addIndexShard(new IndexShardRoutingTable.Builder(secondRouting.shardId()).addShard(secondRouting).build())
            );
        ClusterState clusterState = ClusterState.builder(baseClusterState).routingTable(builder.build()).build();
        RoutingAllocation routingAllocation = new RoutingAllocation(
            null,
            new RoutingNodes(clusterState),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        routingAllocation.debugDecision(true);
        Decision decision = diskThresholdDecider.canRemain(firstRouting, firstRoutingNode, routingAllocation);

        // Two shards should start happily
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), containsString("there is only a single data node present"));
        ClusterInfoService cis = () -> {
            logger.info("--> calling fake getClusterInfo");
            return clusterInfo;
        };

        AllocationDeciders deciders = new AllocationDeciders(
            new HashSet<>(
                Arrays.asList(
                    new SameShardAllocationDecider(
                        Settings.EMPTY,
                        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                    ),
                    diskThresholdDecider
                )
            )
        );

        AllocationService strategy = new AllocationService(
            deciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            cis,
            EmptySnapshotsInfoService.INSTANCE
        );
        ClusterState result = strategy.reroute(clusterState, "reroute");

        assertThat(result.routingTable().index("test").getShards().get(0).primaryShard().state(), equalTo(STARTED));
        assertThat(result.routingTable().index("test").getShards().get(0).primaryShard().currentNodeId(), equalTo("node2"));
        assertThat(result.routingTable().index("test").getShards().get(0).primaryShard().relocatingNodeId(), nullValue());
        assertThat(result.routingTable().index("test").getShards().get(1).primaryShard().state(), equalTo(STARTED));
        assertThat(result.routingTable().index("test").getShards().get(1).primaryShard().currentNodeId(), equalTo("node2"));
        assertThat(result.routingTable().index("test").getShards().get(1).primaryShard().relocatingNodeId(), nullValue());

        // Add another datanode, it should relocate.
        logger.info("--> adding node3");
        DiscoveryNode discoveryNode3 = new DiscoveryNode(
            "",
            "node3",
            buildNewFakeTransportAddress(),
            emptyMap(),
            singleton(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        ClusterState updateClusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(discoveryNode3))
            .build();

        firstRouting = TestShardRouting.newShardRouting("test", 0, "node2", null, true, ShardRoutingState.STARTED);
        secondRouting = TestShardRouting.newShardRouting("test", 1, "node2", "node3", true, ShardRoutingState.RELOCATING);
        firstRoutingNode = new RoutingNode("node2", discoveryNode2, firstRouting, secondRouting);
        builder = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(firstRouting.index())
                    .addIndexShard(new IndexShardRoutingTable.Builder(firstRouting.shardId()).addShard(firstRouting).build())
                    .addIndexShard(new IndexShardRoutingTable.Builder(secondRouting.shardId()).addShard(secondRouting).build())
            );

        clusterState = ClusterState.builder(updateClusterState).routingTable(builder.build()).build();
        routingAllocation = new RoutingAllocation(null, new RoutingNodes(clusterState), clusterState, clusterInfo, null, System.nanoTime());
        routingAllocation.debugDecision(true);
        decision = diskThresholdDecider.canRemain(firstRouting, firstRoutingNode, routingAllocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(
            ((Decision.Single) decision).getExplanation(),
            containsString("there is enough disk on this node for the shard to remain, free: [60b]")
        );

        result = strategy.reroute(clusterState, "reroute");
        assertThat(result.routingTable().index("test").getShards().get(0).primaryShard().state(), equalTo(STARTED));
        assertThat(result.routingTable().index("test").getShards().get(0).primaryShard().currentNodeId(), equalTo("node2"));
        assertThat(result.routingTable().index("test").getShards().get(0).primaryShard().relocatingNodeId(), nullValue());
        assertThat(result.routingTable().index("test").getShards().get(1).primaryShard().state(), equalTo(RELOCATING));
        assertThat(result.routingTable().index("test").getShards().get(1).primaryShard().currentNodeId(), equalTo("node2"));
        assertThat(result.routingTable().index("test").getShards().get(1).primaryShard().relocatingNodeId(), equalTo("node3"));
    }

    public void testWatermarksEnabledForSingleDataNode() {
        Settings diskSettings = Settings.builder()
            .put(DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "60%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "70%")
            .build();

        final Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("data", new DiskUsage("data", "data", "/dev/null", 100, 20));  // 80% used

        // We have an index with 1 primary shard, taking 40 bytes. The single data node has only 20 bytes free.
        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 40L);
        final ClusterInfo clusterInfo = new DevNullClusterInfo(usages, usages, shardSizes);

        DiskThresholdDecider diskThresholdDecider = makeDecider(diskSettings);
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();
        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        DiscoveryNode clusterManagerNode = new DiscoveryNode(
            "cluster-manager",
            "cluster-manager",
            buildNewFakeTransportAddress(),
            emptyMap(),
            singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        DiscoveryNode dataNode = new DiscoveryNode(
            "data",
            "data",
            buildNewFakeTransportAddress(),
            emptyMap(),
            singleton(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder().add(dataNode);
        if (randomBoolean()) {
            discoveryNodesBuilder.add(clusterManagerNode);
        }
        DiscoveryNodes discoveryNodes = discoveryNodesBuilder.build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        // validate that the shard cannot be allocated
        ClusterInfoService cis = () -> clusterInfo;
        AllocationDeciders deciders = new AllocationDeciders(
            new HashSet<>(
                Arrays.asList(
                    new SameShardAllocationDecider(
                        Settings.EMPTY,
                        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                    ),
                    diskThresholdDecider
                )
            )
        );
        AllocationService strategy = new AllocationService(
            deciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            cis,
            EmptySnapshotsInfoService.INSTANCE
        );
        ClusterState result = strategy.reroute(clusterState, "reroute");

        ShardRouting shardRouting = result.routingTable().index("test").getShards().get(0).primaryShard();
        assertThat(shardRouting.state(), equalTo(UNASSIGNED));
        assertThat(shardRouting.currentNodeId(), nullValue());
        assertThat(shardRouting.relocatingNodeId(), nullValue());

        // force assign shard and validate that it cannot remain.
        ShardId shardId = shardRouting.shardId();
        ShardRouting startedShard = shardRouting.initialize("data", null, 40L).moveToStarted();
        RoutingTable forceAssignedRoutingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(startedShard).build())
            )
            .build();
        clusterState = ClusterState.builder(clusterState).routingTable(forceAssignedRoutingTable).build();

        RoutingAllocation routingAllocation = new RoutingAllocation(
            null,
            new RoutingNodes(clusterState),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        routingAllocation.debugDecision(true);
        Decision decision = diskThresholdDecider.canRemain(startedShard, clusterState.getRoutingNodes().node("data"), routingAllocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(
            decision.getExplanation(),
            containsString(
                "the shard cannot remain on this node because it is above the high watermark cluster setting"
                    + " [cluster.routing.allocation.disk.watermark.high=70%] and there is less than the required [30.0%] free disk on node,"
                    + " actual free: [20.0%]"
            )
        );
    }

    public void testDiskThresholdWithSnapshotShardSizes() {
        final long shardSizeInBytes = randomBoolean() ? 10L : 50L;
        logger.info("--> using shard size [{}]", shardSizeInBytes);

        final Settings diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "90%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "95%")
            .build();

        final Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node1", new DiskUsage("node1", "n1", "/dev/null", 100, 21));  // 79% used
        usages.put("node2", new DiskUsage("node2", "n2", "/dev/null", 100, 1)); // 99% used
        final ClusterInfoService clusterInfoService = () -> new DevNullClusterInfo(usages, usages, Map.of());

        final AllocationDeciders deciders = new AllocationDeciders(
            new HashSet<>(
                Arrays.asList(
                    new RestoreInProgressAllocationDecider(),
                    new SameShardAllocationDecider(
                        Settings.EMPTY,
                        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                    ),
                    makeDecider(diskSettings)
                )
            )
        );

        final Snapshot snapshot = new Snapshot("_repository", new SnapshotId("_snapshot_name", UUIDs.randomBase64UUID(random())));
        final IndexId indexId = new IndexId("_indexid_name", UUIDs.randomBase64UUID(random()));
        final ShardId shardId = new ShardId(new Index("test", IndexMetadata.INDEX_UUID_NA_VALUE), 0);

        final Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putInSyncAllocationIds(0, singleton(AllocationId.newInitializing().getId()))
            )
            .build();

        final RoutingTable routingTable = RoutingTable.builder()
            .addAsNewRestore(
                metadata.index("test"),
                new RecoverySource.SnapshotRecoverySource("_restore_uuid", snapshot, Version.CURRENT, indexId),
                new HashSet<>()
            )
            .build();

        final Map<ShardId, RestoreInProgress.ShardRestoreStatus> shards = new HashMap<>();
        shards.put(shardId, new RestoreInProgress.ShardRestoreStatus("node1"));

        final RestoreInProgress.Builder restores = new RestoreInProgress.Builder().add(
            new RestoreInProgress.Entry("_restore_uuid", snapshot, RestoreInProgress.State.INIT, singletonList("test"), shards)
        );

        ClusterState clusterState = ClusterState.builder(new ClusterName(getTestName()))
            .metadata(metadata)
            .routingTable(routingTable)
            .putCustom(RestoreInProgress.TYPE, restores.build())
            .nodes(
                DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")) // node2 is added because DiskThresholdDecider
                                                                                     // automatically ignore single-node clusters
            )
            .build();

        assertThat(
            clusterState.getRoutingNodes()
                .shardsWithState(UNASSIGNED)
                .stream()
                .map(ShardRouting::unassignedInfo)
                .allMatch(unassignedInfo -> Reason.NEW_INDEX_RESTORED.equals(unassignedInfo.getReason())),
            is(true)
        );
        assertThat(
            clusterState.getRoutingNodes()
                .shardsWithState(UNASSIGNED)
                .stream()
                .map(ShardRouting::unassignedInfo)
                .allMatch(unassignedInfo -> AllocationStatus.NO_ATTEMPT.equals(unassignedInfo.getLastAllocationStatus())),
            is(true)
        );
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(1));

        final AtomicReference<SnapshotShardSizeInfo> snapshotShardSizeInfoRef = new AtomicReference<>(SnapshotShardSizeInfo.EMPTY);
        final AllocationService strategy = new AllocationService(
            deciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            clusterInfoService,
            snapshotShardSizeInfoRef::get
        );

        // reroute triggers snapshot shard size fetching
        clusterState = strategy.reroute(clusterState, "reroute");
        logShardStates(clusterState);

        // shard cannot be assigned yet as the snapshot shard size is unknown
        assertThat(
            clusterState.getRoutingNodes()
                .shardsWithState(UNASSIGNED)
                .stream()
                .map(ShardRouting::unassignedInfo)
                .allMatch(unassignedInfo -> AllocationStatus.FETCHING_SHARD_DATA.equals(unassignedInfo.getLastAllocationStatus())),
            is(true)
        );
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(1));

        final SnapshotShard snapshotShard = new SnapshotShard(snapshot, indexId, shardId);
        final Map<SnapshotShard, Long> snapshotShardSizes = new HashMap<>();

        final boolean shouldAllocate;
        if (randomBoolean()) {
            logger.info("--> simulating snapshot shards size retrieval success");
            snapshotShardSizes.put(snapshotShard, shardSizeInBytes);
            logger.info("--> shard allocation depends on its size");
            shouldAllocate = shardSizeInBytes < usages.get("node1").getFreeBytes();
        } else {
            logger.info("--> simulating snapshot shards size retrieval failure");
            snapshotShardSizes.put(snapshotShard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
            logger.info("--> shard is always allocated when its size could not be retrieved");
            shouldAllocate = true;
        }
        snapshotShardSizeInfoRef.set(new SnapshotShardSizeInfo(snapshotShardSizes));

        // reroute uses the previous snapshot shard size
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        logShardStates(clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(shouldAllocate ? 0 : 1));
        assertThat(clusterState.getRoutingNodes().shardsWithState("test", INITIALIZING, STARTED).size(), equalTo(shouldAllocate ? 1 : 0));
    }

    public void logShardStates(ClusterState state) {
        RoutingNodes rn = state.getRoutingNodes();
        logger.info(
            "--> counts: total: {}, unassigned: {}, initializing: {}, relocating: {}, started: {}",
            rn.shards(shard -> true).size(),
            rn.shardsWithState(UNASSIGNED).size(),
            rn.shardsWithState(INITIALIZING).size(),
            rn.shardsWithState(RELOCATING).size(),
            rn.shardsWithState(STARTED).size()
        );
        logger.info(
            "--> unassigned: {}, initializing: {}, relocating: {}, started: {}",
            rn.shardsWithState(UNASSIGNED),
            rn.shardsWithState(INITIALIZING),
            rn.shardsWithState(RELOCATING),
            rn.shardsWithState(STARTED)
        );
    }

    /**
     * ClusterInfo that always reports /dev/null for the shards' data paths.
     */
    static class DevNullClusterInfo extends ClusterInfo {
        DevNullClusterInfo(
            final Map<String, DiskUsage> leastAvailableSpaceUsage,
            final Map<String, DiskUsage> mostAvailableSpaceUsage,
            final Map<String, Long> shardSizes
        ) {
            this(leastAvailableSpaceUsage, mostAvailableSpaceUsage, shardSizes, Map.of(), Map.of());
        }

        DevNullClusterInfo(
            final Map<String, DiskUsage> leastAvailableSpaceUsage,
            final Map<String, DiskUsage> mostAvailableSpaceUsage,
            final Map<String, Long> shardSizes,
            final Map<String, FileCacheStats> nodeFileCacheStats
        ) {
            this(leastAvailableSpaceUsage, mostAvailableSpaceUsage, shardSizes, Map.of(), nodeFileCacheStats);
        }

        DevNullClusterInfo(
            final Map<String, DiskUsage> leastAvailableSpaceUsage,
            final Map<String, DiskUsage> mostAvailableSpaceUsage,
            final Map<String, Long> shardSizes,
            Map<NodeAndPath, ReservedSpace> reservedSpace,
            final Map<String, FileCacheStats> nodeFileCacheStats
        ) {
            super(leastAvailableSpaceUsage, mostAvailableSpaceUsage, shardSizes, null, reservedSpace, nodeFileCacheStats);
        }

        @Override
        public String getDataPath(ShardRouting shardRouting) {
            return "/dev/null";
        }
    }
}
