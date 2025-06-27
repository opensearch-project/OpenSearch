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
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiskUsage;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RecoverySource.EmptyStoreRecoverySource;
import org.opensearch.cluster.routing.RecoverySource.LocalShardsRecoverySource;
import org.opensearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingHelper;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for the DiskThresholdDecider
 */
public class DiskThresholdDeciderUnitTests extends OpenSearchAllocationTestCase {

    public void testCanAllocateUsesMaxAvailableSpace() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdDecider decider = new DiskThresholdDecider(Settings.EMPTY, nss);

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        final Index index = metadata.index("test").getIndex();

        ShardRouting test_0 = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        DiscoveryNode node_0 = new DiscoveryNode(
            "node_0",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            new HashSet<>(DiscoveryNodeRole.BUILT_IN_ROLES),
            Version.CURRENT
        );
        DiscoveryNode node_1 = new DiscoveryNode(
            "node_1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            new HashSet<>(DiscoveryNodeRole.BUILT_IN_ROLES),
            Version.CURRENT
        );

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(node_0).add(node_1)).build();

        // actual test -- after all that bloat :)
        final Map<String, DiskUsage> leastAvailableUsages = new HashMap<>();
        leastAvailableUsages.put("node_0", new DiskUsage("node_0", "node_0", "_na_", 100, 0)); // all full
        leastAvailableUsages.put("node_1", new DiskUsage("node_1", "node_1", "_na_", 100, 0)); // all full

        final Map<String, DiskUsage> mostAvailableUsage = new HashMap<>();
        // 20 - 99 percent since after allocation there must be at least 10% left and shard is 10byte
        mostAvailableUsage.put("node_0", new DiskUsage("node_0", "node_0", "_na_", 100, randomIntBetween(20, 100)));
        // this is weird and smells like a bug! it should be up to 20%?
        mostAvailableUsage.put("node_1", new DiskUsage("node_1", "node_1", "_na_", 100, randomIntBetween(0, 10)));

        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 10L); // 10 bytes
        final ClusterInfo clusterInfo = new ClusterInfo(
            leastAvailableUsages,
            mostAvailableUsage,
            shardSizes,
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of()
        );
        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(decider)),
            clusterState.getRoutingNodes(),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        allocation.debugDecision(true);
        Decision decision = decider.canAllocate(test_0, new RoutingNode("node_0", node_0), allocation);
        assertEquals(mostAvailableUsage.toString(), Decision.Type.YES, decision.type());
        assertThat(((Decision.Single) decision).getExplanation(), containsString("enough disk for shard on node"));
        decision = decider.canAllocate(test_0, new RoutingNode("node_1", node_1), allocation);
        assertEquals(mostAvailableUsage.toString(), Decision.Type.NO, decision.type());
        assertThat(
            ((Decision.Single) decision).getExplanation(),
            containsString(
                "the node is above the high watermark cluster "
                    + "setting [cluster.routing.allocation.disk.watermark.high=90%], using more disk space than the maximum allowed [90.0%]"
            )
        );
    }

    public void testCannotAllocateDueToLackOfDiskResources() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdDecider decider = new DiskThresholdDecider(Settings.EMPTY, nss);

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        final Index index = metadata.index("test").getIndex();

        ShardRouting test_0 = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        DiscoveryNode node_0 = new DiscoveryNode(
            "node_0",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            new HashSet<>(DiscoveryNodeRole.BUILT_IN_ROLES),
            Version.CURRENT
        );
        DiscoveryNode node_1 = new DiscoveryNode(
            "node_1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            new HashSet<>(DiscoveryNodeRole.BUILT_IN_ROLES),
            Version.CURRENT
        );

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(node_0).add(node_1)).build();

        // actual test -- after all that bloat :)

        final Map<String, DiskUsage> leastAvailableUsages = new HashMap<>();
        leastAvailableUsages.put("node_0", new DiskUsage("node_0", "node_0", "_na_", 100, 0)); // all full
        final Map<String, DiskUsage> mostAvailableUsage = new HashMap<>();
        final int freeBytes = randomIntBetween(20, 100);
        mostAvailableUsage.put("node_0", new DiskUsage("node_0", "node_0", "_na_", 100, freeBytes));

        final Map<String, Long> shardSizes = new HashMap<>();
        // way bigger than available space
        final long shardSize = randomIntBetween(110, 1000);
        shardSizes.put("[test][0][p]", shardSize);
        ClusterInfo clusterInfo = new ClusterInfo(
            leastAvailableUsages,
            mostAvailableUsage,
            shardSizes,
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of()
        );
        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(decider)),
            clusterState.getRoutingNodes(),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        allocation.debugDecision(true);
        Decision decision = decider.canAllocate(test_0, new RoutingNode("node_0", node_0), allocation);
        assertEquals(Decision.Type.NO, decision.type());

        assertThat(
            decision.getExplanation(),
            containsString(
                "allocating the shard to this node will bring the node above the high watermark cluster setting "
                    + "[cluster.routing.allocation.disk.watermark.high=90%] "
                    + "and cause it to have less than the minimum required [0b] of free space "
                    + "(free: ["
                    + freeBytes
                    + "b], estimated shard size: ["
                    + shardSize
                    + "b])"
            )
        );
    }

    public void testCanRemainUsesLeastAvailableSpace() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdDecider decider = new DiskThresholdDecider(Settings.EMPTY, nss);
        final Map<ShardRouting, String> shardRoutingMap = new HashMap<>();

        DiscoveryNode node_0 = new DiscoveryNode(
            "node_0",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            new HashSet<>(DiscoveryNodeRole.BUILT_IN_ROLES),
            Version.CURRENT
        );
        DiscoveryNode node_1 = new DiscoveryNode(
            "node_1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            new HashSet<>(DiscoveryNodeRole.BUILT_IN_ROLES),
            Version.CURRENT
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();
        final IndexMetadata indexMetadata = metadata.index("test");

        ShardRouting test_0 = ShardRouting.newUnassigned(
            new ShardId(indexMetadata.getIndex(), 0),
            true,
            EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        test_0 = ShardRoutingHelper.initialize(test_0, node_0.getId());
        test_0 = ShardRoutingHelper.moveToStarted(test_0);
        shardRoutingMap.put(test_0, "/node0/least");

        ShardRouting test_1 = ShardRouting.newUnassigned(
            new ShardId(indexMetadata.getIndex(), 1),
            true,
            EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        test_1 = ShardRoutingHelper.initialize(test_1, node_1.getId());
        test_1 = ShardRoutingHelper.moveToStarted(test_1);
        shardRoutingMap.put(test_1, "/node1/least");

        ShardRouting test_2 = ShardRouting.newUnassigned(
            new ShardId(indexMetadata.getIndex(), 2),
            true,
            EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        test_2 = ShardRoutingHelper.initialize(test_2, node_1.getId());
        test_2 = ShardRoutingHelper.moveToStarted(test_2);
        shardRoutingMap.put(test_2, "/node1/most");

        ShardRouting test_3 = ShardRouting.newUnassigned(
            new ShardId(indexMetadata.getIndex(), 3),
            true,
            EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        test_3 = ShardRoutingHelper.initialize(test_3, node_1.getId());
        test_3 = ShardRoutingHelper.moveToStarted(test_3);
        // Intentionally not in the shardRoutingMap. We want to test what happens when we don't know where it is.

        RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();

        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        logger.info("--> adding two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(node_0).add(node_1)).build();

        // actual test -- after all that bloat :)
        final Map<String, DiskUsage> leastAvailableUsages = new HashMap<>();
        leastAvailableUsages.put("node_0", new DiskUsage("node_0", "node_0", "/node0/least", 100, 10)); // 90% used
        leastAvailableUsages.put("node_1", new DiskUsage("node_1", "node_1", "/node1/least", 100, 9)); // 91% used

        final Map<String, DiskUsage> mostAvailableUsage = new HashMap<>();
        mostAvailableUsage.put("node_0", new DiskUsage("node_0", "node_0", "/node0/most", 100, 90)); // 10% used
        mostAvailableUsage.put("node_1", new DiskUsage("node_1", "node_1", "/node1/most", 100, 90)); // 10% used

        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 10L); // 10 bytes
        shardSizes.put("[test][1][p]", 10L);
        shardSizes.put("[test][2][p]", 10L);

        final ClusterInfo clusterInfo = new ClusterInfo(
            leastAvailableUsages,
            mostAvailableUsage,
            shardSizes,
            shardRoutingMap,
            Map.of(),
            Map.of(),
            Map.of()
        );
        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(decider)),
            clusterState.getRoutingNodes(),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        allocation.debugDecision(true);
        Decision decision = decider.canRemain(test_0, new RoutingNode("node_0", node_0), allocation);
        assertEquals(Decision.Type.YES, decision.type());
        assertThat(
            ((Decision.Single) decision).getExplanation(),
            containsString("there is enough disk on this node for the shard to remain, free: [10b]")
        );
        decision = decider.canRemain(test_1, new RoutingNode("node_1", node_1), allocation);
        assertEquals(Decision.Type.NO, decision.type());
        assertThat(
            ((Decision.Single) decision).getExplanation(),
            containsString(
                "the shard cannot remain on this node because it is "
                    + "above the high watermark cluster setting [cluster.routing.allocation.disk.watermark.high=90%] and there is less than "
                    + "the required [10.0%] free disk on node, actual free: [9.0%]"
            )
        );
        try {
            decider.canRemain(test_0, new RoutingNode("node_1", node_1), allocation);
            fail("not allocated on this node");
        } catch (IllegalArgumentException ex) {
            // not allocated on that node
        }
        try {
            decider.canRemain(test_1, new RoutingNode("node_0", node_0), allocation);
            fail("not allocated on this node");
        } catch (IllegalArgumentException ex) {
            // not allocated on that node
        }

        decision = decider.canRemain(test_2, new RoutingNode("node_1", node_1), allocation);
        assertEquals("can stay since allocated on a different path with enough space", Decision.Type.YES, decision.type());
        assertThat(
            ((Decision.Single) decision).getExplanation(),
            containsString("this shard is not allocated on the most utilized disk and can remain")
        );

        decision = decider.canRemain(test_2, new RoutingNode("node_1", node_1), allocation);
        assertEquals("can stay since we don't have information about this shard", Decision.Type.YES, decision.type());
        assertThat(
            ((Decision.Single) decision).getExplanation(),
            containsString("this shard is not allocated on the most utilized disk and can remain")
        );
    }

    public void testShardSizeAndRelocatingSize() {
        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][r]", 10L);
        shardSizes.put("[test][1][r]", 100L);
        shardSizes.put("[test][2][r]", 1000L);
        shardSizes.put("[other][0][p]", 10000L);
        ClusterInfo info = new DiskThresholdDeciderTests.DevNullClusterInfo(Map.of(), Map.of(), shardSizes);
        Metadata.Builder metaBuilder = Metadata.builder();
        metaBuilder.put(
            IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT).put("index.uuid", "1234"))
                .numberOfShards(3)
                .numberOfReplicas(1)
        );
        metaBuilder.put(
            IndexMetadata.builder("other")
                .settings(settings(Version.CURRENT).put("index.uuid", "5678"))
                .numberOfShards(1)
                .numberOfReplicas(1)
        );
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsNew(metadata.index("test"));
        routingTableBuilder.addAsNew(metadata.index("other"));
        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTableBuilder.build())
            .build();
        RoutingAllocation allocation = new RoutingAllocation(null, null, clusterState, info, null, 0);

        final Index index = new Index("test", "1234");
        ShardRouting test_0 = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            false,
            PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        test_0 = ShardRoutingHelper.initialize(test_0, "node1");
        test_0 = ShardRoutingHelper.moveToStarted(test_0);
        test_0 = ShardRoutingHelper.relocate(test_0, "node2");

        ShardRouting test_1 = ShardRouting.newUnassigned(
            new ShardId(index, 1),
            false,
            PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        test_1 = ShardRoutingHelper.initialize(test_1, "node2");
        test_1 = ShardRoutingHelper.moveToStarted(test_1);
        test_1 = ShardRoutingHelper.relocate(test_1, "node1");

        ShardRouting test_2 = ShardRouting.newUnassigned(
            new ShardId(index, 2),
            false,
            PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        test_2 = ShardRoutingHelper.initialize(test_2, "node1");
        test_2 = ShardRoutingHelper.moveToStarted(test_2);

        assertEquals(1000L, getExpectedShardSize(test_2, 0L, allocation));
        assertEquals(100L, getExpectedShardSize(test_1, 0L, allocation));
        assertEquals(10L, getExpectedShardSize(test_0, 0L, allocation));

        RoutingNode node = new RoutingNode(
            "node1",
            new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            test_0,
            test_1.getTargetRelocatingShard(),
            test_2
        );
        assertEquals(100L, sizeOfRelocatingShards(allocation, node, false, "/dev/null"));
        assertEquals(90L, sizeOfRelocatingShards(allocation, node, true, "/dev/null"));
        assertEquals(0L, sizeOfRelocatingShards(allocation, node, true, "/dev/some/other/dev"));
        assertEquals(0L, sizeOfRelocatingShards(allocation, node, true, "/dev/some/other/dev"));

        ShardRouting test_3 = ShardRouting.newUnassigned(
            new ShardId(index, 3),
            false,
            PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        test_3 = ShardRoutingHelper.initialize(test_3, "node1");
        test_3 = ShardRoutingHelper.moveToStarted(test_3);
        assertEquals(0L, getExpectedShardSize(test_3, 0L, allocation));

        ShardRouting other_0 = ShardRouting.newUnassigned(
            new ShardId("other", "5678", 0),
            randomBoolean(),
            PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        other_0 = ShardRoutingHelper.initialize(other_0, "node2");
        other_0 = ShardRoutingHelper.moveToStarted(other_0);
        other_0 = ShardRoutingHelper.relocate(other_0, "node1");

        node = new RoutingNode(
            "node1",
            new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            test_0,
            test_1.getTargetRelocatingShard(),
            test_2,
            other_0.getTargetRelocatingShard()
        );
        if (other_0.primary()) {
            assertEquals(10100L, sizeOfRelocatingShards(allocation, node, false, "/dev/null"));
            assertEquals(10090L, sizeOfRelocatingShards(allocation, node, true, "/dev/null"));
        } else {
            assertEquals(100L, sizeOfRelocatingShards(allocation, node, false, "/dev/null"));
            assertEquals(90L, sizeOfRelocatingShards(allocation, node, true, "/dev/null"));
        }
    }

    public long sizeOfRelocatingShards(RoutingAllocation allocation, RoutingNode node, boolean subtractShardsMovingAway, String dataPath) {
        return DiskThresholdDecider.sizeOfRelocatingShards(
            node,
            subtractShardsMovingAway,
            dataPath,
            allocation.clusterInfo(),
            allocation.metadata(),
            allocation.routingTable()
        );
    }

    public void testSizeShrinkIndex() {
        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 10L);
        shardSizes.put("[test][1][p]", 100L);
        shardSizes.put("[test][2][p]", 500L);
        shardSizes.put("[test][3][p]", 500L);

        ClusterInfo info = new DiskThresholdDeciderTests.DevNullClusterInfo(Map.of(), Map.of(), shardSizes);
        Metadata.Builder metaBuilder = Metadata.builder();
        metaBuilder.put(
            IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT).put("index.uuid", "1234"))
                .numberOfShards(4)
                .numberOfReplicas(0)
        );
        metaBuilder.put(
            IndexMetadata.builder("target")
                .settings(
                    settings(Version.CURRENT).put("index.uuid", "5678")
                        .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY, "test")
                        .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, "1234")
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
        );
        metaBuilder.put(
            IndexMetadata.builder("target2")
                .settings(
                    settings(Version.CURRENT).put("index.uuid", "9101112")
                        .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY, "test")
                        .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, "1234")
                )
                .numberOfShards(2)
                .numberOfReplicas(0)
        );
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsNew(metadata.index("test"));
        routingTableBuilder.addAsNew(metadata.index("target"));
        routingTableBuilder.addAsNew(metadata.index("target2"));
        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTableBuilder.build())
            .build();

        AllocationService allocationService = createAllocationService();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        clusterState = allocationService.reroute(clusterState, "foo");

        clusterState = startShardsAndReroute(
            allocationService,
            clusterState,
            clusterState.getRoutingTable().index("test").shardsWithState(ShardRoutingState.UNASSIGNED)
        );

        RoutingAllocation allocation = new RoutingAllocation(null, clusterState.getRoutingNodes(), clusterState, info, null, 0);

        final Index index = new Index("test", "1234");
        ShardRouting test_0 = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            LocalShardsRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        test_0 = ShardRoutingHelper.initialize(test_0, "node1");
        test_0 = ShardRoutingHelper.moveToStarted(test_0);

        ShardRouting test_1 = ShardRouting.newUnassigned(
            new ShardId(index, 1),
            true,
            LocalShardsRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        test_1 = ShardRoutingHelper.initialize(test_1, "node2");
        test_1 = ShardRoutingHelper.moveToStarted(test_1);

        ShardRouting test_2 = ShardRouting.newUnassigned(
            new ShardId(index, 2),
            true,
            LocalShardsRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        test_2 = ShardRoutingHelper.initialize(test_2, "node1");

        ShardRouting test_3 = ShardRouting.newUnassigned(
            new ShardId(index, 3),
            true,
            LocalShardsRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        test_3 = ShardRoutingHelper.initialize(test_3, "node1");
        assertEquals(500L, getExpectedShardSize(test_3, 0L, allocation));
        assertEquals(500L, getExpectedShardSize(test_2, 0L, allocation));
        assertEquals(100L, getExpectedShardSize(test_1, 0L, allocation));
        assertEquals(10L, getExpectedShardSize(test_0, 0L, allocation));

        ShardRouting target = ShardRouting.newUnassigned(
            new ShardId(new Index("target", "5678"), 0),
            true,
            LocalShardsRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        assertEquals(1110L, getExpectedShardSize(target, 0L, allocation));

        ShardRouting target2 = ShardRouting.newUnassigned(
            new ShardId(new Index("target2", "9101112"), 0),
            true,
            LocalShardsRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        assertEquals(110L, getExpectedShardSize(target2, 0L, allocation));

        target2 = ShardRouting.newUnassigned(
            new ShardId(new Index("target2", "9101112"), 1),
            true,
            LocalShardsRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        assertEquals(1000L, getExpectedShardSize(target2, 0L, allocation));

        // check that the DiskThresholdDecider still works even if the source index has been deleted
        ClusterState clusterStateWithMissingSourceIndex = ClusterState.builder(clusterState)
            .metadata(Metadata.builder(metadata).remove("test"))
            .routingTable(RoutingTable.builder(clusterState.routingTable()).remove("test").build())
            .build();

        allocationService.reroute(clusterState, "foo");
        RoutingAllocation allocationWithMissingSourceIndex = new RoutingAllocation(
            null,
            clusterStateWithMissingSourceIndex.getRoutingNodes(),
            clusterStateWithMissingSourceIndex,
            info,
            null,
            0
        );
        assertEquals(42L, getExpectedShardSize(target, 42L, allocationWithMissingSourceIndex));
        assertEquals(42L, getExpectedShardSize(target2, 42L, allocationWithMissingSourceIndex));
    }

    private static long getExpectedShardSize(ShardRouting shardRouting, long defaultSize, RoutingAllocation allocation) {
        return DiskThresholdDecider.getExpectedShardSize(
            shardRouting,
            defaultSize,
            allocation.clusterInfo(),
            allocation.snapshotShardSizeInfo(),
            allocation.metadata(),
            allocation.routingTable()
        );
    }

    public void testDiskUsageWithRelocations() {
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 1000L), 0).getFreeBytes(),
            equalTo(1000L)
        );
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 1000L), 9).getFreeBytes(),
            equalTo(991L)
        );
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 1000L), -9).getFreeBytes(),
            equalTo(1009L)
        );

        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 1000L), 0)
                .getFreeDiskAsPercentage(),
            equalTo(100.0)
        );
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 500L), 0)
                .getFreeDiskAsPercentage(),
            equalTo(50.0)
        );
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 500L), 100)
                .getFreeDiskAsPercentage(),
            equalTo(40.0)
        );

        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 1000L), 0)
                .getUsedDiskAsPercentage(),
            equalTo(0.0)
        );
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 500L), 0)
                .getUsedDiskAsPercentage(),
            equalTo(50.0)
        );
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 500L), 100)
                .getUsedDiskAsPercentage(),
            equalTo(60.0)
        );

        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", Long.MAX_VALUE, Long.MAX_VALUE), 0)
                .getFreeBytes(),
            equalTo(Long.MAX_VALUE)
        );
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", Long.MAX_VALUE, Long.MAX_VALUE), 10)
                .getFreeBytes(),
            equalTo(Long.MAX_VALUE - 10)
        );
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", Long.MAX_VALUE, Long.MAX_VALUE), -10)
                .getFreeBytes(),
            equalTo(Long.MAX_VALUE)
        );
    }
}
