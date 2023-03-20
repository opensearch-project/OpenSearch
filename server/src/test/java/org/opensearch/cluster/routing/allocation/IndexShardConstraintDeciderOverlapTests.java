/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiskUsage;
import org.opensearch.cluster.OpenSearchAllocationWithConstraintsTestCase;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.ShardId;
import org.opensearch.test.VersionUtils;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;
import static org.opensearch.cluster.routing.ShardRoutingState.UNASSIGNED;

public class IndexShardConstraintDeciderOverlapTests extends OpenSearchAllocationWithConstraintsTestCase {

    /**
     * High watermark breach blocks new shard allocations to affected nodes. If shard count on such
     * nodes is low, this will cause IndexShardPerNodeConstraint to breach.
     *
     * This test verifies that this doesn't lead to unassigned shards, and there are no hot spots in eligible
     * nodes.
     */
    public void testHighWatermarkBreachWithLowShardCount() {
        setupInitialCluster(3, 15, 10, 1);
        addNodesWithIndexing(1, "high_watermark_node_", 6, 5, 1);

        // Disk threshold settings enabled
        Settings settings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), 0.7)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), 0.8)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), 0.95)
            .put("cluster.routing.allocation.node_concurrent_recoveries", 1)
            .put("cluster.routing.allocation.cluster_concurrent_recoveries", 1)
            .build();

        // Build Shard size and disk usages
        ImmutableOpenMap.Builder<String, DiskUsage> usagesBuilder = ImmutableOpenMap.builder();
        usagesBuilder.put("node_0", new DiskUsage("node_0", "node_0", "/dev/null", 100, 80)); // 20% used
        usagesBuilder.put("node_1", new DiskUsage("node_1", "node_1", "/dev/null", 100, 55)); // 45% used
        usagesBuilder.put("node_2", new DiskUsage("node_2", "node_2", "/dev/null", 100, 35)); // 65% used
        usagesBuilder.put("high_watermark_node_0", new DiskUsage("high_watermark_node_0", "high_watermark_node_0", "/dev/null", 100, 10)); // 90%
                                                                                                                                           // used

        ImmutableOpenMap<String, DiskUsage> usages = usagesBuilder.build();
        ImmutableOpenMap.Builder<String, Long> shardSizesBuilder = ImmutableOpenMap.builder();
        clusterState.getRoutingTable().allShards().forEach(shard -> shardSizesBuilder.put(shardIdentifierFromRouting(shard), 1L)); // Each
                                                                                                                                   // shard
                                                                                                                                   // is 1
                                                                                                                                   // byte
        ImmutableOpenMap<String, Long> shardSizes = shardSizesBuilder.build();

        final ImmutableOpenMap<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpace = new ImmutableOpenMap.Builder<
            ClusterInfo.NodeAndPath,
            ClusterInfo.ReservedSpace>().fPut(getNodeAndDevNullPath("node_0"), getReservedSpace())
            .fPut(getNodeAndDevNullPath("node_1"), getReservedSpace())
            .fPut(getNodeAndDevNullPath("node_2"), getReservedSpace())
            .fPut(getNodeAndDevNullPath("high_watermark_node_0"), getReservedSpace())
            .build();
        final ClusterInfo clusterInfo = new DevNullClusterInfo(usages, usages, shardSizes, reservedSpace);
        ClusterInfoService cis = () -> clusterInfo;
        allocation = createAllocationService(settings, cis);

        allocateAndCheckIndexShardHotSpots(false, 3, "node_0", "node_1", "node_2");
        assertForIndexShardHotSpots(true, 4);
        assertTrue(clusterState.getRoutingTable().shardsWithState(UNASSIGNED).isEmpty());
        assertTrue(clusterState.getRoutingNodes().node("high_watermark_node_0").isEmpty());

        /* Shard sizes that would breach high watermark on node_2 if allocated.
         */
        addIndices("big_index_", 1, 10, 0);
        ImmutableOpenMap.Builder<String, Long> bigIndexShardSizeBuilder = ImmutableOpenMap.builder(shardSizes);
        clusterState.getRoutingNodes().unassigned().forEach(shard -> bigIndexShardSizeBuilder.put(shardIdentifierFromRouting(shard), 20L));
        shardSizes = bigIndexShardSizeBuilder.build();
        final ClusterInfo bigIndexClusterInfo = new DevNullClusterInfo(usages, usages, shardSizes, reservedSpace);
        cis = () -> bigIndexClusterInfo;
        allocation = createAllocationService(settings, cis);

        allocateAndCheckIndexShardHotSpots(false, 2, "node_0", "node_1");
        assertForIndexShardHotSpots(true, 4);
        assertTrue(clusterState.getRoutingTable().shardsWithState(UNASSIGNED).isEmpty());
        for (ShardRouting shard : clusterState.getRoutingTable().index("big_index_0").shardsWithState(STARTED)) {
            assertNotEquals("node_2", shard.currentNodeId());
        }
    }

    private ClusterInfo.NodeAndPath getNodeAndDevNullPath(String node) {
        return new ClusterInfo.NodeAndPath(node, "/dev/null");
    }

    private ClusterInfo.ReservedSpace getReservedSpace() {
        return new ClusterInfo.ReservedSpace.Builder().add(new ShardId("", "", 0), 2).build();
    }

    /**
     * Test clusters with subset of nodes on older version.
     * New version shards should not migrate to old version nodes, even if this creates potential hot spots.
     */
    public void testNodeVersionCompatibilityOverlap() {
        setupInitialCluster(3, 6, 10, 1);

        // Add an old version node and exclude a new version node
        DiscoveryNodes.Builder nb = DiscoveryNodes.builder(clusterState.nodes())
            .add(newNode("old_node", VersionUtils.getPreviousVersion()));
        clusterState = ClusterState.builder(clusterState).nodes(nb.build()).build();
        buildAllocationService("node_0");

        // Shards should only go to remaining new version nodes
        allocateAndCheckIndexShardHotSpots(false, 2, "node_1", "node_2");
        assertForIndexShardHotSpots(true, 4);
        assertTrue(clusterState.getRoutingTable().shardsWithState(UNASSIGNED).isEmpty());

        for (ShardRouting shard : clusterState.getRoutingTable().allShards()) {
            assertNotEquals("node_0", shard.currentNodeId());
            assertNotEquals("old_node", shard.currentNodeId());
        }
    }

    /**
     * Test zone aware clusters with balanced zones.
     * No hot spots expected.
     */
    public void testZoneBalanced() {
        Map<String, Integer> nodesPerZone = new HashMap<>();
        nodesPerZone.put("zone_0", 3);
        nodesPerZone.put("zone_1", 3);
        createEmptyZoneAwareCluster(nodesPerZone);
        addIndices("index_", 4, 5, 1);

        buildZoneAwareAllocationService();
        allocateAndCheckIndexShardHotSpots(false, 6);

        resetCluster();
        buildZoneAwareAllocationService();
        allocateAndCheckIndexShardHotSpots(false, 6);
    }

    /**
     * Test zone aware clusters with unbalanced zones.
     * Hot spots expected as awareness forces shards per zone restrictions.
     */
    public void testZoneUnbalanced() {
        Map<String, Integer> nodesPerZone = new HashMap<>();
        nodesPerZone.put("zone_0", 5);
        nodesPerZone.put("zone_1", 1);
        createEmptyZoneAwareCluster(nodesPerZone);
        addIndices("index_", 1, 5, 1);
        updateInitialCluster();

        buildZoneAwareAllocationService();
        clusterState = allocateShardsAndBalance(clusterState);
        assertForIndexShardHotSpots(true, 6);
        assertTrue(clusterState.getRoutingTable().shardsWithState(UNASSIGNED).isEmpty());

        resetCluster();
        buildZoneAwareAllocationService();
        clusterState = allocateShardsAndBalance(clusterState);
        assertForIndexShardHotSpots(true, 6);
        assertTrue(clusterState.getRoutingTable().shardsWithState(UNASSIGNED).isEmpty());
    }

    /**
     * ClusterInfo that always points to DevNull.
     */
    public static class DevNullClusterInfo extends ClusterInfo {
        public DevNullClusterInfo(
            ImmutableOpenMap<String, DiskUsage> leastAvailableSpaceUsage,
            ImmutableOpenMap<String, DiskUsage> mostAvailableSpaceUsage,
            ImmutableOpenMap<String, Long> shardSizes,
            ImmutableOpenMap<NodeAndPath, ReservedSpace> reservedSpace
        ) {
            super(leastAvailableSpaceUsage, mostAvailableSpaceUsage, shardSizes, null, reservedSpace);
        }

        @Override
        public String getDataPath(ShardRouting shardRouting) {
            return "/dev/null";
        }
    }
}
