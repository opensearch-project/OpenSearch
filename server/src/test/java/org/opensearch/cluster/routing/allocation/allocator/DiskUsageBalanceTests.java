/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.allocator;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for the {@code cluster.routing.allocation.balance.disk_usage} balance factor
 * and its contribution to the balancer's weight function.
 */
public class DiskUsageBalanceTests extends OpenSearchAllocationTestCase {

    private static final String INDEX_SMALL = "small-index";
    private static final String INDEX_LARGE = "large-index";

    /**
     * A minimal concrete ShardsBalancer that exposes fixed averages so we can exercise
     * {@link BalancedShardsAllocator.WeightFunction#weight} in isolation without a full
     * routing allocation.
     */
    private static final class FixedAverageShardsBalancer extends ShardsBalancer {
        private final float avgShards;
        private final float avgShardsForIndex;
        private final float avgDiskUsageInBytes;

        FixedAverageShardsBalancer(float avgShards, float avgShardsForIndex, float avgDiskUsageInBytes) {
            this.avgShards = avgShards;
            this.avgShardsForIndex = avgShardsForIndex;
            this.avgDiskUsageInBytes = avgDiskUsageInBytes;
        }

        @Override
        public float avgShardsPerNode() {
            return avgShards;
        }

        @Override
        public float avgShardsPerNode(String index) {
            return avgShardsForIndex;
        }

        @Override
        public float avgPrimaryShardsPerNode() {
            return avgShards;
        }

        @Override
        public float avgPrimaryShardsPerNode(String index) {
            return avgShardsForIndex;
        }

        @Override
        public float avgDiskUsageInBytesPerNode() {
            return avgDiskUsageInBytes;
        }

        @Override
        void allocateUnassigned() {}

        @Override
        void moveShards() {}

        @Override
        void balance() {}

        @Override
        org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision decideAllocateUnassigned(ShardRouting shardRouting) {
            return org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision.NOT_TAKEN;
        }

        @Override
        org.opensearch.cluster.routing.allocation.MoveDecision decideMove(ShardRouting shardRouting) {
            return org.opensearch.cluster.routing.allocation.MoveDecision.NOT_TAKEN;
        }

        @Override
        org.opensearch.cluster.routing.allocation.MoveDecision decideRebalance(ShardRouting shardRouting) {
            return org.opensearch.cluster.routing.allocation.MoveDecision.NOT_TAKEN;
        }
    }

    private static BalancedShardsAllocator.ModelNode makeModelNode(String nodeId, long perShardBytes, int shardsToAdd) {
        DiscoveryNode discoveryNode = newNode(nodeId);
        RoutingNode routingNode = new RoutingNode(nodeId, discoveryNode);
        Map<String, Long> shardSizes = new HashMap<>();
        for (int i = 0; i < shardsToAdd; i++) {
            ShardRouting shard = TestShardRouting.newShardRouting(
                new ShardId(INDEX_SMALL, IndexMetadata.INDEX_UUID_NA_VALUE, i),
                nodeId,
                true,
                ShardRoutingState.STARTED
            );
            shardSizes.put(shardIdentifier(shard), perShardBytes);
        }
        ClusterInfo clusterInfo = new ClusterInfo(Map.of(), Map.of(), shardSizes, Map.of(), Map.of(), Map.of(), Map.of());
        BalancedShardsAllocator.ModelNode modelNode = new BalancedShardsAllocator.ModelNode(routingNode, clusterInfo);
        for (int i = 0; i < shardsToAdd; i++) {
            ShardRouting shard = TestShardRouting.newShardRouting(
                new ShardId(INDEX_SMALL, IndexMetadata.INDEX_UUID_NA_VALUE, i),
                nodeId,
                true,
                ShardRoutingState.STARTED
            );
            modelNode.addShard(shard);
        }
        return modelNode;
    }

    /** Mirrors {@code ClusterInfo.shardIdentifierFromRouting} (package-private) for test-side key construction. */
    private static String shardIdentifier(ShardRouting shardRouting) {
        return shardRouting.shardId().toString() + "[" + (shardRouting.primary() ? "p" : "r") + "]";
    }

    /** 1. WeightFunction must include the disk-usage term when diskUsageBalance > 0. */
    public void testWeightFunctionIncludesDiskUsageTerm() {
        final float indexBalance = 0.0f;
        final float shardBalance = 0.0f;
        final float diskUsageBalance = 1.0f;
        BalancedShardsAllocator.WeightFunction wf = new BalancedShardsAllocator.WeightFunction(
            indexBalance,
            shardBalance,
            diskUsageBalance,
            0.0f,
            0L,
            false,
            false
        );

        // A node with 3 shards of 100 bytes each and cluster average of 150 bytes.
        BalancedShardsAllocator.ModelNode node = makeModelNode("n1", 100L, 3);
        assertEquals(300L, node.diskUsageInBytes());

        float avgDisk = 150.0f;
        ShardsBalancer balancer = new FixedAverageShardsBalancer(3.0f, 3.0f, avgDisk);
        // With shard/index balance == 0 and diskUsage == 1 (theta2 == 1), weight must be (disk - avgDisk).
        float expected = 300.0f - avgDisk;
        assertEquals(expected, wf.weight(balancer, node, INDEX_SMALL), 1e-4f);

        // Sign sanity: an under-used node (below the average) produces a negative weight.
        BalancedShardsAllocator.ModelNode coldNode = makeModelNode("cold", 10L, 2);
        assertTrue("under-used node must produce negative disk-usage weight", wf.weight(balancer, coldNode, INDEX_SMALL) < 0.0f);
    }

    /** 2. With diskUsageBalance = 0 the weights must equal the count-and-index two-term computation. */
    public void testDefaultFactorPreservesExistingBehavior() {
        final float indexBalance = 0.55f;
        final float shardBalance = 0.45f;
        BalancedShardsAllocator.WeightFunction withDisk = new BalancedShardsAllocator.WeightFunction(
            indexBalance,
            shardBalance,
            0.0f,
            0.0f,
            0L,
            false,
            false
        );

        BalancedShardsAllocator.ModelNode node = makeModelNode("n1", 999_999L, 4); // huge disk, should be ignored
        ShardsBalancer balancer = new FixedAverageShardsBalancer(2.5f, 1.5f, 10.0f);

        // Compute expected weight using only the count-and-index two-term formula.
        float sum = indexBalance + shardBalance;
        float theta0 = shardBalance / sum;
        float theta1 = indexBalance / sum;
        float expected = theta0 * (node.numShards() - balancer.avgShardsPerNode()) + theta1 * (node.numShards(INDEX_SMALL) - balancer
            .avgShardsPerNode(INDEX_SMALL));

        assertEquals(expected, withDisk.weight(balancer, node, INDEX_SMALL), 1e-4f);
    }

    /** 3. Sum of balance factors must be strictly positive. */
    public void testSumMustBePositive() {
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> new BalancedShardsAllocator.WeightFunction(0.0f, 0.0f, 0.0f, 0.0f, 0L, false, false)
        );
        String msg = iae.getMessage() == null ? "" : iae.getMessage();
        assertTrue("expected message to mention sum or '> 0' but was: " + msg, msg.contains("sum") || msg.contains("> 0"));
    }

    /** 4. Dynamic setting update of balance.disk_usage must rebuild the weight function. */
    public void testDynamicSettingUpdate() throws Exception {
        Settings settings = Settings.builder()
            .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), 1.0f)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(settings, clusterSettings);

        // Observe initial factor via public accessor — no reflection needed.
        assertEquals(1.0f, allocator.getDiskUsageBalance(), 1e-6f);
        assertEquals(0.0f, allocator.getIndexBalance(), 1e-6f);

        // Apply dynamic update: introduce an index-balance term and shrink disk-usage factor.
        Settings updated = Settings.builder()
            .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 1.0f)
            .put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), 0.3f)
            .build();
        clusterSettings.applySettings(updated);

        // After the update the internal weightFunction must have been rebuilt with the new factors.
        assertEquals("indexBalance must reflect the updated setting", 1.0f, allocator.getIndexBalance(), 1e-6f);
        assertEquals("diskUsageBalance must reflect the updated setting", 0.3f, allocator.getDiskUsageBalance(), 1e-6f);
    }

    /** 5. Negative values for the disk_usage setting must be rejected. */
    public void testNegativeSettingRejected() {
        Settings bad = Settings.builder().put("cluster.routing.allocation.balance.disk_usage", -0.1f).build();
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.get(bad)
        );
        String msg = iae.getMessage() == null ? "" : iae.getMessage();
        assertTrue(
            "expected rejection message to mention the lower bound, got: " + msg,
            msg.contains("must be >=") || msg.contains("-0.1") || msg.toLowerCase(java.util.Locale.ROOT).contains("disk_usage")
        );
    }

    /** 6. End-to-end: allocation completes with a heterogeneous ClusterInfoService and disk balance factor. */
    public void testAllocationWithHeterogeneousShardSizes() {
        final int numberOfNodes = 3;
        final int smallShards = 4; // 1MB each
        final int largeShards = 2; // 1GB each

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder(INDEX_SMALL).settings(settings(Version.CURRENT)).numberOfShards(smallShards).numberOfReplicas(0))
            .put(IndexMetadata.builder(INDEX_LARGE).settings(settings(Version.CURRENT)).numberOfShards(largeShards).numberOfReplicas(0))
            .build();

        RoutingTable.Builder rtBuilder = RoutingTable.builder();
        for (IndexMetadata im : metadata) {
            rtBuilder.addAsNew(im);
        }
        RoutingTable initialRouting = rtBuilder.build();

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfNodes; i++) {
            nodes.add(newNode("node" + i));
        }

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRouting)
            .nodes(nodes)
            .build();

        // Build a ClusterInfoService that reports per-shard sizes.
        Map<String, Long> shardSizes = new HashMap<>();
        for (int i = 0; i < smallShards; i++) {
            ShardId sid = new ShardId(clusterState.metadata().index(INDEX_SMALL).getIndex(), i);
            ShardRouting r = TestShardRouting.newShardRouting(sid, null, true, ShardRoutingState.UNASSIGNED);
            shardSizes.put(shardIdentifier(r), 1L << 20); // 1MB
        }
        for (int i = 0; i < largeShards; i++) {
            ShardId sid = new ShardId(clusterState.metadata().index(INDEX_LARGE).getIndex(), i);
            ShardRouting r = TestShardRouting.newShardRouting(sid, null, true, ShardRoutingState.UNASSIGNED);
            shardSizes.put(shardIdentifier(r), 1L << 30); // 1GB
        }
        final ClusterInfo clusterInfo = new ClusterInfo(Map.of(), Map.of(), shardSizes, Map.of(), Map.of(), Map.of(), Map.of());
        ClusterInfoService cis = () -> clusterInfo;

        Settings settings = Settings.builder()
            .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), 1.0f)
            .put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 1.0f)
            .build();

        AllocationService strategy = createAllocationService(settings, cis);
        // Reroute and start all shards.
        clusterState = strategy.reroute(clusterState, "init");
        // Start any initializing shards.
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        // Verify every shard is assigned.
        assertEquals(
            "all shards should be assigned, unassigned = " + clusterState.getRoutingNodes().unassigned().size(),
            0,
            clusterState.getRoutingNodes().unassigned().size()
        );

        // Compute bytes per node and count per node from the authoritative routing table.
        Map<String, Long> bytesPerNode = new HashMap<>();
        Map<String, Integer> shardsPerNode = new HashMap<>();
        for (IndexMetadata im : clusterState.metadata()) {
            for (IndexShardRoutingTable isrt : clusterState.routingTable().index(im.getIndex())) {
                for (ShardRouting sr : isrt) {
                    if (sr.assignedToNode() == false) continue;
                    String nodeId = sr.currentNodeId();
                    shardsPerNode.merge(nodeId, 1, Integer::sum);
                    Long size = shardSizes.get(shardIdentifier(sr));
                    bytesPerNode.merge(nodeId, size == null ? 0L : size, Long::sum);
                }
            }
        }

        // Total bytes should be spread somewhat reasonably (no node holds >90% of the bytes).
        long totalBytes = 0L;
        for (long b : bytesPerNode.values()) {
            totalBytes += b;
        }
        assertTrue("total bytes should be non-zero", totalBytes > 0L);
        for (Map.Entry<String, Long> e : bytesPerNode.entrySet()) {
            double frac = (double) e.getValue() / (double) totalBytes;
            assertTrue("node " + e.getKey() + " holds " + frac + " of bytes which is too skewed under disk_usage balance", frac < 0.9);
        }
        // At least one node should be holding part of the large-index bytes.
        boolean someNodeHasLarge = false;
        for (long b : bytesPerNode.values()) {
            if (b >= (1L << 30)) {
                someNodeHasLarge = true;
                break;
            }
        }
        assertTrue("some node should carry at least one large shard", someNodeHasLarge);

        int totalShards = 0;
        for (int s : shardsPerNode.values()) {
            totalShards += s;
        }
        assertEquals(smallShards + largeShards, totalShards);
    }

    /**
     * Builds a cluster state where shard counts are balanced 3/3/3 but bytes are skewed:
     * all three 100MB shards of idx_large are pinned to node-0, while idx_small's 6 shards
     * are split 3/3 across node-1 and node-2. node-0 has 300MB, the others have only 3MB each.
     *
     * Returns the common base state; callers supply the disk_usage factor.
     */
    private ClusterState buildCountBalancedButByteSkewedState(Map<String, Long> shardSizesOut) {
        final String idxSmall = "idx_small";
        final String idxLarge = "idx_large";
        final long smallBytes = 1L << 20; // 1 MB
        final long largeBytes = 100L * (1L << 20); // 100 MB

        IndexMetadata smallMetaStub = IndexMetadata.builder(idxSmall)
            .settings(settings(Version.CURRENT))
            .numberOfShards(6)
            .numberOfReplicas(0)
            .build();
        IndexMetadata largeMetaStub = IndexMetadata.builder(idxLarge)
            .settings(settings(Version.CURRENT))
            .numberOfShards(3)
            .numberOfReplicas(0)
            .build();

        // Layout (counts 3/3/3, bytes 300MB/3MB/3MB):
        // node-0: idx_large shards 0,1,2 (3 x 100 MB)
        // node-1: idx_small shards 0,1,2 (3 x 1 MB)
        // node-2: idx_small shards 3,4,5 (3 x 1 MB)
        IndexRoutingTable.Builder largeRt = IndexRoutingTable.builder(largeMetaStub.getIndex());
        for (int i = 0; i < 3; i++) {
            largeRt.addShard(
                TestShardRouting.newShardRouting(new ShardId(largeMetaStub.getIndex(), i), "node-0", true, ShardRoutingState.STARTED)
            );
        }
        IndexRoutingTable.Builder smallRt = IndexRoutingTable.builder(smallMetaStub.getIndex());
        for (int i = 0; i < 6; i++) {
            String host = i < 3 ? "node-1" : "node-2";
            smallRt.addShard(
                TestShardRouting.newShardRouting(new ShardId(smallMetaStub.getIndex(), i), host, true, ShardRoutingState.STARTED)
            );
        }
        IndexRoutingTable largeIrt = largeRt.build();
        IndexRoutingTable smallIrt = smallRt.build();

        // Populate in-sync allocation IDs so RoutingTable#validate accepts the started shards.
        IndexMetadata.Builder smallMetaBuilder = IndexMetadata.builder(smallMetaStub);
        for (IndexShardRoutingTable isrt : smallIrt) {
            smallMetaBuilder.putInSyncAllocationIds(isrt.shardId().id(), isrt.getAllAllocationIds());
        }
        IndexMetadata.Builder largeMetaBuilder = IndexMetadata.builder(largeMetaStub);
        for (IndexShardRoutingTable isrt : largeIrt) {
            largeMetaBuilder.putInSyncAllocationIds(isrt.shardId().id(), isrt.getAllAllocationIds());
        }
        Metadata metadata = Metadata.builder().put(smallMetaBuilder).put(largeMetaBuilder).build();
        RoutingTable routingTable = RoutingTable.builder().add(largeIrt).add(smallIrt).build();

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder().add(newNode("node-0")).add(newNode("node-1")).add(newNode("node-2"));

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(nodes)
            .build();

        // Populate shard-sizes map so ClusterInfo can report per-shard bytes to the balancer.
        for (IndexShardRoutingTable isrt : clusterState.routingTable().index(idxLarge)) {
            for (ShardRouting sr : isrt) {
                shardSizesOut.put(shardIdentifier(sr), largeBytes);
            }
        }
        for (IndexShardRoutingTable isrt : clusterState.routingTable().index(idxSmall)) {
            for (ShardRouting sr : isrt) {
                shardSizesOut.put(shardIdentifier(sr), smallBytes);
            }
        }
        return clusterState;
    }

    /** Helper: count the idx_large shards still on node-0. */
    private int largeShardsOnNode0(ClusterState clusterState) {
        int count = 0;
        IndexMetadata im = clusterState.metadata().index("idx_large");
        for (IndexShardRoutingTable isrt : clusterState.routingTable().index(im.getIndex())) {
            for (ShardRouting sr : isrt) {
                if (sr.assignedToNode() && "node-0".equals(sr.currentNodeId())) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Proves the disk_usage factor actually drives rebalancing: starting from a count-balanced
     * (3/3/3) but byte-skewed (300MB/3MB/3MB) state, enabling {@code balance.disk_usage} should
     * cause at least one large shard to leave node-0.
     */
    public void testDiskUsageBalanceMovesShardWhenCountBalanced() {
        Map<String, Long> shardSizes = new HashMap<>();
        ClusterState clusterState = buildCountBalancedButByteSkewedState(shardSizes);
        assertEquals("precondition: all 3 large shards start on node-0", 3, largeShardsOnNode0(clusterState));

        final ClusterInfo clusterInfo = new ClusterInfo(Map.of(), Map.of(), shardSizes, Map.of(), Map.of(), Map.of(), Map.of());
        ClusterInfoService cis = () -> clusterInfo;

        // Zero out shard / index balance so disk_usage is the sole driver of rebalancing. A large
        // factor (1.0f) is fine here since theta2 is normalised by the sum of all balance factors.
        Settings settings = Settings.builder()
            .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), 1.0f)
            .put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 1.0f)
            .build();

        AllocationService strategy = createAllocationService(settings, cis);
        ClusterState after = strategy.reroute(clusterState, "disk-usage-test");
        after = startInitializingShardsAndReroute(strategy, after);
        after = startInitializingShardsAndReroute(strategy, after);

        int remainingLargeOnNode0 = largeShardsOnNode0(after);
        assertTrue(
            "disk_usage balance should have moved at least one large shard off node-0, but " + remainingLargeOnNode0 + " of 3 remain",
            remainingLargeOnNode0 < 3
        );
    }

    /**
     * Control test: with {@code balance.disk_usage = 0} the allocator should NOT touch a
     * count-balanced state even when bytes are heavily skewed. This proves the assertion in
     * {@link #testDiskUsageBalanceMovesShardWhenCountBalanced()} is attributable to the setting.
     */
    public void testDiskUsageBalanceNoOpWhenZero() {
        Map<String, Long> shardSizes = new HashMap<>();
        ClusterState clusterState = buildCountBalancedButByteSkewedState(shardSizes);
        assertEquals("precondition: all 3 large shards start on node-0", 3, largeShardsOnNode0(clusterState));

        final ClusterInfo clusterInfo = new ClusterInfo(Map.of(), Map.of(), shardSizes, Map.of(), Map.of(), Map.of(), Map.of());
        ClusterInfoService cis = () -> clusterInfo;

        Settings settings = Settings.builder()
            .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 1.0f)
            .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 1.0f)
            .build();

        AllocationService strategy = createAllocationService(settings, cis);
        ClusterState after = strategy.reroute(clusterState, "no-disk-usage");
        after = startInitializingShardsAndReroute(strategy, after);
        after = startInitializingShardsAndReroute(strategy, after);

        assertEquals("with disk_usage=0 a count-balanced layout must not move any large shards off node-0", 3, largeShardsOnNode0(after));
    }
}
