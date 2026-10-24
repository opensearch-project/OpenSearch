/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.routing.allocation;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiskUsage;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.common.logging.LogConfigurator;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats.FileCacheStatsType;
import org.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.opensearch.index.store.remote.filecache.FileCacheStats;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Measures {@link AllocationService#reroute} on a fully-started warm cluster as the number of warm shards per node grows,
 * with {@code cluster.routing.allocation.disk.warm_threshold_enabled} on and off. The delta isolates the cost of
 * {@code WarmDiskThresholdDecider.canRemain}, which the remote and local shard balancers call once per started warm shard
 * on every reroute. Growth should be linear in shards per node; a quadratic trend indicates the decider is rescanning the
 * node's shards inside each {@code canRemain} call.
 * <p>
 * Indices use the {@code remote_snapshot} store type so {@code RoutingPool.getIndexPool} resolves REMOTE_CAPABLE via
 * {@code IndexMetadata.isRemoteSnapshot()} without a feature flag. Writable-warm ({@code index.warm}) indices take the
 * slower {@code FeatureFlags.isEnabled} + {@code Settings} path in {@code getIndexPool}, so absolute times here understate
 * production cost per shard visit; the growth trend with shards per node is the same for both index types.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class WarmDiskThresholdDeciderBenchmark {

    @Param({
        // shardsPerNode| warmNodes| warmThresholdEnabled
        "          500|         2|  true|",
        "         1000|         2|  true|",
        "         2000|         2|  true|",
        "         4000|         2|  true|",
        "         4000|         2| false|",
        "          400|       140|  true|",
        "          400|       140| false|" })
    public String shardsPerNodeWarmNodesEnabled = "500|2|true";

    public int shardsPerNode;
    public int warmNodes;
    public boolean warmThresholdEnabled;
    public int shardsPerIndex = 8;

    private static final long FILE_CACHE_BYTES = 1L << 40; // 1 TiB per warm node
    private static final double REMOTE_DATA_RATIO = 5.0;

    private AllocationService allocationService;
    private ClusterState clusterState;

    @Setup
    public void setUp() throws Exception {
        LogConfigurator.setNodeName("test");
        final String[] params = shardsPerNodeWarmNodesEnabled.split("\\|");
        shardsPerNode = toInt(params[0]);
        warmNodes = toInt(params[1]);
        warmThresholdEnabled = Boolean.parseBoolean(params[2].trim());

        final int totalShards = shardsPerNode * warmNodes;
        if (totalShards % shardsPerIndex != 0) {
            throw new IllegalArgumentException("shardsPerNode * warmNodes must be a multiple of shardsPerIndex=" + shardsPerIndex);
        }
        final int indexCount = totalShards / shardsPerIndex;

        // Warm (REMOTE_CAPABLE) indices, as created by the warm tier: remote_snapshot store type + index.warm.
        final Settings.Builder indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey())
            .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true);

        final Metadata.Builder mb = Metadata.builder();
        for (int i = 0; i < indexCount; i++) {
            mb.put(IndexMetadata.builder(indexName(i)).settings(indexSettings).numberOfShards(shardsPerIndex).numberOfReplicas(0));
        }
        final Metadata metadata = mb.build();

        // Build a fully STARTED routing table directly (round-robin over warm nodes) rather than allocating through the
        // service: the measured operation is a steady-state reroute, not initial allocation.
        final RoutingTable.Builder rb = RoutingTable.builder();
        int shardOrdinal = 0;
        for (int i = 0; i < indexCount; i++) {
            final IndexMetadata indexMetadata = metadata.index(indexName(i));
            final IndexRoutingTable.Builder irb = IndexRoutingTable.builder(indexMetadata.getIndex());
            for (int shardId = 0; shardId < shardsPerIndex; shardId++) {
                final String nodeId = nodeId(shardOrdinal++ % warmNodes);
                final ShardRouting started = ShardRouting.newUnassigned(
                    new ShardId(indexMetadata.getIndex(), shardId),
                    true,
                    RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "benchmark")
                ).initialize(nodeId, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE).moveToStarted();
                irb.addShard(started);
            }
            rb.add(irb.build());
        }

        final Set<DiscoveryNodeRole> warmRoles = Set.of(
            DiscoveryNodeRole.CLUSTER_MANAGER_ROLE,
            DiscoveryNodeRole.DATA_ROLE,
            DiscoveryNodeRole.WARM_ROLE
        );
        final DiscoveryNodes.Builder nb = DiscoveryNodes.builder();
        final Map<String, DiskUsage> usages = new HashMap<>();
        final Map<String, AggregateFileCacheStats> fileCacheStats = new HashMap<>();
        final long addressableBytes = (long) (FILE_CACHE_BYTES * REMOTE_DATA_RATIO);
        for (int n = 0; n < warmNodes; n++) {
            final String nodeId = nodeId(n);
            nb.add(Allocators.newNode(nodeId, Map.of(), warmRoles));
            // Ample free addressable space: every canRemain is a YES, so the full decider path runs without relocations.
            usages.put(nodeId, new DiskUsage(nodeId, nodeId, "/dev/null", addressableBytes, addressableBytes / 2));
            fileCacheStats.put(nodeId, fileCacheStatsFor(FILE_CACHE_BYTES));
        }

        clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(rb.build())
            .nodes(nb)
            .build();

        // Populated ClusterInfo is required: with EmptyClusterInfoService the decider fails open in earlyTerminate()
        // before reaching the per-shard path and the benchmark would measure nothing.
        final ClusterInfo clusterInfo = new ClusterInfo(usages, usages, Map.of(), Map.of(), Map.of(), fileCacheStats, Map.of());
        final ClusterInfoService clusterInfoService = () -> clusterInfo;

        final Settings settings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_WARM_DISK_THRESHOLD_ENABLED_SETTING.getKey(), warmThresholdEnabled)
            .put(FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey(), REMOTE_DATA_RATIO)
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allocationService = Allocators.createAllocationService(
            Allocators.defaultAllocationDeciders(settings, clusterSettings),
            clusterInfoService,
            settings
        );

        // Sanity: a balanced, fully-started cluster must not relocate anything, otherwise the measurement would include
        // relocation work rather than the pure decider sweep.
        final ClusterState after = allocationService.reroute(clusterState, "setup");
        final int relocating = after.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size();
        if (relocating != 0) {
            throw new IllegalStateException("expected no relocations on a balanced started cluster, found " + relocating);
        }
    }

    @Benchmark
    public ClusterState measureRerouteOnStartedWarmCluster() {
        return allocationService.reroute(clusterState, "reroute");
    }

    private static String indexName(int i) {
        return "warm_" + i;
    }

    private static String nodeId(int n) {
        return "warm_node_" + n;
    }

    private static AggregateFileCacheStats fileCacheStatsFor(long fileCacheSize) {
        return new AggregateFileCacheStats(
            0,
            new FileCacheStats(0, fileCacheSize, 0, 0, 0, 0, 0, 0, FileCacheStatsType.OVER_ALL_STATS),
            new FileCacheStats(0, fileCacheSize, 0, 0, 0, 0, 0, 0, FileCacheStatsType.FULL_FILE_STATS),
            new FileCacheStats(0, fileCacheSize, 0, 0, 0, 0, 0, 0, FileCacheStatsType.BLOCK_FILE_STATS),
            new FileCacheStats(0, fileCacheSize, 0, 0, 0, 0, 0, 0, FileCacheStatsType.PINNED_FILE_STATS)
        );
    }

    private int toInt(String v) {
        return Integer.valueOf(v.trim());
    }
}
