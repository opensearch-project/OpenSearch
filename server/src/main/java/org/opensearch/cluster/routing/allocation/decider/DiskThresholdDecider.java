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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.DiskUsage;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.opensearch.index.store.remote.filecache.FileCacheStats;
import org.opensearch.snapshots.SnapshotShardSizeInfo;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.opensearch.cluster.routing.RoutingPool.REMOTE_CAPABLE;
import static org.opensearch.cluster.routing.RoutingPool.getNodePool;
import static org.opensearch.cluster.routing.RoutingPool.getShardPool;
import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING;
import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING;

/**
 * The {@link DiskThresholdDecider} checks that the node a shard is potentially
 * being allocated to has enough disk space.
 * <p>
 * It has three configurable settings, all of which can be changed dynamically:
 * <p>
 * <code>cluster.routing.allocation.disk.watermark.low</code> is the low disk
 * watermark. New shards will not allocated to a node with usage higher than this,
 * although this watermark may be passed by allocating a shard. It defaults to
 * 0.85 (85.0%).
 * <p>
 * <code>cluster.routing.allocation.disk.watermark.high</code> is the high disk
 * watermark. If a node has usage higher than this, shards are not allowed to
 * remain on the node. In addition, if allocating a shard to a node causes the
 * node to pass this watermark, it will not be allowed. It defaults to
 * 0.90 (90.0%).
 * <p>
 * Both watermark settings are expressed in terms of used disk percentage, or
 * exact byte values for free space (like "500mb")
 * <p>
 * <code>cluster.routing.allocation.disk.threshold_enabled</code> is used to
 * enable or disable this decider. It defaults to true (enabled).
 *
 * @opensearch.internal
 */
public class DiskThresholdDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(DiskThresholdDecider.class);

    public static final String NAME = "disk_threshold";

    public static final Setting<Boolean> ENABLE_FOR_SINGLE_DATA_NODE = Setting.boolSetting(
        "cluster.routing.allocation.disk.watermark.enable_for_single_data_node",
        false,
        Setting.Property.NodeScope
    );

    private final DiskThresholdSettings diskThresholdSettings;
    private final boolean enableForSingleDataNode;
    private final FileCacheSettings fileCacheSettings;

    public DiskThresholdDecider(Settings settings, ClusterSettings clusterSettings) {
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterSettings);
        assert Version.CURRENT.major < 9 : "remove enable_for_single_data_node in 9";
        this.enableForSingleDataNode = ENABLE_FOR_SINGLE_DATA_NODE.get(settings);
        this.fileCacheSettings = new FileCacheSettings(settings, clusterSettings);
    }

    /**
     * Returns the size of all shards that are currently being relocated to
     * the node, but may not be finished transferring yet.
     * <p>
     * If subtractShardsMovingAway is true then the size of shards moving away is subtracted from the total size of all shards
     */
    public static long sizeOfRelocatingShards(
        RoutingNode node,
        boolean subtractShardsMovingAway,
        String dataPath,
        ClusterInfo clusterInfo,
        Metadata metadata,
        RoutingTable routingTable
    ) {
        // Account for reserved space wherever it is available
        final ClusterInfo.ReservedSpace reservedSpace = clusterInfo.getReservedSpace(node.nodeId(), dataPath);
        long totalSize = reservedSpace.getTotal();
        // NB this counts all shards on the node when the ClusterInfoService retrieved the node stats, which may include shards that are
        // no longer initializing because their recovery failed or was cancelled.

        // Where reserved space is unavailable (e.g. stats are out-of-sync) compute a conservative estimate for initialising shards
        final List<ShardRouting> initializingShards = node.shardsWithState(ShardRoutingState.INITIALIZING);
        for (ShardRouting routing : initializingShards) {
            if (routing.relocatingNodeId() == null || reservedSpace.containsShardId(routing.shardId())) {
                // in practice the only initializing-but-not-relocating shards with a nonzero expected shard size will be ones created
                // by a resize (shrink/split/clone) operation which we expect to happen using hard links, so they shouldn't be taking
                // any additional space and can be ignored here
                continue;
            }

            final String actualPath = clusterInfo.getDataPath(routing);
            // if we don't yet know the actual path of the incoming shard then conservatively assume it's going to the path with the least
            // free space
            if (actualPath == null || actualPath.equals(dataPath)) {
                totalSize += getExpectedShardSize(routing, 0L, clusterInfo, null, metadata, routingTable);
            }
        }

        if (subtractShardsMovingAway) {
            for (ShardRouting routing : node.shardsWithState(ShardRoutingState.RELOCATING)) {
                String actualPath = clusterInfo.getDataPath(routing);
                if (actualPath == null) {
                    // we might know the path of this shard from before when it was relocating
                    actualPath = clusterInfo.getDataPath(routing.cancelRelocation());
                }
                if (dataPath.equals(actualPath)) {
                    totalSize -= getExpectedShardSize(routing, 0L, clusterInfo, null, metadata, routingTable);
                }
            }
        }

        return totalSize;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        ClusterInfo clusterInfo = allocation.clusterInfo();

        /*
         The following block enables allocation for remote shards within safeguard limits of the filecache.
         */
        if (REMOTE_CAPABLE.equals(getNodePool(node)) && REMOTE_CAPABLE.equals(getShardPool(shardRouting, allocation))) {
            final double dataToFileCacheSizeRatio = fileCacheSettings.getRemoteDataRatio();
            // we don't need to check the ratio
            if (dataToFileCacheSizeRatio <= 0.1f) {
                return Decision.YES;
            }

            final List<ShardRouting> remoteShardsOnNode = StreamSupport.stream(node.spliterator(), false)
                .filter(shard -> shard.primary() && REMOTE_CAPABLE.equals(getShardPool(shard, allocation)))
                .collect(Collectors.toList());
            final long currentNodeRemoteShardSize = remoteShardsOnNode.stream()
                .map(ShardRouting::getExpectedShardSize)
                .mapToLong(Long::longValue)
                .sum();

            final long shardSize = getExpectedShardSize(
                shardRouting,
                0L,
                allocation.clusterInfo(),
                allocation.snapshotShardSizeInfo(),
                allocation.metadata(),
                allocation.routingTable()
            );

            final FileCacheStats fileCacheStats = clusterInfo.getNodeFileCacheStats().getOrDefault(node.nodeId(), null);
            final long nodeCacheSize = fileCacheStats != null ? fileCacheStats.getTotal().getBytes() : 0;
            final long totalNodeRemoteShardSize = currentNodeRemoteShardSize + shardSize;
            if (dataToFileCacheSizeRatio > 0.0f && totalNodeRemoteShardSize > dataToFileCacheSizeRatio * nodeCacheSize) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "file cache limit reached - remote shard size will exceed configured safeguard ratio"
                );
            }
            return Decision.YES;
        } else if (REMOTE_CAPABLE.equals(getShardPool(shardRouting, allocation))) {
            return Decision.NO;
        }

        Map<String, DiskUsage> usages = clusterInfo.getNodeMostAvailableDiskUsages();
        final Decision decision = earlyTerminate(allocation, usages);
        if (decision != null) {
            return decision;
        }

        final double usedDiskThresholdLow = 100.0 - diskThresholdSettings.getFreeDiskThresholdLow();
        final double usedDiskThresholdHigh = 100.0 - diskThresholdSettings.getFreeDiskThresholdHigh();

        // subtractLeavingShards is passed as false here, because they still use disk space, and therefore we should be extra careful
        // and take the size into account
        final DiskUsageWithRelocations usage = getDiskUsage(
            node,
            allocation,
            usages,
            clusterInfo.getAvgFreeByte(),
            clusterInfo.getAvgTotalBytes(),
            false
        );
        // First, check that the node currently over the low watermark
        double freeDiskPercentage = usage.getFreeDiskAsPercentage();
        // Cache the used disk percentage for displaying disk percentages consistent with documentation
        double usedDiskPercentage = usage.getUsedDiskAsPercentage();
        long freeBytes = usage.getFreeBytes();
        if (freeBytes < 0L) {
            final long sizeOfRelocatingShards = sizeOfRelocatingShards(
                node,
                false,
                usage.getPath(),
                allocation.clusterInfo(),
                allocation.metadata(),
                allocation.routingTable()
            );
            logger.debug(
                "fewer free bytes remaining than the size of all incoming shards: "
                    + "usage {} on node {} including {} bytes of relocations, preventing allocation",
                usage,
                node.nodeId(),
                sizeOfRelocatingShards
            );

            return allocation.decision(
                Decision.NO,
                NAME,
                "the node has fewer free bytes remaining than the total size of all incoming shards: "
                    + "free space [%sB], relocating shards [%sB]",
                freeBytes + sizeOfRelocatingShards,
                sizeOfRelocatingShards
            );
        }

        ByteSizeValue freeBytesValue = new ByteSizeValue(freeBytes);
        if (logger.isTraceEnabled()) {
            logger.trace("node [{}] has {}% used disk", node.nodeId(), usedDiskPercentage);
        }

        // flag that determines whether the low threshold checks below can be skipped. We use this for a primary shard that is freshly
        // allocated and empty.
        boolean skipLowThresholdChecks = shardRouting.primary()
            && shardRouting.active() == false
            && shardRouting.recoverySource().getType() == RecoverySource.Type.EMPTY_STORE;

        // checks for exact byte comparisons
        if (freeBytes < diskThresholdSettings.getFreeBytesThresholdLow().getBytes()) {
            if (skipLowThresholdChecks == false) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "less than the required {} free bytes threshold ({} free) on node {}, preventing allocation",
                        diskThresholdSettings.getFreeBytesThresholdLow(),
                        freeBytesValue,
                        node.nodeId()
                    );
                }
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "the node is above the low watermark cluster setting [%s=%s], having less than the minimum required [%s] free "
                        + "space, actual free: [%s]",
                    CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
                    diskThresholdSettings.getLowWatermarkRaw(),
                    diskThresholdSettings.getFreeBytesThresholdLow(),
                    freeBytesValue
                );
            } else if (freeBytes > diskThresholdSettings.getFreeBytesThresholdHigh().getBytes()) {
                // Allow the shard to be allocated because it is primary that
                // has never been allocated if it's under the high watermark
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "less than the required {} free bytes threshold ({} free) on node {}, "
                            + "but allowing allocation because primary has never been allocated",
                        diskThresholdSettings.getFreeBytesThresholdLow(),
                        freeBytesValue,
                        node.nodeId()
                    );
                }
                return allocation.decision(
                    Decision.YES,
                    NAME,
                    "the node is above the low watermark, but less than the high watermark, and this primary shard has "
                        + "never been allocated before"
                );
            } else {
                // Even though the primary has never been allocated, the node is
                // above the high watermark, so don't allow allocating the shard
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "less than the required {} free bytes threshold ({} free) on node {}, "
                            + "preventing allocation even though primary has never been allocated",
                        diskThresholdSettings.getFreeBytesThresholdHigh(),
                        freeBytesValue,
                        node.nodeId()
                    );
                }
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "the node is above the high watermark cluster setting [%s=%s], having less than the minimum required [%s] free "
                        + "space, actual free: [%s]",
                    CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                    diskThresholdSettings.getHighWatermarkRaw(),
                    diskThresholdSettings.getFreeBytesThresholdHigh(),
                    freeBytesValue
                );
            }
        }

        // checks for percentage comparisons
        if (freeDiskPercentage < diskThresholdSettings.getFreeDiskThresholdLow()) {
            // If the shard is a replica or is a non-empty primary, check the low threshold
            if (skipLowThresholdChecks == false) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "more than the allowed {} used disk threshold ({} used) on node [{}], preventing allocation",
                        Strings.format1Decimals(usedDiskThresholdLow, "%"),
                        Strings.format1Decimals(usedDiskPercentage, "%"),
                        node.nodeId()
                    );
                }
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "the node is above the low watermark cluster setting [%s=%s], using more disk space than the maximum allowed "
                        + "[%s%%], actual free: [%s%%]",
                    CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
                    diskThresholdSettings.getLowWatermarkRaw(),
                    usedDiskThresholdLow,
                    freeDiskPercentage
                );
            } else if (freeDiskPercentage > diskThresholdSettings.getFreeDiskThresholdHigh()) {
                // Allow the shard to be allocated because it is primary that
                // has never been allocated if it's under the high watermark
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "more than the allowed {} used disk threshold ({} used) on node [{}], "
                            + "but allowing allocation because primary has never been allocated",
                        Strings.format1Decimals(usedDiskThresholdLow, "%"),
                        Strings.format1Decimals(usedDiskPercentage, "%"),
                        node.nodeId()
                    );
                }
                return allocation.decision(
                    Decision.YES,
                    NAME,
                    "the node is above the low watermark, but less than the high watermark, and this primary shard has "
                        + "never been allocated before"
                );
            } else {
                // Even though the primary has never been allocated, the node is
                // above the high watermark, so don't allow allocating the shard
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "less than the required {} free bytes threshold ({} bytes free) on node {}, "
                            + "preventing allocation even though primary has never been allocated",
                        Strings.format1Decimals(diskThresholdSettings.getFreeDiskThresholdHigh(), "%"),
                        Strings.format1Decimals(freeDiskPercentage, "%"),
                        node.nodeId()
                    );
                }
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "the node is above the high watermark cluster setting [%s=%s], using more disk space than the maximum allowed "
                        + "[%s%%], actual free: [%s%%]",
                    CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                    diskThresholdSettings.getHighWatermarkRaw(),
                    usedDiskThresholdHigh,
                    freeDiskPercentage
                );
            }
        }

        // Secondly, check that allocating the shard to this node doesn't put it above the high watermark
        final long shardSize = getExpectedShardSize(
            shardRouting,
            0L,
            allocation.clusterInfo(),
            allocation.snapshotShardSizeInfo(),
            allocation.metadata(),
            allocation.routingTable()
        );
        assert shardSize >= 0 : shardSize;
        double freeSpaceAfterShard = freeDiskPercentageAfterShardAssigned(usage, shardSize);
        long freeBytesAfterShard = freeBytes - shardSize;
        if (freeBytesAfterShard < diskThresholdSettings.getFreeBytesThresholdHigh().getBytes()) {
            logger.warn(
                "after allocating [{}] node [{}] would have less than the required threshold of "
                    + "{} free (currently {} free, estimated shard size is {}), preventing allocation",
                shardRouting,
                node.nodeId(),
                diskThresholdSettings.getFreeBytesThresholdHigh(),
                freeBytesValue,
                new ByteSizeValue(shardSize)
            );
            return allocation.decision(
                Decision.NO,
                NAME,
                "allocating the shard to this node will bring the node above the high watermark cluster setting [%s=%s] "
                    + "and cause it to have less than the minimum required [%s] of free space (free: [%s], estimated shard size: [%s])",
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                diskThresholdSettings.getHighWatermarkRaw(),
                diskThresholdSettings.getFreeBytesThresholdHigh(),
                freeBytesValue,
                new ByteSizeValue(shardSize)
            );
        }
        if (freeSpaceAfterShard < diskThresholdSettings.getFreeDiskThresholdHigh()) {
            logger.warn(
                "after allocating [{}] node [{}] would have more than the allowed "
                    + "{} free disk threshold ({} free), preventing allocation",
                shardRouting,
                node.nodeId(),
                Strings.format1Decimals(diskThresholdSettings.getFreeDiskThresholdHigh(), "%"),
                Strings.format1Decimals(freeSpaceAfterShard, "%")
            );
            return allocation.decision(
                Decision.NO,
                NAME,
                "allocating the shard to this node will bring the node above the high watermark cluster setting [%s=%s] "
                    + "and cause it to use more disk space than the maximum allowed [%s%%] (free space after shard added: [%s%%])",
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                diskThresholdSettings.getHighWatermarkRaw(),
                usedDiskThresholdHigh,
                freeSpaceAfterShard
            );
        }

        assert freeBytesAfterShard >= 0 : freeBytesAfterShard;
        return allocation.decision(
            Decision.YES,
            NAME,
            "enough disk for shard on node, free: [%s], shard size: [%s], free after allocating shard: [%s]",
            freeBytesValue,
            new ByteSizeValue(shardSize),
            new ByteSizeValue(freeBytesAfterShard)
        );
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.currentNodeId().equals(node.nodeId()) == false) {
            throw new IllegalArgumentException("Shard [" + shardRouting + "] is not allocated on node: [" + node.nodeId() + "]");
        }

        /*
         The following block prevents movement for remote shards since they do not use the local storage as
         the primary source of data storage.
         */
        if (REMOTE_CAPABLE.equals(getNodePool(node)) && REMOTE_CAPABLE.equals(getShardPool(shardRouting, allocation))) {
            return Decision.ALWAYS;
        }

        final ClusterInfo clusterInfo = allocation.clusterInfo();
        final Map<String, DiskUsage> usages = clusterInfo.getNodeLeastAvailableDiskUsages();
        final Decision decision = earlyTerminate(allocation, usages);
        if (decision != null) {
            return decision;
        }

        // subtractLeavingShards is passed as true here, since this is only for shards remaining, we will *eventually* have enough disk
        // since shards are moving away. No new shards will be incoming since in canAllocate we pass false for this check.
        final DiskUsageWithRelocations usage = getDiskUsage(
            node,
            allocation,
            usages,
            clusterInfo.getAvgFreeByte(),
            clusterInfo.getAvgTotalBytes(),
            true
        );
        final String dataPath = clusterInfo.getDataPath(shardRouting);
        // If this node is already above the high threshold, the shard cannot remain (get it off!)
        final double freeDiskPercentage = usage.getFreeDiskAsPercentage();
        final long freeBytes = usage.getFreeBytes();
        if (logger.isTraceEnabled()) {
            logger.trace("node [{}] has {}% free disk ({} bytes)", node.nodeId(), freeDiskPercentage, freeBytes);
        }
        if (dataPath == null || usage.getPath().equals(dataPath) == false) {
            return allocation.decision(Decision.YES, NAME, "this shard is not allocated on the most utilized disk and can remain");
        }
        if (freeBytes < 0L) {
            final long sizeOfRelocatingShards = sizeOfRelocatingShards(
                node,
                true,
                usage.getPath(),
                allocation.clusterInfo(),
                allocation.metadata(),
                allocation.routingTable()
            );
            logger.debug(
                "fewer free bytes remaining than the size of all incoming shards: "
                    + "usage {} on node {} including {} bytes of relocations, shard cannot remain",
                usage,
                node.nodeId(),
                sizeOfRelocatingShards
            );
            return allocation.decision(
                Decision.NO,
                NAME,
                "the shard cannot remain on this node because the node has fewer free bytes remaining than the total size of all "
                    + "incoming shards: free space [%s], relocating shards [%s]",
                freeBytes + sizeOfRelocatingShards,
                sizeOfRelocatingShards
            );
        }
        if (freeBytes < diskThresholdSettings.getFreeBytesThresholdHigh().getBytes()) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "less than the required {} free bytes threshold ({} bytes free) on node {}, shard cannot remain",
                    diskThresholdSettings.getFreeBytesThresholdHigh(),
                    freeBytes,
                    node.nodeId()
                );
            }
            return allocation.decision(
                Decision.NO,
                NAME,
                "the shard cannot remain on this node because it is above the high watermark cluster setting [%s=%s] "
                    + "and there is less than the required [%s] free space on node, actual free: [%s]",
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                diskThresholdSettings.getHighWatermarkRaw(),
                diskThresholdSettings.getFreeBytesThresholdHigh(),
                new ByteSizeValue(freeBytes)
            );
        }
        if (freeDiskPercentage < diskThresholdSettings.getFreeDiskThresholdHigh()) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "less than the required {}% free disk threshold ({}% free) on node {}, shard cannot remain",
                    diskThresholdSettings.getFreeDiskThresholdHigh(),
                    freeDiskPercentage,
                    node.nodeId()
                );
            }
            return allocation.decision(
                Decision.NO,
                NAME,
                "the shard cannot remain on this node because it is above the high watermark cluster setting [%s=%s] "
                    + "and there is less than the required [%s%%] free disk on node, actual free: [%s%%]",
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                diskThresholdSettings.getHighWatermarkRaw(),
                diskThresholdSettings.getFreeDiskThresholdHigh(),
                freeDiskPercentage
            );
        }

        return allocation.decision(
            Decision.YES,
            NAME,
            "there is enough disk on this node for the shard to remain, free: [%s]",
            new ByteSizeValue(freeBytes)
        );
    }

    private DiskUsageWithRelocations getDiskUsage(
        RoutingNode node,
        RoutingAllocation allocation,
        final Map<String, DiskUsage> usages,
        final long avgFreeBytes,
        final long avgTotalBytes,
        boolean subtractLeavingShards
    ) {
        DiskUsage usage = usages.get(node.nodeId());
        if (usage == null) {
            // If there is no usage, and we have other nodes in the cluster,
            // use the average usage for all nodes as the usage for this node
            usage = new DiskUsage(node.nodeId(), node.node().getName(), "_na_", avgTotalBytes, avgFreeBytes);
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "unable to determine disk usage for {}, defaulting to average across nodes [{} total] [{} free] [{}% free]",
                    node.nodeId(),
                    usage.getTotalBytes(),
                    usage.getFreeBytes(),
                    usage.getFreeDiskAsPercentage()
                );
            }
        }

        final DiskUsageWithRelocations diskUsageWithRelocations = new DiskUsageWithRelocations(
            usage,
            diskThresholdSettings.includeRelocations()
                ? sizeOfRelocatingShards(
                    node,
                    subtractLeavingShards,
                    usage.getPath(),
                    allocation.clusterInfo(),
                    allocation.metadata(),
                    allocation.routingTable()
                )
                : 0
        );
        if (logger.isTraceEnabled()) {
            logger.trace("getDiskUsage(subtractLeavingShards={}) returning {}", subtractLeavingShards, diskUsageWithRelocations);
        }

        return diskUsageWithRelocations;
    }

    /**
     * Given the DiskUsage for a node and the size of the shard, return the
     * percentage of free disk if the shard were to be allocated to the node.
     * @param usage A DiskUsage for the node to have space computed for
     * @param shardSize Size in bytes of the shard
     * @return Percentage of free space after the shard is assigned to the node
     */
    double freeDiskPercentageAfterShardAssigned(DiskUsageWithRelocations usage, Long shardSize) {
        shardSize = (shardSize == null) ? 0 : shardSize;
        DiskUsage newUsage = new DiskUsage(
            usage.getNodeId(),
            usage.getNodeName(),
            usage.getPath(),
            usage.getTotalBytes(),
            usage.getFreeBytes() - shardSize
        );
        return newUsage.getFreeDiskAsPercentage();
    }

    private Decision earlyTerminate(RoutingAllocation allocation, final Map<String, DiskUsage> usages) {
        // Always allow allocation if the decider is disabled
        if (diskThresholdSettings.isEnabled() == false) {
            return allocation.decision(Decision.YES, NAME, "the disk threshold decider is disabled");
        }

        // Allow allocation regardless if only a single data node is available
        if (enableForSingleDataNode == false && allocation.nodes().getDataNodes().size() <= 1) {
            if (logger.isTraceEnabled()) {
                logger.trace("only a single data node is present, allowing allocation");
            }
            return allocation.decision(Decision.YES, NAME, "there is only a single data node present");
        }

        // Fail open there is no info available
        final ClusterInfo clusterInfo = allocation.clusterInfo();
        if (clusterInfo == null) {
            if (logger.isTraceEnabled()) {
                logger.trace("cluster info unavailable for disk threshold decider, allowing allocation.");
            }
            return allocation.decision(Decision.YES, NAME, "the cluster info is unavailable");
        }

        // Fail open if there are no disk usages available
        if (usages.isEmpty()) {
            if (logger.isTraceEnabled()) {
                logger.trace("unable to determine disk usages for disk-aware allocation, allowing allocation");
            }
            return allocation.decision(Decision.YES, NAME, "disk usages are unavailable");
        }
        return null;
    }

    /**
     * Returns the expected shard size for the given shard or the default value provided if not enough information are available
     * to estimate the shards size.
     */
    public static long getExpectedShardSize(
        ShardRouting shard,
        long defaultValue,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo snapshotShardSizeInfo,
        Metadata metadata,
        RoutingTable routingTable
    ) {
        final IndexMetadata indexMetadata = metadata.getIndexSafe(shard.index());
        if (indexMetadata.getResizeSourceIndex() != null
            && shard.active() == false
            && shard.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
            // in the shrink index case we sum up the source index shards since we basically make a copy of the shard in
            // the worst case
            long targetShardSize = 0;
            final Index mergeSourceIndex = indexMetadata.getResizeSourceIndex();
            final IndexMetadata sourceIndexMeta = metadata.index(mergeSourceIndex);
            if (sourceIndexMeta != null) {
                final Set<ShardId> shardIds = IndexMetadata.selectRecoverFromShards(
                    shard.id(),
                    sourceIndexMeta,
                    indexMetadata.getNumberOfShards()
                );
                for (IndexShardRoutingTable shardRoutingTable : routingTable.index(mergeSourceIndex.getName())) {
                    if (shardIds.contains(shardRoutingTable.shardId())) {
                        targetShardSize += clusterInfo.getShardSize(shardRoutingTable.primaryShard(), 0);
                    }
                }
            }
            return targetShardSize == 0 ? defaultValue : targetShardSize;
        } else {
            if (shard.unassigned() && shard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                return snapshotShardSizeInfo.getShardSize(shard, defaultValue);
            }
            return clusterInfo.getShardSize(shard, defaultValue);
        }
    }

    static class DiskUsageWithRelocations {

        private final DiskUsage diskUsage;
        private final long relocatingShardSize;

        DiskUsageWithRelocations(DiskUsage diskUsage, long relocatingShardSize) {
            this.diskUsage = diskUsage;
            this.relocatingShardSize = relocatingShardSize;
        }

        @Override
        public String toString() {
            return "DiskUsageWithRelocations{" + "diskUsage=" + diskUsage + ", relocatingShardSize=" + relocatingShardSize + '}';
        }

        double getFreeDiskAsPercentage() {
            if (getTotalBytes() == 0L) {
                return 100.0;
            }
            return 100.0 * ((double) getFreeBytes() / getTotalBytes());
        }

        double getUsedDiskAsPercentage() {
            return 100.0 - getFreeDiskAsPercentage();
        }

        long getFreeBytes() {
            try {
                return Math.subtractExact(diskUsage.getFreeBytes(), relocatingShardSize);
            } catch (ArithmeticException e) {
                return Long.MAX_VALUE;
            }
        }

        String getPath() {
            return diskUsage.getPath();
        }

        String getNodeId() {
            return diskUsage.getNodeId();
        }

        String getNodeName() {
            return diskUsage.getNodeName();
        }

        long getTotalBytes() {
            return diskUsage.getTotalBytes();
        }
    }

}
