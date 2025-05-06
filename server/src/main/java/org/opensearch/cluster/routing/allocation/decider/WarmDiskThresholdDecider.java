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
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.opensearch.index.store.remote.filecache.FileCacheStats;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.opensearch.cluster.routing.RoutingPool.REMOTE_CAPABLE;
import static org.opensearch.cluster.routing.RoutingPool.getNodePool;
import static org.opensearch.cluster.routing.RoutingPool.getShardPool;
import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING;
import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING;
import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.ENABLE_FOR_SINGLE_DATA_NODE;

/**
 * The {@link WarmDiskThresholdDecider} checks that the node a shard is potentially
 * being allocated to has enough remote addressable space (calculated using remote
 * data ratio and total file cache size).
 * <p>
 * It has three configurable settings, all of which can be changed dynamically:
 * <p>
 * <code>cluster.routing.allocation.disk.watermark.low</code> is the low disk
 * watermark. New shards will not be allocated to a node with remote shard size higher than this
 * It defaults to 0.85 (85.0%).
 * <p>
 * <code>cluster.routing.allocation.disk.watermark.high</code> is the high disk
 * watermark. If a node has total remote shard size higher than this, shards are not allowed to
 * remain on the node. In addition, if allocating a shard to a node causes the
 * node to pass this watermark, it will not be allowed. It defaults to
 * 0.90 (90.0%).
 * <p>
 * Both watermark settings are expressed in terms of used disk percentage, or
 * exact byte values for free space (like "500mb")
 * <p>
 * <code>cluster.routing.allocation.disk.warm_threshold_enabled</code> is used to
 * enable or disable this decider. It defaults to true (enabled).
 *
 * @opensearch.internal
 */
public class WarmDiskThresholdDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(WarmDiskThresholdDecider.class);

    public static final String NAME = "warm_disk_threshold";

    private final FileCacheSettings fileCacheSettings;
    private final DiskThresholdSettings diskThresholdSettings;
    private final boolean enableForSingleDataNode;

    public WarmDiskThresholdDecider(Settings settings, ClusterSettings clusterSettings) {
        this.fileCacheSettings = new FileCacheSettings(settings, clusterSettings);
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterSettings);
        assert Version.CURRENT.major < 9 : "remove enable_for_single_data_node in 9";
        this.enableForSingleDataNode = ENABLE_FOR_SINGLE_DATA_NODE.get(settings);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        var nodeRoutingPool = getNodePool(node);
        var shardRoutingPool = getShardPool(shardRouting, allocation);
        if (nodeRoutingPool != REMOTE_CAPABLE || shardRoutingPool != REMOTE_CAPABLE) {
            return Decision.ALWAYS;
        }

        final Decision decision = earlyTerminate(node, allocation);
        if (decision != null) {
            return decision;
        }

        final long shardSize = DiskThresholdDecider.getExpectedShardSize(
            shardRouting,
            0L,
            allocation.clusterInfo(),
            allocation.snapshotShardSizeInfo(),
            allocation.metadata(),
            allocation.routingTable()
        );

        final long totalAddressableSpace = calculateTotalAddressableSpace(node, allocation);
        final long currentNodeRemoteShardSize = calculateCurrentNodeRemoteShardSize(node, allocation, false);
        final long freeSpace = totalAddressableSpace - currentNodeRemoteShardSize;
        final long freeSpaceAfterAllocation = freeSpace > shardSize ? freeSpace - shardSize : 0;
        final long freeSpaceLowThreshold = calculateFreeSpaceLowThreshold(diskThresholdSettings, totalAddressableSpace);

        final ByteSizeValue freeSpaceLowThresholdInByteSize = new ByteSizeValue(freeSpaceLowThreshold);
        final ByteSizeValue freeSpaceInByteSize = new ByteSizeValue(freeSpace);
        final ByteSizeValue freeSpaceAfterAllocationInByteSize = new ByteSizeValue(freeSpaceAfterAllocation);
        final ByteSizeValue shardSizeInByteSize = new ByteSizeValue(shardSize);

        if (freeSpaceAfterAllocation < freeSpaceLowThreshold) {
            logger.warn(
                "after allocating [{}] node [{}] would have less than the required threshold of "
                    + "{} free (currently {} free, estimated shard size is {}), preventing allocation",
                shardRouting,
                node.nodeId(),
                freeSpaceLowThresholdInByteSize,
                freeSpaceInByteSize,
                shardSizeInByteSize
            );
            return allocation.decision(
                Decision.NO,
                NAME,
                "allocating the shard to this node will bring the node above the low watermark cluster setting [%s=%s] "
                    + "and cause it to have less than the minimum required [%s] of addressable remote free space (free: [%s], estimated remote shard size: [%s])",
                CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
                freeSpaceLowThresholdInByteSize,
                freeSpaceInByteSize,
                shardSizeInByteSize
            );
        }

        return allocation.decision(
            Decision.YES,
            NAME,
            "enough available remote addressable space for shard on node, free: [%s], shard size: [%s], free after allocating shard: [%s]",
            freeSpaceInByteSize,
            shardSizeInByteSize,
            freeSpaceAfterAllocationInByteSize
        );
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.currentNodeId().equals(node.nodeId()) == false) {
            throw new IllegalArgumentException("Shard [" + shardRouting + "] is not allocated on node: [" + node.nodeId() + "]");
        }

        var nodeRoutingPool = getNodePool(node);
        var shardRoutingPool = getShardPool(shardRouting, allocation);
        if (nodeRoutingPool != REMOTE_CAPABLE || shardRoutingPool != REMOTE_CAPABLE) {
            return Decision.ALWAYS;
        }

        final Decision decision = earlyTerminate(node, allocation);
        if (decision != null) {
            return decision;
        }

        final long totalAddressableSpace = calculateTotalAddressableSpace(node, allocation);
        final long currentNodeRemoteShardSize = calculateCurrentNodeRemoteShardSize(node, allocation, true);
        final long freeSpace = totalAddressableSpace - currentNodeRemoteShardSize;

        final long freeSpaceHighThreshold = calculateFreeSpaceHighThreshold(diskThresholdSettings, totalAddressableSpace);

        final ByteSizeValue freeSpaceHighThresholdInByteSize = new ByteSizeValue(freeSpaceHighThreshold);
        final ByteSizeValue freeSpaceInByteSize = new ByteSizeValue(freeSpace);

        if (freeSpace < freeSpaceHighThreshold) {
            logger.warn(
                "less than the required {} of free remote addressable space threshold left ({} free) on node [{}], shard cannot remain",
                freeSpaceHighThresholdInByteSize,
                freeSpaceInByteSize,
                node.nodeId()
            );
            return allocation.decision(
                Decision.NO,
                NAME,
                "the shard cannot remain on this node because it is above the high watermark cluster setting [%s=%s] "
                    + "and there is less than the required [%s%%] free remote addressable space on node, actual free: [%s%%]",
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                freeSpaceHighThresholdInByteSize,
                freeSpaceInByteSize
            );
        }

        return allocation.decision(
            Decision.YES,
            NAME,
            "there is enough remote addressable space on this node for the shard to remain, free: [%s]",
            freeSpaceInByteSize
        );
    }

    private long calculateFreeSpaceLowThreshold(DiskThresholdSettings diskThresholdSettings, long totalAddressableSpace) {
        // Check for percentage-based threshold
        double percentageThreshold = diskThresholdSettings.getFreeDiskThresholdLow();
        if (percentageThreshold > 0) {
            return (long) (totalAddressableSpace * percentageThreshold / 100.0);
        }

        // Check for absolute bytes threshold
        final double dataToFileCacheSizeRatio = fileCacheSettings.getRemoteDataRatio();
        ByteSizeValue bytesThreshold = diskThresholdSettings.getFreeBytesThresholdLow();
        if (bytesThreshold != null && bytesThreshold.getBytes() > 0) {
            return bytesThreshold.getBytes() * (long) dataToFileCacheSizeRatio;
        }

        // Default fallback
        return 0;
    }

    private long calculateFreeSpaceHighThreshold(DiskThresholdSettings diskThresholdSettings, long totalAddressableSpace) {
        // Check for percentage-based threshold
        double percentageThreshold = diskThresholdSettings.getFreeDiskThresholdHigh();
        if (percentageThreshold > 0) {
            return (long) (totalAddressableSpace * percentageThreshold / 100.0);
        }

        // Check for absolute bytes threshold
        final double dataToFileCacheSizeRatio = fileCacheSettings.getRemoteDataRatio();
        ByteSizeValue bytesThreshold = diskThresholdSettings.getFreeBytesThresholdHigh();
        if (bytesThreshold != null && bytesThreshold.getBytes() > 0) {
            return bytesThreshold.getBytes() * (long) dataToFileCacheSizeRatio;
        }

        // Default fallback
        return 0;
    }

    private long calculateCurrentNodeRemoteShardSize(RoutingNode node, RoutingAllocation allocation, boolean subtractLeavingShards) {
        final List<ShardRouting> remoteShardsOnNode = StreamSupport.stream(node.spliterator(), false)
            .filter(
                shard -> shard.primary()
                    && REMOTE_CAPABLE.equals(getShardPool(shard, allocation))
                    && (!subtractLeavingShards || !shard.relocating())
            )
            .collect(Collectors.toList());

        var remoteShardSize = 0L;
        for (ShardRouting shard : remoteShardsOnNode) {
            remoteShardSize += DiskThresholdDecider.getExpectedShardSize(
                shard,
                0L,
                allocation.clusterInfo(),
                allocation.snapshotShardSizeInfo(),
                allocation.metadata(),
                allocation.routingTable()
            );
        }

        return remoteShardSize;
    }

    private long calculateTotalAddressableSpace(RoutingNode node, RoutingAllocation allocation) {
        ClusterInfo clusterInfo = allocation.clusterInfo();
        // TODO: Change the default value to 5 instead of 0
        final double dataToFileCacheSizeRatio = fileCacheSettings.getRemoteDataRatio();
        final FileCacheStats fileCacheStats = clusterInfo.getNodeFileCacheStats().getOrDefault(node.nodeId(), null);
        final long nodeCacheSize = fileCacheStats != null ? fileCacheStats.getTotal().getBytes() : 0;
        return (long) dataToFileCacheSizeRatio * nodeCacheSize;
    }

    private Decision earlyTerminate(RoutingNode node, RoutingAllocation allocation) {
        // Always allow allocation if the decider is disabled
        if (diskThresholdSettings.isWarmThresholdEnabled() == false) {
            return allocation.decision(Decision.YES, NAME, "the warm disk threshold decider is disabled");
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
                logger.trace("cluster info unavailable for file cache threshold decider, allowing allocation.");
            }
            return allocation.decision(Decision.YES, NAME, "the cluster info is unavailable");
        }

        // Fail open if there are no file cache stats available
        final FileCacheStats fileCacheStats = clusterInfo.getNodeFileCacheStats().getOrDefault(node.nodeId(), null);
        if (fileCacheStats == null) {
            if (logger.isTraceEnabled()) {
                logger.trace("unable to get file cache stats for node [{}], allowing allocation", node.nodeId());
            }
            return allocation.decision(Decision.YES, NAME, "File Cache Stat is unavailable");
        }

        double remoteDataRatio = fileCacheSettings.getRemoteDataRatio();
        if (remoteDataRatio == 0) {
            return allocation.decision(Decision.YES, NAME, "Remote data ratio is set to 0, no limit on allocation");
        }

        return null;
    }

}
