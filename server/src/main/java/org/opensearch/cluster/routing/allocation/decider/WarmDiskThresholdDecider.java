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
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.DiskThresholdEvaluator;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.WarmNodeDiskThresholdEvaluator;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats;
import org.opensearch.index.store.remote.filecache.FileCacheSettings;

import java.util.List;
import java.util.Map;
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
    private final DiskThresholdEvaluator diskThresholdEvaluator;

    public WarmDiskThresholdDecider(Settings settings, ClusterSettings clusterSettings) {
        this.fileCacheSettings = new FileCacheSettings(settings, clusterSettings);
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterSettings);
        assert Version.CURRENT.major < 9 : "remove enable_for_single_data_node in 9";
        this.enableForSingleDataNode = ENABLE_FOR_SINGLE_DATA_NODE.get(settings);
        this.diskThresholdEvaluator = new WarmNodeDiskThresholdEvaluator(diskThresholdSettings, fileCacheSettings::getRemoteDataRatio);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        var nodeRoutingPool = getNodePool(node);
        var shardRoutingPool = getShardPool(shardRouting, allocation);
        if (nodeRoutingPool != REMOTE_CAPABLE || shardRoutingPool != REMOTE_CAPABLE) {
            return Decision.ALWAYS;
        }

        ClusterInfo clusterInfo = allocation.clusterInfo();
        Map<String, DiskUsage> usages = clusterInfo.getNodeMostAvailableDiskUsages();
        final Decision decision = earlyTerminate(node, allocation, usages);
        if (decision != null) {
            return decision;
        }

        DiskUsage usage = usages.get(node.nodeId());
        final long shardSize = DiskThresholdDecider.getExpectedShardSize(
            shardRouting,
            0L,
            allocation.clusterInfo(),
            allocation.snapshotShardSizeInfo(),
            allocation.metadata(),
            allocation.routingTable()
        );

        final DiskUsage usageAfterShardAssigned = new DiskUsage(
            usage.getNodeId(),
            usage.getNodeName(),
            usage.getPath(),
            usage.getTotalBytes(),
            Math.max(0, usage.getFreeBytes() - shardSize)
        );
        final long freeSpaceLowThreshold = diskThresholdEvaluator.getFreeSpaceLowThreshold(usage.getTotalBytes());

        final ByteSizeValue freeSpaceLowThresholdInByteSize = new ByteSizeValue(freeSpaceLowThreshold);
        final ByteSizeValue freeSpaceInByteSize = new ByteSizeValue(usage.getFreeBytes());
        final ByteSizeValue freeSpaceAfterAllocationInByteSize = new ByteSizeValue(usageAfterShardAssigned.getFreeBytes());
        final ByteSizeValue shardSizeInByteSize = new ByteSizeValue(shardSize);

        if (diskThresholdEvaluator.isNodeExceedingLowWatermark(usageAfterShardAssigned)) {
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
                "allocating the shard to this node will bring the node above the low watermark cluster setting [%s] "
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

        ClusterInfo clusterInfo = allocation.clusterInfo();
        Map<String, DiskUsage> usages = clusterInfo.getNodeMostAvailableDiskUsages();
        final Decision decision = earlyTerminate(node, allocation, usages);
        if (decision != null) {
            return decision;
        }

        final long leavingRemoteShardSize = calculateCurrentNodeLeavingRemoteShardSize(node, allocation);
        final DiskUsage usage = usages.get(node.nodeId());
        final DiskUsage usageAfterSubtractingLeavingShard = new DiskUsage(
            usage.getNodeId(),
            usage.getNodeName(),
            usage.getPath(),
            usage.getTotalBytes(),
            Math.min(usage.getFreeBytes() + leavingRemoteShardSize, usage.getTotalBytes())
        );

        final long freeSpaceHighThreshold = diskThresholdEvaluator.getFreeSpaceHighThreshold(usage.getTotalBytes());

        final ByteSizeValue freeSpaceHighThresholdInByteSize = new ByteSizeValue(freeSpaceHighThreshold);
        final ByteSizeValue freeSpaceInByteSize = new ByteSizeValue(usageAfterSubtractingLeavingShard.getFreeBytes());

        if (diskThresholdEvaluator.isNodeExceedingHighWatermark(usageAfterSubtractingLeavingShard)) {
            logger.warn(
                "less than the required {} of free remote addressable space threshold left ({} free) on node [{}], shard cannot remain",
                freeSpaceHighThresholdInByteSize,
                freeSpaceInByteSize,
                node.nodeId()
            );
            return allocation.decision(
                Decision.NO,
                NAME,
                "the shard cannot remain on this node because it is above the high watermark cluster setting [%s] "
                    + "and there is less than the required [%s] free remote addressable space on node, actual free: [%s]",
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

    private long calculateCurrentNodeLeavingRemoteShardSize(RoutingNode node, RoutingAllocation allocation) {
        final List<ShardRouting> leavingRemoteShardsOnNode = StreamSupport.stream(node.spliterator(), false)
            .filter(shard -> shard.primary() && REMOTE_CAPABLE.equals(getShardPool(shard, allocation)) && (shard.relocating() == true))
            .collect(Collectors.toList());

        var leavingRemoteShardSize = 0L;
        for (ShardRouting shard : leavingRemoteShardsOnNode) {
            leavingRemoteShardSize += DiskThresholdDecider.getExpectedShardSize(
                shard,
                0L,
                allocation.clusterInfo(),
                allocation.snapshotShardSizeInfo(),
                allocation.metadata(),
                allocation.routingTable()
            );
        }

        return leavingRemoteShardSize;
    }

    private Decision earlyTerminate(RoutingNode node, RoutingAllocation allocation, final Map<String, DiskUsage> usages) {
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
        final AggregateFileCacheStats fileCacheStats = clusterInfo.getNodeFileCacheStats().getOrDefault(node.nodeId(), null);
        if (fileCacheStats == null) {
            if (logger.isTraceEnabled()) {
                logger.trace("unable to get file cache stats for node [{}], allowing allocation", node.nodeId());
            }
            return allocation.decision(Decision.YES, NAME, "File Cache Stat is unavailable");
        }

        // Fail open if there are no disk usages available
        if (usages.isEmpty() || usages.containsKey(node.nodeId()) == false) {
            if (logger.isTraceEnabled()) {
                logger.trace("unable to determine disk usages for disk-aware allocation, allowing allocation");
            }
            return allocation.decision(Decision.YES, NAME, "disk usages are unavailable");
        }

        return null;
    }

}
