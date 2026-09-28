/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.DiskUsage;

import java.util.function.Supplier;

/**
 * Evaluates disk usage thresholds for hot and warm nodes in the cluster.
 * This class provides methods to check if nodes are exceeding various disk watermark levels
 * based on their type (hot or warm) and current disk usage.
 */
public class NodeDiskEvaluator implements DiskThresholdEvaluator {

    /**
     * Enum representing different node types for disk threshold evaluation
     *
     * @opensearch.internal
     */
    enum NodeType {
        HOT,
        WARM
    }

    private final DiskThresholdEvaluator hotNodeEvaluator;
    private final DiskThresholdEvaluator warmNodeEvaluator;
    private NodeType nodeType;

    /**
     * Constructs a NodeDiskEvaluator with separate evaluators for hot and warm nodes
     *
     * @param diskThresholdSettings  Disk Threshold Settings
     * @param dataToFileCacheSizeRatioSupplier Supplier for remote_data_ratio
     */
    public NodeDiskEvaluator(DiskThresholdSettings diskThresholdSettings, Supplier<Double> dataToFileCacheSizeRatioSupplier) {
        this.hotNodeEvaluator = new HotNodeDiskThresholdEvaluator(diskThresholdSettings);
        this.warmNodeEvaluator = new WarmNodeDiskThresholdEvaluator(diskThresholdSettings, dataToFileCacheSizeRatioSupplier);
    }

    public void setNodeType(boolean isWarmNode) {
        if (isWarmNode) {
            nodeType = NodeType.WARM;
        } else {
            nodeType = NodeType.HOT;
        }
    }

    @Override
    public boolean isNodeExceedingLowWatermark(DiskUsage diskUsage) {
        if (nodeType == NodeType.HOT) {
            return hotNodeEvaluator.isNodeExceedingLowWatermark(diskUsage);
        }
        return warmNodeEvaluator.isNodeExceedingLowWatermark(diskUsage);
    }

    @Override
    public boolean isNodeExceedingHighWatermark(DiskUsage diskUsage) {
        if (nodeType == NodeType.HOT) {
            return hotNodeEvaluator.isNodeExceedingHighWatermark(diskUsage);
        }
        return warmNodeEvaluator.isNodeExceedingHighWatermark(diskUsage);
    }

    @Override
    public boolean isNodeExceedingFloodStageWatermark(DiskUsage diskUsage) {
        if (nodeType == NodeType.HOT) {
            return hotNodeEvaluator.isNodeExceedingFloodStageWatermark(diskUsage);
        }
        return warmNodeEvaluator.isNodeExceedingFloodStageWatermark(diskUsage);
    }

    @Override
    public long getFreeSpaceLowThreshold(long totalSpace) {
        if (nodeType == NodeType.HOT) {
            return hotNodeEvaluator.getFreeSpaceLowThreshold(totalSpace);
        }
        return warmNodeEvaluator.getFreeSpaceLowThreshold(totalSpace);
    }

    @Override
    public long getFreeSpaceHighThreshold(long totalSpace) {
        if (nodeType == NodeType.HOT) {
            return hotNodeEvaluator.getFreeSpaceHighThreshold(totalSpace);
        }
        return warmNodeEvaluator.getFreeSpaceHighThreshold(totalSpace);
    }

    @Override
    public long getFreeSpaceFloodStageThreshold(long totalSpace) {
        if (nodeType == NodeType.HOT) {
            return hotNodeEvaluator.getFreeSpaceFloodStageThreshold(totalSpace);
        }
        return warmNodeEvaluator.getFreeSpaceFloodStageThreshold(totalSpace);
    }
}
