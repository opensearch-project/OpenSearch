/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.DiskUsage;

/**
 * Evaluates disk usage thresholds for hot and warm nodes in the cluster.
 * This class provides methods to check if nodes are exceeding various disk watermark levels
 * based on their type (hot or warm) and current disk usage.
 */
public class NodeDiskEvaluator {

    private final DiskThresholdEvaluator hotNodeEvaluator;
    private final DiskThresholdEvaluator warmNodeEvaluator;

    /**
     * Constructs a NodeDiskEvaluator with separate evaluators for hot and warm nodes
     *
     * @param hotEvaluator  The evaluator for hot nodes
     * @param warmEvaluator The evaluator for warm nodes
     */
    public NodeDiskEvaluator(DiskThresholdEvaluator hotEvaluator, DiskThresholdEvaluator warmEvaluator) {
        this.hotNodeEvaluator = hotEvaluator;
        this.warmNodeEvaluator = warmEvaluator;
    }

    /**
     * Checks if a node is exceeding the flood stage watermark based on its type and disk usage
     *
     * @param diskUsage  The current disk usage of the node
     * @param isWarmNode Whether the node is a warm node
     * @return true if the node is exceeding flood stage watermark, false otherwise
     */
    public boolean isNodeExceedingFloodStageWatermark(DiskUsage diskUsage, boolean isWarmNode) {
        return isWarmNode
            ? warmNodeEvaluator.isNodeExceedingFloodStageWatermark(diskUsage)
            : hotNodeEvaluator.isNodeExceedingFloodStageWatermark(diskUsage);
    }

    /**
     * Checks if a node is exceeding the high watermark based on its type and disk usage
     *
     * @param diskUsage  The current disk usage of the node
     * @param isWarmNode Whether the node is a warm node
     * @return true if the node is exceeding high watermark, false otherwise
     */
    public boolean isNodeExceedingHighWatermark(DiskUsage diskUsage, boolean isWarmNode) {
        return isWarmNode
            ? warmNodeEvaluator.isNodeExceedingHighWatermark(diskUsage)
            : hotNodeEvaluator.isNodeExceedingHighWatermark(diskUsage);
    }

    /**
     * Checks if a node is exceeding the low watermark based on its type and disk usage
     *
     * @param diskUsage  The current disk usage of the node
     * @param isWarmNode Whether the node is a warm node
     * @return true if the node is exceeding low watermark, false otherwise
     */
    public boolean isNodeExceedingLowWatermark(DiskUsage diskUsage, boolean isWarmNode) {
        return isWarmNode
            ? warmNodeEvaluator.isNodeExceedingLowWatermark(diskUsage)
            : hotNodeEvaluator.isNodeExceedingLowWatermark(diskUsage);
    }
}
