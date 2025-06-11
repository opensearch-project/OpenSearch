/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.DiskUsage;

public class NodeDiskEvaluator {

    private final DiskThresholdEvaluator hotNodeEvaluator;
    private final DiskThresholdEvaluator warmNodeEvaluator;

    public NodeDiskEvaluator(DiskThresholdEvaluator hotEvaluator, DiskThresholdEvaluator warmEvaluator) {
        this.hotNodeEvaluator = hotEvaluator;
        this.warmNodeEvaluator = warmEvaluator;
    }

    public boolean isNodeExceedingFloodStageWatermark(DiskUsage diskUsage, boolean isWarmNode) {
        return isWarmNode
            ? warmNodeEvaluator.isNodeExceedingFloodStageWatermark(diskUsage)
            : hotNodeEvaluator.isNodeExceedingFloodStageWatermark(diskUsage);
    }

    public boolean isNodeExceedingHighWatermark(DiskUsage diskUsage, boolean isWarmNode) {
        return isWarmNode
            ? warmNodeEvaluator.isNodeExceedingHighWatermark(diskUsage)
            : hotNodeEvaluator.isNodeExceedingHighWatermark(diskUsage);
    }

    public boolean isNodeExceedingLowWatermark(DiskUsage diskUsage, boolean isWarmNode) {
        return isWarmNode
            ? warmNodeEvaluator.isNodeExceedingLowWatermark(diskUsage)
            : hotNodeEvaluator.isNodeExceedingLowWatermark(diskUsage);
    }
}
