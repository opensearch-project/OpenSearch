/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.DiskUsage;
import org.opensearch.core.common.unit.ByteSizeValue;

/**
 * DiskThresholdEvaluator implementation for hot data nodes.
 * This evaluator uses standard disk usage metrics and thresholds
 * for determining if a node exceeds watermarks.
 *
 * @opensearch.internal
 */
public class HotNodeDiskThresholdEvaluator implements DiskThresholdEvaluator {

    private final DiskThresholdSettings diskThresholdSettings;

    public HotNodeDiskThresholdEvaluator(DiskThresholdSettings diskThresholdSettings) {
        this.diskThresholdSettings = diskThresholdSettings;
    }

    @Override
    public boolean isNodeExceedingLowWatermark(DiskUsage diskUsage) {
        return diskUsage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdLow().getBytes()
            || diskUsage.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdLow();
    }

    @Override
    public boolean isNodeExceedingHighWatermark(DiskUsage diskUsage) {
        return diskUsage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdHigh().getBytes()
            || diskUsage.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdHigh();
    }

    @Override
    public boolean isNodeExceedingFloodStageWatermark(DiskUsage diskUsage) {
        return diskUsage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdFloodStage().getBytes()
            || diskUsage.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdFloodStage();
    }

    @Override
    public long getFreeSpaceLowThreshold(long totalAddressableSpace) {
        return calculateFreeSpaceWatermarkThreshold(
            diskThresholdSettings.getFreeDiskThresholdLow(),
            diskThresholdSettings.getFreeBytesThresholdLow(),
            totalAddressableSpace
        );
    }

    @Override
    public long getFreeSpaceHighThreshold(long totalAddressableSpace) {
        return calculateFreeSpaceWatermarkThreshold(
            diskThresholdSettings.getFreeDiskThresholdHigh(),
            diskThresholdSettings.getFreeBytesThresholdHigh(),
            totalAddressableSpace
        );
    }

    @Override
    public long getFreeSpaceFloodStageThreshold(long totalAddressableSpace) {
        return calculateFreeSpaceWatermarkThreshold(
            diskThresholdSettings.getFreeDiskThresholdFloodStage(),
            diskThresholdSettings.getFreeBytesThresholdFloodStage(),
            totalAddressableSpace
        );
    }

    private long calculateFreeSpaceWatermarkThreshold(
        double freeDiskWatermarkThreshold,
        ByteSizeValue freeBytesWatermarkThreshold,
        long totalAddressableSpace
    ) {
        // For hot data nodes, we use the standard disk threshold settings
        // Check for absolute bytes threshold first
        ByteSizeValue bytesThreshold = freeBytesWatermarkThreshold;
        if (bytesThreshold != null && bytesThreshold.getBytes() > 0) {
            return bytesThreshold.getBytes();
        }

        // Check for percentage-based threshold
        double percentageThreshold = freeDiskWatermarkThreshold;
        if (percentageThreshold > 0) {
            return (long) (totalAddressableSpace * percentageThreshold / 100.0);
        }

        // Default fallback
        return 0;
    }
}
