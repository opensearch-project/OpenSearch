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

import java.util.function.Supplier;

/**
 * DiskThresholdEvaluator implementation for warm nodes.
 * This evaluator uses file cache size ratio and addressable space
 * calculations specific to warm nodes for determining if a node exceeds watermarks.
 *
 * @opensearch.internal
 */
public class WarmNodeDiskThresholdEvaluator implements DiskThresholdEvaluator {

    private final DiskThresholdSettings diskThresholdSettings;
    private final Supplier<Double> dataToFileCacheSizeRatioSupplier;

    public WarmNodeDiskThresholdEvaluator(DiskThresholdSettings diskThresholdSettings, Supplier<Double> dataToFileCacheSizeRatioSupplier) {
        this.diskThresholdSettings = diskThresholdSettings;
        this.dataToFileCacheSizeRatioSupplier = dataToFileCacheSizeRatioSupplier;
    }

    @Override
    public boolean isNodeExceedingLowWatermark(DiskUsage diskUsage) {
        if (dataToFileCacheSizeRatioSupplier.get() <= 0) {
            return false;
        }
        long totalBytes = diskUsage.getTotalBytes();
        long freeSpace = diskUsage.getFreeBytes();
        long freeSpaceLowThreshold = calculateFreeSpaceLowThreshold(totalBytes);

        return freeSpace < freeSpaceLowThreshold;
    }

    @Override
    public boolean isNodeExceedingHighWatermark(DiskUsage diskUsage) {
        if (dataToFileCacheSizeRatioSupplier.get() <= 0) {
            return false;
        }
        long totalBytes = diskUsage.getTotalBytes();
        long freeSpace = diskUsage.getFreeBytes();
        long freeSpaceHighThreshold = calculateFreeSpaceHighThreshold(totalBytes);

        return freeSpace < freeSpaceHighThreshold;
    }

    @Override
    public boolean isNodeExceedingFloodStageWatermark(DiskUsage diskUsage) {
        if (dataToFileCacheSizeRatioSupplier.get() <= 0) {
            return false;
        }
        long totalBytes = diskUsage.getTotalBytes();
        long freeSpace = diskUsage.getFreeBytes();
        long freeSpaceFloodStageThreshold = calculateFreeSpaceFloodStageThreshold(totalBytes);

        return freeSpace < freeSpaceFloodStageThreshold;
    }

    @Override
    public long calculateFreeSpaceLowThreshold(long totalAddressableSpace) {
        // Check for percentage-based threshold
        double percentageThreshold = diskThresholdSettings.getFreeDiskThresholdLow();
        if (percentageThreshold > 0) {
            return (long) (totalAddressableSpace * percentageThreshold / 100.0);
        }

        // Check for absolute bytes threshold
        final double dataToFileCacheSizeRatio = dataToFileCacheSizeRatioSupplier.get();
        ByteSizeValue bytesThreshold = diskThresholdSettings.getFreeBytesThresholdLow();
        if (bytesThreshold != null && bytesThreshold.getBytes() > 0) {
            return bytesThreshold.getBytes() * (long) dataToFileCacheSizeRatio;
        }

        // Default fallback
        return 0;
    }

    @Override
    public long calculateFreeSpaceHighThreshold(long totalAddressableSpace) {
        // Check for percentage-based threshold
        double percentageThreshold = diskThresholdSettings.getFreeDiskThresholdHigh();
        if (percentageThreshold > 0) {
            return (long) (totalAddressableSpace * percentageThreshold / 100.0);
        }

        // Check for absolute bytes threshold
        final double dataToFileCacheSizeRatio = dataToFileCacheSizeRatioSupplier.get();
        ByteSizeValue bytesThreshold = diskThresholdSettings.getFreeBytesThresholdHigh();
        if (bytesThreshold != null && bytesThreshold.getBytes() > 0) {
            return bytesThreshold.getBytes() * (long) dataToFileCacheSizeRatio;
        }

        // Default fallback
        return 0;
    }

    @Override
    public long calculateFreeSpaceFloodStageThreshold(long totalAddressableSpace) {
        // Check for percentage-based threshold
        double percentageThreshold = diskThresholdSettings.getFreeDiskThresholdFloodStage();
        if (percentageThreshold > 0) {
            return (long) (totalAddressableSpace * percentageThreshold / 100.0);
        }

        // Check for absolute bytes threshold
        final double dataToFileCacheSizeRatio = dataToFileCacheSizeRatioSupplier.get();
        ByteSizeValue bytesThreshold = diskThresholdSettings.getFreeBytesThresholdFloodStage();
        if (bytesThreshold != null && bytesThreshold.getBytes() > 0) {
            return bytesThreshold.getBytes() * (long) dataToFileCacheSizeRatio;
        }

        // Default fallback
        return 0;
    }
}
