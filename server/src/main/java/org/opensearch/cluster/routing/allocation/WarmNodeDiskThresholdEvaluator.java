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

import java.util.function.Function;
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
        return isNodeExceedingWatermark(diskUsage, this::getFreeSpaceLowThreshold);
    }

    @Override
    public boolean isNodeExceedingHighWatermark(DiskUsage diskUsage) {
        return isNodeExceedingWatermark(diskUsage, this::getFreeSpaceHighThreshold);
    }

    @Override
    public boolean isNodeExceedingFloodStageWatermark(DiskUsage diskUsage) {
        return isNodeExceedingWatermark(diskUsage, this::getFreeSpaceFloodStageThreshold);
    }

    private boolean isNodeExceedingWatermark(DiskUsage diskUsage, Function<Long, Long> thresholdFunction) {
        long totalBytes = diskUsage.getTotalBytes();
        if (dataToFileCacheSizeRatioSupplier.get() <= 0 || totalBytes <= 0) {
            return false;
        }
        long freeSpace = diskUsage.getFreeBytes();
        long freeSpaceThreshold = thresholdFunction.apply(totalBytes);

        return freeSpace < freeSpaceThreshold;
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
        // Check for percentage-based threshold
        if (freeDiskWatermarkThreshold > 0) {
            return (long) (totalAddressableSpace * freeDiskWatermarkThreshold / 100.0);
        }

        // Check for absolute bytes threshold
        final double dataToFileCacheSizeRatio = dataToFileCacheSizeRatioSupplier.get();
        if (freeBytesWatermarkThreshold != null && freeBytesWatermarkThreshold.getBytes() > 0) {
            return freeBytesWatermarkThreshold.getBytes() * (long) dataToFileCacheSizeRatio;
        }

        // Default fallback
        return 0;
    }
}
