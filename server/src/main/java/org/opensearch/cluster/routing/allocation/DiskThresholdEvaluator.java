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
 * Base interface for disk threshold evaluation logic.
 * This interface defines methods for evaluating whether a node exceeds
 * various watermarks based on disk usage.
 *
 * @opensearch.internal
 */
public interface DiskThresholdEvaluator {

    /**
     * Checks if a node is exceeding the low watermark threshold
     *
     * @param diskUsage disk usage for the node
     * @return true if the node is exceeding the low watermark, false otherwise
     */
    boolean isNodeExceedingLowWatermark(DiskUsage diskUsage);

    /**
     * Checks if a node is exceeding the high watermark threshold
     *
     * @param diskUsage disk usage for the node
     * @return true if the node is exceeding the high watermark, false otherwise
     */
    boolean isNodeExceedingHighWatermark(DiskUsage diskUsage);

    /**
     * Checks if a node is exceeding the flood stage watermark threshold
     *
     * @param diskUsage disk usage for the node
     * @return true if the node is exceeding the flood stage watermark, false otherwise
     */
    boolean isNodeExceedingFloodStageWatermark(DiskUsage diskUsage);

    /**
     * Get the free space low threshold for a given total space
     *
     * @param totalSpace total available space
     * @return free space low threshold in bytes
     */
    long getFreeSpaceLowThreshold(long totalSpace);

    /**
     * Get the free space high threshold for a given total space
     *
     * @param totalSpace total available space
     * @return free space high threshold in bytes
     */
    long getFreeSpaceHighThreshold(long totalSpace);

    /**
     * Get the free space flood stage threshold for a given total space
     *
     * @param totalSpace total available space
     * @return free space flood stage threshold in bytes
     */
    long getFreeSpaceFloodStageThreshold(long totalSpace);
}
