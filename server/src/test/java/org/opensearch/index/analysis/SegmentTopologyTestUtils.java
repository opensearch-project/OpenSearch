/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.analysis;

import org.opensearch.index.AdaptiveMergePolicyCalculator;

/**
 * Shared test utilities for segment topology analysis tests.
 * Provides helper methods for calculating recommended merge policy settings
 * based on shard size, delegating to the production calculator to ensure
 * test behavior accurately reflects production behavior across all shard sizes.
 */
public class SegmentTopologyTestUtils {

    /**
     * Calculates the recommended maximum segment size based on shard size.
     * Delegates to {@link AdaptiveMergePolicyCalculator} to ensure consistency with production code.
     *
     * @param shardSizeBytes the total size of the shard in bytes
     * @return the recommended maximum segment size in bytes
     */
    public static long getRecommendedMaxSegmentSize(long shardSizeBytes) {
        return AdaptiveMergePolicyCalculator.calculateSmoothMaxSegmentSize(shardSizeBytes);
    }

    /**
     * Calculates the recommended floor segment size based on shard size.
     * Delegates to {@link AdaptiveMergePolicyCalculator} to ensure consistency with production code.
     *
     * @param shardSizeBytes the total size of the shard in bytes
     * @return the recommended floor segment size in bytes
     */
    public static long getRecommendedFloorSegmentSize(long shardSizeBytes) {
        return AdaptiveMergePolicyCalculator.calculateSmoothFloorSegmentSize(shardSizeBytes);
    }

    /**
     * Calculates the recommended segments per tier based on shard size.
     * Delegates to {@link AdaptiveMergePolicyCalculator} to ensure consistency with production code.
     *
     * @param shardSizeBytes the total size of the shard in bytes
     * @return the recommended segments per tier (as a double to match production behavior)
     */
    public static double getRecommendedSegmentsPerTier(long shardSizeBytes) {
        return AdaptiveMergePolicyCalculator.calculateSmoothSegmentsPerTier(shardSizeBytes);
    }
}
