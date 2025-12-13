/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

/**
 * Shared utility class for calculating adaptive merge policy settings based on shard size.
 *
 * This class centralizes the smooth logarithmic interpolation logic used to calculate
 * recommended merge policy parameters (max segment size, floor segment size, segments per tier)
 * to avoid duplication across production and test code.
 *
 * The calculations use smooth interpolation across logarithmic size ranges to avoid
 * abrupt parameter jumps as shards grow. Thresholds are based on decimal powers of 10
 * (log10), while returned sizes use binary units (MiB/GiB via 1024²).
 *
 * @opensearch.internal
 */
public final class AdaptiveMergePolicyCalculator {

    private AdaptiveMergePolicyCalculator() {
        // Utility class - no instantiation
    }

    /**
     * Calculates the recommended maximum segment size based on shard size.
     * Uses smooth logarithmic interpolation to avoid dramatic jumps at category boundaries.
     * Values are capped at 5 GiB to align with Lucene defaults.
     *
     * @param shardSizeBytes the total size of the shard in bytes
     * @return the recommended maximum segment size in bytes
     */
    public static long calculateSmoothMaxSegmentSize(long shardSizeBytes) {
        double logSize = Math.log10(Math.max(1L, shardSizeBytes));
        // Thresholds are based on decimal powers: log10 < 8.0 ≈ < 100MB, < 9.0 ≈ < 1GB, etc.
        // Returned sizes use binary units (MiB/GiB via 1024²)
        if (logSize < 8.0) { // < 10^8 bytes (≈ 95.4 MiB)
            return 50L * 1024 * 1024; // 50 MiB
        } else if (logSize < 9.0) { // 10^8 - 10^9 bytes (≈ 95.4 MiB - 953.7 MiB)
            double ratio = (logSize - 8.0) / 1.0;
            long a = 50L * 1024 * 1024;
            long b = 200L * 1024 * 1024;
            return (long) (a + ratio * (b - a));
        } else if (logSize < 10.0) { // 10^9 - 10^10 bytes (≈ 953.7 MiB - 9.31 GiB)
            double ratio = (logSize - 9.0) / 1.0;
            long a = 200L * 1024 * 1024;
            long b = 1L * 1024 * 1024 * 1024;
            return (long) (a + ratio * (b - a));
        } else if (logSize < 11.0) { // 10^10 - 10^11 bytes (≈ 9.31 GiB - 93.1 GiB)
            double ratio = (logSize - 10.0) / 1.0;
            long a = 1L * 1024 * 1024 * 1024;
            long b = 5L * 1024 * 1024 * 1024; // cap at 5 GiB
            return (long) (a + ratio * (b - a));
        } else { // >= 10^11 bytes (≈ 93.1 GiB)
            return 5L * 1024 * 1024 * 1024; // 5 GiB
        }
    }

    /**
     * Calculates the recommended floor segment size based on shard size.
     * Uses smooth logarithmic interpolation to avoid dramatic jumps at category boundaries.
     *
     * @param shardSizeBytes the total size of the shard in bytes
     * @return the recommended floor segment size in bytes
     */
    public static long calculateSmoothFloorSegmentSize(long shardSizeBytes) {
        double logSize = Math.log10(Math.max(1L, shardSizeBytes));
        // Thresholds are based on decimal powers: log10 < 8.0 ≈ < 100MB, < 9.0 ≈ < 1GB, etc.
        // Returned sizes use binary units (MiB/GiB via 1024²)
        if (logSize < 8.0) { // < 10^8 bytes (≈ 95.4 MiB)
            return 10L * 1024 * 1024; // 10 MiB
        } else if (logSize < 9.0) { // 10^8 - 10^9 bytes (≈ 95.4 MiB - 953.7 MiB)
            double ratio = (logSize - 8.0) / 1.0;
            long a = 10L * 1024 * 1024;
            long b = 25L * 1024 * 1024;
            return (long) (a + ratio * (b - a));
        } else if (logSize < 10.0) { // 10^9 - 10^10 bytes (≈ 953.7 MiB - 9.31 GiB)
            double ratio = (logSize - 9.0) / 1.0;
            long a = 25L * 1024 * 1024;
            long b = 50L * 1024 * 1024;
            return (long) (a + ratio * (b - a));
        } else if (logSize < 11.0) { // 10^10 - 10^11 bytes (≈ 9.31 GiB - 93.1 GiB)
            double ratio = (logSize - 10.0) / 1.0;
            long a = 50L * 1024 * 1024;
            long b = 100L * 1024 * 1024;
            return (long) (a + ratio * (b - a));
        } else { // >= 10^11 bytes (≈ 93.1 GiB)
            return 100L * 1024 * 1024; // 100 MiB
        }
    }

    /**
     * Calculates the recommended segments per tier based on shard size.
     * Uses smooth logarithmic interpolation to avoid dramatic jumps at category boundaries.
     *
     * @param shardSizeBytes the total size of the shard in bytes
     * @return the recommended segments per tier (as a double to allow fractional values)
     */
    public static double calculateSmoothSegmentsPerTier(long shardSizeBytes) {
        double logSize = Math.log10(Math.max(1L, shardSizeBytes));
        // Thresholds are based on decimal powers: log10 < 8.0 ≈ < 100MB, < 9.0 ≈ < 1GB, etc.
        if (logSize < 8.0) { // < 10^8 bytes (≈ 95.4 MiB)
            return 5.0;
        } else if (logSize < 9.0) { // 10^8 - 10^9 bytes (≈ 95.4 MiB - 953.7 MiB)
            double ratio = (logSize - 8.0) / 1.0;
            return 5.0 + ratio * (8.0 - 5.0);
        } else if (logSize < 10.0) { // 10^9 - 10^10 bytes (≈ 953.7 MiB - 9.31 GiB)
            double ratio = (logSize - 9.0) / 1.0;
            return 8.0 + ratio * (10.0 - 8.0);
        } else if (logSize < 11.0) { // 10^10 - 10^11 bytes (≈ 9.31 GiB - 93.1 GiB)
            double ratio = (logSize - 10.0) / 1.0;
            return 10.0 + ratio * (12.0 - 10.0);
        } else { // >= 10^11 bytes (≈ 93.1 GiB)
            return 12.0;
        }
    }
}
