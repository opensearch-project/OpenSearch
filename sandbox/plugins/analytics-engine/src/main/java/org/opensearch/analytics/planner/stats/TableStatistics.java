/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.stats;

/**
 * Per-index statistics for the cost model. {@code rowCount} is 0 when unavailable (callers fall
 * back via {@link #rowCountOrEstimate}); {@code shardCount} is always non-zero.
 *
 * @opensearch.internal
 */
public record TableStatistics(String indexName, long rowCount, int shardCount) {
    /** Best-effort row count: the real stat when present, else {@code shardCount × defaultRowsPerShard}. */
    public double rowCountOrEstimate(double defaultRowsPerShard) {
        if (rowCount > 0) {
            return rowCount;
        }
        return (long) shardCount * (long) defaultRowsPerShard;
    }
}
