/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.search.suggest.completion.CompletionStats;

/**
 * Statistics and metrics for the indexer.
 * Provides access to various performance metrics, resource usage, and operational statistics.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexerStatistics {

    /**
     * Returns statistics about index commits.
     *
     * @return commit statistics including generation and user data
     */
    CommitStats commitStats();

    /**
     * Returns document-level statistics.
     *
     * @return statistics including document count, deleted documents, and total size
     */
    DocsStats docStats();

    /**
     * Returns segment-level statistics.
     *
     * @param includeSegmentFileSizes if true, includes individual file sizes
     * @param includeUnloadedSegments if true, includes segments not currently loaded
     * @return segment statistics including count, memory usage, and file information
     */
    SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments);

    /**
     * Returns completion suggester statistics for specified field patterns.
     *
     * @param fieldNamePatterns field name patterns to match (supports wildcards)
     * @return completion statistics including memory usage per field
     */
    CompletionStats completionStats(String... fieldNamePatterns);

    /**
     * Returns statistics for pull-based ingestion operations.
     *
     * @return polling ingestion statistics
     */
    PollingIngestStats pollingIngestStats();

    /**
     * Returns merge operation statistics.
     *
     * @return merge statistics including total merges, time, and data volume
     */
    MergeStats getMergeStats();

    /**
     * Returns the total time spent under indexing throttling.
     *
     * @return throttle time in milliseconds
     */
    long getIndexThrottleTimeInMillis();

    /**
     * Returns the current amount of data being written to disk.
     *
     * @return bytes currently being written
     */
    long getWritingBytes();

    /**
     * Returns the number of unreferenced file cleanup operations performed.
     * Used to track garbage collection of unused index files.
     *
     * @return count of cleanup operations
     */
    long unreferencedFileCleanUpsPerformed();

    /**
     * Returns the amount of native memory used by the indexer.
     * This includes off-heap memory used by native components.
     *
     * @return native memory usage in bytes
     */
    default long getNativeBytesUsed() {
        return 0;
    }
}
