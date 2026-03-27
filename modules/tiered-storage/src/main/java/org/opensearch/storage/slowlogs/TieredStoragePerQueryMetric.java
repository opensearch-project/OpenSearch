/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.slowlogs;

import org.apache.lucene.util.Accountable;

/**
 * Interface for collecting per-query metrics on tiered storage reads.
 * Tracks file cache access, prefetch operations, and read-ahead operations.
 */
public interface TieredStoragePerQueryMetric extends Accountable {

    /** Records a file cache access for the given block file.
     * @param blockFileName the block file name accessed
     * @param hit true if the access was a cache hit, false otherwise
     */
    void recordFileAccess(String blockFileName, boolean hit);

    /** Records a prefetch operation for the given file and block.
     * @param fileName the file name being prefetched
     * @param blockId the block identifier being prefetched
     */
    void recordPrefetch(String fileName, int blockId);

    /** Records a read-ahead operation for the given file and block.
     * @param fileName the file name being read ahead
     * @param blockId the block identifier being read ahead
     */
    void recordReadAhead(String fileName, int blockId);

    /** Records the end time of the query. */
    void recordEndTime();

    /** Returns the parent task ID associated with this query. */
    String getParentTaskId();

    /** Returns the shard ID associated with this query. */
    String getShardId();
}
