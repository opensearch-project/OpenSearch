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
 * Interface that needs to be implemented by any per query metric collector.
 *
 * @opensearch.experimental
 */
public interface TieredStoragePerQueryMetric extends Accountable {

    /**
     * Records a file access event.
     * @param blockFileName the block file name
     * @param hit whether the access was a cache hit
     */
    void recordFileAccess(String blockFileName, boolean hit);

    /**
     * Records a prefetch event.
     * @param fileName the file name
     * @param blockId the block id
     */
    void recordPrefetch(String fileName, int blockId);

    /**
     * Records a read-ahead event.
     * @param fileName the file name
     * @param blockId the block id
     */
    void recordReadAhead(String fileName, int blockId);

    /** Records the end time of the metric collection. */
    void recordEndTime();

    /**
     * Returns the parent task id.
     * @return the parent task id
     */
    String getParentTaskId();

    /**
     * Returns the shard id.
     * @return the shard id
     */
    String getShardId();
}
