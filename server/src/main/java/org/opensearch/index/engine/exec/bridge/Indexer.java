/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.bridge;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

@PublicApi(since = "1.0.0")
public interface Indexer {

    Engine.IndexResult index(Engine.Index index) throws IOException;

    Engine.DeleteResult delete(Engine.Delete delete) throws IOException;

    Engine.NoOpResult noOp(Engine.NoOp noOp) throws IOException;

    /**
     * Counts the number of history operations in the given sequence number range
     * @param source       source of the request
     * @param fromSeqNo    from sequence number; included
     * @param toSeqNumber  to sequence number; included
     * @return             number of history operations
     */
    int countNumberOfHistoryOperations(String source, long fromSeqNo, long toSeqNumber) throws IOException;

    boolean hasCompleteOperationHistory(String reason, long startingSeqNo);

    long getIndexBufferRAMBytesUsed();

    List<Segment> segments(boolean verbose);

    /**
     * Returns the maximum auto_id_timestamp of all append-only index requests have been processed by this engine
     * or the auto_id_timestamp received from its primary shard via {@link #updateMaxUnsafeAutoIdTimestamp(long)}.
     * Notes this method returns the auto_id_timestamp of all append-only requests, not max_unsafe_auto_id_timestamp.
     */
    long getMaxSeenAutoIdTimestamp();

    /**
     * Forces this engine to advance its max_unsafe_auto_id_timestamp marker to at least the given timestamp.
     * The engine will disable optimization for all append-only whose timestamp at most {@code newTimestamp}.
     */
    void updateMaxUnsafeAutoIdTimestamp(long newTimestamp);

    int fillSeqNoGaps(long primaryTerm) throws IOException;

    // File format methods follow below
    void forceMerge(
        boolean flush,
        int maxNumSegments,
        boolean onlyExpungeDeletes,
        boolean upgrade,
        boolean upgradeOnlyAncientSegments,
        String forceMergeUUID
    ) throws EngineException, IOException;

    void writeIndexingBuffer() throws EngineException;

    void refresh(String source) throws EngineException;

    void flush(boolean force, boolean waitIfOngoing) throws EngineException;

    SafeCommitInfo getSafeCommitInfo();

    // Translog methods follow below
    TranslogManager translogManager();

    Closeable acquireHistoryRetentionLock();

    Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean accurateCount
    ) throws IOException;

    String getHistoryUUID();

    /**
     * Applies changes to input settings.
     */
    void onSettingsChanged(TimeValue translogRetentionAge, ByteSizeValue translogRetentionSize, long softDeletesRetentionOps);
}
