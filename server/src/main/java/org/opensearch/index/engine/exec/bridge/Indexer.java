/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.bridge;

import org.opensearch.common.annotation.ExperimentalApi;
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

/**
 * Encapsulates indexing methods which can be used by OpenSearch.
 */
@ExperimentalApi
public interface Indexer {

    /**
     * Perform document index operation on the engine
     * @param index operation to perform
     * @return {@link Engine.IndexResult} containing updated translog location, version and
     * document specific failures
     *
     * Note: engine level failures (i.e. persistent engine failures) are thrown
     */
    Engine.IndexResult index(Engine.Index index) throws IOException;

    /**
     * Perform document delete operation on the engine
     * @param delete operation to perform
     * @return {@link Engine.DeleteResult} containing updated translog location, version and
     * document specific failures
     *
     * Note: engine level failures (i.e. persistent engine failures) are thrown
     */
    Engine.DeleteResult delete(Engine.Delete delete) throws IOException;

    /**
     * Perform a no-op equivalent operation on the engine.
     * @param noOp
     * @return
     * @throws IOException
     */
    Engine.NoOpResult noOp(Engine.NoOp noOp) throws IOException;

    /**
     * Counts the number of history operations in the given sequence number range
     * @param source       source of the request
     * @param fromSeqNo    from sequence number; included
     * @param toSeqNumber  to sequence number; included
     * @return             number of history operations
     */
    int countNumberOfHistoryOperations(String source, long fromSeqNo, long toSeqNumber) throws IOException;

    /**
     * @param reason why is the history requested
     * @param startingSeqNo sequence number beyond which history should exist
     * @return tru iff minimum retained sequence number during indexing is not less than startingSeqNo
     */
    boolean hasCompleteOperationHistory(String reason, long startingSeqNo);

    /**
     * Total amount of RAM bytes used for active indexing to buffer unflushed documents.
     */
    long getIndexBufferRAMBytesUsed();

    /**
     * @param verbose unused param
     * @return list of segments the indexer is aware of (previously created + new ones)
     */
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

    /**
     * @return Time in nanos for last write through the indexer, always increasing.
     */
    long getLastWriteNanos();

    /**
     * Fills up the local checkpoints history with no-ops until the local checkpoint
     * and the max seen sequence ID are identical.
     * @param primaryTerm the shards primary term this indexer was created for
     * @return the number of no-ops added
     */
    int fillSeqNoGaps(long primaryTerm) throws IOException;

    /**
     * Performs a force merge operation on this engine.
     */
    void forceMerge(
        boolean flush,
        int maxNumSegments,
        boolean onlyExpungeDeletes,
        boolean upgrade,
        boolean upgradeOnlyAncientSegments,
        String forceMergeUUID
    ) throws EngineException, IOException;

    /**
     * Applies changes to input settings.
     */
    void onSettingsChanged(TimeValue translogRetentionAge, ByteSizeValue translogRetentionSize, long softDeletesRetentionOps);

    /**
     * Flushes active indexing buffer to disk.
     */
    void writeIndexingBuffer() throws EngineException;

    /**
     * Creates segments for data in buffers, and make them available for search.
     */
    void refresh(String source) throws EngineException;

    /**
     * Commits the data and state to disk, resulting in documents being persisted onto the underlying formats.
     */
    void flush(boolean force, boolean waitIfOngoing) throws EngineException;

    /**
     * Checks if data should be committed to disk, mainly based on translog thresholds.
     * @return true iff flush should trigger.
     */
    boolean shouldPeriodicallyFlush();

    /**
     * Returns info about the safe commit.
     */
    SafeCommitInfo getSafeCommitInfo();


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
}
