/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.seqno.SeqNoStats;

import java.io.IOException;

/**
 * State management for sequence numbers, checkpoints, and timestamps.
 * Manages the critical state information required for replication, recovery, and optimistic concurrency control.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexerStateManager {

    /**
     * Returns the maximum auto-generated ID timestamp seen by this indexer.
     * Used to optimize append-only operations by avoiding version lookups when safe.
     *
     * @return the maximum auto-generated ID timestamp in milliseconds
     */
    long getMaxSeenAutoIdTimestamp();

    /**
     * Advances the max unsafe auto-generated ID timestamp marker.
     * Disables append-only optimization for operations with timestamps at or below the specified value.
     *
     * @param newTimestamp the new timestamp threshold in milliseconds
     */
    void updateMaxUnsafeAutoIdTimestamp(long newTimestamp);

    /**
     * Returns the maximum sequence number of update or delete operations processed.
     * Used for the MSU (max_seq_no_of_updates_or_deletes) optimization to safely convert updates to appends.
     * <p>
     * An index request is considered an update if it overwrites an existing document with the same ID.
     * This marker helps replicas determine when they can safely use addDocument instead of updateDocument.
     *
     * @return the maximum sequence number of updates or deletes
     * @see #advanceMaxSeqNoOfUpdatesOrDeletes(long)
     */
    long getMaxSeqNoOfUpdatesOrDeletes();

    /**
     * Advances the max sequence number of updates or deletes marker.
     * Called by replicas when receiving this value from the primary shard.
     *
     * @param maxSeqNoOfUpdatesOnPrimary the max sequence number from the primary
     */
    void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary);

    /**
     * Returns the timestamp of the last write operation in nanoseconds.
     * This value is monotonically increasing and used for tracking write activity.
     *
     * @return the last write timestamp in nanoseconds
     */
    long getLastWriteNanos();

    /**
     * Returns the local checkpoint that has been persisted to disk.
     * All operations up to and including this sequence number are durably stored.
     *
     * @return the persisted local checkpoint
     */
    long getPersistedLocalCheckpoint();

    /**
     * Returns the local checkpoint that has been processed but not necessarily persisted.
     * This may be ahead of the persisted checkpoint if operations are buffered.
     *
     * @return the processed local checkpoint
     * @see #getPersistedLocalCheckpoint()
     */
    long getProcessedLocalCheckpoint();

    /**
     * Returns sequence number statistics for this indexer.
     *
     * @param globalCheckpoint the global checkpoint to use in the statistics
     * @return sequence number statistics including max seq no, local checkpoint, and global checkpoint
     */
    SeqNoStats getSeqNoStats(long globalCheckpoint);

    /**
     * Returns the latest global checkpoint that has been synced to the translog.
     * This represents the sequence number up to which all shards have acknowledged.
     *
     * @return the last synced global checkpoint
     */
    long getLastSyncedGlobalCheckpoint();

    /**
     * Returns the minimum sequence number that must be retained for recovery and replication.
     * Operations below this sequence number may be pruned.
     *
     * @return the minimum retained sequence number
     */
    long getMinRetainedSeqNo();

    /**
     * Returns the checkpoint of the last completed refresh operation.
     * Used to track which operations have been made searchable.
     *
     * @return the last refreshed checkpoint
     * @throws UnsupportedOperationException if not implemented
     */
    default long lastRefreshedCheckpoint() {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the checkpoint of the currently ongoing refresh operation.
     * Returns -1 if no refresh is in progress.
     *
     * @return the current ongoing refresh checkpoint, or -1 if none
     * @throws UnsupportedOperationException if not implemented
     */
    default long currentOngoingRefreshCheckpoint() {
        throw new UnsupportedOperationException();
    }

    /**
     * Counts the number of operations in the translog history within the specified sequence number range.
     * Used for recovery and replication to determine the amount of history available.
     *
     * @param source description of why the count is being requested
     * @param fromSeqNo starting sequence number (inclusive)
     * @param toSeqNumber ending sequence number (inclusive)
     * @return the number of operations in the specified range
     * @throws IOException if an I/O error occurs while reading history
     */
    int countNumberOfHistoryOperations(String source, long fromSeqNo, long toSeqNumber) throws IOException;

    /**
     * Checks whether complete operation history exists from the specified sequence number.
     * Returns true if all operations from startingSeqNo onwards are available in the translog.
     *
     * @param reason description of why the check is being performed
     * @param startingSeqNo the sequence number from which complete history is required
     * @return true if complete history exists from startingSeqNo, false otherwise
     */
    boolean hasCompleteOperationHistory(String reason, long startingSeqNo);

    /**
     * Fills sequence number gaps with no-op operations to maintain continuity.
     * This ensures the local checkpoint can advance past gaps in the sequence number history.
     *
     * @param primaryTerm the primary term for the no-op operations
     * @return the number of no-op operations added
     * @throws IOException if an I/O error occurs while filling gaps
     */
    int fillSeqNoGaps(long primaryTerm) throws IOException;
}
