/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.common.annotation.PublicApi;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * The interface that orchestrates Translog operations and manages the {@link Translog} and interfaces with the Engine
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface TranslogManager {

    /**
     * Rolls the translog generation and cleans unneeded.
     */
    void rollTranslogGeneration() throws TranslogException, IOException;

    /**
     * Performs recovery from the transaction log up to {@code recoverUpToSeqNo} (inclusive).
     * This operation will close the engine if the recovery fails.
     *
     * @param translogRecoveryRunner the translog recovery runner
     * @param recoverUpToSeqNo       the upper bound, inclusive, of sequence number to be recovered
     * @return ops recovered
     */
    int recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long localCheckpoint, long recoverUpToSeqNo) throws IOException;

    /**
     * Creates a new history snapshot from the translog file instead of the lucene index.
     */
    Translog.Snapshot newChangesSnapshot(long fromSeqNo, long toSeqNo, boolean requiredFullRange) throws IOException;

    /**
     * Checks if the underlying storage sync is required.
     */
    boolean isTranslogSyncNeeded();

    /**
     * Ensures that all locations in the given stream have been written to the underlying storage.
     */
    boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException;

    /**
     * Syncs translog to disk
     * @throws IOException the exception while performing the sync operation
     */
    void syncTranslog() throws IOException;

    /**
     * Translog operation stats
     * @return the translog stats
     */
    TranslogStats getTranslogStats();

    /**
     * Returns the last location that the translog of this engine has written into.
     */
    Translog.Location getTranslogLastWriteLocation();

    /**
     * checks and removes translog files that no longer need to be retained. See
     * {@link org.opensearch.index.translog.TranslogDeletionPolicy} for details
     */
    void trimUnreferencedTranslogFiles() throws TranslogException;

    /**
     * Tests whether or not the translog generation should be rolled to a new generation.
     * This test is based on the size of the current generation compared to the configured generation threshold size.
     *
     * @return {@code true} if the current generation should be rolled to a new generation
     */
    boolean shouldRollTranslogGeneration();

    /**
     * Trims translog for terms below <code>belowTerm</code> and seq# above <code>aboveSeqNo</code>
     *
     * @see Translog#trimOperations(long, long)
     */
    void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws TranslogException;

    /**
     * This method replays translog to restore the Lucene index which might be reverted previously.
     * This ensures that all acknowledged writes are restored correctly when this engine is promoted.
     *
     * @return the number of translog operations have been recovered
     */
    int restoreLocalHistoryFromTranslog(long processedCheckpoint, TranslogRecoveryRunner translogRecoveryRunner) throws IOException;

    /**
     * Do not replay translog operations, but make the engine be ready.
     */
    void skipTranslogRecovery();

    /**
     * Reads operations for the translog
     * @param location the location in the translog
     * @return the translog operation
     * @throws IOException throws an IO exception when reading the file fails
     */
    Translog.Operation readOperation(Translog.Location location) throws IOException;

    /**
     * Adds an operation to the translog
     * @param operation to add to translog
     * @return the location in the translog
     * @throws IOException throws an IO exception if adding an operation fails
     */
    Translog.Location add(Translog.Operation operation) throws IOException;

    /**
     * Checks if the translog has a pending recovery
     */
    void ensureCanFlush();

    /**
     *
     * @param seqNo : operations greater or equal to seqNo should be persisted
     * This might be required when segments are persisted via other mechanism than flush.
     */
    void setMinSeqNoToKeep(long seqNo);

    /*
    Clean up if any needed on deletion of index
     */
    void onDelete();

    Translog.TranslogGeneration getTranslogGeneration();
}
