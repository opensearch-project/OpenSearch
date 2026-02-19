/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.LifecycleAware;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import static org.opensearch.index.engine.Engine.HISTORY_UUID_KEY;

/**
 * Unified interface for indexing operations in OpenSearch.
 * <p>
 * This interface provides a complete abstraction for document indexing, combining:
 * <ul>
 *   <li>{@link IndexerEngineOperations} - Core CRUD operations (index, delete, no-op)</li>
 *   <li>{@link IndexerStateManager} - Sequence numbers, checkpoints, and timestamps</li>
 *   <li>{@link IndexerLifecycleOperations} - Flush, refresh, merge, and throttling</li>
 *   <li>{@link IndexerStatistics} - Performance metrics and resource usage</li>
 * </ul>
 * <p>
 * The interface is designed to support multiple implementations:
 * <ul>
 *   <li>Engine-backed indexing (traditional Lucene-based)</li>
 *   <li>Alternative storage engines</li>
 *   <li>Delegating or decorating implementations</li>
 * </ul>
 * <p>
 * Key responsibilities:
 * <ul>
 *   <li>Document lifecycle management (create, update, delete)</li>
 *   <li>Translog and durability guarantees</li>
 *   <li>Replication and recovery support via sequence numbers</li>
 *   <li>Search visibility through refresh operations</li>
 *   <li>Resource management and cleanup</li>
 * </ul>
 *
 * @see org.opensearch.index.engine.EngineBackedIndexer for the primary implementation
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Indexer
    extends
        LifecycleAware,
        Closeable,
        IndexerEngineOperations,
        IndexerStateManager,
        IndexerLifecycleOperations,
        IndexerStatistics {

    /**
     * Returns the engine configuration for this indexer.
     * Contains settings for merge policy, translog, codec, and other engine parameters.
     *
     * @return the engine configuration
     */
    EngineConfig config();

    /**
     * Returns information about the safe commit point.
     * The safe commit represents a consistent state that can be used for recovery.
     *
     * @return safe commit information including local and global checkpoints
     */
    SafeCommitInfo getSafeCommitInfo();

    /**
     * Returns the translog manager for this indexer.
     * Provides access to the transaction log for durability and recovery.
     *
     * @return the translog manager
     */
    TranslogManager translogManager();

    /**
     * Acquires a lock to prevent history pruning.
     * Used during operations that need to read historical operations (e.g., recovery, replication).
     *
     * @return a closeable lock that must be released when history access is complete
     */
    Closeable acquireHistoryRetentionLock();

    /**
     * Creates a snapshot of operations within the specified sequence number range.
     * Used for replication and recovery to replay operations on other nodes.
     *
     * @param source description of why the snapshot is being created
     * @param fromSeqNo starting sequence number (inclusive)
     * @param toSeqNo ending sequence number (inclusive)
     * @param requiredFullRange if true, fails if the full range is not available
     * @param accurateCount if true, provides accurate operation count (may be slower)
     * @return a snapshot of operations in the specified range
     * @throws IOException if an I/O error occurs while creating the snapshot
     */
    Translog.Snapshot newChangesSnapshot(String source, long fromSeqNo, long toSeqNo, boolean requiredFullRange, boolean accurateCount)
        throws IOException;

    /**
     * Returns the unique identifier for this index's history.
     * Used to ensure replicas are synchronized with the correct primary.
     *
     * @return the history UUID
     */
    String getHistoryUUID();

    /**
     * Flushes all pending changes and closes the indexer.
     * Ensures all data is persisted before shutdown.
     *
     * @throws IOException if an I/O error occurs during flush or close
     */
    void flushAndClose() throws IOException;

    /**
     * Marks the engine as failed and prevents further operations.
     * Called when an unrecoverable error occurs.
     *
     * @param reason description of why the engine failed
     * @param failure the exception that caused the failure, or null
     */
    void failEngine(String reason, @Nullable Exception failure);

    /**
     * Acquires a snapshot of the current catalog state.
     * The snapshot contains segment metadata and file information for replication.
     *
     * @return a gated closeable wrapping the catalog snapshot
     */
    GatedCloseable<CatalogSnapshot> acquireSnapshot();

    /**
     * Checks if the throwable contains a fatal error and throws it if present.
     * Fatal errors (like OutOfMemoryError) should not be caught and must propagate
     * to the uncaught exception handler.
     *
     * @param logger the logger to use for error messages
     * @param maybeMessage the message to log if a fatal error is found
     * @param maybeFatal the throwable to check for fatal errors
     */
    @SuppressWarnings("finally")
    default void maybeDie(final Logger logger, final String maybeMessage, final Throwable maybeFatal) {
        ExceptionsHelper.maybeError(maybeFatal).ifPresent(error -> {
            try {
                logger.error(maybeMessage, error);
            } finally {
                throw error;
            }
        });
    }

    /**
     * Reads the history UUID from commit user data.
     * The history UUID identifies the lineage of operations in this index.
     *
     * @param commitData the commit user data map
     * @return the history UUID
     * @throws IllegalStateException if the commit data doesn't contain a history UUID
     */
    default String loadHistoryUUID(Map<String, String> commitData) {
        final String uuid = commitData.get(HISTORY_UUID_KEY);
        if (uuid == null) {
            throw new IllegalStateException("commit doesn't contain history uuid");
        }
        return uuid;
    }

    /**
     * Acquires a safe index commit for snapshot or recovery operations.
     * The commit is guaranteed to be consistent and will not be deleted while held.
     *
     * @return a gated closeable wrapping the index commit
     * @throws EngineException if acquiring the commit fails
     */
    GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException;

}
