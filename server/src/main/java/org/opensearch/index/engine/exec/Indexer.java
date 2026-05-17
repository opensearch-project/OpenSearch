/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.apache.lucene.index.IndexCommit;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.LifecycleAware;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogManager;

import java.io.Closeable;
import java.io.IOException;

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
        IndexerStatistics,
        IndexReaderProvider {

    /**
     * Returns the engine configuration for this indexer.
     * Contains settings for merge policy, translog, codec, and other engine parameters.
     *
     * @return the engine configuration
     */
    EngineConfig config();

    /**
     * Returns {@code true} when this indexer represents a REPLICA engine (one that receives
     * segments via segment replication rather than writing them directly). Implementations
     * encapsulate the dispatch so callers don't need {@code instanceof} checks on the concrete
     * engine type. Default: {@code false} (primary).
     */
    default boolean isReplicaIndexer() {
        return false;
    }

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
     * Serializes the given {@link CatalogSnapshot} to a byte array in the format expected by the
     * remote-store metadata file (Lucene {@code SegmentInfos} layout). Implementations:
     * <ul>
     *   <li>Non-DFA (segrep Lucene engine): writes the real Lucene {@code SegmentInfos}.</li>
     *   <li>DFA primary: delegates to the {@link org.opensearch.index.engine.exec.coord.CatalogSnapshotManager},
     *       which dispatches to the {@link org.opensearch.index.engine.exec.CommitFileManager}
     *       for the shard's Lucene format; that implementation uses the reader registered for
     *       the snapshot to produce bytes strictly consistent with the catalog's Lucene files.</li>
     *   <li>DFA replica (NRT): unsupported — replicas don't produce upload bytes.</li>
     * </ul>
     */
    byte[] serializeSnapshotToRemoteMetadata(CatalogSnapshot catalogSnapshot) throws IOException;

    /**
     * Acquires a safe index commit for snapshot or recovery operations.
     * The commit is guaranteed to be consistent and will not be deleted while held.
     *
     * @return a gated closeable wrapping the index commit
     * @throws EngineException if acquiring the commit fails
     * @deprecated Use {@link #acquireSafeCatalogSnapshot()} which avoids the extra
     *             {@code segments_N} disk read required to materialize an {@link IndexCommit}.
     */
    @Deprecated
    GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException;

    /**
     * Acquires a safe {@link CatalogSnapshot} for the latest commit. Preferred over
     * {@link #acquireSafeIndexCommit()} because it avoids re-reading {@code segments_N}
     * to materialize a {@link IndexCommit}. Implementations with a native catalog
     * (DFA engines) return the committed snapshot directly; legacy engines wrap
     * their in-memory {@link org.apache.lucene.index.SegmentInfos} as a
     * {@link org.opensearch.index.engine.exec.coord.SegmentInfosCatalogSnapshot}.
     * <p>
     * The caller owns the returned handle and MUST close it to release the underlying refcount.
     */
    GatedCloseable<CatalogSnapshot> acquireSafeCatalogSnapshot() throws EngineException;

    /**
     * Acquires a {@link CatalogSnapshot} pinned to the most recent commit on disk,
     * regardless of retention policy. Used for peer-recovery phase-1 metadata diffing.
     * Caller MUST close the returned handle to release the refcount.
     */
    GatedCloseable<CatalogSnapshot> acquireLastCommittedSnapshot(boolean flushFirst) throws EngineException, IOException;
}
