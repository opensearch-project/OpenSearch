/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.concurrent.GatedConditionalCloseable;
import org.opensearch.index.engine.exec.CatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.CatalogSnapshotLifecycleListener;
import org.opensearch.index.engine.exec.FileDeleter;
import org.opensearch.index.engine.exec.FilesListener;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.shard.ShardPath;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages the lifecycle of {@link CatalogSnapshot} instances for the composite multi-format engine
 * and coordinates file deletion through an internally owned {@link IndexFileDeleter}.
 *
 * <p>Tracks all live snapshots in a map keyed by generation. When a snapshot's reference count reaches
 * zero (via {@link #decRefAndMaybeDelete}), it is automatically removed from the map and its files
 * are cleaned up through the deleter.</p>
 *
 * <p>The write path (commit) is single-threaded (refresh is serialized per shard), while the read
 * path (acquireSnapshot) is safe for concurrent access via volatile reads and {@code tryIncRef}.</p>
 */
@ExperimentalApi
public class CatalogSnapshotManager implements Closeable {

    private volatile CatalogSnapshot latestCatalogSnapshot;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Map<Long, CatalogSnapshot> catalogSnapshotMap = new ConcurrentHashMap<>();
    private final IndexFileDeleter indexFileDeleter;
    private final CatalogSnapshotDeletionPolicy deletionPolicy;
    private final List<CatalogSnapshotLifecycleListener> snapshotListeners;

    /**
     * Constructs a new CatalogSnapshotManager.
     *
     * @param id                   the unique snapshot identifier
     * @param generation           the initial generation number
     * @param version              the schema version
     * @param segments             the initial segments
     * @param lastWriterGeneration the last writer generation
     * @param userData             user-defined metadata
     * @param deletionPolicy       decides which committed snapshots to keep
     * @param fileDeleters         per-format deleters for actual file deletion
     * @param filesListeners       per-format listeners notified on file add/delete
     * @param snapshotListeners    listeners notified on snapshot deletion
     * @param shardPath            for orphan cleanup on init, or null if not needed
     */
    public CatalogSnapshotManager(
        long id,
        long generation,
        long version,
        List<Segment> segments,
        long lastWriterGeneration,
        Map<String, String> userData,
        CatalogSnapshotDeletionPolicy deletionPolicy,
        Map<String, FileDeleter> fileDeleters,
        Map<String, FilesListener> filesListeners,
        List<CatalogSnapshotLifecycleListener> snapshotListeners,
        ShardPath shardPath
    ) throws IOException {
        DataformatAwareCatalogSnapshot initialSnapshot = new DataformatAwareCatalogSnapshot(
            id,
            generation,
            version,
            segments,
            lastWriterGeneration,
            userData
        );
        this.latestCatalogSnapshot = initialSnapshot;
        catalogSnapshotMap.put(initialSnapshot.getGeneration(), initialSnapshot);
        this.deletionPolicy = deletionPolicy;
        this.snapshotListeners = snapshotListeners;
        this.indexFileDeleter = new IndexFileDeleter(deletionPolicy, fileDeleters, filesListeners, initialSnapshot, shardPath);
    }

    // ---- Refresh path ----

    /**
     * Creates a new CatalogSnapshot from refreshed segments, replacing the current latest.
     * Notifies the IndexFileDeleter of new files. Releases the manager's own reference to
     * the old snapshot — the snapshot will only be fully cleaned up when all references
     * (including any commit references) are released.
     *
     * @param refreshedSegments the segments produced by the latest refresh
     */
    public synchronized void commitNewSnapshot(List<Segment> refreshedSegments) {
        if (closed.get()) {
            throw new IllegalStateException("CatalogSnapshotManager is closed");
        }

        DataformatAwareCatalogSnapshot newSnapshot = new DataformatAwareCatalogSnapshot(
            latestCatalogSnapshot.getId() + 1,
            latestCatalogSnapshot.getGeneration() + 1,
            latestCatalogSnapshot.getVersion(),
            refreshedSegments,
            latestCatalogSnapshot.getLastWriterGeneration() + 1,
            latestCatalogSnapshot.getUserData()
        );

        try {
            indexFileDeleter.addFileReferences(newSnapshot);
        } catch (IOException e) {
            throw new RuntimeException("Failed to add file references for snapshot [gen=" + newSnapshot.getGeneration() + "]", e);
        }
        catalogSnapshotMap.put(newSnapshot.getGeneration(), newSnapshot);

        CatalogSnapshot oldSnapshot = latestCatalogSnapshot;
        latestCatalogSnapshot = newSnapshot;

        // Release the manager's own reference to the old snapshot.
        // The snapshot won't be deleted if the commit path still holds a reference.
        decRefAndMaybeDelete(oldSnapshot);
    }

    // ---- Acquire path ----

    /**
     * Acquires the current latest snapshot with an incremented reference count.
     * Read-path only — the returned {@link GatedCloseable}'s close will decRef the snapshot.
     *
     * @return a {@link GatedCloseable} wrapping the current {@link CatalogSnapshot}
     * @throws IllegalStateException if the manager or snapshot is already closed
     */
    public GatedCloseable<CatalogSnapshot> acquireSnapshot() {
        if (closed.get()) {
            throw new IllegalStateException("CatalogSnapshotManager is closed");
        }
        final CatalogSnapshot snapshot = latestCatalogSnapshot;
        if (snapshot.tryIncRef() == false) {
            throw new IllegalStateException("CatalogSnapshot [gen=" + snapshot.getGeneration() + "] is already closed");
        }
        return new GatedCloseable<>(snapshot, () -> decRefAndMaybeDelete(snapshot));
    }

    /**
     * Acquires the current latest snapshot for committing (flushing).
     * <p>
     * The caller must call {@code markSuccess()} on the returned handle after a successful commit.
     * On close:
     * <ul>
     *   <li>If successful — registers the snapshot with the deletion policy via {@link IndexFileDeleter#onCommit}</li>
     *   <li>If not successful (failure) — releases the ref via {@link #decRefAndMaybeDelete}</li>
     * </ul>
     *
     * @return a {@link GatedConditionalCloseable} wrapping the current {@link CatalogSnapshot}
     * @throws IllegalStateException if the manager or snapshot is already closed
     */
    public GatedConditionalCloseable<CatalogSnapshot> acquireSnapshotForCommit() {
        if (closed.get()) {
            throw new IllegalStateException("CatalogSnapshotManager is closed");
        }
        final CatalogSnapshot snapshot = latestCatalogSnapshot;
        if (snapshot.tryIncRef() == false) {
            throw new IllegalStateException("CatalogSnapshot [gen=" + snapshot.getGeneration() + "] is already closed");
        }
        return new GatedConditionalCloseable<>(snapshot, () -> {
            try {
                indexFileDeleter.onCommit(snapshot);
            } catch (IOException e) {
                throw new RuntimeException("Failed to register commit [gen=" + snapshot.getGeneration() + "]", e);
            }
        }, () -> decRefAndMaybeDelete(snapshot));
    }

    // ---- Snapshot protection for _snapshot API / peer recovery ----

    /**
     * Acquire a committed snapshot for _snapshot API or peer recovery.
     * The snapshot won't be deleted until the returned {@link GatedCloseable} is closed.
     * On close, the policy releases the hold and the deleter revisits for cleanup.
     *
     * @param acquiringSafe if true, acquires the safe commit (for peer recovery);
     *                      otherwise the last commit (for _snapshot API)
     */
    public GatedCloseable<CatalogSnapshot> acquireCommittedSnapshot(boolean acquiringSafe) {
        GatedCloseable<CatalogSnapshot> policyRef = deletionPolicy.acquireCommittedSnapshot(acquiringSafe);
        return new GatedCloseable<>(policyRef.get(), () -> {
            try {
                policyRef.close();
                indexFileDeleter.revisitPolicy();
            } catch (IOException e) {
                throw new RuntimeException("Failed to release committed snapshot [gen=" + policyRef.get().getGeneration() + "]", e);
            }
        });
    }

    // ---- Internal ----

    private void decRefAndMaybeDelete(CatalogSnapshot snapshot) {
        final long gen = snapshot.getGeneration();
        if (snapshot.decRef()) {
            catalogSnapshotMap.remove(gen);
            try {
                indexFileDeleter.removeFileReferences(snapshot);
            } catch (IOException e) {
                throw new RuntimeException("Failed to clean up files for snapshot [gen=" + gen + "]", e);
            }
            for (CatalogSnapshotLifecycleListener listener : snapshotListeners) {
                try {
                    listener.onDeleted(snapshot);
                } catch (IOException e) {
                    throw new RuntimeException("Listener failed on snapshot deletion [gen=" + gen + "]", e);
                }
            }
        }
    }

    /**
     * Closes this manager. Idempotent. DecRefs the current snapshot and cleans up if count reaches zero.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            decRefAndMaybeDelete(latestCatalogSnapshot);
        }
    }
}
