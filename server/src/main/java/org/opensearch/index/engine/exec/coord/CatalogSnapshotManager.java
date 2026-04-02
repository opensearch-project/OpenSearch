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
import org.opensearch.index.engine.exec.Segment;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages the lifecycle of {@link CatalogSnapshot} instances for the composite multi-format engine.
 *
 * <p>Tracks all live snapshots in a map keyed by generation. When a snapshot's reference count reaches
 * zero (via {@link #decRefAndRemove}), it is automatically removed from the map. All {@code decRef}
 * calls on managed snapshots go through this method to ensure consistent cleanup.</p>
 *
 * <p>The write path (commit) is single-threaded (refresh is serialized per shard), while the read
 * path (acquireSnapshot) is safe for concurrent access via volatile reads and {@code tryIncRef}.</p>
 */
@ExperimentalApi
public class CatalogSnapshotManager implements Closeable {

    private volatile CatalogSnapshot latestCatalogSnapshot;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Map<Long, CatalogSnapshot> catalogSnapshotMap = new ConcurrentHashMap<>();

    /**
     * Constructs a new CatalogSnapshotManager with an initial snapshot built from the given parameters.
     *
     * @param id the unique snapshot identifier
     * @param generation the initial generation number
     * @param version the schema version
     * @param segments the initial segments
     * @param lastWriterGeneration the last writer generation
     * @param userData user-defined metadata
     */
    public CatalogSnapshotManager(
        long id,
        long generation,
        long version,
        List<Segment> segments,
        long lastWriterGeneration,
        Map<String, String> userData
    ) {
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
    }

    /**
     * Acquires the current snapshot with an incremented reference count, wrapped in a {@link GatedCloseable}
     * that calls {@link #decRefAndRemove} on close.
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
        return new GatedCloseable<>(snapshot, () -> decRefAndRemove(snapshot));
    }

    /**
     * Commits a new snapshot built from the given refreshed segments, replacing the current one.
     * The new snapshot inherits user data from the current snapshot and increments the generation.
     * The old snapshot is decRef'd and removed from the map if its count reaches zero.
     *
     * @param refreshedSegments the segments produced by the latest refresh
     */
    public synchronized void commitNewSnapshot(List<Segment> refreshedSegments) {
        assert closed.get() == false : "Cannot commit to a closed CatalogSnapshotManager";

        DataformatAwareCatalogSnapshot newSnapshot = new DataformatAwareCatalogSnapshot(
            latestCatalogSnapshot.getId() + 1,
            latestCatalogSnapshot.getGeneration() + 1,
            latestCatalogSnapshot.getVersion(),
            refreshedSegments,
            latestCatalogSnapshot.getLastWriterGeneration() + 1,
            latestCatalogSnapshot.getUserData()
        );

        CatalogSnapshot oldSnapshot = latestCatalogSnapshot;
        latestCatalogSnapshot = newSnapshot;
        decRefAndRemove(oldSnapshot);
    }

    /**
     * Decrements the reference count and removes the snapshot from the tracking map if it reaches zero.
     * Generation is captured before decRef to avoid accessing the snapshot after closeInternal.
     */
    private void decRefAndRemove(CatalogSnapshot snapshot) {
        final long gen = snapshot.getGeneration();
        if (snapshot.decRef()) {
            catalogSnapshotMap.remove(gen);
        }
    }

    /**
     * Closes this manager. Idempotent. DecRefs the current snapshot and removes it if count reaches zero.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            decRefAndRemove(latestCatalogSnapshot);
        }
    }

}
