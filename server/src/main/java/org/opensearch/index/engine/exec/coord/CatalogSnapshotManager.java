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
import org.opensearch.index.engine.exec.CatalogSnapshot;

import java.io.Closeable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

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
     * Constructs a new CatalogSnapshotManager. The supplier is invoked exactly once to produce the
     * initial snapshot, which is then tracked in the live snapshot map.
     *
     * @param initialSnapshotSupplier supplier for the initial snapshot; must not be null or return null
     */
    public CatalogSnapshotManager(Supplier<CatalogSnapshot> initialSnapshotSupplier) {
        Objects.requireNonNull(initialSnapshotSupplier, "initialSnapshotSupplier must not be null");
        CatalogSnapshot initialSnapshot = Objects.requireNonNull(
            initialSnapshotSupplier.get(),
            "initialSnapshotSupplier must not return null"
        );
        this.latestCatalogSnapshot = initialSnapshot;
        if (catalogSnapshotMap.putIfAbsent(initialSnapshot.getGeneration(), initialSnapshot) != null) {
            throw new IllegalStateException(
                "Duplicate snapshot generation [" + initialSnapshot.getGeneration() + "] in catalog snapshot map"
            );
        }
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
     * Commits a new snapshot, replacing the current one. The old snapshot is decRef'd and removed
     * from the map if its count reaches zero.
     *
     * @param newSnapshot the new catalog snapshot to commit
     */
    public synchronized void commitNewSnapshot(CatalogSnapshot newSnapshot) {
        assert closed.get() == false : "Cannot commit to a closed CatalogSnapshotManager";
        assert newSnapshot.getGeneration() > latestCatalogSnapshot.getGeneration() : "New snapshot generation must be greater than current";

        if (catalogSnapshotMap.putIfAbsent(newSnapshot.getGeneration(), newSnapshot) != null) {
            throw new IllegalStateException("Duplicate snapshot generation [" + newSnapshot.getGeneration() + "] in catalog snapshot map");
        }
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
     * Returns the current snapshot. Note: this does not increment the reference count.
     * Use {@link #acquireSnapshot()} for safe concurrent access.
     *
     * @return the current {@link CatalogSnapshot}
     */
    public CatalogSnapshot getCurrentSnapshot() {
        return latestCatalogSnapshot;
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
