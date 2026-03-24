/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.CatalogSnapshot;

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
    private final AtomicLong generation;
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
        this.generation = new AtomicLong(initialSnapshot.getGeneration());
        catalogSnapshotMap.put(initialSnapshot.getGeneration(), initialSnapshot);
    }

    /**
     * Acquires the current snapshot with an incremented reference count, wrapped in a {@link ReleasableRef}
     * that calls {@link #decRefAndRemove} on close.
     *
     * @return a {@link ReleasableRef} wrapping the current {@link CatalogSnapshot}
     * @throws IllegalStateException if the manager or snapshot is already closed
     */
    public ReleasableRef<CatalogSnapshot> acquireSnapshot() {
        if (closed.get()) {
            throw new IllegalStateException("CatalogSnapshotManager is closed");
        }
        final CatalogSnapshot snapshot = latestCatalogSnapshot;
        if (snapshot.tryIncRef() == false) {
            throw new IllegalStateException("CatalogSnapshot [gen=" + snapshot.getGeneration() + "] is already closed");
        }
        return new ReleasableRef<>(snapshot) {
            @Override
            public void close() {
                decRefAndRemove(snapshot);
            }
        };
    }

    /**
     * Commits a new snapshot, replacing the current one. The old snapshot is decRef'd and removed
     * from the map if its count reaches zero.
     *
     * @param newSnapshot the new catalog snapshot to commit
     */
    public void commitNewSnapshot(CatalogSnapshot newSnapshot) {
        assert closed.get() == false : "Cannot commit to a closed CatalogSnapshotManager";
        assert newSnapshot.getGeneration() > latestCatalogSnapshot.getGeneration() : "New snapshot generation must be greater than current";

        catalogSnapshotMap.put(newSnapshot.getGeneration(), newSnapshot);
        generation.set(newSnapshot.getGeneration());
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
     * Returns an unmodifiable view of all live snapshots keyed by generation.
     *
     * @return unmodifiable map of generation to catalog snapshot
     */
    public Map<Long, CatalogSnapshot> getCatalogSnapshotMap() {
        return Collections.unmodifiableMap(catalogSnapshotMap);
    }

    /**
     * Returns the current generation counter value.
     *
     * @return the current generation
     */
    public long getCurrentGeneration() {
        return generation.get();
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

    /**
     * A generic reference wrapper for safe resource management via try-with-resources.
     */
    @ExperimentalApi
    public abstract static class ReleasableRef<T> implements AutoCloseable {

        private final T ref;

        public ReleasableRef(T ref) {
            this.ref = ref;
        }

        public T getRef() {
            return ref;
        }
    }
}
