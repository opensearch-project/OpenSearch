/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.composite.queue.LockableConcurrentQueue;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A pool of {@link CompositeWriter} instances backed by a {@link LockableConcurrentQueue}.
 * Writers are locked on checkout and unlocked on release, ensuring thread-safe reuse.
 *
 * @opensearch.experimental
 */
public class CompositeDataFormatWriterPool implements Iterable<CompositeWriter>, Closeable {

    private volatile Set<CompositeWriter> writers;
    private volatile LockableConcurrentQueue<CompositeWriter> availableWriters;
    private final Supplier<CompositeWriter> writerSupplier;
    private final Supplier<Queue<CompositeWriter>> queueSupplier;
    private final int concurrency;
    private volatile boolean closed;

    /**
     * Creates a new writer pool.
     *
     * @param writerSupplier factory for creating new {@link CompositeWriter} instances
     * @param queueSupplier  supplier for the underlying queue instances
     * @param concurrency    the concurrency level (number of stripes)
     */
    public CompositeDataFormatWriterPool(
        Supplier<CompositeWriter> writerSupplier,
        Supplier<Queue<CompositeWriter>> queueSupplier,
        int concurrency
    ) {
        this.writers = Collections.newSetFromMap(new IdentityHashMap<>());
        this.writerSupplier = writerSupplier;
        this.queueSupplier = queueSupplier;
        this.concurrency = concurrency;
        this.availableWriters = new LockableConcurrentQueue<>(queueSupplier, concurrency);
    }

    /**
     * This method is used by CompositeIndexingExecutionEngine to grab a writer from the pool to perform an indexing
     * operation.
     *
     * @return a pooled CompositeWriter if available, or a newly created instance if none are available
     */
    public CompositeWriter getAndLock() {
        ensureOpen();
        CompositeWriter writer = availableWriters.lockAndPoll();
        return Objects.requireNonNullElseGet(writer, this::fetchWriter);
    }

    /**
     * Create a new {@link CompositeWriter} to be added to this pool.
     *
     * @return a new instance of {@link CompositeWriter}
     */
    private synchronized CompositeWriter fetchWriter() {
        ensureOpen();
        CompositeWriter writer = writerSupplier.get();
        writer.lock();
        writers.add(writer);
        return writer;
    }

    /**
     * Release the given {@link CompositeWriter} to this pool for reuse if it is currently managed by this
     * pool. If the writer belongs to a previous generation (swapped out during {@link #checkoutAll()}),
     * it is silently discarded since it will be flushed and closed by the refresh thread.
     *
     * @param state {@link CompositeWriter} to release to the pool.
     */
    public void releaseAndUnlock(CompositeWriter state) {
        assert state.isFlushPending() == false && state.isAborted() == false : "CompositeWriter has pending flush: "
            + state.isFlushPending()
            + " aborted="
            + state.isAborted();
        if (isRegistered(state) == false) {
            // Writer belongs to a previous generation that was swapped out during checkoutAll().
            // Just unlock it — the refresh thread owns it now.
            state.unlock();
            return;
        }
        availableWriters.addAndUnlock(state);
    }

    /**
     * Atomically swaps the pool's writer set and queue with fresh instances, then waits for
     * any in-flight writes on the old writers to complete. This minimizes the time the pool
     * lock is held — indexing threads see the new empty pool immediately and can create fresh
     * writers without waiting for the flush to finish.
     * <p>
     * This approach mirrors the proven rotation pattern from
     * {@code CompositeIndexWriter.LiveIndexWriterDeletesMap.buildTransitionMap()}.
     *
     * @return Unmodifiable list of all CompositeWriters ready for flush.
     */
    public List<CompositeWriter> checkoutAll() {
        ensureOpen();

        // Step 1: Atomic swap — hold pool lock only for the reference swap.
        // After this, indexing threads immediately use the new empty pool.
        Set<CompositeWriter> oldWriters;
        synchronized (this) {
            oldWriters = this.writers;
            this.writers = Collections.newSetFromMap(new IdentityHashMap<>());
            this.availableWriters = new LockableConcurrentQueue<>(queueSupplier, concurrency);
        }
        // Pool lock released — indexing threads can proceed immediately with fresh writers.

        // Step 2: Wait for in-flight writes on old writers to complete, then mark for flush.
        // No pool lock held here, so no contention with indexing threads.
        List<CompositeWriter> checkedOutWriters = new ArrayList<>(oldWriters.size());
        for (CompositeWriter writer : oldWriters) {
            writer.lock();
            try {
                writer.setFlushPending();
                checkedOutWriters.add(writer);
            } finally {
                writer.unlock();
            }
        }
        return Collections.unmodifiableList(checkedOutWriters);
    }

    /**
     * Check if {@link CompositeWriter} is part of this pool.
     *
     * @param perThread {@link CompositeWriter} to validate.
     * @return true if {@link CompositeWriter} is part of this pool, false otherwise.
     */
    synchronized boolean isRegistered(CompositeWriter perThread) {
        return writers.contains(perThread);
    }

    private void ensureOpen() {
        if (closed) {
            throw new AlreadyClosedException("CompositeDocumentWriterPool is already closed");
        }
    }

    @Override
    public synchronized Iterator<CompositeWriter> iterator() {
        return List.copyOf(writers).iterator();
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
    }
}
