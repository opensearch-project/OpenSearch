/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.queue.Lockable;
import org.opensearch.common.queue.LockableConcurrentQueue;

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
 * A data-format-aware pool of {@link Writer} instances backed by a {@link LockableConcurrentQueue}.
 * Writers are locked on checkout and unlocked on release, ensuring thread-safe reuse.
 * <p>
 * The pool is created with queue configuration only. The writer supplier is set later
 * via {@link #initialize(Supplier)}, allowing the engine that owns the pool to provide
 * the writer factory after construction (avoiding circular dependencies).
 *
 * @param <W> the writer type, must implement both {@link Writer} and {@link Lockable}
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class DataformatAwareLockableWriterPool<W extends Writer<?> & Lockable> implements Iterable<W>, Closeable {

    private final Set<W> writers;
    private final LockableConcurrentQueue<W> availableWriters;
    private final SetOnce<Boolean> closed = new SetOnce<>();
    private volatile Supplier<W> writerSupplier;

    /**
     * Creates a new writer pool. The pool must be initialized
     * with a writer supplier via {@link #initialize(Supplier)} before use.
     *
     * @param queueSupplier supplier for the underlying queue instances
     * @param concurrency   the concurrency level (number of stripes)
     */
    public DataformatAwareLockableWriterPool(Supplier<Queue<W>> queueSupplier, int concurrency) {
        this.writers = Collections.newSetFromMap(new IdentityHashMap<>());
        this.availableWriters = new LockableConcurrentQueue<>(queueSupplier, concurrency);
    }

    /**
     * Initializes the pool with a writer supplier. Must be called exactly once before
     * any checkout operations.
     *
     * @param writerSupplier factory for creating new writer instances
     * @throws IllegalStateException if already initialized
     */
    public void initialize(Supplier<W> writerSupplier) {
        if (this.writerSupplier != null) {
            throw new IllegalStateException("DataformatAwareLockableWriterPool is already initialized");
        }
        this.writerSupplier = Objects.requireNonNull(writerSupplier, "writerSupplier must not be null");
    }

    /**
     * Locks and polls a writer from the pool, or creates a new one if none are available.
     *
     * @return a locked writer instance
     */
    public W getAndLock() {
        ensureOpen();
        ensureInitialized();
        W writer = availableWriters.lockAndPoll();
        return Objects.requireNonNullElseGet(writer, this::fetchWriter);
    }

    private synchronized W fetchWriter() {
        ensureOpen();
        W writer = writerSupplier.get();
        writer.lock();
        writers.add(writer);
        return writer;
    }

    /**
     * Releases the given writer back to this pool for reuse.
     *
     * @param writer the writer to release
     */
    public void releaseAndUnlock(W writer) {
        assert isRegistered(writer) : "WriterPool doesn't know about this writer";
        availableWriters.addAndUnlock(writer);
    }

    /**
     * Lock and checkout all writers from the pool for flush.
     *
     * @return unmodifiable list of all writers locked by current thread
     */
    public List<W> checkoutAll() {
        ensureOpen();
        List<W> lockedWriters = new ArrayList<>();
        List<W> checkedOutWriters = new ArrayList<>();
        for (W writer : this) {
            writer.lock();
            lockedWriters.add(writer);
        }
        synchronized (this) {
            for (W writer : lockedWriters) {
                try {
                    if (isRegistered(writer) && writers.remove(writer)) {
                        availableWriters.remove(writer);
                        checkedOutWriters.add(writer);
                    }
                } finally {
                    writer.unlock();
                }
            }
        }
        return Collections.unmodifiableList(checkedOutWriters);
    }

    /**
     * Check if a writer is part of this pool.
     *
     * @param writer writer to validate
     * @return true if the writer is part of this pool
     */
    public synchronized boolean isRegistered(W writer) {
        return writers.contains(writer);
    }

    private void ensureOpen() {
        if (closed.get() != null) {
            throw new AlreadyClosedException("DataformatAwareLockableWriterPool is already closed");
        }
    }

    private void ensureInitialized() {
        if (writerSupplier == null) {
            throw new IllegalStateException("DataformatAwareLockableWriterPool has not been initialized with a writer supplier");
        }
    }

    @Override
    public synchronized Iterator<W> iterator() {
        return List.copyOf(writers).iterator();
    }

    @Override
    public void close() throws IOException {
        if (closed.trySet(true) == false) {
            throw new AlreadyClosedException("DataformatAwareLockableWriterPool is already closed");
        }
    }
}
