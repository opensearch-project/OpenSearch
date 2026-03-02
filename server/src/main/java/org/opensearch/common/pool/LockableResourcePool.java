/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.pool;

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
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

/**
 * A thread-safe pool for managing lockable resources.
 * <p>
 * This pool manages a collection of resources that extend {@link Lock}, providing
 * thread-safe acquisition and release mechanisms. Resources are locked when acquired
 * and unlocked when released back to the pool.
 * </p>
 *
 * @param <T> the type of lockable resources managed by this pool, must extend {@link Lock}
 * @opensearch.internal
 */
public class LockableResourcePool<T extends Lock> implements Iterable<T>, Closeable {

    private final Set<T> entries;
    private final LockableConcurrentQueue<T> availableEntries;
    private final Supplier<T> entrySupplier;
    private volatile boolean closed;

    /**
     * Constructs a lockable resource pool.
     *
     * @param entrySupplier supplier that creates new resource instances when needed
     * @param queueSupplier supplier that creates queue instances for managing available resources
     * @param concurrency the concurrency level for the underlying queue
     */
    public LockableResourcePool(Supplier<T> entrySupplier, Supplier<Queue<T>> queueSupplier, int concurrency) {
        this.entries = Collections.newSetFromMap(new IdentityHashMap<>());
        this.entrySupplier = entrySupplier;
        this.availableEntries = new LockableConcurrentQueue<>(queueSupplier, concurrency);
    }

    /**
     * Acquires and locks a resource from the pool.
     *
     * @return a locked resource from the pool
     * @throws AlreadyClosedException if the pool has been closed
     */
    public T getAndLock() {
        ensureOpen();
        T entry = availableEntries.lockAndPoll();
        return Objects.requireNonNullElseGet(entry, this::fetchEntry);
    }

    /**
     * Creates a new resource and adds it to the pool.
     *
     * @return a new locked resource instance
     * @throws AlreadyClosedException if the pool has been closed
     */
    private synchronized T fetchEntry() {
        ensureOpen();
        T entry = entrySupplier.get();
        entry.lock();
        entries.add(entry);
        return entry;
    }

    /**
     * Releases a resource back to the pool and unlocks it. After release, the resource becomes available for other threads to acquire.
     *
     * @param entry the resource to release and unlock
     * @throws AssertionError if assertions are enabled and the entry is not registered with this pool
     */
    public void releaseAndUnlock(T entry) {
        assert isRegistered(entry) : "Resource pool doesn't know about this resource";
        availableEntries.addAndUnlock(entry);
    }

    /**
     * Checks out all resources from the pool.
     * <p>
     * This method locks and removes all resources currently managed by the pool.
     * The returned resources are no longer tracked by the pool and will not be
     * returned by subsequent {@link #getAndLock()} calls unless explicitly added back.
     * </p>
     * <p>
     * This is typically used for operations that require exclusive access to all resources,
     * such as flushing or closing operations.
     * </p>
     *
     * @return an unmodifiable list of all resources that were checked out
     * @throws AlreadyClosedException if the pool has been closed
     */
    public List<T> checkoutAll() {
        ensureOpen();
        List<T> lockedEntries = new ArrayList<>();
        List<T> checkedOutEntries = new ArrayList<>();
        for (T entry : this) {
            entry.lock();
            lockedEntries.add(entry);
        }
        synchronized (this) {
            for (T entry : lockedEntries) {
                try {
                    // Release this resource if it’s no longer managed by this pool; otherwise, check it out.
                    if (isRegistered(entry) && entries.remove(entry)) {
                        availableEntries.remove(entry);
                        checkedOutEntries.add(entry);
                    }
                } finally {
                    entry.unlock();
                }
            }
        }
        return Collections.unmodifiableList(checkedOutEntries);
    }

    /**
     * Checks if a resource is registered with this pool.
     *
     * @param entry the resource to check
     * @return {@code true} if the resource is managed by this pool, {@code false} otherwise
     */
    synchronized boolean isRegistered(T entry) {
        return entries.contains(entry);
    }

    /**
     * Ensures the pool is open and throws an exception if it has been closed.
     *
     * @throws AlreadyClosedException if the pool has been closed
     */
    private void ensureOpen() {
        if (closed) {
            throw new AlreadyClosedException("resource pool is already closed");
        }
    }

    /**
     * Returns an iterator over all resources currently managed by this pool.
     *
     * @return an iterator over the pool's resources
     */
    @Override
    public synchronized Iterator<T> iterator() {
        return List.copyOf(entries).iterator();
    }

    /**
     * Closes this pool and prevents further resource acquisition.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        this.closed = true;
    }
}
