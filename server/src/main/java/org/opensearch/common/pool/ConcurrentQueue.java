/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.pool;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * A thread-safe queue implementation that distributes entries across multiple internal queues
 * to reduce lock contention.
 * <p>
 * This queue uses a striping pattern where entries are distributed across a configurable number
 * of internal queues, each protected by its own lock. This design allows multiple threads to
 * operate on the queue concurrently with minimal contention.
 * </p>
 * <p>
 * Thread affinity is achieved by using the current thread's hash code to determine which
 * internal queue to access first, improving cache locality and reducing contention.
 * </p>
 *
 * @param <T> the type of elements held in this queue
 * @opensearch.internal
 */
final class ConcurrentQueue<T> {

    /** Minimum allowed concurrency level. */
    static final int MIN_CONCURRENCY = 1;

    /** Maximum allowed concurrency level. */
    static final int MAX_CONCURRENCY = 256;

    private final int concurrency;
    private final Lock[] locks;
    private final Queue<T>[] queues;
    private final Supplier<Queue<T>> queueSupplier;

    /**
     * Constructs a concurrent queue with the specified concurrency level.
     *
     * @param queueSupplier supplier that creates the underlying queue instances
     * @param concurrency the number of internal queues to use, must be between
     *                    {@link #MIN_CONCURRENCY} and {@link #MAX_CONCURRENCY}
     * @throws IllegalArgumentException if concurrency is out of valid range
     */
    ConcurrentQueue(Supplier<Queue<T>> queueSupplier, int concurrency) {
        if (concurrency < MIN_CONCURRENCY || concurrency > MAX_CONCURRENCY) {
            throw new IllegalArgumentException(
                "concurrency must be in [" + MIN_CONCURRENCY + ", " + MAX_CONCURRENCY + "], got " + concurrency
            );
        }
        this.concurrency = concurrency;
        this.queueSupplier = queueSupplier;
        locks = new Lock[concurrency];
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Queue<T>[] queues = new Queue[concurrency];
        this.queues = queues;
        for (int i = 0; i < concurrency; ++i) {
            locks[i] = new ReentrantLock();
            queues[i] = queueSupplier.get();
        }
    }

    /**
     * Adds an entry to the queue.
     *
     * @param entry the entry to add to the queue
     */
    void add(T entry) {
        final int threadHash = Thread.currentThread().hashCode() & 0xFFFF;
        for (int i = 0; i < concurrency; ++i) {
            final int index = (threadHash + i) % concurrency;
            final Lock lock = locks[index];
            final Queue<T> queue = queues[index];
            if (lock.tryLock()) {
                try {
                    queue.add(entry);
                    return;
                } finally {
                    lock.unlock();
                }
            }
        }
        final int index = threadHash % concurrency;
        final Lock lock = locks[index];
        final Queue<T> queue = queues[index];
        lock.lock();
        try {
            queue.add(entry);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Polls an entry from the queue that matches the given predicate.
     *
     * @param predicate the condition that the entry must satisfy
     * @return the first entry that matches the predicate, or {@code null} if none found
     */
    T poll(Predicate<T> predicate) {
        final int threadHash = Thread.currentThread().hashCode() & 0xFFFF;
        for (int i = 0; i < concurrency; ++i) {
            final int index = (threadHash + i) % concurrency;
            final Lock lock = locks[index];
            final Queue<T> queue = queues[index];
            if (lock.tryLock()) {
                try {
                    Iterator<T> it = queue.iterator();
                    while (it.hasNext()) {
                        T entry = it.next();
                        if (predicate.test(entry)) {
                            it.remove();
                            return entry;
                        }
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
        for (int i = 0; i < concurrency; ++i) {
            final int index = (threadHash + i) % concurrency;
            final Lock lock = locks[index];
            final Queue<T> queue = queues[index];
            lock.lock();
            try {
                Iterator<T> it = queue.iterator();
                while (it.hasNext()) {
                    T entry = it.next();
                    if (predicate.test(entry)) {
                        it.remove();
                        return entry;
                    }
                }
            } finally {
                lock.unlock();
            }
        }
        return null;
    }

    /**
     * Removes the specified entry from the queue.
     *
     * @param entry the entry to remove
     * @return {@code true} if the entry was found and removed, {@code false} otherwise
     */
    boolean remove(T entry) {
        for (int i = 0; i < concurrency; ++i) {
            final Lock lock = locks[i];
            final Queue<T> queue = queues[i];
            lock.lock();
            try {
                if (queue.remove(entry)) {
                    return true;
                }
            } finally {
                lock.unlock();
            }
        }
        return false;
    }
}
