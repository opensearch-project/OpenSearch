/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.queue;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * A striped concurrent queue that distributes entries across multiple internal
 * queues using thread-affinity-based hashing. This reduces contention by allowing
 * concurrent threads to operate on different stripes without blocking each other.
 *
 * @param <T> the type of elements held in this queue
 * @opensearch.experimental
 */
public final class ConcurrentQueue<T> {

    static final int MIN_CONCURRENCY = 1;
    static final int MAX_CONCURRENCY = 256;

    private final int concurrency;
    private final Lock[] locks;
    private final Queue<T>[] queues;

    ConcurrentQueue(Supplier<Queue<T>> queueSupplier, int concurrency) {
        if (concurrency < MIN_CONCURRENCY || concurrency > MAX_CONCURRENCY) {
            throw new IllegalArgumentException(
                "concurrency must be in [" + MIN_CONCURRENCY + ", " + MAX_CONCURRENCY + "], got " + concurrency
            );
        }
        this.concurrency = concurrency;
        locks = new Lock[concurrency];
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Queue<T>[] queues = new Queue[concurrency];
        this.queues = queues;
        for (int i = 0; i < concurrency; ++i) {
            locks[i] = new ReentrantLock();
            queues[i] = queueSupplier.get();
        }
    }

    void add(T entry) {
        // Seed the order in which to look at entries based on the current thread. This helps distribute
        // entries across queues and gives a bit of thread affinity between entries and threads, which
        // can't hurt.
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

    T poll(Predicate<T> predicate) {
        return pollAndDropIncompatible(e -> true, predicate);
    }

    T pollAndDropIncompatible(Predicate<T> isCompatible, Predicate<T> predicate) {
        final int threadHash = Thread.currentThread().hashCode() & 0xFFFF;
        for (int i = 0; i < concurrency; ++i) {
            final int index = (threadHash + i) % concurrency;
            final Lock lock = locks[index];
            final Queue<T> queue = queues[index];
            if (lock.tryLock()) {
                try {
                    T matched = scanAndDropIncompatible(queue, isCompatible, predicate);
                    if (matched != null) return matched;
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
                T matched = scanAndDropIncompatible(queue, isCompatible, predicate);
                if (matched != null) return matched;
            } finally {
                lock.unlock();
            }
        }
        return null;
    }

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

    private T scanAndDropIncompatible(Queue<T> queue, Predicate<T> isCompatible, Predicate<T> predicate) {
        Iterator<T> it = queue.iterator();
        while (it.hasNext()) {
            T entry = it.next();
            if (isCompatible.test(entry) == false) {
                it.remove();
            } else if (predicate.test(entry)) {
                it.remove();
                return entry;
            }
        }
        return null;
    }
}
