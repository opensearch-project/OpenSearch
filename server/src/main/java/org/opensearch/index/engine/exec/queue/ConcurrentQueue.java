/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.queue;

import java.util.Queue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.function.Supplier;

public final class ConcurrentQueue<T> {

    static final int MIN_CONCURRENCY = 1;
    static final int MAX_CONCURRENCY = 256;

    final int concurrency;
    final Lock[] locks;
    final Queue<T>[] queues;
    final Supplier<Queue<T>> queueSupplier;

    ConcurrentQueue(Supplier<Queue<T>> queueSupplier, int concurrency) {
        if (concurrency < MIN_CONCURRENCY || concurrency > MAX_CONCURRENCY) {
            throw new IllegalArgumentException(
                "concurrency must be in [" + MIN_CONCURRENCY + ", " + MAX_CONCURRENCY + "], got " + concurrency);
        }
        this.concurrency = concurrency;
        this.queueSupplier = queueSupplier;
        locks = new Lock[concurrency];
        @SuppressWarnings({ "rawtypes", "unchecked" }) Queue<T>[] queues = new Queue[concurrency];
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
        final int threadHash = Thread.currentThread().hashCode() & 0xFFFF;
        for (int i = 0; i < concurrency; ++i) {
            final int index = (threadHash + i) % concurrency;
            final Lock lock = locks[index];
            final Queue<T> queue = queues[index];
            if (lock.tryLock()) {
                try {
                    for (T entry : queue) {
                        if (predicate.test(entry)) {
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
                for (T entry : queue) {
                    if (predicate.test(entry)) {
                        return entry;
                    }
                }
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
}
