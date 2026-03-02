/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.pool;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

/**
 * A thread-safe queue for managing lockable entries.
 *
 * @param <T> the type of lockable entries, must extend {@link Lock}
 * @opensearch.internal
 */
final class LockableConcurrentQueue<T extends Lock> {

    private final ConcurrentQueue<T> queue;
    private final AtomicInteger addAndUnlockCounter = new AtomicInteger();

    /**
     * Constructs a lockable concurrent queue with the specified concurrency level.
     *
     * @param queueSupplier supplier that creates the underlying queue instances
     * @param concurrency the number of internal queues to use for distributing entries
     */
    LockableConcurrentQueue(Supplier<Queue<T>> queueSupplier, int concurrency) {
        this.queue = new ConcurrentQueue<>(queueSupplier, concurrency);
    }

    /**
     * Locks an entry and polls it from the queue, in that order.
     *
     * @return a locked entry from the queue, or {@code null} if no entry could be locked
     */
    T lockAndPoll() {
        int addAndUnlockCount;
        do {
            addAndUnlockCount = addAndUnlockCounter.get();
            T entry = queue.poll(Lock::tryLock);
            if (entry != null) {
                return entry;
            }
            // If an entry has been added to the queue in the meantime, try again.
        } while (addAndUnlockCount != addAndUnlockCounter.get());

        return null;
    }

    /**
     * Removes an entry from the queue.
     *
     * @param entry the entry to remove
     * @return {@code true} if the entry was found and removed, {@code false} otherwise
     */
    boolean remove(T entry) {
        return queue.remove(entry);
    }

    /**
     * Adds an entry to the queue and unlocks it, in that order.
     *
     * @param entry the locked entry to add and unlock
     */
    void addAndUnlock(T entry) {
        queue.add(entry);
        entry.unlock();
        addAndUnlockCounter.incrementAndGet();
    }
}
