/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.queue;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * A concurrent queue wrapper that adds lock-and-poll / add-and-unlock semantics
 * on top of {@link ConcurrentQueue}. Entries must implement {@link Lockable} so that
 * they can be atomically locked when polled and unlocked when returned.
 * <p>
 * This is used by the composite writer pool to ensure that a writer is locked
 * before it is handed out and unlocked when it is returned.
 *
 * @param <T> the type of lockable elements held in this queue
 * @opensearch.experimental
 */
public final class LockableConcurrentQueue<T extends Lockable> {

    private final ConcurrentQueue<T> queue;
    private final AtomicInteger addAndUnlockCounter = new AtomicInteger();

    /**
     * Creates a new lockable concurrent queue.
     *
     * @param queueSupplier supplier for the underlying queue instances
     * @param concurrency   the concurrency level (number of stripes)
     */
    public LockableConcurrentQueue(Supplier<Queue<T>> queueSupplier, int concurrency) {
        this.queue = new ConcurrentQueue<>(queueSupplier, concurrency);
    }

    /**
     * Lock an entry, and poll it from the queue, in that order. If no entry can be found and locked,
     * {@code null} is returned.
     */
    public T lockAndPoll() {
        int addAndUnlockCount;
        do {
            addAndUnlockCount = addAndUnlockCounter.get();
            T entry = queue.poll(Lockable::tryLock);
            if (entry != null) {
                return entry;
            }
            // If an entry has been added to the queue in the meantime, try again.
        } while (addAndUnlockCount != addAndUnlockCounter.get());

        return null;
    }

    /**
     * Remove an entry from the queue.
     *
     * @param entry the entry to remove
     * @return {@code true} if the entry was removed
     */
    public boolean remove(T entry) {
        return queue.remove(entry);
    }

    /**
     * Add an entry to the queue and unlock it, in that order.
     *
     * @param entry the entry to add and unlock
     */
    public void addAndUnlock(T entry) {
        queue.add(entry);
        entry.unlock();
        addAndUnlockCounter.incrementAndGet();
    }
}
