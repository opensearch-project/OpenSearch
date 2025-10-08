/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.queue;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

public final class LockableConcurrentQueue<T extends Lock> {

    private final ConcurrentQueue<T> queue;
    private final AtomicInteger addAndUnlockCounter = new AtomicInteger();

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
            T entry = queue.poll(Lock::tryLock);
            if (entry != null) {
                return entry;
            }
            // If an entry has been added to the queue in the meantime, try again.
        } while (addAndUnlockCount != addAndUnlockCounter.get());

        return null;
    }

    /** Remove an entry from the queue. */
    public boolean remove(T entry) {
        return queue.remove(entry);
    }

    /** Add an entry to the queue and unlock it, in that order. */
    public void addAndUnlock(T entry) {
        queue.add(entry);
        entry.unlock();
        addAndUnlockCounter.incrementAndGet();
    }
}
