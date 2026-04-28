/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.queue;

import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tests for {@link LockableConcurrentQueue}.
 */
public class LockableConcurrentQueueTests extends OpenSearchTestCase {

    /**
     * A simple lockable entry for testing.
     */
    static class LockableEntry implements Lockable {
        final String id;
        private final ReentrantLock delegate = new ReentrantLock();

        LockableEntry(String id) {
            this.id = id;
        }

        @Override
        public void lock() {
            delegate.lock();
        }

        @Override
        public boolean tryLock() {
            return delegate.tryLock();
        }

        @Override
        public void unlock() {
            delegate.unlock();
        }

        boolean isHeldByCurrentThread() {
            return delegate.isHeldByCurrentThread();
        }

        boolean isLocked() {
            return delegate.isLocked();
        }

        @Override
        public String toString() {
            return "LockableEntry{" + id + "}";
        }
    }

    /** Helper: lock the entry, add it to the queue, which unlocks it. */
    private static void seedEntry(LockableConcurrentQueue<LockableEntry> queue, LockableEntry entry) {
        entry.lock();
        queue.addAndUnlock(entry);
    }

    public void testLockAndPollReturnsLockedEntry() {
        LockableConcurrentQueue<LockableEntry> queue = new LockableConcurrentQueue<>(LinkedList::new, 1);
        LockableEntry entry = new LockableEntry("a");
        seedEntry(queue, entry);

        LockableEntry polled = queue.lockAndPoll();
        assertNotNull(polled);
        assertSame(entry, polled);
        assertTrue(polled.isHeldByCurrentThread());
        polled.unlock();
    }

    public void testLockAndPollReturnsNullOnEmpty() {
        LockableConcurrentQueue<LockableEntry> queue = new LockableConcurrentQueue<>(LinkedList::new, 1);
        assertNull(queue.lockAndPoll());
    }

    public void testAddAndUnlockMakesEntryAvailable() {
        LockableConcurrentQueue<LockableEntry> queue = new LockableConcurrentQueue<>(LinkedList::new, 1);
        LockableEntry entry = new LockableEntry("a");
        entry.lock();
        queue.addAndUnlock(entry);
        // Entry should be unlocked after addAndUnlock
        assertFalse(entry.isLocked());

        LockableEntry polled = queue.lockAndPoll();
        assertNotNull(polled);
        assertTrue(polled.isHeldByCurrentThread());
        polled.unlock();
    }

    public void testRemoveEntry() {
        LockableConcurrentQueue<LockableEntry> queue = new LockableConcurrentQueue<>(LinkedList::new, 1);
        LockableEntry entry = new LockableEntry("a");
        seedEntry(queue, entry);

        assertTrue(queue.remove(entry));
        assertNull(queue.lockAndPoll());
    }

    public void testRemoveNonExistentEntry() {
        LockableConcurrentQueue<LockableEntry> queue = new LockableConcurrentQueue<>(LinkedList::new, 1);
        LockableEntry entry = new LockableEntry("a");
        assertFalse(queue.remove(entry));
    }

    public void testMultipleEntries() {
        LockableConcurrentQueue<LockableEntry> queue = new LockableConcurrentQueue<>(LinkedList::new, 2);
        LockableEntry a = new LockableEntry("a");
        LockableEntry b = new LockableEntry("b");
        LockableEntry c = new LockableEntry("c");

        seedEntry(queue, a);
        seedEntry(queue, b);
        seedEntry(queue, c);

        int count = 0;
        LockableEntry polled;
        while ((polled = queue.lockAndPoll()) != null) {
            assertTrue(polled.isHeldByCurrentThread());
            polled.unlock();
            count++;
        }
        assertEquals(3, count);
    }

    public void testLockAndPollSkipsAlreadyLockedEntries() throws Exception {
        LockableConcurrentQueue<LockableEntry> queue = new LockableConcurrentQueue<>(LinkedList::new, 1);
        LockableEntry a = new LockableEntry("a");
        LockableEntry b = new LockableEntry("b");

        seedEntry(queue, a);
        seedEntry(queue, b);

        // Lock 'a' from a different thread so tryLock fails for the current thread
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread locker = new Thread(() -> {
            a.lock();
            locked.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                a.unlock();
            }
        });
        locker.start();
        locked.await();

        try {
            LockableEntry polled = queue.lockAndPoll();
            assertNotNull(polled);
            assertSame(b, polled);
            assertTrue(polled.isHeldByCurrentThread());
            polled.unlock();
        } finally {
            release.countDown();
            locker.join();
        }
    }

    public void testConcurrentLockAndPollAndAddAndUnlock() throws Exception {
        LockableConcurrentQueue<LockableEntry> queue = new LockableConcurrentQueue<>(LinkedList::new, 4);
        int numEntries = 20;
        for (int i = 0; i < numEntries; i++) {
            LockableEntry entry = new LockableEntry("entry-" + i);
            seedEntry(queue, entry);
        }

        int numThreads = 4;
        int opsPerThread = 50;
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger pollCount = new AtomicInteger();

        for (int t = 0; t < numThreads; t++) {
            new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        LockableEntry entry = queue.lockAndPoll();
                        if (entry != null) {
                            pollCount.incrementAndGet();
                            assertTrue(entry.isHeldByCurrentThread());
                            // Return it — entry is already locked by lockAndPoll
                            queue.addAndUnlock(entry);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();
        // All entries should still be in the queue (returned after use)
        int remaining = 0;
        LockableEntry entry;
        while ((entry = queue.lockAndPoll()) != null) {
            entry.unlock();
            remaining++;
        }
        assertEquals(numEntries, remaining);
    }
}
