/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.pool;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class LockableConcurrentQueueTests extends OpenSearchTestCase {

    public void testConstructor() {
        LockableConcurrentQueue<ReentrantLock> queue = new LockableConcurrentQueue<>(LinkedList::new, 4);
        assertNotNull(queue);
    }

    public void testLockAndPollReturnsNullWhenEmpty() {
        LockableConcurrentQueue<ReentrantLock> queue = new LockableConcurrentQueue<>(LinkedList::new, 4);

        ReentrantLock lock = queue.lockAndPoll();
        assertNull(lock);
    }

    public void testAddAndUnlockThenLockAndPoll() {
        LockableConcurrentQueue<ReentrantLock> queue = new LockableConcurrentQueue<>(LinkedList::new, 4);

        ReentrantLock lock1 = new ReentrantLock();
        ReentrantLock lock2 = new ReentrantLock();

        lock1.lock();
        lock2.lock();

        queue.addAndUnlock(lock1);
        queue.addAndUnlock(lock2);

        ReentrantLock polled1 = queue.lockAndPoll();
        assertNotNull(polled1);
        assertTrue(polled1.isHeldByCurrentThread());

        ReentrantLock polled2 = queue.lockAndPoll();
        assertNotNull(polled2);
        assertTrue(polled2.isHeldByCurrentThread());

        polled1.unlock();
        polled2.unlock();
    }

    public void testLockAndPollSkipsLockedEntries() {
        LockableConcurrentQueue<ReentrantLock> queue = new LockableConcurrentQueue<>(LinkedList::new, 4);

        ReentrantLock lock1 = new ReentrantLock();
        ReentrantLock lock2 = new ReentrantLock();

        lock1.lock();
        queue.addAndUnlock(lock1);

        lock2.lock();
        queue.addAndUnlock(lock2);

        lock1.lock();

        ReentrantLock polled = queue.lockAndPoll();
        assertNotNull(polled);
        assertEquals(lock2, polled);
        assertTrue(polled.isHeldByCurrentThread());

        polled.unlock();
        lock1.unlock();
    }

    public void testRemove() {
        LockableConcurrentQueue<ReentrantLock> queue = new LockableConcurrentQueue<>(LinkedList::new, 4);

        ReentrantLock lock1 = new ReentrantLock();
        ReentrantLock lock2 = new ReentrantLock();

        lock1.lock();
        lock2.lock();

        queue.addAndUnlock(lock1);
        queue.addAndUnlock(lock2);

        assertTrue(queue.remove(lock1));
        assertFalse(queue.remove(lock1));

        ReentrantLock polled = queue.lockAndPoll();
        assertEquals(lock2, polled);
        polled.unlock();
    }

    public void testRemoveNonExistingEntry() {
        LockableConcurrentQueue<ReentrantLock> queue = new LockableConcurrentQueue<>(LinkedList::new, 4);

        ReentrantLock lock = new ReentrantLock();
        assertFalse(queue.remove(lock));
    }

    public void testConcurrentAddAndLockAndPoll() throws Exception {
        LockableConcurrentQueue<ReentrantLock> queue = new LockableConcurrentQueue<>(LinkedList::new, 8);
        int numProducers = 5;
        int numConsumers = 5;
        int itemsPerProducer = 20;
        CountDownLatch latch = new CountDownLatch(numProducers + numConsumers);
        AtomicInteger addedCount = new AtomicInteger();
        AtomicInteger polledCount = new AtomicInteger();

        for (int t = 0; t < numProducers; t++) {
            Thread producer = new Thread(() -> {
                for (int i = 0; i < itemsPerProducer; i++) {
                    ReentrantLock lock = new ReentrantLock();
                    lock.lock();
                    queue.addAndUnlock(lock);
                    addedCount.incrementAndGet();
                }
                latch.countDown();
            });
            producer.start();
        }

        for (int t = 0; t < numConsumers; t++) {
            Thread consumer = new Thread(() -> {
                for (int i = 0; i < itemsPerProducer; i++) {
                    ReentrantLock lock = queue.lockAndPoll();
                    if (lock != null) {
                        assertTrue(lock.isHeldByCurrentThread());
                        polledCount.incrementAndGet();
                        lock.unlock();
                    }
                }
                latch.countDown();
            });
            consumer.start();
        }

        latch.await();
        assertEquals(numProducers * itemsPerProducer, addedCount.get());
        assertTrue(polledCount.get() > 0);
    }

    public void testLockAndPollRetriesOnConcurrentAdd() throws Exception {
        LockableConcurrentQueue<ReentrantLock> queue = new LockableConcurrentQueue<>(LinkedList::new, 4);
        CountDownLatch consumerStarted = new CountDownLatch(1);
        CountDownLatch producerDone = new CountDownLatch(1);
        AtomicInteger pollAttempts = new AtomicInteger();

        Thread consumer = new Thread(() -> {
            consumerStarted.countDown();
            ReentrantLock lock = queue.lockAndPoll();
            pollAttempts.incrementAndGet();
            if (lock != null) {
                lock.unlock();
            }
        });
        consumer.start();

        consumerStarted.await();
        Thread.sleep(10);

        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        queue.addAndUnlock(lock);
        producerDone.countDown();

        consumer.join();
        assertTrue(pollAttempts.get() > 0);
    }

    public void testConcurrentRemove() throws Exception {
        LockableConcurrentQueue<ReentrantLock> queue = new LockableConcurrentQueue<>(LinkedList::new, 8);
        int numLocks = 50;
        List<ReentrantLock> locks = new ArrayList<>();

        for (int i = 0; i < numLocks; i++) {
            ReentrantLock lock = new ReentrantLock();
            lock.lock();
            queue.addAndUnlock(lock);
            locks.add(lock);
        }

        int numThreads = 10;
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger removedCount = new AtomicInteger();

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            Thread remover = new Thread(() -> {
                for (int i = threadId; i < numLocks; i += numThreads) {
                    if (queue.remove(locks.get(i))) {
                        removedCount.incrementAndGet();
                    }
                }
                latch.countDown();
            });
            remover.start();
        }

        latch.await();
        assertEquals(numLocks, removedCount.get());
    }

    public void testAddAndUnlockUnlocksEntry() {
        LockableConcurrentQueue<ReentrantLock> queue = new LockableConcurrentQueue<>(LinkedList::new, 4);

        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        assertTrue(lock.isHeldByCurrentThread());

        queue.addAndUnlock(lock);
        assertFalse(lock.isHeldByCurrentThread());
    }

    public void testMultipleAddAndPollCycles() {
        LockableConcurrentQueue<ReentrantLock> queue = new LockableConcurrentQueue<>(LinkedList::new, 4);

        for (int cycle = 0; cycle < 3; cycle++) {
            ReentrantLock lock = new ReentrantLock();
            lock.lock();
            queue.addAndUnlock(lock);

            ReentrantLock polled = queue.lockAndPoll();
            assertNotNull(polled);
            assertEquals(lock, polled);
            assertTrue(polled.isHeldByCurrentThread());
            polled.unlock();
        }
    }

    public void testLockAndPollWithHighContention() throws Exception {
        LockableConcurrentQueue<ReentrantLock> queue = new LockableConcurrentQueue<>(LinkedList::new, 2);
        int numThreads = 20;
        int itemsPerThread = 10;
        CountDownLatch producerLatch = new CountDownLatch(numThreads);
        CountDownLatch consumerLatch = new CountDownLatch(numThreads);
        AtomicInteger successfulPolls = new AtomicInteger();

        for (int t = 0; t < numThreads; t++) {
            Thread producer = new Thread(() -> {
                for (int i = 0; i < itemsPerThread; i++) {
                    ReentrantLock lock = new ReentrantLock();
                    lock.lock();
                    queue.addAndUnlock(lock);
                }
                producerLatch.countDown();
            });
            producer.start();
        }

        producerLatch.await();

        for (int t = 0; t < numThreads; t++) {
            Thread consumer = new Thread(() -> {
                for (int i = 0; i < itemsPerThread; i++) {
                    ReentrantLock lock = queue.lockAndPoll();
                    if (lock != null) {
                        successfulPolls.incrementAndGet();
                        lock.unlock();
                    }
                }
                consumerLatch.countDown();
            });
            consumer.start();
        }

        consumerLatch.await();
        assertEquals(numThreads * itemsPerThread, successfulPolls.get());
    }
}
