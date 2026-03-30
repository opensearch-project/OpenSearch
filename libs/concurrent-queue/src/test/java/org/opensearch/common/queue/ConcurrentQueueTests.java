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

/**
 * Tests for {@link ConcurrentQueue}.
 */
public class ConcurrentQueueTests extends OpenSearchTestCase {

    public void testAddAndPollSingleThread() {
        ConcurrentQueue<String> queue = new ConcurrentQueue<>(LinkedList::new, 1);
        queue.add("a");
        queue.add("b");
        assertEquals("a", queue.poll(e -> true));
        assertEquals("b", queue.poll(e -> true));
        assertNull(queue.poll(e -> true));
    }

    public void testPollWithPredicateFiltering() {
        ConcurrentQueue<Integer> queue = new ConcurrentQueue<>(LinkedList::new, 1);
        queue.add(1);
        queue.add(2);
        queue.add(3);
        // Poll only even numbers
        assertEquals(Integer.valueOf(2), queue.poll(n -> n % 2 == 0));
        // Remaining: 1, 3
        assertNull(queue.poll(n -> n % 2 == 0));
        assertEquals(Integer.valueOf(1), queue.poll(e -> true));
        assertEquals(Integer.valueOf(3), queue.poll(e -> true));
    }

    public void testPollReturnsNullOnEmpty() {
        ConcurrentQueue<String> queue = new ConcurrentQueue<>(LinkedList::new, 4);
        assertNull(queue.poll(e -> true));
    }

    public void testPollPredicateAlwaysFalse() {
        ConcurrentQueue<String> queue = new ConcurrentQueue<>(LinkedList::new, 2);
        queue.add("a");
        assertNull(queue.poll(e -> false));
        // Entry should still be there
        assertEquals("a", queue.poll(e -> true));
    }

    public void testRemoveExistingEntry() {
        ConcurrentQueue<String> queue = new ConcurrentQueue<>(LinkedList::new, 2);
        queue.add("a");
        queue.add("b");
        assertTrue(queue.remove("a"));
        assertEquals("b", queue.poll(e -> true));
        assertNull(queue.poll(e -> true));
    }

    public void testRemoveNonExistentEntry() {
        ConcurrentQueue<String> queue = new ConcurrentQueue<>(LinkedList::new, 2);
        queue.add("a");
        assertFalse(queue.remove("z"));
        assertEquals("a", queue.poll(e -> true));
    }

    public void testRemoveFromEmpty() {
        ConcurrentQueue<String> queue = new ConcurrentQueue<>(LinkedList::new, 1);
        assertFalse(queue.remove("a"));
    }

    public void testConcurrencyBoundsLow() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ConcurrentQueue<>(LinkedList::new, 0));
        assertTrue(e.getMessage().contains("concurrency must be in"));
    }

    public void testConcurrencyBoundsHigh() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ConcurrentQueue<>(LinkedList::new, 257));
        assertTrue(e.getMessage().contains("concurrency must be in"));
    }

    public void testMinConcurrency() {
        ConcurrentQueue<String> queue = new ConcurrentQueue<>(LinkedList::new, ConcurrentQueue.MIN_CONCURRENCY);
        queue.add("a");
        assertEquals("a", queue.poll(e -> true));
    }

    public void testMaxConcurrency() {
        ConcurrentQueue<String> queue = new ConcurrentQueue<>(LinkedList::new, ConcurrentQueue.MAX_CONCURRENCY);
        queue.add("a");
        assertEquals("a", queue.poll(e -> true));
    }

    public void testMultipleStripes() {
        // With higher concurrency, entries distribute across stripes
        ConcurrentQueue<Integer> queue = new ConcurrentQueue<>(LinkedList::new, 8);
        int count = 100;
        for (int i = 0; i < count; i++) {
            queue.add(i);
        }
        AtomicInteger polled = new AtomicInteger();
        Integer entry;
        while ((entry = queue.poll(e -> true)) != null) {
            polled.incrementAndGet();
        }
        assertEquals(count, polled.get());
    }

    public void testConcurrentAddAndPoll() throws Exception {
        ConcurrentQueue<Integer> queue = new ConcurrentQueue<>(LinkedList::new, 4);
        int numThreads = 4;
        int itemsPerThread = 250;
        CyclicBarrier barrier = new CyclicBarrier(numThreads * 2);
        CountDownLatch addLatch = new CountDownLatch(numThreads);
        CountDownLatch pollLatch = new CountDownLatch(numThreads);
        AtomicInteger totalPolled = new AtomicInteger();

        // Producer threads
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < itemsPerThread; i++) {
                        queue.add(threadId * itemsPerThread + i);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    addLatch.countDown();
                }
            }).start();
        }

        // Consumer threads
        for (int t = 0; t < numThreads; t++) {
            new Thread(() -> {
                try {
                    barrier.await();
                    addLatch.await(); // Wait for all adds to complete
                    Integer item;
                    while ((item = queue.poll(e -> true)) != null) {
                        totalPolled.incrementAndGet();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    pollLatch.countDown();
                }
            }).start();
        }

        pollLatch.await();
        assertEquals(numThreads * itemsPerThread, totalPolled.get());
    }
}
