/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.pool;

import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentQueueTests extends OpenSearchTestCase {

    public void testConstructorValidConcurrency() {
        ConcurrentQueue<String> queue = new ConcurrentQueue<>(LinkedList::new, 1);
        assertNotNull(queue);

        queue = new ConcurrentQueue<>(LinkedList::new, 128);
        assertNotNull(queue);

        queue = new ConcurrentQueue<>(LinkedList::new, 256);
        assertNotNull(queue);
    }

    public void testConstructorInvalidConcurrencyTooLow() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ConcurrentQueue<>(LinkedList::new, 0));
        assertTrue(e.getMessage().contains("concurrency must be in"));
    }

    public void testConstructorInvalidConcurrencyTooHigh() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ConcurrentQueue<>(LinkedList::new, 257));
        assertTrue(e.getMessage().contains("concurrency must be in"));
    }

    public void testAddAndPollSingleThread() {
        ConcurrentQueue<Integer> queue = new ConcurrentQueue<>(LinkedList::new, 4);

        queue.add(1);
        queue.add(2);
        queue.add(3);

        Integer result = queue.poll(i -> i == 2);
        assertEquals(Integer.valueOf(2), result);

        result = queue.poll(i -> i == 1);
        assertEquals(Integer.valueOf(1), result);

        result = queue.poll(i -> i == 3);
        assertEquals(Integer.valueOf(3), result);
    }

    public void testPollReturnsNullWhenEmpty() {
        ConcurrentQueue<String> queue = new ConcurrentQueue<>(LinkedList::new, 4);

        String result = queue.poll(s -> true);
        assertNull(result);
    }

    public void testPollWithPredicateNoMatch() {
        ConcurrentQueue<Integer> queue = new ConcurrentQueue<>(LinkedList::new, 4);

        queue.add(1);
        queue.add(2);
        queue.add(3);

        Integer result = queue.poll(i -> i == 10);
        assertNull(result);
    }

    public void testRemoveExistingEntry() {
        ConcurrentQueue<String> queue = new ConcurrentQueue<>(LinkedList::new, 4);

        queue.add("a");
        queue.add("b");
        queue.add("c");

        assertTrue(queue.remove("b"));

        String result = queue.poll(s -> s.equals("b"));
        assertNull(result);
    }

    public void testRemoveNonExistingEntry() {
        ConcurrentQueue<String> queue = new ConcurrentQueue<>(LinkedList::new, 4);

        queue.add("a");
        queue.add("b");

        assertFalse(queue.remove("c"));
    }

    public void testConcurrentAddAndPoll() throws Exception {
        ConcurrentQueue<Integer> queue = new ConcurrentQueue<>(LinkedList::new, 8);
        int numThreads = 10;
        int itemsPerThread = 100;
        CountDownLatch latch = new CountDownLatch(numThreads * 2);
        AtomicInteger addedCount = new AtomicInteger();
        AtomicInteger polledCount = new AtomicInteger();

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            Thread producer = new Thread(() -> {
                for (int i = 0; i < itemsPerThread; i++) {
                    queue.add(threadId * itemsPerThread + i);
                    addedCount.incrementAndGet();
                }
                latch.countDown();
            });
            producer.start();
        }

        for (int t = 0; t < numThreads; t++) {
            Thread consumer = new Thread(() -> {
                for (int i = 0; i < itemsPerThread; i++) {
                    Integer value = queue.poll(v -> true);
                    if (value != null) {
                        polledCount.incrementAndGet();
                    }
                }
                latch.countDown();
            });
            consumer.start();
        }

        latch.await();
        assertEquals(numThreads * itemsPerThread, addedCount.get());
        assertTrue(polledCount.get() > 0);
    }

    public void testConcurrentRemove() throws Exception {
        ConcurrentQueue<Integer> queue = new ConcurrentQueue<>(LinkedList::new, 8);
        int numItems = 100;

        for (int i = 0; i < numItems; i++) {
            queue.add(i);
        }

        int numThreads = 10;
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger removedCount = new AtomicInteger();

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            Thread remover = new Thread(() -> {
                for (int i = threadId; i < numItems; i += numThreads) {
                    if (queue.remove(i)) {
                        removedCount.incrementAndGet();
                    }
                }
                latch.countDown();
            });
            remover.start();
        }

        latch.await();
        assertEquals(numItems, removedCount.get());
    }

    public void testPollWithComplexPredicate() {
        ConcurrentQueue<Integer> queue = new ConcurrentQueue<>(LinkedList::new, 4);

        for (int i = 1; i <= 10; i++) {
            queue.add(i);
        }

        Integer result = queue.poll(i -> i % 2 == 0 && i > 5);
        assertNotNull(result);
        assertTrue(result % 2 == 0);
        assertTrue(result > 5);
    }

    public void testAddWithHighContention() throws Exception {
        ConcurrentQueue<Integer> queue = new ConcurrentQueue<>(LinkedList::new, 2);
        int numThreads = 20;
        int itemsPerThread = 50;
        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            Thread thread = new Thread(() -> {
                for (int i = 0; i < itemsPerThread; i++) {
                    queue.add(threadId * itemsPerThread + i);
                }
                latch.countDown();
            });
            thread.start();
        }

        latch.await();

        int count = 0;
        while (queue.poll(v -> true) != null) {
            count++;
        }
        assertEquals(numThreads * itemsPerThread, count);
    }

    public void testMultipleAddsAndRemoves() {
        ConcurrentQueue<String> queue = new ConcurrentQueue<>(LinkedList::new, 4);

        queue.add("item1");
        queue.add("item2");
        queue.add("item3");

        assertTrue(queue.remove("item2"));

        queue.add("item4");

        assertNotNull(queue.poll(s -> s.equals("item1")));
        assertNotNull(queue.poll(s -> s.equals("item3")));
        assertNotNull(queue.poll(s -> s.equals("item4")));
        assertNull(queue.poll(s -> true));
    }
}
