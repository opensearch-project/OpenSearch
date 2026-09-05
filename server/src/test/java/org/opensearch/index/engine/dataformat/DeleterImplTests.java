/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeleterImplTests extends OpenSearchTestCase {

    private Writer<?> mockWriter;
    private DeleterImpl<Writer<?>> deleter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockWriter = mock(Writer.class);
        when(mockWriter.generation()).thenReturn(1L);
        deleter = new DeleterImpl<>(mockWriter);
    }

    // ===== Basic functionality tests =====

    public void testBasicFunctionality() {
        assertEquals(1L, deleter.generation());
        assertTrue(deleter.isActive());
    }

    public void testGenerationMatchesWriter() {
        Writer<?> writer = mock(Writer.class);
        when(writer.generation()).thenReturn(42L);
        DeleterImpl<Writer<?>> d = new DeleterImpl<>(writer);
        assertEquals(42L, d.generation());
    }

    public void testRecordBufferedDeletes() {
        assertTrue(deleter.recordBufferedDeletes("doc1"));
        assertTrue(deleter.recordBufferedDeletes("doc2"));
        assertTrue(deleter.isActive());
    }

    public void testRecordBufferedDeletesDuplicateIds() {
        assertTrue(deleter.recordBufferedDeletes("doc1"));
        assertTrue(deleter.recordBufferedDeletes("doc1"));

        Queue<String> bufferedDeletes = deleter.deactivate();
        assertEquals(2, bufferedDeletes.size());
    }

    public void testRecordBufferedDeletesWhenInactive() {
        deleter.deactivate();

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> deleter.recordBufferedDeletes("doc1"));

        assertTrue(exception.getMessage().contains("Cannot record a delete on a closed deleter"));
    }

    public void testDeactivate() {
        deleter.recordBufferedDeletes("doc1");
        deleter.recordBufferedDeletes("doc2");

        Queue<String> bufferedDeletes = deleter.deactivate();

        assertFalse(deleter.isActive());
        assertEquals(2, bufferedDeletes.size());
        assertTrue(bufferedDeletes.contains("doc1"));
        assertTrue(bufferedDeletes.contains("doc2"));
    }

    public void testDeactivatePreservesOrder() {
        deleter.recordBufferedDeletes("first");
        deleter.recordBufferedDeletes("second");
        deleter.recordBufferedDeletes("third");

        Queue<String> bufferedDeletes = deleter.deactivate();

        assertEquals("first", bufferedDeletes.poll());
        assertEquals("second", bufferedDeletes.poll());
        assertEquals("third", bufferedDeletes.poll());
    }

    public void testDeactivateWhenAlreadyInactive() {
        deleter.deactivate();

        Queue<String> bufferedDeletes = deleter.deactivate();

        assertFalse(deleter.isActive());
        assertTrue(bufferedDeletes.isEmpty());
    }

    public void testDeactivateWithNoBufferedDeletes() {
        Queue<String> bufferedDeletes = deleter.deactivate();

        assertFalse(deleter.isActive());
        assertTrue(bufferedDeletes.isEmpty());
    }

    public void testClose() throws IOException {
        deleter.recordBufferedDeletes("doc1");
        assertTrue(deleter.isActive());

        deleter.close();

        assertFalse(deleter.isActive());
    }

    public void testCloseIsIdempotent() throws IOException {
        deleter.close();
        deleter.close();
        assertFalse(deleter.isActive());
    }

    // ===== Multi-threading tests =====

    public void testConcurrentBufferedDeleteRecording() throws Exception {
        int numThreads = 20;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    boolean success = deleter.recordBufferedDeletes("doc" + threadId);
                    if (success) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    // Expected for some threads if deleter becomes inactive
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(completeLatch.await(10, TimeUnit.SECONDS));

        assertEquals(numThreads, successCount.get());

        Queue<String> bufferedDeletes = deleter.deactivate();
        assertEquals(numThreads, bufferedDeletes.size());

        executor.shutdown();
    }

    /** Concurrent recording and deactivation must leave the buffer consistent. */
    public void testConcurrentRecordAndDeactivate() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CyclicBarrier barrier = new CyclicBarrier(3);
        AtomicReference<Exception> exception = new AtomicReference<>();
        AtomicBoolean deactivateCompleted = new AtomicBoolean(false);

        // First recording thread
        Future<?> firstFuture = executor.submit(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 100; i++) {
                    try {
                        deleter.recordBufferedDeletes("first" + i);
                    } catch (IllegalStateException e) {
                        // Expected when deleter becomes inactive
                        break;
                    }
                    Thread.yield();
                }
            } catch (Exception e) {
                exception.set(e);
            }
        });

        // Second recording thread
        Future<?> secondFuture = executor.submit(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 50; i++) {
                    try {
                        deleter.recordBufferedDeletes("second" + i);
                    } catch (IllegalStateException e) {
                        // Expected when deleter becomes inactive
                        break;
                    }
                    Thread.yield();
                }
            } catch (Exception e) {
                exception.set(e);
            }
        });

        // Deactivate thread
        Future<?> deactivateFuture = executor.submit(() -> {
            try {
                barrier.await();
                Thread.sleep(50); // Let other threads run first
                deleter.deactivate();
                deactivateCompleted.set(true);
            } catch (Exception e) {
                exception.set(e);
            }
        });

        firstFuture.get(10, TimeUnit.SECONDS);
        secondFuture.get(10, TimeUnit.SECONDS);
        deactivateFuture.get(10, TimeUnit.SECONDS);

        assertTrue(deactivateCompleted.get());
        assertFalse(deleter.isActive());
        assertNull(exception.get());

        executor.shutdown();
    }

    public void testConcurrentMultipleDeactivations() throws Exception {
        // Add some buffered deletes first
        deleter.recordBufferedDeletes("doc1");
        deleter.recordBufferedDeletes("doc2");
        deleter.recordBufferedDeletes("doc3");

        int numThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(numThreads);
        List<Queue<String>> results = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Queue<String> result = deleter.deactivate();
                    results.add(result);
                } catch (Exception e) {
                    // Unexpected
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(completeLatch.await(10, TimeUnit.SECONDS));

        // Only one thread should get the buffered deletes, others should get empty queues
        int nonEmptyResults = 0;
        int totalDeletes = 0;
        for (Queue<String> result : results) {
            if (!result.isEmpty()) {
                nonEmptyResults++;
                totalDeletes += result.size();
            }
        }

        assertEquals(1, nonEmptyResults);
        assertEquals(3, totalDeletes);
        assertFalse(deleter.isActive());

        executor.shutdown();
    }

    public void testConcurrentReadWriteLockContention() throws Exception {
        int numReaders = 10;
        int numWriters = 2;
        ExecutorService executor = Executors.newFixedThreadPool(numReaders + numWriters);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(numReaders + numWriters);
        AtomicInteger readerSuccessCount = new AtomicInteger(0);
        AtomicInteger writerSuccessCount = new AtomicInteger(0);

        // Reader threads (recordBufferedDeletes holds the read lock)
        for (int i = 0; i < numReaders; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < 10; j++) {
                        try {
                            boolean success = deleter.recordBufferedDeletes("buffered" + threadId + "_" + j);
                            if (success) {
                                readerSuccessCount.incrementAndGet();
                            }
                        } catch (IllegalStateException e) {
                            // Expected when deleter becomes inactive
                        }
                        Thread.yield();
                    }
                } catch (Exception e) {
                    // Some failures expected
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        // Writer threads (deactivate)
        for (int i = 0; i < numWriters; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Thread.sleep(100); // Let readers run first
                    deleter.deactivate();
                    writerSuccessCount.incrementAndGet();
                } catch (Exception e) {
                    // Unexpected
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(completeLatch.await(15, TimeUnit.SECONDS));

        // At least one writer should succeed
        assertTrue(writerSuccessCount.get() >= 1);
        assertFalse(deleter.isActive());

        executor.shutdown();
    }

    public void testStateConsistencyAfterConcurrentOperations() throws Exception {
        int numOperations = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(4);

        // Mixed operations thread: guarded recording
        executor.submit(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < numOperations / 4; i++) {
                    if (deleter.isActive()) {
                        try {
                            deleter.recordBufferedDeletes("mixed" + i);
                        } catch (IllegalStateException e) {
                            break;
                        }
                    }
                    if (i % 100 == 0) Thread.yield();
                }
            } catch (Exception e) {
                // Expected when deleter becomes inactive
            } finally {
                completeLatch.countDown();
            }
        });

        // Buffer-only thread
        executor.submit(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < numOperations / 4; i++) {
                    try {
                        deleter.recordBufferedDeletes("buffer" + i);
                    } catch (IllegalStateException e) {
                        break; // Deleter became inactive
                    }
                    if (i % 100 == 0) Thread.yield();
                }
            } catch (Exception e) {
                // Unexpected
            } finally {
                completeLatch.countDown();
            }
        });

        // Unguarded recording thread — must either succeed or throw IllegalStateException, never corrupt
        executor.submit(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < numOperations / 4; i++) {
                    try {
                        deleter.recordBufferedDeletes("unguarded" + i);
                    } catch (IllegalStateException e) {
                        break;
                    }
                    if (i % 100 == 0) Thread.yield();
                }
            } catch (Exception e) {
                // Expected when deleter becomes inactive
            } finally {
                completeLatch.countDown();
            }
        });

        // Deactivation thread
        executor.submit(() -> {
            try {
                startLatch.await();
                Thread.sleep(200); // Let other threads run
                Queue<String> bufferedDeletes = deleter.deactivate();
                assertNotNull(bufferedDeletes);
            } catch (Exception e) {
                // Unexpected
            } finally {
                completeLatch.countDown();
            }
        });

        startLatch.countDown();
        assertTrue(completeLatch.await(30, TimeUnit.SECONDS));

        // Final state should be inactive
        assertFalse(deleter.isActive());

        // Any subsequent operations should fail appropriately
        expectThrows(IllegalStateException.class, () -> deleter.recordBufferedDeletes("final"));

        executor.shutdown();
    }

    // ===== Race condition: exactly-once draining guarantee =====

    public void testBufferedDeletesNotLostDuringConcurrentDeactivation() throws Exception {
        int numThreads = 8;
        int deletesPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads + 1);
        CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
        Set<String> successfullyRecorded = ConcurrentHashMap.newKeySet();
        AtomicReference<Queue<String>> drainedRef = new AtomicReference<>();

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < deletesPerThread; i++) {
                        String id = "t" + threadId + "_d" + i;
                        try {
                            deleter.recordBufferedDeletes(id);
                            successfullyRecorded.add(id);
                        } catch (IllegalStateException e) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    // Expected
                }
            });
        }

        executor.submit(() -> {
            try {
                barrier.await();
                Thread.sleep(30);
                drainedRef.set(deleter.deactivate());
            } catch (Exception e) {
                // Expected
            }
        });

        executor.shutdown();
        assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS));

        Queue<String> drained = drainedRef.get();
        assertNotNull(drained);

        Set<String> drainedSet = new HashSet<>(drained);
        for (String id : drainedSet) {
            assertTrue("Drained id '" + id + "' was not successfully recorded", successfullyRecorded.contains(id));
        }

        for (String id : successfullyRecorded) {
            assertTrue("Successfully recorded id '" + id + "' was lost during deactivation", drainedSet.contains(id));
        }
    }

    public void testDeactivateUnderHighWriteContention() throws Exception {
        int numWriters = 16;
        int writesPerWriter = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numWriters + 1);
        CyclicBarrier barrier = new CyclicBarrier(numWriters + 1);
        AtomicInteger totalRecorded = new AtomicInteger(0);
        AtomicReference<Queue<String>> drainedRef = new AtomicReference<>();

        for (int t = 0; t < numWriters; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < writesPerWriter; i++) {
                        try {
                            deleter.recordBufferedDeletes("w" + threadId + "_" + i);
                            totalRecorded.incrementAndGet();
                        } catch (IllegalStateException e) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    // Expected
                }
            });
        }

        executor.submit(() -> {
            try {
                barrier.await();
                Thread.sleep(50);
                drainedRef.set(deleter.deactivate());
            } catch (Exception e) {
                // Expected
            }
        });

        executor.shutdown();
        assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS));

        Queue<String> drained = drainedRef.get();
        assertNotNull(drained);
        assertEquals(totalRecorded.get(), drained.size());
    }

    // ===== High-volume stress test =====

    public void testHighVolumeBufferedDeletesThenDrain() {
        int numDeletes = 10_000;
        for (int i = 0; i < numDeletes; i++) {
            assertTrue(deleter.recordBufferedDeletes("doc" + i));
        }

        Queue<String> drained = deleter.deactivate();
        assertEquals(numDeletes, drained.size());

        Set<String> seen = new HashSet<>();
        for (String id : drained) {
            assertTrue("Duplicate id found: " + id, seen.add(id));
        }
    }

    public void testRepeatedCloseDeactivateCycles() throws IOException {
        for (int cycle = 0; cycle < 5; cycle++) {
            Writer<?> w = mock(Writer.class);
            when(w.generation()).thenReturn((long) cycle);
            DeleterImpl<Writer<?>> d = new DeleterImpl<>(w);

            d.recordBufferedDeletes("doc" + cycle);
            assertTrue(d.isActive());

            d.close();
            assertFalse(d.isActive());

            Queue<String> drained = d.deactivate();
            assertTrue(drained.isEmpty());
        }
    }
}
