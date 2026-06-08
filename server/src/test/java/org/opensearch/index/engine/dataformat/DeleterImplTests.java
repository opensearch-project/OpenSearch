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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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

    public void testDeleteDoc() throws IOException {
        DeleteInput deleteInput = new DeleteInput("_id", "doc1", 1L);
        DeleteResult expectedResult = new DeleteResult.Success(1L, 1L, 1L);
        when(mockWriter.deleteDocument(deleteInput)).thenReturn(expectedResult);

        DeleteResult result = deleter.deleteDoc(deleteInput);

        assertEquals(expectedResult, result);
        verify(mockWriter).deleteDocument(deleteInput);
    }

    public void testDeleteDocReturnsFailureResult() throws IOException {
        DeleteInput deleteInput = new DeleteInput("_id", "doc1", 1L);
        DeleteResult failureResult = new DeleteResult.Failure(new RuntimeException("oops"));
        when(mockWriter.deleteDocument(deleteInput)).thenReturn(failureResult);

        DeleteResult result = deleter.deleteDoc(deleteInput);

        assertEquals(failureResult, result);
    }

    public void testDeleteDocWhenInactive() throws IOException {
        deleter.deactivate();

        DeleteInput deleteInput = new DeleteInput("_id", "doc1", 1L);

        DeleteResult result = deleter.deleteDoc(deleteInput);

        assertNull(result);
    }

    public void testDeleteDocMultipleCalls() throws IOException {
        DeleteResult mockResult = new DeleteResult.Success(1L, 1L, 1L);
        when(mockWriter.deleteDocument(any(DeleteInput.class))).thenReturn(mockResult);

        for (int i = 0; i < 10; i++) {
            DeleteInput deleteInput = new DeleteInput("_id", "doc" + i, 1L);
            DeleteResult result = deleter.deleteDoc(deleteInput);
            assertNotNull(result);
            assertTrue(result instanceof DeleteResult.Success);
        }

        verify(mockWriter, times(10)).deleteDocument(any(DeleteInput.class));
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

    public void testDeleteDocAfterClose() throws IOException {
        deleter.close();

        DeleteInput deleteInput = new DeleteInput("_id", "doc1", 1L);
        DeleteResult result = deleter.deleteDoc(deleteInput);

        assertNull(result);
    }

    // ===== Exception handling tests =====

    public void testExceptionHandlingInDeleteDoc() throws IOException {
        IOException expectedException = new IOException("Test exception");
        when(mockWriter.deleteDocument(any(DeleteInput.class))).thenThrow(expectedException);

        DeleteInput deleteInput = new DeleteInput("_id", "doc1", 1L);

        IOException thrownException = expectThrows(IOException.class, () -> deleter.deleteDoc(deleteInput));

        assertEquals(expectedException, thrownException);
        assertTrue(deleter.isActive());
    }

    public void testIOExceptionDoesNotAffectBufferedDeletes() throws IOException {
        IOException expectedException = new IOException("disk error");
        when(mockWriter.deleteDocument(any(DeleteInput.class))).thenThrow(expectedException);

        deleter.recordBufferedDeletes("doc1");

        try {
            deleter.deleteDoc(new DeleteInput("_id", "doc2", 1L));
            fail("Expected IOException");
        } catch (IOException e) {
            // expected
        }

        deleter.recordBufferedDeletes("doc3");
        assertTrue(deleter.isActive());

        Queue<String> buffered = deleter.deactivate();
        assertEquals(2, buffered.size());
        assertTrue(buffered.contains("doc1"));
        assertTrue(buffered.contains("doc3"));
    }

    public void testRuntimeExceptionFromWriterDoesNotDeactivate() throws IOException {
        when(mockWriter.deleteDocument(any(DeleteInput.class))).thenThrow(new RuntimeException("unexpected"));

        DeleteInput deleteInput = new DeleteInput("_id", "doc1", 1L);

        expectThrows(RuntimeException.class, () -> deleter.deleteDoc(deleteInput));

        assertTrue(deleter.isActive());
        assertTrue(deleter.recordBufferedDeletes("doc2"));
    }

    // ===== Multi-threading tests =====

    public void testConcurrentDeleteOperations() throws Exception {
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        DeleteResult mockResult = new DeleteResult.Success(1L, 1L, 1L);
        when(mockWriter.deleteDocument(any(DeleteInput.class))).thenReturn(mockResult);

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    DeleteInput deleteInput = new DeleteInput("_id", "doc" + threadId, 1L);
                    DeleteResult result = deleter.deleteDoc(deleteInput);
                    if (result != null) {
                        successCount.incrementAndGet();
                    } else {
                        failureCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(completeLatch.await(10, TimeUnit.SECONDS));

        assertEquals(numThreads, successCount.get());
        assertEquals(0, failureCount.get());

        executor.shutdown();
    }

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

    public void testConcurrentDeleteAndDeactivate() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CyclicBarrier barrier = new CyclicBarrier(3);
        AtomicReference<Exception> exception = new AtomicReference<>();
        AtomicBoolean deactivateCompleted = new AtomicBoolean(false);

        DeleteResult mockResult = new DeleteResult.Success(1L, 1L, 1L);
        when(mockWriter.deleteDocument(any(DeleteInput.class))).thenReturn(mockResult);

        // Delete thread
        Future<?> deleteFuture = executor.submit(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 100; i++) {
                    DeleteInput deleteInput = new DeleteInput("_id", "doc" + i, 1L);
                    deleter.deleteDoc(deleteInput);
                    Thread.yield();
                }
            } catch (Exception e) {
                exception.set(e);
            }
        });

        // Buffered delete recording thread
        Future<?> bufferFuture = executor.submit(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 50; i++) {
                    try {
                        deleter.recordBufferedDeletes("buffered" + i);
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

        deleteFuture.get(10, TimeUnit.SECONDS);
        bufferFuture.get(10, TimeUnit.SECONDS);
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

        DeleteResult mockResult = new DeleteResult.Success(1L, 1L, 1L);
        when(mockWriter.deleteDocument(any(DeleteInput.class))).thenReturn(mockResult);

        // Reader threads (deleteDoc and recordBufferedDeletes)
        for (int i = 0; i < numReaders; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < 10; j++) {
                        if (threadId % 2 == 0) {
                            // Delete operation
                            DeleteInput deleteInput = new DeleteInput("_id", "doc" + threadId + "_" + j, 1L);
                            DeleteResult result = deleter.deleteDoc(deleteInput);
                            if (result != null) {
                                readerSuccessCount.incrementAndGet();
                            }
                        } else {
                            // Buffer delete operation
                            try {
                                boolean success = deleter.recordBufferedDeletes("buffered" + threadId + "_" + j);
                                if (success) {
                                    readerSuccessCount.incrementAndGet();
                                }
                            } catch (IllegalStateException e) {
                                // Expected when deleter becomes inactive
                            }
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

        DeleteResult mockResult = new DeleteResult.Success(1L, 1L, 1L);
        when(mockWriter.deleteDocument(any(DeleteInput.class))).thenReturn(mockResult);

        // Mixed operations thread
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
                        DeleteInput deleteInput = new DeleteInput("_id", "mixed" + i, 1L);
                        deleter.deleteDoc(deleteInput);
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

        // Delete-only thread
        executor.submit(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < numOperations / 4; i++) {
                    if (deleter.isActive()) {
                        DeleteInput deleteInput = new DeleteInput("_id", "delete" + i, 1L);
                        deleter.deleteDoc(deleteInput);
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
        assertNull(deleter.deleteDoc(new DeleteInput("_id", "final", 1L)));

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

    // ===== Race condition: close while deleteDoc is in flight =====

    public void testCloseWhileDeleteDocInFlight() throws Exception {
        DeleteResult mockResult = new DeleteResult.Success(1L, 1L, 1L);
        when(mockWriter.deleteDocument(any(DeleteInput.class))).thenAnswer(invocation -> {
            Thread.sleep(50);
            return mockResult;
        });

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicReference<DeleteResult> deleteResult = new AtomicReference<>();

        executor.submit(() -> {
            try {
                barrier.await();
                DeleteInput deleteInput = new DeleteInput("_id", "doc1", 1L);
                deleteResult.set(deleter.deleteDoc(deleteInput));
            } catch (Exception e) {
                // Expected
            }
        });

        executor.submit(() -> {
            try {
                barrier.await();
                Thread.sleep(10);
                deleter.close();
            } catch (Exception e) {
                // Expected
            }
        });

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        assertFalse(deleter.isActive());
    }

    // ===== Race condition: deleteDoc returns null after deactivation =====

    public void testDeleteDocReturnsNullAfterDeactivation() throws Exception {
        DeleteResult mockResult = new DeleteResult.Success(1L, 1L, 1L);
        when(mockWriter.deleteDocument(any(DeleteInput.class))).thenReturn(mockResult);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicInteger nullResults = new AtomicInteger(0);
        AtomicInteger successResults = new AtomicInteger(0);

        executor.submit(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 200; i++) {
                    DeleteInput deleteInput = new DeleteInput("_id", "doc" + i, 1L);
                    DeleteResult result = deleter.deleteDoc(deleteInput);
                    if (result == null) {
                        nullResults.incrementAndGet();
                    } else {
                        successResults.incrementAndGet();
                    }
                }
            } catch (Exception e) {
                // Expected
            }
        });

        executor.submit(() -> {
            try {
                barrier.await();
                Thread.sleep(5);
                deleter.deactivate();
            } catch (Exception e) {
                // Expected
            }
        });

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        // All 200 operations must complete — some succeed, some return null after deactivation
        assertEquals(200, successResults.get() + nullResults.get());
        assertTrue(successResults.get() > 0);
        // After deactivation, subsequent calls must return null
        assertFalse(deleter.isActive());
        assertNull(deleter.deleteDoc(new DeleteInput("_id", "final_check", 1L)));
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
