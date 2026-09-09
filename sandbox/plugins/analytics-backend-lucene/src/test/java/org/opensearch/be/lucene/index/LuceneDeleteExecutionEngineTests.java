/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergeIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.be.lucene.stats.LuceneShardStatsTracker;
import org.opensearch.common.queue.Lockable;
import org.opensearch.common.queue.LockablePool;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DeleteInput;
import org.opensearch.index.engine.dataformat.DeleteResult;
import org.opensearch.index.engine.dataformat.Deleter;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.Optional;
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
import java.util.concurrent.locks.ReentrantLock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LuceneDeleteExecutionEngineTests extends OpenSearchTestCase {

    private LuceneCommitter mockCommitter;
    private MergeIndexWriter mockParentWriter;
    private DataFormat mockDataFormat;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockCommitter = mock(LuceneCommitter.class);
        mockParentWriter = mock(MergeIndexWriter.class);
        mockDataFormat = mock(DataFormat.class);

        when(mockCommitter.getIndexWriter()).thenReturn(mockParentWriter);
    }

    private LuceneDeleteExecutionEngine createDeleteEngine() {
        return new LuceneDeleteExecutionEngine(mockDataFormat, mockCommitter);
    }

    // ===== Basic functionality tests =====

    public void testCreateDeleter() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);

        Deleter deleter = deleteEngine.createDeleter(mockWriter);

        assertNotNull(deleter);
        assertEquals(1L, deleter.generation());
        assertTrue(deleter.isActive());
    }

    public void testCreateDeleterWithoutLuceneWriter() {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = mock(Writer.class);
        when(mockWriter.generation()).thenReturn(1L);
        when(mockWriter.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME)).thenReturn(Optional.empty());

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> deleteEngine.createDeleter(mockWriter));

        assertTrue(exception.getMessage().contains("no Lucene writer found"));
    }

    public void testCreateMultipleDeletersForDifferentGenerations() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        Writer<?> writer2 = createMockWriter(2L);
        Writer<?> writer3 = createMockWriter(3L);

        Deleter deleter1 = deleteEngine.createDeleter(writer1);
        Deleter deleter2 = deleteEngine.createDeleter(writer2);
        Deleter deleter3 = deleteEngine.createDeleter(writer3);

        assertEquals(1L, deleter1.generation());
        assertEquals(2L, deleter2.generation());
        assertEquals(3L, deleter3.generation());
        assertTrue(deleter1.isActive());
        assertTrue(deleter2.isActive());
        assertTrue(deleter3.isActive());
    }

    public void testDeleteDocumentWithCurrentGeneration() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        Deleter deleter = deleteEngine.createDeleter(mockWriter);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 1L);

        DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> null);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);
    }

    public void testDeleteDocumentWithPreviousGeneration() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        // Setup current generation deleter
        Writer<?> currentWriter = createMockWriter(2L);
        deleteEngine.createDeleter(currentWriter);

        // Setup previous generation deleter with a trackable LuceneWriter
        LuceneWriter prevLuceneWriter = mock(LuceneWriter.class);
        when(prevLuceneWriter.generation()).thenReturn(1L);
        when(prevLuceneWriter.deleteDocument(any(DeleteInput.class))).thenReturn(new DeleteResult.Success(1L, 1L, 1L));
        Writer<?> previousWriter = mock(Writer.class);
        when(previousWriter.generation()).thenReturn(1L);
        when(previousWriter.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME)).thenReturn(Optional.of(prevLuceneWriter));
        deleteEngine.createDeleter(previousWriter);

        // Record a write for the document in previous generation
        deleteEngine.recordWrite("doc1", 1L);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 2L);
        Closeable mockLock = mock(Closeable.class);

        DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> gen == 1L ? mockLock : null);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);
        // Verify the delete was actually issued on the previous generation's writer
        verify(prevLuceneWriter).deleteDocument(deleteInput);
        verify(mockLock).close();
    }

    public void testDeleteDocumentPreviousGenLockReturnsNull() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        Writer<?> writer2 = createMockWriter(2L);
        deleteEngine.createDeleter(writer1);
        deleteEngine.createDeleter(writer2);

        deleteEngine.recordWrite("doc1", 1L);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 2L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> null);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);
    }

    public void testDeleteDocumentNoRecordedWriteReturnsSuccess() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        deleteEngine.createDeleter(mockWriter);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "nonexistent_doc", 1L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> {
            fail("Should not be called when no previous gen recorded");
            return null;
        });

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);
    }

    public void testDeleteDocumentPropagatesIOExceptionFromDeleter() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriterWithDeleteFailure(1L, new IOException("disk full"));
        Writer<?> writer2 = createMockWriter(2L);
        deleteEngine.createDeleter(writer1);
        deleteEngine.createDeleter(writer2);

        deleteEngine.recordWrite("doc1", 1L);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 2L);
        Closeable mockLock = mock(Closeable.class);

        IOException thrown = expectThrows(
            IOException.class,
            () -> deleteEngine.deleteDocument(deleteInput, gen -> gen == 1L ? mockLock : null)
        );

        assertEquals("disk full", thrown.getMessage());
        verify(mockLock).close();
    }

    public void testDeleteDocumentReleasesLockOnException() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriterWithDeleteFailure(1L, new IOException("failure"));
        Writer<?> writer2 = createMockWriter(2L);
        deleteEngine.createDeleter(writer1);
        deleteEngine.createDeleter(writer2);

        deleteEngine.recordWrite("doc1", 1L);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 2L);
        Closeable mockLock = mock(Closeable.class);

        try {
            deleteEngine.deleteDocument(deleteInput, gen -> gen == 1L ? mockLock : null);
            fail("Expected IOException");
        } catch (IOException expected) {}

        verify(mockLock).close();
    }

    public void testRecordWrite() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();

        // Setup: Create deleters for both generations
        Writer<?> previousWriter = createMockWriter(1L);
        deleteEngine.createDeleter(previousWriter);
        Writer<?> currentWriter = createMockWriter(2L);
        deleteEngine.createDeleter(currentWriter);

        // Test: Record write for previous generation using Uid.encodeId (matching the lookup path)
        long previousGeneration = 1L;
        deleteEngine.recordWrite("doc1", previousGeneration);

        // Verify: Delete from current generation should find previous generation
        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 2L);
        AtomicBoolean previousGenLookupCalled = new AtomicBoolean(false);

        try {
            deleteEngine.deleteDocument(deleteInput, gen -> {
                if (gen == previousGeneration) {
                    previousGenLookupCalled.set(true);
                    return mock(Closeable.class);
                }
                return null;
            });
        } catch (Exception e) {
            fail("Should not throw exception: " + e.getMessage());
        }

        assertTrue(previousGenLookupCalled.get());
    }

    public void testRecordWriteOverwritesPreviousGeneration() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        Writer<?> writer2 = createMockWriter(2L);
        Writer<?> writer3 = createMockWriter(3L);
        deleteEngine.createDeleter(writer1);
        deleteEngine.createDeleter(writer2);
        deleteEngine.createDeleter(writer3);

        deleteEngine.recordWrite("doc1", 1L);
        deleteEngine.recordWrite("doc1", 2L);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 3L);
        AtomicReference<Long> requestedGen = new AtomicReference<>();

        deleteEngine.deleteDocument(deleteInput, gen -> {
            requestedGen.set(gen);
            return mock(Closeable.class);
        });

        assertEquals(Long.valueOf(2L), requestedGen.get());
    }

    public void testOnWriterCheckedOut() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        Deleter deleter = deleteEngine.createDeleter(mockWriter);

        // Record some buffered deletes
        deleter.recordBufferedDeletes("doc1");
        deleter.recordBufferedDeletes("doc2");

        boolean result = deleteEngine.onWriterCheckedOut(1L);

        assertTrue(result);
        verify(mockParentWriter, times(2)).deleteDocuments(any(Term.class));
        assertFalse(deleter.isActive());
    }

    public void testOnWriterCheckedOutWithNoDeleter() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();

        boolean result = deleteEngine.onWriterCheckedOut(999L);

        assertFalse(result);
        verify(mockParentWriter, never()).deleteDocuments(any(Term.class));
    }

    public void testOnWriterCheckedOutWithNoBufferedDeletes() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        Deleter deleter = deleteEngine.createDeleter(mockWriter);

        boolean result = deleteEngine.onWriterCheckedOut(1L);

        assertFalse(result);
        verify(mockParentWriter, never()).deleteDocuments(any(Term.class));
        assertFalse(deleter.isActive());
    }

    public void testOnWriterCheckedOutRemovesIdToGenEntries() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        Writer<?> writer2 = createMockWriter(2L);
        deleteEngine.createDeleter(writer1);
        deleteEngine.createDeleter(writer2);

        deleteEngine.recordWrite("doc1", 1L);
        deleteEngine.recordWrite("doc2", 1L);
        deleteEngine.recordWrite("doc3", 2L);

        deleteEngine.onWriterCheckedOut(1L);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 2L);
        AtomicBoolean genLookupCalled = new AtomicBoolean(false);
        deleteEngine.deleteDocument(deleteInput, gen -> {
            genLookupCalled.set(true);
            return null;
        });

        assertFalse(genLookupCalled.get());
    }

    public void testOnWriterCheckedOutCalledTwiceForSameGeneration() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        Deleter deleter = deleteEngine.createDeleter(mockWriter);
        deleter.recordBufferedDeletes("doc1");

        assertTrue(deleteEngine.onWriterCheckedOut(1L));
        assertFalse(deleteEngine.onWriterCheckedOut(1L));
    }

    // ===== Close behavior tests =====

    public void testClose() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter1 = createMockWriter(1L);
        Writer<?> mockWriter2 = createMockWriter(2L);

        Deleter deleter1 = deleteEngine.createDeleter(mockWriter1);
        Deleter deleter2 = deleteEngine.createDeleter(mockWriter2);

        assertTrue(deleter1.isActive());
        assertTrue(deleter2.isActive());

        deleteEngine.close();

        assertFalse(deleter1.isActive());
        assertFalse(deleter2.isActive());
    }

    public void testCloseIsIdempotent() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        deleteEngine.createDeleter(mockWriter);

        deleteEngine.close();
        deleteEngine.close();
    }

    public void testCloseWithPendingBufferedDeletes() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        Deleter deleter = deleteEngine.createDeleter(mockWriter);
        deleter.recordBufferedDeletes("doc1");
        deleter.recordBufferedDeletes("doc2");

        deleteEngine.close();

        assertFalse(deleter.isActive());
        verify(mockParentWriter, never()).deleteDocuments(any(Term.class));
    }

    // ===== Multi-threading tests =====

    public void testConcurrentDeleterCreation() throws Exception {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            final long generation = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Writer<?> mockWriter = createMockWriter(generation);
                    Deleter deleter = deleteEngine.createDeleter(mockWriter);
                    if (deleter != null && deleter.generation() == generation) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    // Expected for some threads due to race conditions
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(completeLatch.await(10, TimeUnit.SECONDS));
        assertEquals(numThreads, successCount.get());

        executor.shutdown();
    }

    public void testConcurrentDeleteOperations() throws Exception {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        deleteEngine.createDeleter(mockWriter);

        int numThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            final String docId = "doc" + i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, docId, 1L);
                    DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> null);
                    if (result instanceof DeleteResult.Success) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    // Some failures expected due to race conditions
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(completeLatch.await(10, TimeUnit.SECONDS));
        assertEquals(numThreads, successCount.get());

        executor.shutdown();
    }

    public void testConcurrentRecordWriteAndDelete() throws Exception {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        deleteEngine.createDeleter(mockWriter);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicReference<Exception> exception = new AtomicReference<>();

        // Writer thread — uses Uid.encodeId so the key matches what deleteDocument looks up
        Future<?> writerFuture = executor.submit(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < 100; i++) {
                    deleteEngine.recordWrite("doc" + i, 1L);
                    Thread.yield();
                }
            } catch (Exception e) {
                exception.set(e);
            }
        });

        // Delete thread — lookupGen uses Term(_id, Uid.encodeId("doc"+i)).bytes() which matches above
        Future<?> deleterFuture = executor.submit(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < 100; i++) {
                    DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc" + i, 1L);
                    deleteEngine.deleteDocument(deleteInput, gen -> null);
                    Thread.yield();
                }
            } catch (Exception e) {
                exception.set(e);
            }
        });

        startLatch.countDown();
        writerFuture.get(10, TimeUnit.SECONDS);
        deleterFuture.get(10, TimeUnit.SECONDS);

        assertNull(exception.get());
        executor.shutdown();
    }

    public void testConcurrentWriterCheckoutAndDelete() throws Exception {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        Deleter deleter = deleteEngine.createDeleter(mockWriter);

        // Add some buffered deletes
        deleter.recordBufferedDeletes("doc1");
        deleter.recordBufferedDeletes("doc2");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean checkoutCompleted = new AtomicBoolean(false);
        AtomicBoolean deleteCompleted = new AtomicBoolean(false);

        // Checkout thread
        Future<?> checkoutFuture = executor.submit(() -> {
            try {
                startLatch.await();
                deleteEngine.onWriterCheckedOut(1L);
                checkoutCompleted.set(true);
            } catch (Exception e) {
                // Expected
            }
        });

        // Delete thread — after checkout, the deleter for gen=1 is removed so deleteDocument
        // will hit an assertion error. This is expected: in production the caller holds the
        // writer lock preventing concurrent checkout of the same generation.
        Future<?> deleteFuture = executor.submit(() -> {
            try {
                startLatch.await();
                Thread.sleep(10); // Small delay to increase race condition chance
                DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc3", 1L);
                deleteEngine.deleteDocument(deleteInput, gen -> null);
                deleteCompleted.set(true);
            } catch (Exception | AssertionError e) {
                // Expected if deleter becomes inactive or was already removed
            }
        });

        startLatch.countDown();
        checkoutFuture.get(5, TimeUnit.SECONDS);
        deleteFuture.get(5, TimeUnit.SECONDS);

        assertTrue(checkoutCompleted.get());
        // Delete may or may not complete depending on timing

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    public void testConcurrentDeleteSameDocumentId() throws Exception {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        Writer<?> writer2 = createMockWriter(2L);
        deleteEngine.createDeleter(writer1);
        deleteEngine.createDeleter(writer2);

        deleteEngine.recordWrite("shared_doc", 1L);

        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicReference<Exception> unexpectedException = new AtomicReference<>();

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    barrier.await();
                    DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "shared_doc", 2L);
                    DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> mock(Closeable.class));
                    if (result != null) {
                        successCount.incrementAndGet();
                    }
                } catch (IOException e) {
                    // Some may fail if deleter becomes inactive
                } catch (Exception e) {
                    unexpectedException.set(e);
                }
            });
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        assertNull(unexpectedException.get());
        assertTrue(successCount.get() > 0);
    }

    public void testConcurrentRecordWriteAndOnWriterCheckedOut() throws Exception {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        Writer<?> writer2 = createMockWriter(2L);
        deleteEngine.createDeleter(writer1);
        deleteEngine.createDeleter(writer2);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicReference<Exception> exception = new AtomicReference<>();

        executor.submit(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 200; i++) {
                    deleteEngine.recordWrite("doc" + i, 1L);
                }
            } catch (Exception e) {
                exception.set(e);
            }
        });

        executor.submit(() -> {
            try {
                barrier.await();
                Thread.sleep(5);
                deleteEngine.onWriterCheckedOut(1L);
            } catch (Exception e) {
                exception.set(e);
            }
        });

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        assertNull(exception.get());
    }

    public void testConcurrentCloseAndDeleteDocument() throws Exception {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        deleteEngine.createDeleter(mockWriter);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicBoolean closeDone = new AtomicBoolean(false);

        executor.submit(() -> {
            try {
                barrier.await();
                Thread.sleep(5);
                deleteEngine.close();
                closeDone.set(true);
            } catch (Exception e) {
                // Expected
            }
        });

        executor.submit(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 50; i++) {
                    try {
                        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc" + i, 1L);
                        deleteEngine.deleteDocument(deleteInput, gen -> null);
                    } catch (Exception e) {
                        // Expected when engine closes
                    }
                    Thread.yield();
                }
            } catch (Exception e) {
                // Expected
            }
        });

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        assertTrue(closeDone.get());
    }

    public void testConcurrentCloseAndOnWriterCheckedOut() throws Exception {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        Deleter deleter = deleteEngine.createDeleter(mockWriter);
        deleter.recordBufferedDeletes("doc1");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CyclicBarrier barrier = new CyclicBarrier(2);

        executor.submit(() -> {
            try {
                barrier.await();
                deleteEngine.close();
            } catch (Exception e) {
                // Expected
            }
        });

        executor.submit(() -> {
            try {
                barrier.await();
                deleteEngine.onWriterCheckedOut(1L);
            } catch (Exception e) {
                // Expected
            }
        });

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        assertFalse(deleter.isActive());
    }

    // ===== Integration tests covering interaction between LuceneDeleteExecutionEngine and DeleterImpl =====

    public void testCompleteDeleteWorkflow() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        Deleter deleter = deleteEngine.createDeleter(mockWriter);

        deleteEngine.recordWrite("doc1", 1L);

        deleter.recordBufferedDeletes("doc2");

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 1L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> null);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);

        boolean hasDeletes = deleteEngine.onWriterCheckedOut(1L);

        assertTrue(hasDeletes);
        // "doc2" was buffered directly, "doc1" was buffered by deleteDocument → 2 total
        verify(mockParentWriter, times(2)).deleteDocuments(any(Term.class));
        assertFalse(deleter.isActive());
    }

    public void testMultiGenerationDeleteWorkflow() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        Writer<?> writer2 = createMockWriter(2L);

        Deleter deleter1 = deleteEngine.createDeleter(writer1);
        Deleter deleter2 = deleteEngine.createDeleter(writer2);

        deleteEngine.recordWrite("doc1", 1L);

        deleter1.recordBufferedDeletes("buffered1");
        deleter2.recordBufferedDeletes("buffered2");

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 2L);
        AtomicBoolean previousGenLocked = new AtomicBoolean(false);

        DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> {
            if (gen == 1L) {
                previousGenLocked.set(true);
                return mock(Closeable.class);
            }
            return null;
        });

        assertNotNull(result);
        assertTrue(previousGenLocked.get());

        assertTrue(deleteEngine.onWriterCheckedOut(1L));
        assertTrue(deleteEngine.onWriterCheckedOut(2L));

        assertFalse(deleter1.isActive());
        assertFalse(deleter2.isActive());
    }

    public void testConcurrentDeleteAndWriterCheckout() throws Exception {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        Deleter deleter = deleteEngine.createDeleter(mockWriter);

        deleter.recordBufferedDeletes("doc1");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(2);
        AtomicInteger successCount = new AtomicInteger(0);

        executor.submit(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < 5; i++) {
                    DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "concurrent" + i, 1L);
                    DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> null);
                    if (result instanceof DeleteResult.Success) {
                        successCount.incrementAndGet();
                    }
                }
            } catch (Exception e) {
                // Expected
            } finally {
                completeLatch.countDown();
            }
        });

        executor.submit(() -> {
            try {
                startLatch.await();
                Thread.sleep(10);
                deleteEngine.onWriterCheckedOut(1L);
            } catch (Exception e) {
                // Expected
            } finally {
                completeLatch.countDown();
            }
        });

        startLatch.countDown();
        assertTrue(completeLatch.await(5, TimeUnit.SECONDS));

        assertFalse(deleter.isActive());
        executor.shutdown();
    }

    // ===== Test cases inspired by CompositeIndexWriter patterns =====

    public void testDeleteWithRefreshConcurrency() throws Exception {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        Deleter deleter = deleteEngine.createDeleter(mockWriter);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean refreshing = new AtomicBoolean(false);
        AtomicInteger deleteCount = new AtomicInteger(0);

        executor.submit(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < 10; i++) {
                    if (!refreshing.get()) {
                        deleter.recordBufferedDeletes("doc" + i);
                        deleteCount.incrementAndGet();
                    }
                    Thread.yield();
                }
            } catch (Exception e) {
                // Expected when deleter becomes inactive
            }
        });

        executor.submit(() -> {
            try {
                startLatch.await();
                Thread.sleep(5);
                refreshing.set(true);
                deleteEngine.onWriterCheckedOut(1L);
            } catch (Exception e) {
                // Expected
            }
        });

        startLatch.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        assertFalse(deleter.isActive());
        assertTrue(deleteCount.get() > 0);
    }

    public void testDeleteWithOldAndCurrentGenerations() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> oldWriter = createMockWriter(1L);
        Deleter oldDeleter = deleteEngine.createDeleter(oldWriter);

        deleteEngine.recordWrite("old_doc", 1L);
        oldDeleter.recordBufferedDeletes("old_buffered");

        deleteEngine.onWriterCheckedOut(1L);
        assertFalse(oldDeleter.isActive());

        Writer<?> newWriter = createMockWriter(2L);
        Deleter newDeleter = deleteEngine.createDeleter(newWriter);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "new_doc", 2L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> null);

        assertNotNull(result);
        verify(mockParentWriter, times(1)).deleteDocuments(any(Term.class));
    }

    public void testDeleteWithDocumentInBothGenerations() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        String docId = "shared_doc";

        Writer<?> writer1 = createMockWriter(1L);
        deleteEngine.createDeleter(writer1);
        deleteEngine.recordWrite(docId, 1L);

        Writer<?> writer2 = createMockWriter(2L);
        deleteEngine.createDeleter(writer2);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, docId, 2L);
        AtomicBoolean previousGenFound = new AtomicBoolean(false);

        DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> {
            if (gen == 1L) {
                previousGenFound.set(true);
                return mock(Closeable.class);
            }
            return null;
        });

        assertNotNull(result);
        assertTrue(previousGenFound.get());
    }

    public void testConcurrentGenerationRotation() throws Exception {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        // Pre-create gen 1 so deletes can start immediately
        Writer<?> writer1 = createMockWriter(1L);
        deleteEngine.createDeleter(writer1);

        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger successfulDeletes = new AtomicInteger(0);

        // Thread that creates additional generations
        executor.submit(() -> {
            try {
                startLatch.await();
                for (int i = 2; i <= 5; i++) {
                    Writer<?> writer = createMockWriter(i);
                    deleteEngine.createDeleter(writer);
                    Thread.sleep(10);
                }
            } catch (Exception e) {
                // Expected
            }
        });

        // Delete thread — uses gen 1; once gen 1 is checked out, the assert will fire
        executor.submit(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < 10; i++) {
                    try {
                        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc" + i, 1L);
                        DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> null);
                        if (result instanceof DeleteResult.Success) {
                            successfulDeletes.incrementAndGet();
                        }
                    } catch (AssertionError e) {
                        // Expected after gen 1 is checked out
                        break;
                    }
                    Thread.yield();
                }
            } catch (Exception e) {
                // Expected
            }
        });

        // Checkout thread
        executor.submit(() -> {
            try {
                startLatch.await();
                Thread.sleep(15);
                deleteEngine.onWriterCheckedOut(1L);
                deleteEngine.onWriterCheckedOut(2L);
            } catch (Exception e) {
                // Expected
            }
        });

        startLatch.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        // At least some deletes should have succeeded before checkout
        assertTrue(successfulDeletes.get() > 0);
    }

    public void testBufferedDeletesWithGenerationCleanup() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        Writer<?> writer2 = createMockWriter(2L);

        Deleter deleter1 = deleteEngine.createDeleter(writer1);
        Deleter deleter2 = deleteEngine.createDeleter(writer2);

        deleter1.recordBufferedDeletes("gen1_doc1");
        deleter1.recordBufferedDeletes("gen1_doc2");
        deleter2.recordBufferedDeletes("gen2_doc1");

        boolean hasDeletes1 = deleteEngine.onWriterCheckedOut(1L);
        assertTrue(hasDeletes1);
        assertFalse(deleter1.isActive());

        verify(mockParentWriter, times(2)).deleteDocuments(any(Term.class));

        assertTrue(deleter2.isActive());

        boolean hasDeletes2 = deleteEngine.onWriterCheckedOut(2L);
        assertTrue(hasDeletes2);
        assertFalse(deleter2.isActive());

        verify(mockParentWriter, times(3)).deleteDocuments(any(Term.class));
    }

    // ===== Analogous to CompositeIndexWriter update/delete patterns =====

    public void testDeleteDocumentInParentWriterEquivalent() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        Deleter deleter1 = deleteEngine.createDeleter(writer1);

        deleteEngine.recordWrite("test", 1L);
        deleter1.recordBufferedDeletes("test");

        deleteEngine.onWriterCheckedOut(1L);

        Writer<?> writer2 = createMockWriter(2L);
        deleteEngine.createDeleter(writer2);
        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "test", 2L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> null);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);
        verify(mockParentWriter, times(1)).deleteDocuments(any(Term.class));
    }

    public void testDeleteDocumentInChildWriterEquivalent() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        deleteEngine.createDeleter(writer1);

        deleteEngine.recordWrite("test", 1L);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "test", 1L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> null);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);
    }

    public void testDeleteDocumentInBothChildAndParentWriterEquivalent() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        deleteEngine.createDeleter(writer1);
        deleteEngine.recordWrite("test", 1L);

        deleteEngine.onWriterCheckedOut(1L);

        Writer<?> writer2 = createMockWriter(2L);
        deleteEngine.createDeleter(writer2);
        deleteEngine.recordWrite("test", 2L);

        Writer<?> writer3 = createMockWriter(3L);
        deleteEngine.createDeleter(writer3);
        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "test", 3L);
        AtomicBoolean gen2Locked = new AtomicBoolean(false);

        DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> {
            if (gen == 2L) {
                gen2Locked.set(true);
                return mock(Closeable.class);
            }
            return null;
        });

        assertNotNull(result);
        assertTrue(gen2Locked.get());
    }

    public void testDeleteDocumentInOldChildWriterEquivalent() throws Exception {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        deleteEngine.createDeleter(writer1);
        deleteEngine.recordWrite("test", 1L);

        Writer<?> writer2 = createMockWriter(2L);
        deleteEngine.createDeleter(writer2);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "test", 2L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> {
            if (gen == 1L) {
                return mock(Closeable.class);
            }
            return null;
        });

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);

        deleteEngine.onWriterCheckedOut(1L);
    }

    public void testUpdateDocumentEquivalent() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        deleteEngine.createDeleter(writer1);
        deleteEngine.recordWrite("test", 1L);

        deleteEngine.onWriterCheckedOut(1L);

        Writer<?> writer2 = createMockWriter(2L);
        deleteEngine.createDeleter(writer2);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "test", 2L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> null);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);

        deleteEngine.recordWrite("test", 2L);
    }

    public void testConcurrentIndexAndDeleteDuringRefreshEquivalent() throws Exception {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        Deleter deleter = deleteEngine.createDeleter(mockWriter);

        int numDocs = scaledRandomIntBetween(50, 200);
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CyclicBarrier barrier = new CyclicBarrier(3);
        AtomicBoolean done = new AtomicBoolean(false);
        AtomicInteger indexed = new AtomicInteger(0);
        AtomicInteger deleted = new AtomicInteger(0);

        executor.submit(() -> {
            try {
                barrier.await();
                for (int i = 0; i < numDocs && !done.get(); i++) {
                    deleteEngine.recordWrite("doc" + i, 1L);
                    indexed.incrementAndGet();
                    Thread.yield();
                }
            } catch (Exception e) {
                // Expected
            }
        });

        executor.submit(() -> {
            try {
                barrier.await();
                for (int i = 0; i < numDocs && !done.get(); i++) {
                    try {
                        deleter.recordBufferedDeletes("doc" + i);
                        deleted.incrementAndGet();
                    } catch (IllegalStateException e) {
                        break;
                    }
                    Thread.yield();
                }
            } catch (Exception e) {
                // Expected
            }
        });

        executor.submit(() -> {
            try {
                barrier.await();
                Thread.sleep(50);
                done.set(true);
                deleteEngine.onWriterCheckedOut(1L);
            } catch (Exception e) {
                // Expected
            }
        });

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        assertFalse(deleter.isActive());
        assertTrue(indexed.get() > 0);
        assertTrue(deleted.get() > 0);
    }

    // ===== Stress tests =====

    public void testHighVolumeDeletesAcrossMultipleGenerations() throws Exception {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        int numGenerations = 5;
        int docsPerGeneration = 50;

        for (int gen = 1; gen <= numGenerations; gen++) {
            Writer<?> writer = createMockWriter(gen);
            Deleter deleter = deleteEngine.createDeleter(writer);
            for (int doc = 0; doc < docsPerGeneration; doc++) {
                deleteEngine.recordWrite("gen" + gen + "_doc" + doc, gen);
                deleter.recordBufferedDeletes("gen" + gen + "_doc" + doc);
            }
        }

        for (int gen = 1; gen <= numGenerations; gen++) {
            boolean hasDeletes = deleteEngine.onWriterCheckedOut(gen);
            assertTrue(hasDeletes);
        }

        verify(mockParentWriter, times(numGenerations * docsPerGeneration)).deleteDocuments(any(Term.class));
    }

    public void testRapidGenerationCreationAndCheckout() throws Exception {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CyclicBarrier barrier = new CyclicBarrier(4);
        AtomicInteger generation = new AtomicInteger(0);
        AtomicReference<Exception> exception = new AtomicReference<>();

        for (int t = 0; t < 3; t++) {
            executor.submit(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < 20; i++) {
                        long gen = generation.incrementAndGet();
                        Writer<?> writer = createMockWriter(gen);
                        Deleter deleter = deleteEngine.createDeleter(writer);
                        deleter.recordBufferedDeletes("doc_gen_" + gen);
                        Thread.yield();
                    }
                } catch (Exception e) {
                    exception.set(e);
                }
            });
        }

        executor.submit(() -> {
            try {
                barrier.await();
                Thread.sleep(20);
                for (int i = 1; i <= 60; i++) {
                    try {
                        deleteEngine.onWriterCheckedOut(i);
                    } catch (Exception e) {
                        // Generation may not exist yet
                    }
                    Thread.yield();
                }
            } catch (Exception e) {
                exception.set(e);
            }
        });

        executor.shutdown();
        assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS));
        assertNull(exception.get());
    }

    public void testDeleteAfterAllGenerationsCheckedOut() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        Deleter deleter1 = deleteEngine.createDeleter(writer1);
        deleter1.recordBufferedDeletes("doc1");
        deleteEngine.onWriterCheckedOut(1L);

        Writer<?> writer2 = createMockWriter(2L);
        deleteEngine.createDeleter(writer2);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "new_doc", 2L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput, gen -> null);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);
    }

    public void testBufferedDeletesNotLostOnConcurrentDeactivation() throws Exception {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        Deleter deleter = deleteEngine.createDeleter(mockWriter);

        int numDeletes = 100;
        Set<String> recordedDeletes = ConcurrentHashMap.newKeySet();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CyclicBarrier barrier = new CyclicBarrier(5);

        for (int t = 0; t < 4; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < numDeletes; i++) {
                        String id = "t" + threadId + "_doc" + i;
                        try {
                            deleter.recordBufferedDeletes(id);
                            recordedDeletes.add(id);
                        } catch (IllegalStateException e) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    // Expected
                }
            });
        }

        barrier.await();
        Thread.sleep(20);
        deleteEngine.onWriterCheckedOut(1L);

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        assertFalse(deleter.isActive());
    }

    public void testGetDataFormat() {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        assertSame(mockDataFormat, deleteEngine.getDataFormat());
    }

    public void testRefreshReturnsNull() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        assertNull(deleteEngine.refresh(null));
    }

    // ===== Integration tests using LockablePool + LuceneWriter (matching DataFormatAwareEngine) =====
    // These replicate the exact DataFormatAwareEngine pattern:
    // Pool: new LockablePool<>(() -> { Writer w = new LuceneWriter(...); deleteEngine.createDeleter(w); return WriterHolder(w); })
    // Update: pool.getAndLock() → deleteEngine.deleteDocument(input, gen -> pool.evaluateAllAndLock(...)) → recordWrite →
    // pool.releaseAndUnlock()
    // Refresh: pool.checkoutAll(w -> deleteEngine.onWriterCheckedOut(w.get().generation()))

    private static class WriterHolder implements Lockable {
        private final Writer<?> writer;
        private final ReentrantLock lock = new ReentrantLock();

        WriterHolder(Writer<?> writer) {
            this.writer = writer;
        }

        Writer<?> get() {
            return writer;
        }

        @Override
        public void lock() {
            lock.lock();
        }

        @Override
        public boolean tryLock() {
            return lock.tryLock();
        }

        @Override
        public void unlock() {
            lock.unlock();
        }
    }

    private LuceneDeleteExecutionEngine createDeleteEngineWithParentWriter(MergeIndexWriter parentWriter) {
        LuceneCommitter committer = mock(LuceneCommitter.class);
        when(committer.getIndexWriter()).thenReturn(parentWriter);
        return new LuceneDeleteExecutionEngine(new LuceneDataFormat(), committer);
    }

    private LockablePool<WriterHolder> buildWriterPool(LuceneDeleteExecutionEngine deleteEngine, LuceneWriter... writers) {
        java.util.Iterator<LuceneWriter> iter = java.util.Arrays.asList(writers).iterator();
        LockablePool<WriterHolder> pool = new LockablePool<>(() -> {
            LuceneWriter w = iter.next();
            deleteEngine.createDeleter(w);
            return new WriterHolder(w);
        }, LinkedList::new, 1);
        for (int i = 0; i < writers.length; i++) {
            WriterHolder h = pool.getAndLock();
            pool.releaseAndUnlock(h);
        }
        return pool;
    }

    /**
     * Full update flow exactly as DataFormatAwareEngine.updateDocs does it:
     *   1. pool.getAndLock() → currentWriter (gen 2)
     *   2. deleteEngine.deleteDocument(DeleteInput(id, currentWriter.gen()),
     *        gen -> pool.evaluateAllAndLock(h -> h.get().generation() == gen))
     *   3. deleteEngine.recordWrite(uid, currentWriter.gen())
     *   4. pool.releaseAndUnlock()
     */
    public void testUpdateFlowWithLockablePool() throws IOException {
        Path baseDir = createTempDir();
        LuceneDataFormat luceneDataFormat = new LuceneDataFormat();

        Path parentDir = baseDir.resolve("parent");
        java.nio.file.Files.createDirectories(parentDir);
        Directory parentDirectory = new MMapDirectory(parentDir);
        MergeIndexWriter parentWriter = new MergeIndexWriter(parentDirectory, new IndexWriterConfig());

        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngineWithParentWriter(parentWriter);

        LuceneWriter writer1 = new LuceneWriter(
            1L,
            0L,
            luceneDataFormat,
            baseDir,
            null,
            Codec.getDefault(),
            null,
            ConcurrentHashMap.newKeySet(),
            new LuceneShardStatsTracker()
        );
        LuceneWriter writer2 = new LuceneWriter(
            2L,
            0L,
            luceneDataFormat,
            baseDir,
            null,
            Codec.getDefault(),
            null,
            ConcurrentHashMap.newKeySet(),
            new LuceneShardStatsTracker()
        );

        LockablePool<WriterHolder> writerPool = buildWriterPool(deleteEngine, writer1, writer2);

        // Index doc1 in gen 1
        LuceneDocumentInput docInput = new LuceneDocumentInput();
        docInput.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
        writer1.addDoc(docInput);
        deleteEngine.recordWrite("doc1", 1L);

        // === updateDocs flow ===
        WriterHolder lockedWriter = writerPool.getAndLock(h -> h.get().generation() == 2L);
        try {
            DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", lockedWriter.get().generation());
            DeleteResult result = deleteEngine.deleteDocument(
                deleteInput,
                generation -> writerPool.evaluateAllAndLock(h -> h.get().generation() == generation)
            );

            assertTrue(result instanceof DeleteResult.Success);
            deleteEngine.recordWrite("doc1", lockedWriter.get().generation());
        } finally {
            writerPool.releaseAndUnlock(lockedWriter);
        }

        writer1.close();
        writer2.close();
        parentWriter.close();
        parentDirectory.close();
    }

    /**
     * Full refresh flow exactly as DataFormatAwareEngine.refresh does it:
     *   pool.checkoutAll(w -> deleteEngine.onWriterCheckedOut(w.get().generation()))
     *
     * Verifies buffered deletes are applied to parentWriter and pool items are removed.
     */
    public void testRefreshFlowWithLockablePool() throws IOException {
        Path baseDir = createTempDir();
        LuceneDataFormat luceneDataFormat = new LuceneDataFormat();

        Path parentDir = baseDir.resolve("parent");
        java.nio.file.Files.createDirectories(parentDir);
        Directory parentDirectory = new MMapDirectory(parentDir);
        MergeIndexWriter parentWriter = new MergeIndexWriter(parentDirectory, new IndexWriterConfig());

        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngineWithParentWriter(parentWriter);

        LuceneWriter writer1 = new LuceneWriter(
            1L,
            0L,
            luceneDataFormat,
            baseDir,
            null,
            Codec.getDefault(),
            null,
            ConcurrentHashMap.newKeySet(),
            new LuceneShardStatsTracker()
        );
        LockablePool<WriterHolder> writerPool = buildWriterPool(deleteEngine, writer1);

        // Index doc1 in gen 1
        LuceneDocumentInput docInput = new LuceneDocumentInput();
        docInput.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
        writer1.addDoc(docInput);
        deleteEngine.recordWrite("doc1", 1L);

        // deleteDocument buffers "doc1" on gen 1's deleter
        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 1L);
        deleteEngine.deleteDocument(deleteInput, generation -> writerPool.evaluateAllAndLock(h -> h.get().generation() == generation));

        // === refresh flow ===
        writerPool.checkoutAll(checkedOutWriter -> {
            try {
                deleteEngine.onWriterCheckedOut(checkedOutWriter.get().generation());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Buffered delete applied to parentWriter
        assertTrue(parentWriter.hasUncommittedChanges());

        // Pool item removed — evaluateAllAndLock returns null
        Closeable lock = writerPool.evaluateAllAndLock(h -> h.get().generation() == 1L);
        assertNull(lock);

        writer1.close();
        parentWriter.close();
        parentDirectory.close();
    }

    /**
     * Concurrent delete and refresh: evaluateAllAndLock holds pool item lock during deleteDoc,
     * preventing checkoutAll from checking out that writer until delete completes.
     *
     * Thread A (indexer): pool.getAndLock(gen2) → deleteDocument(evaluateAllAndLock locks gen1)
     *                     → deleteDoc on gen1 writer → lock released
     * Thread B (refresh): pool.checkoutAll → item.lock() on gen1 → BLOCKS until Thread A done
     *                     → onWriterCheckedOut(gen1)
     */
    public void testConcurrentDeleteAndRefreshWithLockablePool() throws Exception {
        Path baseDir = createTempDir();
        LuceneDataFormat luceneDataFormat = new LuceneDataFormat();

        Path parentDir = baseDir.resolve("parent");
        java.nio.file.Files.createDirectories(parentDir);
        Directory parentDirectory = new MMapDirectory(parentDir);
        MergeIndexWriter parentWriter = new MergeIndexWriter(parentDirectory, new IndexWriterConfig());

        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngineWithParentWriter(parentWriter);

        LuceneWriter writer1 = new LuceneWriter(
            1L,
            0L,
            luceneDataFormat,
            baseDir,
            null,
            Codec.getDefault(),
            null,
            ConcurrentHashMap.newKeySet(),
            new LuceneShardStatsTracker()
        );
        LuceneWriter writer2 = new LuceneWriter(
            2L,
            0L,
            luceneDataFormat,
            baseDir,
            null,
            Codec.getDefault(),
            null,
            ConcurrentHashMap.newKeySet(),
            new LuceneShardStatsTracker()
        );
        LockablePool<WriterHolder> writerPool = buildWriterPool(deleteEngine, writer1, writer2);

        // Index doc in gen 1
        LuceneDocumentInput docInput = new LuceneDocumentInput();
        docInput.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
        writer1.addDoc(docInput);
        deleteEngine.recordWrite("doc1", 1L);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicBoolean deleteCompleted = new AtomicBoolean(false);
        AtomicBoolean refreshCompleted = new AtomicBoolean(false);

        // Thread A (indexer): updateDocs
        executor.submit(() -> {
            try {
                barrier.await();
                WriterHolder locked = writerPool.getAndLock(h -> h.get().generation() == 2L);
                try {
                    DeleteInput di = new DeleteInput(IdFieldMapper.NAME, "doc1", locked.get().generation());
                    deleteEngine.deleteDocument(di, generation -> writerPool.evaluateAllAndLock(h -> h.get().generation() == generation));
                    deleteEngine.recordWrite("doc1", locked.get().generation());
                } finally {
                    writerPool.releaseAndUnlock(locked);
                }
                deleteCompleted.set(true);
            } catch (Exception e) {
                // Unexpected
            }
        });

        // Thread B (refresh): checkoutAll
        executor.submit(() -> {
            try {
                barrier.await();
                Thread.sleep(20); // Let Thread A acquire evaluateAllAndLock first
                writerPool.checkoutAll(checkedOutWriter -> {
                    try {
                        deleteEngine.onWriterCheckedOut(checkedOutWriter.get().generation());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                refreshCompleted.set(true);
            } catch (Exception e) {
                // Unexpected
            }
        });

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        assertTrue(deleteCompleted.get());
        assertTrue(refreshCompleted.get());

        writer1.close();
        writer2.close();
        parentWriter.close();
        parentDirectory.close();
    }

    /**
     * Concurrent: multiple indexers updating different docs (different previous gens)
     * while refresh runs. evaluateAllAndLock on different pool items should not block each other.
     */
    public void testConcurrentMultipleUpdatesAndRefreshWithLockablePool() throws Exception {
        Path baseDir = createTempDir();
        LuceneDataFormat luceneDataFormat = new LuceneDataFormat();

        Path parentDir = baseDir.resolve("parent");
        java.nio.file.Files.createDirectories(parentDir);
        Directory parentDirectory = new MMapDirectory(parentDir);
        MergeIndexWriter parentWriter = new MergeIndexWriter(parentDirectory, new IndexWriterConfig());

        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngineWithParentWriter(parentWriter);

        LuceneWriter writer1 = new LuceneWriter(
            1L,
            0L,
            luceneDataFormat,
            baseDir,
            null,
            Codec.getDefault(),
            null,
            ConcurrentHashMap.newKeySet(),
            new LuceneShardStatsTracker()
        );
        LuceneWriter writer2 = new LuceneWriter(
            2L,
            0L,
            luceneDataFormat,
            baseDir,
            null,
            Codec.getDefault(),
            null,
            ConcurrentHashMap.newKeySet(),
            new LuceneShardStatsTracker()
        );
        LuceneWriter writer3 = new LuceneWriter(
            3L,
            0L,
            luceneDataFormat,
            baseDir,
            null,
            Codec.getDefault(),
            null,
            ConcurrentHashMap.newKeySet(),
            new LuceneShardStatsTracker()
        );
        LockablePool<WriterHolder> writerPool = buildWriterPool(deleteEngine, writer1, writer2, writer3);

        // doc_a in gen 1, doc_b in gen 2
        LuceneDocumentInput docA = new LuceneDocumentInput();
        docA.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
        writer1.addDoc(docA);
        deleteEngine.recordWrite("doc_a", 1L);

        LuceneDocumentInput docB = new LuceneDocumentInput();
        docB.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
        writer2.addDoc(docB);
        deleteEngine.recordWrite("doc_b", 2L);

        ExecutorService executor = Executors.newFixedThreadPool(3);
        CyclicBarrier barrier = new CyclicBarrier(3);
        AtomicBoolean t1Done = new AtomicBoolean(false);
        AtomicBoolean t2Done = new AtomicBoolean(false);
        AtomicBoolean refreshDone = new AtomicBoolean(false);

        // Thread 1: update doc_a from gen 3 (previous gen = 1)
        executor.submit(() -> {
            try {
                barrier.await();
                WriterHolder locked = writerPool.getAndLock(h -> h.get().generation() == 3L);
                try {
                    DeleteInput di = new DeleteInput(IdFieldMapper.NAME, "doc_a", locked.get().generation());
                    deleteEngine.deleteDocument(di, gen -> writerPool.evaluateAllAndLock(h -> h.get().generation() == gen));
                    deleteEngine.recordWrite("doc_a", locked.get().generation());
                } finally {
                    writerPool.releaseAndUnlock(locked);
                }
                t1Done.set(true);
            } catch (Exception e) {
                // Unexpected
            }
        });

        // Thread 2: update doc_b from gen 3 (previous gen = 2)
        executor.submit(() -> {
            try {
                barrier.await();
                WriterHolder locked = writerPool.getAndLock(h -> h.get().generation() == 3L);
                try {
                    DeleteInput di = new DeleteInput(IdFieldMapper.NAME, "doc_b", locked.get().generation());
                    deleteEngine.deleteDocument(di, gen -> writerPool.evaluateAllAndLock(h -> h.get().generation() == gen));
                    deleteEngine.recordWrite("doc_b", locked.get().generation());
                } finally {
                    writerPool.releaseAndUnlock(locked);
                }
                t2Done.set(true);
            } catch (Exception e) {
                // Unexpected
            }
        });

        // Thread 3 (refresh): checkoutAll after short delay
        executor.submit(() -> {
            try {
                barrier.await();
                Thread.sleep(30);
                writerPool.checkoutAll(checkedOutWriter -> {
                    try {
                        deleteEngine.onWriterCheckedOut(checkedOutWriter.get().generation());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                refreshDone.set(true);
            } catch (Exception e) {
                // Unexpected
            }
        });

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        assertTrue(t1Done.get());
        assertTrue(t2Done.get());
        assertTrue(refreshDone.get());

        writer1.close();
        writer2.close();
        writer3.close();
        parentWriter.close();
        parentDirectory.close();
    }

    /**
     * Multiple sequential updates then refresh. Verifies all buffered deletes from
     * intermediate updates are correctly applied to parentWriter during checkout.
     */
    public void testMultipleSequentialUpdatesThenRefreshWithLockablePool() throws IOException {
        Path baseDir = createTempDir();
        LuceneDataFormat luceneDataFormat = new LuceneDataFormat();

        Path parentDir = baseDir.resolve("parent");
        java.nio.file.Files.createDirectories(parentDir);
        Directory parentDirectory = new MMapDirectory(parentDir);
        MergeIndexWriter parentWriter = new MergeIndexWriter(parentDirectory, new IndexWriterConfig());

        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngineWithParentWriter(parentWriter);

        LuceneWriter writer1 = new LuceneWriter(
            1L,
            0L,
            luceneDataFormat,
            baseDir,
            null,
            Codec.getDefault(),
            null,
            ConcurrentHashMap.newKeySet(),
            new LuceneShardStatsTracker()
        );
        LuceneWriter writer2 = new LuceneWriter(
            2L,
            0L,
            luceneDataFormat,
            baseDir,
            null,
            Codec.getDefault(),
            null,
            ConcurrentHashMap.newKeySet(),
            new LuceneShardStatsTracker()
        );
        LuceneWriter writer3 = new LuceneWriter(
            3L,
            0L,
            luceneDataFormat,
            baseDir,
            null,
            Codec.getDefault(),
            null,
            ConcurrentHashMap.newKeySet(),
            new LuceneShardStatsTracker()
        );
        LockablePool<WriterHolder> writerPool = buildWriterPool(deleteEngine, writer1, writer2, writer3);

        // Initial write in gen 1
        LuceneDocumentInput docInput = new LuceneDocumentInput();
        docInput.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
        writer1.addDoc(docInput);
        deleteEngine.recordWrite("doc1", 1L);

        // Update 1: gen 2 updates doc1 (evaluateAllAndLock locks gen 1)
        WriterHolder locked2 = writerPool.getAndLock(h -> h.get().generation() == 2L);
        try {
            DeleteInput d1 = new DeleteInput(IdFieldMapper.NAME, "doc1", locked2.get().generation());
            DeleteResult r1 = deleteEngine.deleteDocument(d1, gen -> writerPool.evaluateAllAndLock(h -> h.get().generation() == gen));
            assertTrue(r1 instanceof DeleteResult.Success);
            deleteEngine.recordWrite("doc1", locked2.get().generation());
        } finally {
            writerPool.releaseAndUnlock(locked2);
        }

        // Update 2: gen 3 updates doc1 (evaluateAllAndLock locks gen 2)
        WriterHolder locked3 = writerPool.getAndLock(h -> h.get().generation() == 3L);
        try {
            DeleteInput d2 = new DeleteInput(IdFieldMapper.NAME, "doc1", locked3.get().generation());
            DeleteResult r2 = deleteEngine.deleteDocument(d2, gen -> writerPool.evaluateAllAndLock(h -> h.get().generation() == gen));
            assertTrue(r2 instanceof DeleteResult.Success);
            deleteEngine.recordWrite("doc1", locked3.get().generation());
        } finally {
            writerPool.releaseAndUnlock(locked3);
        }

        // Refresh: checkout all and apply buffered deletes
        writerPool.checkoutAll(checkedOutWriter -> {
            try {
                deleteEngine.onWriterCheckedOut(checkedOutWriter.get().generation());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // parentWriter should have uncommitted changes from applied buffered deletes
        assertTrue(parentWriter.hasUncommittedChanges());

        writer1.close();
        writer2.close();
        writer3.close();
        parentWriter.close();
        parentDirectory.close();
    }

    /**
     * Concurrent: two indexer threads each hold their own current writer (gen 2, gen 3),
     * both updating the same doc that was written in gen 1. Both call evaluateAllAndLock
     * on gen 1 — the pool serializes access to gen 1's lock. Both succeed sequentially.
     */
    public void testConcurrentSameDocUpdatesWithLockablePool() throws Exception {
        Path baseDir = createTempDir();
        LuceneDataFormat luceneDataFormat = new LuceneDataFormat();

        Path parentDir = baseDir.resolve("parent");
        java.nio.file.Files.createDirectories(parentDir);
        Directory parentDirectory = new MMapDirectory(parentDir);
        MergeIndexWriter parentWriter = new MergeIndexWriter(parentDirectory, new IndexWriterConfig());

        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngineWithParentWriter(parentWriter);

        LuceneWriter writer1 = new LuceneWriter(
            1L,
            0L,
            luceneDataFormat,
            baseDir,
            null,
            Codec.getDefault(),
            null,
            ConcurrentHashMap.newKeySet(),
            new LuceneShardStatsTracker()
        );
        LuceneWriter writer2 = new LuceneWriter(
            2L,
            0L,
            luceneDataFormat,
            baseDir,
            null,
            Codec.getDefault(),
            null,
            ConcurrentHashMap.newKeySet(),
            new LuceneShardStatsTracker()
        );
        LuceneWriter writer3 = new LuceneWriter(
            3L,
            0L,
            luceneDataFormat,
            baseDir,
            null,
            Codec.getDefault(),
            null,
            ConcurrentHashMap.newKeySet(),
            new LuceneShardStatsTracker()
        );
        LockablePool<WriterHolder> writerPool = buildWriterPool(deleteEngine, writer1, writer2, writer3);

        // Index doc in gen 1
        LuceneDocumentInput docInput = new LuceneDocumentInput();
        docInput.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
        writer1.addDoc(docInput);
        deleteEngine.recordWrite("doc1", 1L);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicInteger successCount = new AtomicInteger(0);

        // Thread 1: holds gen 2, deletes doc1 from gen 1 via evaluateAllAndLock
        executor.submit(() -> {
            try {
                barrier.await();
                WriterHolder locked = writerPool.getAndLock(h -> h.get().generation() == 2L);
                try {
                    DeleteInput di = new DeleteInput(IdFieldMapper.NAME, "doc1", locked.get().generation());
                    DeleteResult r = deleteEngine.deleteDocument(
                        di,
                        gen -> writerPool.evaluateAllAndLock(h -> h.get().generation() == gen)
                    );
                    if (r instanceof DeleteResult.Success) {
                        successCount.incrementAndGet();
                    }
                } finally {
                    writerPool.releaseAndUnlock(locked);
                }
            } catch (Exception e) {
                // Unexpected
            }
        });

        // Thread 2: holds gen 3, also deletes doc1 from gen 1 via evaluateAllAndLock
        executor.submit(() -> {
            try {
                barrier.await();
                WriterHolder locked = writerPool.getAndLock(h -> h.get().generation() == 3L);
                try {
                    DeleteInput di = new DeleteInput(IdFieldMapper.NAME, "doc1", locked.get().generation());
                    DeleteResult r = deleteEngine.deleteDocument(
                        di,
                        gen -> writerPool.evaluateAllAndLock(h -> h.get().generation() == gen)
                    );
                    if (r instanceof DeleteResult.Success) {
                        successCount.incrementAndGet();
                    }
                } finally {
                    writerPool.releaseAndUnlock(locked);
                }
            } catch (Exception e) {
                // Unexpected
            }
        });

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(2, successCount.get());

        writer1.close();
        writer2.close();
        writer3.close();
        parentWriter.close();
        parentDirectory.close();
    }

    // ===== Helper methods =====

    private Writer<?> createMockWriter(long generation) {
        Writer<?> mockWriter = mock(Writer.class);
        LuceneWriter mockLuceneWriter = mock(LuceneWriter.class);

        when(mockWriter.generation()).thenReturn(generation);
        when(mockWriter.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME)).thenReturn(Optional.of(mockLuceneWriter));
        when(mockLuceneWriter.generation()).thenReturn(generation);

        try {
            when(mockLuceneWriter.deleteDocument(any(DeleteInput.class))).thenReturn(new DeleteResult.Success(1L, 1L, 1L));
        } catch (IOException e) {
            // Won't happen in mock
        }

        return mockWriter;
    }

    private Writer<?> createMockWriterWithDeleteFailure(long generation, IOException failure) {
        Writer<?> mockWriter = mock(Writer.class);
        LuceneWriter mockLuceneWriter = mock(LuceneWriter.class);

        when(mockWriter.generation()).thenReturn(generation);
        when(mockWriter.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME)).thenReturn(Optional.of(mockLuceneWriter));
        when(mockLuceneWriter.generation()).thenReturn(generation);

        try {
            when(mockLuceneWriter.deleteDocument(any(DeleteInput.class))).thenThrow(failure);
        } catch (IOException e) {
            // Won't happen in mock
        }

        return mockWriter;
    }
}
