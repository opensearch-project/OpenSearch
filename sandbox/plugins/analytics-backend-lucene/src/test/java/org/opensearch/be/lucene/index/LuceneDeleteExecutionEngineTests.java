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
import static org.mockito.ArgumentMatchers.anyLong;
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

    public void testCreateDeleterWithoutLuceneWriterReturnsNull() {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = mock(Writer.class);
        when(mockWriter.generation()).thenReturn(1L);
        when(mockWriter.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME)).thenReturn(Optional.empty());

        // Parquet-only (no Lucene sub-writer): createDeleter skips and returns null rather than throwing.
        assertNull(deleteEngine.createDeleter(mockWriter));
    }

    public void testDeleteDocumentWithoutDeleterThrows() {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        // No createDeleter() call -> no deleter registered for gen 1 (parquet-only index shape).
        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 1L);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> deleteEngine.deleteDocument(deleteInput));
        assertTrue(exception.getMessage().contains("not supported"));
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

        DeleteResult result = deleteEngine.deleteDocument(deleteInput);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);
    }

    public void testDeleteDocumentWithPreviousGeneration() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        // Setup current generation deleter
        Writer<?> currentWriter = createMockWriter(2L);
        deleteEngine.createDeleter(currentWriter);

        // Setup previous generation with a trackable LuceneWriter (holds the prior version of doc1).
        LuceneWriter prevLuceneWriter = mock(LuceneWriter.class);
        when(prevLuceneWriter.generation()).thenReturn(1L);
        Writer<?> previousWriter = mock(Writer.class);
        when(previousWriter.generation()).thenReturn(1L);
        when(previousWriter.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME)).thenReturn(Optional.of(prevLuceneWriter));
        deleteEngine.createDeleter(previousWriter);

        // Record the prior version of doc1 in generation 1 at insertion rowId 7.
        deleteEngine.recordWrite("doc1", 1L, 7L);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 2L);

        DeleteResult result = deleteEngine.deleteDocument(deleteInput);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);
        // New child-side routing: a positional (liveDocs-only) delete is deferred to gen 1's writer
        // at its own flush, using the recorded insertion rowId — not an in-generation Term delete.
        verify(prevLuceneWriter).recordPositionalDelete(7L);
    }

    /**
     * Update Case 2: the prior copy lives in a DIFFERENT still-active generation P (not the current
     * writer C, not the committed parent). {@code deleteDocument} must defer the positional delete to
     * P's deleter, never C's — distinguishing Case 2 from Case 3 (prior copy in the same writer).
     */
    public void testUpdateCase2DeletesFromDifferentActiveWriter() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();

        // Previous, still-active generation P=1 holds the prior copy of doc1 (trackable child writer).
        LuceneWriter prevGenWriter = mock(LuceneWriter.class);
        when(prevGenWriter.generation()).thenReturn(1L);
        Writer<?> writer1 = mock(Writer.class);
        when(writer1.generation()).thenReturn(1L);
        when(writer1.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME)).thenReturn(Optional.of(prevGenWriter));
        deleteEngine.createDeleter(writer1);

        // Current generation C=2 (the update lands here) — also trackable, so we can prove it is NOT the
        // target of a direct child delete; it only records a buffered delete drained at its own checkout.
        LuceneWriter currentGenWriter = mock(LuceneWriter.class);
        when(currentGenWriter.generation()).thenReturn(2L);
        Writer<?> writer2 = mock(Writer.class);
        when(writer2.generation()).thenReturn(2L);
        when(writer2.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME)).thenReturn(Optional.of(currentGenWriter));
        deleteEngine.createDeleter(writer2);

        // doc1's prior copy lives in the DIFFERENT, still-active generation 1, at insertion rowId 5.
        deleteEngine.recordWrite("doc1", 1L, 5L);

        // Update doc1 from the current generation 2.
        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 2L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);
        // Case 2: the prior copy's positional delete is deferred to the different active generation (P=1)...
        verify(prevGenWriter).recordPositionalDelete(5L);
        // ...and the current generation (C=2) receives no positional delete (Case 2 != Case 3).
        verify(currentGenWriter, never()).recordPositionalDelete(anyLong());
    }

    public void testDeleteDocumentNoRecordedWriteReturnsSuccess() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> mockWriter = createMockWriter(1L);
        deleteEngine.createDeleter(mockWriter);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "nonexistent_doc", 1L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);
    }

    public void testRecordWrite() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();

        // Previous generation (1) with a trackable Lucene child writer so we can verify the direct delete.
        LuceneWriter prevLuceneWriter = mock(LuceneWriter.class);
        when(prevLuceneWriter.generation()).thenReturn(1L);
        Writer<?> previousWriter = mock(Writer.class);
        when(previousWriter.generation()).thenReturn(1L);
        when(previousWriter.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME)).thenReturn(Optional.of(prevLuceneWriter));
        deleteEngine.createDeleter(previousWriter);

        Writer<?> currentWriter = createMockWriter(2L);
        deleteEngine.createDeleter(currentWriter);

        // recordWrite routes doc1 to generation 1 at insertion rowId 3.
        deleteEngine.recordWrite("doc1", 1L, 3L);

        // Deleting from the current generation (2) must resolve doc1 to generation 1 and defer a
        // positional delete to its writer.
        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 2L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);
        verify(prevLuceneWriter).recordPositionalDelete(3L);
    }

    public void testRecordWriteOverwritesPreviousGeneration() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();

        // Generations 1 and 2 both get trackable Lucene child writers.
        LuceneWriter gen1Lucene = mock(LuceneWriter.class);
        when(gen1Lucene.generation()).thenReturn(1L);
        Writer<?> writer1 = mock(Writer.class);
        when(writer1.generation()).thenReturn(1L);
        when(writer1.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME)).thenReturn(Optional.of(gen1Lucene));

        LuceneWriter gen2Lucene = mock(LuceneWriter.class);
        when(gen2Lucene.generation()).thenReturn(2L);
        Writer<?> writer2 = mock(Writer.class);
        when(writer2.generation()).thenReturn(2L);
        when(writer2.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME)).thenReturn(Optional.of(gen2Lucene));

        Writer<?> writer3 = createMockWriter(3L);
        deleteEngine.createDeleter(writer1);
        deleteEngine.createDeleter(writer2);
        deleteEngine.createDeleter(writer3);

        // Recording doc1 twice: the later write (gen 2, rowId 9) must win.
        deleteEngine.recordWrite("doc1", 1L, 1L);
        deleteEngine.recordWrite("doc1", 2L, 9L);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 3L);
        deleteEngine.deleteDocument(deleteInput);

        // The positional delete must be deferred to generation 2's writer (at rowId 9), never gen 1's.
        verify(gen2Lucene).recordPositionalDelete(9L);
        verify(gen1Lucene, never()).recordPositionalDelete(anyLong());
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

        // Generation 1 (will be checked out) and generation 2 (stays active), both with trackable child writers.
        LuceneWriter gen1Lucene = mock(LuceneWriter.class);
        when(gen1Lucene.generation()).thenReturn(1L);
        Writer<?> writer1 = mock(Writer.class);
        when(writer1.generation()).thenReturn(1L);
        when(writer1.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME)).thenReturn(Optional.of(gen1Lucene));

        LuceneWriter gen2Lucene = mock(LuceneWriter.class);
        when(gen2Lucene.generation()).thenReturn(2L);
        Writer<?> writer2 = mock(Writer.class);
        when(writer2.generation()).thenReturn(2L);
        when(writer2.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME)).thenReturn(Optional.of(gen2Lucene));

        deleteEngine.createDeleter(writer1);
        deleteEngine.createDeleter(writer2);

        deleteEngine.recordWrite("doc1", 1L, 0L);
        deleteEngine.recordWrite("doc2", 1L, 1L);
        deleteEngine.recordWrite("doc3", 2L, 4L);

        // Checking out generation 1 drops its idToGen entries (doc1, doc2) but leaves doc3 -> gen 2 intact.
        deleteEngine.onWriterCheckedOut(1L);

        Writer<?> writer3 = createMockWriter(3L);
        deleteEngine.createDeleter(writer3);

        // doc1 was routed to the retired gen 1: its entry is gone, so no positional delete on gen 1's writer.
        deleteEngine.deleteDocument(new DeleteInput(IdFieldMapper.NAME, "doc1", 3L));
        verify(gen1Lucene, never()).recordPositionalDelete(anyLong());

        // doc3 was routed to the still-active gen 2: its entry survived, so its positional delete lands there.
        DeleteInput deleteDoc3 = new DeleteInput(IdFieldMapper.NAME, "doc3", 3L);
        deleteEngine.deleteDocument(deleteDoc3);
        verify(gen2Lucene).recordPositionalDelete(4L);
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
                    DeleteResult result = deleteEngine.deleteDocument(deleteInput);
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
                    deleteEngine.recordWrite("doc" + i, 1L, -1L);
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
                    deleteEngine.deleteDocument(deleteInput);
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
                deleteEngine.deleteDocument(deleteInput);
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

        deleteEngine.recordWrite("shared_doc", 1L, -1L);

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
                    DeleteResult result = deleteEngine.deleteDocument(deleteInput);
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
                    deleteEngine.recordWrite("doc" + i, 1L, -1L);
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
                        deleteEngine.deleteDocument(deleteInput);
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

        deleteEngine.recordWrite("doc1", 1L, -1L);

        deleter.recordBufferedDeletes("doc2");

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 1L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput);

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

        deleteEngine.recordWrite("doc1", 1L, -1L);

        deleter1.recordBufferedDeletes("buffered1");
        deleter2.recordBufferedDeletes("buffered2");

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 2L);

        DeleteResult result = deleteEngine.deleteDocument(deleteInput);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);

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
                    DeleteResult result = deleteEngine.deleteDocument(deleteInput);
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

        deleteEngine.recordWrite("old_doc", 1L, -1L);
        oldDeleter.recordBufferedDeletes("old_buffered");

        deleteEngine.onWriterCheckedOut(1L);
        assertFalse(oldDeleter.isActive());

        Writer<?> newWriter = createMockWriter(2L);
        Deleter newDeleter = deleteEngine.createDeleter(newWriter);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "new_doc", 2L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput);

        assertNotNull(result);
        verify(mockParentWriter, times(1)).deleteDocuments(any(Term.class));
    }

    public void testDeleteWithDocumentInBothGenerations() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        String docId = "shared_doc";

        Writer<?> writer1 = createMockWriter(1L);
        deleteEngine.createDeleter(writer1);
        deleteEngine.recordWrite(docId, 1L, -1L);

        Writer<?> writer2 = createMockWriter(2L);
        deleteEngine.createDeleter(writer2);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, docId, 2L);

        DeleteResult result = deleteEngine.deleteDocument(deleteInput);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);
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
                        DeleteResult result = deleteEngine.deleteDocument(deleteInput);
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

        deleteEngine.recordWrite("test", 1L, -1L);
        deleter1.recordBufferedDeletes("test");

        deleteEngine.onWriterCheckedOut(1L);

        Writer<?> writer2 = createMockWriter(2L);
        deleteEngine.createDeleter(writer2);
        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "test", 2L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);
        verify(mockParentWriter, times(1)).deleteDocuments(any(Term.class));
    }

    public void testDeleteDocumentInChildWriterEquivalent() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        deleteEngine.createDeleter(writer1);

        deleteEngine.recordWrite("test", 1L, -1L);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "test", 1L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);
    }

    public void testDeleteDocumentInBothChildAndParentWriterEquivalent() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        deleteEngine.createDeleter(writer1);
        deleteEngine.recordWrite("test", 1L, -1L);

        deleteEngine.onWriterCheckedOut(1L);

        Writer<?> writer2 = createMockWriter(2L);
        deleteEngine.createDeleter(writer2);
        deleteEngine.recordWrite("test", 2L, -1L);

        Writer<?> writer3 = createMockWriter(3L);
        deleteEngine.createDeleter(writer3);
        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "test", 3L);

        DeleteResult result = deleteEngine.deleteDocument(deleteInput);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);
    }

    public void testDeleteDocumentInOldChildWriterEquivalent() throws Exception {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        deleteEngine.createDeleter(writer1);
        deleteEngine.recordWrite("test", 1L, -1L);

        Writer<?> writer2 = createMockWriter(2L);
        deleteEngine.createDeleter(writer2);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "test", 2L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);

        deleteEngine.onWriterCheckedOut(1L);
    }

    public void testUpdateDocumentEquivalent() throws IOException {
        LuceneDeleteExecutionEngine deleteEngine = createDeleteEngine();
        Writer<?> writer1 = createMockWriter(1L);
        deleteEngine.createDeleter(writer1);
        deleteEngine.recordWrite("test", 1L, -1L);

        deleteEngine.onWriterCheckedOut(1L);

        Writer<?> writer2 = createMockWriter(2L);
        deleteEngine.createDeleter(writer2);

        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "test", 2L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput);

        assertNotNull(result);
        assertTrue(result instanceof DeleteResult.Success);

        deleteEngine.recordWrite("test", 2L, -1L);
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
                    deleteEngine.recordWrite("doc" + i, 1L, -1L);
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
                deleteEngine.recordWrite("gen" + gen + "_doc" + doc, gen, -1L);
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
        DeleteResult result = deleteEngine.deleteDocument(deleteInput);

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
    // Update: pool.getAndLock() → deleteEngine.deleteDocument(input) → recordWrite → pool.releaseAndUnlock()
    // Refresh: pool.checkoutAll(w -> deleteEngine.onWriterCheckedOut(w.get().generation()))
    //
    // Coordination note (post writerByGenSupplier removal): the update holds the CURRENT writer's
    // pool lock for its whole duration. Because refresh().checkoutAll() must lock every writer, it
    // cannot retire any generation while an update is in flight, so a previous generation resolved
    // by lookupGen is guaranteed still active. deleteDocument therefore deletes the prior copy
    // directly via that generation's own deleter lock — it never takes a second writer-holder lock
    // (the source of the old AB-BA deadlock against checkoutAll).

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
     *   2. deleteEngine.deleteDocument(DeleteInput(id, currentWriter.gen()))
     *        (previous generation resolved internally via lookupGen; deleted via its own deleter lock)
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
        deleteEngine.recordWrite("doc1", 1L, -1L);

        // === updateDocs flow ===
        WriterHolder lockedWriter = writerPool.getAndLock(h -> h.get().generation() == 2L);
        try {
            DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", lockedWriter.get().generation());
            DeleteResult result = deleteEngine.deleteDocument(deleteInput);

            assertTrue(result instanceof DeleteResult.Success);
            deleteEngine.recordWrite("doc1", lockedWriter.get().generation(), -1L);
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
        deleteEngine.recordWrite("doc1", 1L, -1L);

        // deleteDocument buffers "doc1" on gen 1's deleter
        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, "doc1", 1L);
        deleteEngine.deleteDocument(deleteInput);

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
     * Concurrent update and refresh under the new (supplier-free) coordination.
     *
     * Thread A (indexer): pool.getAndLock(gen2) holds the CURRENT writer for the whole update, then
     *                     deleteDocument(gen2) deletes the prior copy directly from gen1 via gen1's
     *                     own deleter lock — no pool lock on gen1.
     * Thread B (refresh): pool.checkoutAll must lock every writer, so it BLOCKS on gen2 (held by A)
     *                     until A releases; it never contends for a second pool lock against A, so
     *                     there is no AB-BA deadlock. Both threads complete.
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
        deleteEngine.recordWrite("doc1", 1L, -1L);

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
                    deleteEngine.deleteDocument(di);
                    deleteEngine.recordWrite("doc1", locked.get().generation(), -1L);
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
                Thread.sleep(20); // Let Thread A acquire the current writer (gen2) and start its update first
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
     * DEADLOCK REGRESSION GUARD — documents exactly why the {@code writerByGenSupplier} parameter was
     * removed from {@code deleteDocument}.
     *
     * <p>The original design had {@code deleteDocument(input, writerByGenSupplier)} block-lock the
     * PREVIOUS generation's writer holder via {@code pool.evaluateAllAndLock(gen == prev)} while the
     * caller already held the CURRENT writer via {@code pool.getAndLock}. A concurrent
     * {@code refresh().checkoutAll()} block-locks EVERY holder in nondeterministic order, producing a
     * classic AB-BA deadlock:
     * <pre>
     *   updater : holds current(gen2) -&gt; waits for prev(gen1)     // evaluateAllAndLock
     *   refresh : holds prev(gen1)    -&gt; waits for current(gen2)   // checkoutAll
     * </pre>
     * Under that design this test hangs and {@code refresh.get(...)} times out (fails). The current
     * design deletes the prior copy through the previous generation's own deleter read/write lock and
     * NEVER takes a second writer-holder lock, so the update and the refresh can only ever contend on
     * the single current-writer lock — and both complete. If anyone reintroduces the second holder
     * lock, this test deadlocks again. That is the concrete answer to "why did we make this change".
     *
     * <p>The ordering is made deterministic (updater grabs the current writer first on the main thread)
     * so the refresh's {@code checkoutAll} reliably blocks on the held current writer — the precise
     * precondition of the old deadlock — rather than racing the pool empty.
     */
    public void testUpdateHoldingCurrentWriterDoesNotDeadlockAgainstRefresh() throws Exception {
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

        // Prior copy of doc1 lives in generation 1 (the DIFFERENT active generation).
        LuceneDocumentInput docInput = new LuceneDocumentInput();
        docInput.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
        writer1.addDoc(docInput);
        deleteEngine.recordWrite("doc1", 1L, -1L);

        // Updater grabs the CURRENT writer (gen2) FIRST — the exact precondition of the old deadlock.
        WriterHolder current = writerPool.getAndLock(h -> h.get().generation() == 2L);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch refreshStarted = new CountDownLatch(1);
        // Refresh: checkoutAll must lock every writer (gen1 + gen2); it will block on gen2 (held above).
        Future<?> refresh = executor.submit(() -> {
            refreshStarted.countDown();
            writerPool.checkoutAll(checkedOut -> {
                try {
                    deleteEngine.onWriterCheckedOut(checkedOut.get().generation());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        });

        // Let the refresh thread reach checkoutAll and block on the current writer we hold.
        assertTrue("refresh thread did not start", refreshStarted.await(5, TimeUnit.SECONDS));
        Thread.sleep(50);

        // Supersede doc1's prior copy in the DIFFERENT active generation (gen1). Current design deletes
        // via gen1's OWN deleter lock — no second writer-holder lock — so this cannot deadlock against
        // the refresh block-locking gen2. Then release gen2 so checkoutAll can finish.
        deleteEngine.deleteDocument(new DeleteInput(IdFieldMapper.NAME, "doc1", current.get().generation()));
        deleteEngine.recordWrite("doc1", current.get().generation(), -1L);
        writerPool.releaseAndUnlock(current);

        // Under the OLD writerByGenSupplier design this never returns (AB-BA deadlock) and get(...) times
        // out. The current design completes well within the timeout.
        refresh.get(30, TimeUnit.SECONDS);

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        writer1.close();
        writer2.close();
        parentWriter.close();
        parentDirectory.close();
    }

    /**
     * Concurrent: multiple indexers updating different docs (prior copies in different gens)
     * while refresh runs. Each indexer holds a different current writer, so their updates proceed
     * in parallel; each prior copy is deleted directly via its generation's own deleter lock.
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
        deleteEngine.recordWrite("doc_a", 1L, -1L);

        LuceneDocumentInput docB = new LuceneDocumentInput();
        docB.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
        writer2.addDoc(docB);
        deleteEngine.recordWrite("doc_b", 2L, -1L);

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
                    deleteEngine.deleteDocument(di);
                    deleteEngine.recordWrite("doc_a", locked.get().generation(), -1L);
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
                    deleteEngine.deleteDocument(di);
                    deleteEngine.recordWrite("doc_b", locked.get().generation(), -1L);
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
        deleteEngine.recordWrite("doc1", 1L, -1L);

        // Update 1: gen 2 updates doc1 (prior copy in gen 1 deleted directly via gen 1's deleter)
        WriterHolder locked2 = writerPool.getAndLock(h -> h.get().generation() == 2L);
        try {
            DeleteInput d1 = new DeleteInput(IdFieldMapper.NAME, "doc1", locked2.get().generation());
            DeleteResult r1 = deleteEngine.deleteDocument(d1);
            assertTrue(r1 instanceof DeleteResult.Success);
            deleteEngine.recordWrite("doc1", locked2.get().generation(), -1L);
        } finally {
            writerPool.releaseAndUnlock(locked2);
        }

        // Update 2: gen 3 updates doc1 (prior copy in gen 2 deleted directly via gen 2's deleter)
        WriterHolder locked3 = writerPool.getAndLock(h -> h.get().generation() == 3L);
        try {
            DeleteInput d2 = new DeleteInput(IdFieldMapper.NAME, "doc1", locked3.get().generation());
            DeleteResult r2 = deleteEngine.deleteDocument(d2);
            assertTrue(r2 instanceof DeleteResult.Success);
            deleteEngine.recordWrite("doc1", locked3.get().generation(), -1L);
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
     * both updating the same doc that was written in gen 1. Each deletes the prior copy directly
     * from gen 1 via that generation's deleter (its read lock permits concurrent deleteDoc), with
     * no pool lock on gen 1. Both succeed.
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
        deleteEngine.recordWrite("doc1", 1L, -1L);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicInteger successCount = new AtomicInteger(0);

        // Thread 1: holds gen 2, deletes doc1 from gen 1 directly via gen 1's deleter
        executor.submit(() -> {
            try {
                barrier.await();
                WriterHolder locked = writerPool.getAndLock(h -> h.get().generation() == 2L);
                try {
                    DeleteInput di = new DeleteInput(IdFieldMapper.NAME, "doc1", locked.get().generation());
                    DeleteResult r = deleteEngine.deleteDocument(di);
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

        // Thread 2: holds gen 3, also deletes doc1 from gen 1 directly via gen 1's deleter
        executor.submit(() -> {
            try {
                barrier.await();
                WriterHolder locked = writerPool.getAndLock(h -> h.get().generation() == 3L);
                try {
                    DeleteInput di = new DeleteInput(IdFieldMapper.NAME, "doc1", locked.get().generation());
                    DeleteResult r = deleteEngine.deleteDocument(di);
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

}
