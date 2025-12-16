/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.CheckedBiFunction;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.BucketedCompositeDirectory;
import org.opensearch.index.VersionType;
import org.opensearch.index.mapper.ParsedDocument;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.mockito.Mockito.mock;

public class CompositeIndexWriterForAppendTests extends CriteriaBasedCompositeIndexWriterBaseTests {

    // For refresh
    public void testGetIndexWriterWithRotatingMapAlwaysPutWriterInCurrentMap() throws IOException, InterruptedException {
        AtomicReference<CompositeIndexWriter.LiveIndexWriterDeletesMap> liveIndexWriterDeletesMap = new AtomicReference<>(
            new CompositeIndexWriter.LiveIndexWriterDeletesMap()
        );
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean run = new AtomicBoolean(true);
        Thread refresher = new Thread(() -> {
            while (run.get()) {
                latch.countDown();
                liveIndexWriterDeletesMap.set(liveIndexWriterDeletesMap.get().buildTransitionMap());
                liveIndexWriterDeletesMap.set(liveIndexWriterDeletesMap.get().invalidateOldMap());
            }
        });

        refresher.start();
        try {
            latch.await();
            int numOps = 100;
            CompositeIndexWriter.DisposableIndexWriter disposableIndexWriter;
            while (numOps > 0) {
                disposableIndexWriter = liveIndexWriterDeletesMap.get()
                    .computeIndexWriterIfAbsentForCriteria("200", this::createChildWriterFactory, new ShardId("foo", "_na_", 1));
                assertNotNull(disposableIndexWriter);
                assertFalse(disposableIndexWriter.getLookupMap().isClosed());
                disposableIndexWriter.getIndexWriter().close();
                disposableIndexWriter.getIndexWriter().getDirectory().close();
                numOps--;
            }
        } finally {
            run.set(false);
            refresher.join();
        }
    }

    public void testConcurrentBuildTransitionAndInvalidateForIndexWriterDeleteMap() throws InterruptedException {
        CompositeIndexWriter.LiveIndexWriterDeletesMap map = new CompositeIndexWriter.LiveIndexWriterDeletesMap();
        AtomicReference<CompositeIndexWriter.LiveIndexWriterDeletesMap> currentMapRef = new AtomicReference<>(map);
        AtomicBoolean running = new AtomicBoolean(true);
        int iterations = 100;

        // Thread that continuously builds transition maps and invalidates old ones
        Thread refreshThread = new Thread(() -> {
            int count = 0;
            while (running.get() && count < iterations) {
                CompositeIndexWriter.LiveIndexWriterDeletesMap current = currentMapRef.get();
                CompositeIndexWriter.LiveIndexWriterDeletesMap transition = current.buildTransitionMap();
                CompositeIndexWriter.LiveIndexWriterDeletesMap invalidated = transition.invalidateOldMap();
                currentMapRef.set(invalidated);
                count++;
                Thread.yield();
            }
        });

        refreshThread.start();

        // Let it run for a bit
        Thread.sleep(100);
        running.set(false);
        refreshThread.join(1000);

        // Verify the final map is in a valid state
        CompositeIndexWriter.LiveIndexWriterDeletesMap finalMap = currentMapRef.get();
        assertNotNull(finalMap.current);
        assertNotNull(finalMap.old);
    }

    public void testConcurrentComputeIndexWriterWithMapRotation() throws Exception {
        AtomicBoolean stopped = new AtomicBoolean();
        Semaphore indexedDocs = new Semaphore(0);
        AtomicInteger computeCount = new AtomicInteger(0);
        AtomicInteger rotationCount = new AtomicInteger(0);
        AtomicReference<CompositeIndexWriter.LiveIndexWriterDeletesMap> mapRef = new AtomicReference<>(
            new CompositeIndexWriter.LiveIndexWriterDeletesMap()
        );
        CheckedBiFunction<
            String,
            CompositeIndexWriter.CriteriaBasedIndexWriterLookup,
            CompositeIndexWriter.DisposableIndexWriter,
            IOException> supplier = (crit, lookup) -> mock(CompositeIndexWriter.DisposableIndexWriter.class);

        // Compute thread
        Thread computeThread = new Thread(() -> {
            while (stopped.get() == false) {
                try {
                    CompositeIndexWriter.LiveIndexWriterDeletesMap currentMap = mapRef.get();
                    currentMap.computeIndexWriterIfAbsentForCriteria("test-criteria", supplier, new ShardId("foo", "_na_", 1));
                    computeCount.incrementAndGet();
                    indexedDocs.release();
                } catch (Exception e) {

                }
            }
        });

        Thread rotationThread = new Thread(() -> {
            while (stopped.get() == false) {
                CompositeIndexWriter.LiveIndexWriterDeletesMap current = mapRef.get();
                CompositeIndexWriter.LiveIndexWriterDeletesMap transition = current.buildTransitionMap();
                CompositeIndexWriter.LiveIndexWriterDeletesMap invalidated = transition.invalidateOldMap();
                mapRef.set(invalidated);
                rotationCount.incrementAndGet();
            }
        });

        try {
            rotationThread.start();
            computeThread.start();
            indexedDocs.acquire(100);
        } finally {
            stopped.set(true);
            computeThread.join();
            rotationThread.join();
        }

        assertTrue("Compute operations completed: " + computeCount.get(), computeCount.get() >= 100);
        assertTrue("Rotation operations completed: " + rotationCount.get(), rotationCount.get() >= 0);
    }

    public void testChildDirectoryDeletedPostRefresh() throws IOException, InterruptedException {
        final EngineConfig engineConfig = config();
        Path dataPath = engineConfig.getStore().shardPath().resolveIndex();
        CompositeIndexWriter compositeIndexWriter = null;

        try {
            compositeIndexWriter = new CompositeIndexWriter(
                engineConfig,
                createWriter(),
                newSoftDeletesPolicy(),
                softDeletesField,
                indexWriterFactory
            );

            Engine.Index operation = indexForDoc(createParsedDoc("id", null, DEFAULT_CRITERIA));
            compositeIndexWriter.addDocuments(operation.docs(), operation.uid());

            operation = indexForDoc(createParsedDoc("id2", null, "testingNewCriteria"));
            compositeIndexWriter.addDocuments(operation.docs(), operation.uid());

            compositeIndexWriter.beforeRefresh();
            compositeIndexWriter.afterRefresh(true);

            // Remove known extra files - "extra0" file is added by the ExtrasFS, which is part of Lucene's test framework.
            long directoryCount = Files.find(
                dataPath,
                1,
                (path, attributes) -> attributes.isDirectory() == true && path.endsWith("extra0") == false
            ).count() - 1;
            // Ensure no child directory is pending here.
            assertEquals(0, directoryCount);
        } finally {
            if (compositeIndexWriter != null) {
                IOUtils.closeWhileHandlingException(compositeIndexWriter);
            }
        }

    }

    public void testConcurrentIndexingDuringRefresh() throws IOException, InterruptedException {
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        try {
            AtomicBoolean run = new AtomicBoolean(true);
            Thread indexer = new Thread(() -> {
                while (run.get()) {
                    String id = Integer.toString(randomIntBetween(1, 100));
                    try {
                        Engine.Index operation = indexForDoc(createParsedDoc(id, null, DEFAULT_CRITERIA));
                        compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    } catch (AlreadyClosedException e) {
                        return;
                    }
                }
            });

            Thread refresher = new Thread(() -> {
                while (run.get()) {
                    try {
                        compositeIndexWriter.beforeRefresh();
                        compositeIndexWriter.afterRefresh(true);
                    } catch (IOException e) {}
                }
            });
            indexer.start();
            refresher.start();
            run.set(false);
            indexer.join();
            refresher.join();
        } finally {
            IOUtils.close(compositeIndexWriter);
        }
    }

    public void testConcurrentIndexAndDeleteDuringRefresh() throws IOException, InterruptedException {
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        int numDocs = scaledRandomIntBetween(100, 1000);
        CountDownLatch latch = new CountDownLatch(2);
        AtomicBoolean done = new AtomicBoolean(false);
        AtomicInteger numDeletes = new AtomicInteger();

        Thread indexer = new Thread(() -> {
            try {
                latch.countDown();
                latch.await();
                for (int i = 0; i < numDocs; i++) {
                    String id = Integer.toString(i);
                    Engine.Index operation = indexForDoc(createParsedDoc(id, null, DEFAULT_CRITERIA));
                    compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
                    if (rarely()) {
                        compositeIndexWriter.deleteDocument(
                            operation.uid(),
                            false,
                            newDeleteTombstoneDoc(id),
                            1,
                            2,
                            primaryTerm.get(),
                            softDeletesField
                        );

                        numDeletes.incrementAndGet();
                    }
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            } finally {
                done.set(true);
            }
        });

        indexer.start();
        latch.countDown();
        latch.await();
        while (done.get() == false) {
            compositeIndexWriter.beforeRefresh();
            compositeIndexWriter.afterRefresh(true);
            Thread.sleep(100);
        }

        indexer.join();
        compositeIndexWriter.beforeRefresh();
        compositeIndexWriter.afterRefresh(true);
        try (DirectoryReader directoryReader = DirectoryReader.open(compositeIndexWriter.getAccumulatingIndexWriter())) {
            assertEquals(numDocs - numDeletes.get(), directoryReader.numDocs());
        } finally {
            IOUtils.close(compositeIndexWriter);
        }
    }

    public void testTreatDocumentFailureAsFatalErrorOnGroupSpecificIndexWriter() throws IOException {
        AtomicReference<IOException> addDocException = new AtomicReference<>();
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        ) {
            @Override
            DisposableIndexWriter createChildWriterUtil(String associatedCriteria, CriteriaBasedIndexWriterLookup lookup)
                throws IOException {
                return new CompositeIndexWriter.DisposableIndexWriter(
                    new IndexWriter(
                        store.newTempDirectory(
                            BucketedCompositeDirectory.CHILD_DIRECTORY_PREFIX + associatedCriteria + "_" + UUID.randomUUID()
                        ),
                        newIndexWriterConfig()
                    ) {
                        @Override
                        public long addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
                            final IOException ex = addDocException.getAndSet(null);
                            if (ex != null) {
                                throw ex;
                            }
                            return super.addDocuments(docs);
                        }
                    },
                    lookup
                );
            }
        };

        String id = Integer.toString(randomIntBetween(1, 100));
        Engine.Index operation = indexForDoc(createParsedDoc(id, null, DEFAULT_CRITERIA));
        try {
            addDocException.set(new IOException("simulated"));
            expectThrows(IOException.class, () -> compositeIndexWriter.addDocuments(operation.docs(), operation.uid()));
        } finally {
            IOUtils.close(compositeIndexWriter);
        }
    }

    public void testGetFlushingBytesAfterSmallDocuments() throws IOException {
        IndexWriter parentWriter = createWriter();
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            parentWriter,
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        compositeIndexWriter.getConfig().setRAMBufferSizeMB(128);

        try {
            // Add a few small documents
            for (int i = 0; i < 10; i++) {
                Engine.Index operation = indexForDoc(createParsedDoc(String.valueOf(i), null, DEFAULT_CRITERIA));
                compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
            }

            // Should still be 0 or small since we haven't triggered a flush
            long flushingBytes = compositeIndexWriter.getFlushingBytes();
            assertEquals("Flushing bytes should be non-negative", 0, flushingBytes);
        } finally {
            IOUtils.close(compositeIndexWriter);
        }
    }

    public void testHasPendingMergesInitiallyFalse() throws IOException {
        IndexWriter parentWriter = createWriter();
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            parentWriter,
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        assertFalse("Should have no pending merges initially", compositeIndexWriter.hasPendingMerges());
    }

    public void testHasPendingMergesDuringForceMerge() throws IOException, InterruptedException {
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
        mergePolicy.setMergeFactor(2);
        compositeIndexWriter.getConfig().setMergePolicy(mergePolicy);

        try {
            for (int i = 0; i < 4; i++) {
                Engine.Index operation = indexForDoc(createParsedDoc(String.valueOf(i), null, DEFAULT_CRITERIA));
                compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
                compositeIndexWriter.beforeRefresh();
                compositeIndexWriter.afterRefresh(true);
            }

            final CountDownLatch mergeLatch = new CountDownLatch(1);
            final AtomicBoolean hadPendingMerges = new AtomicBoolean(false);

            Thread mergeThread = new Thread(() -> {
                try {
                    mergeLatch.countDown();
                    compositeIndexWriter.forceMerge(1, true);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            mergeThread.start();
            mergeLatch.await();

            // Check for pending merges during merge
            Thread.sleep(50); // Give merge time to start
            mergeThread.join();
            // After merge completes, should have no pending merges
            assertFalse("Should have no pending merges after force merge completes", compositeIndexWriter.hasPendingMerges());
        } finally {
            IOUtils.close(compositeIndexWriter);
        }
    }

    public void testGetTragicExceptionWithException() throws IOException {
        IndexWriter parentWriter = createWriter();
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            parentWriter,
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        compositeIndexWriter.getConfig().setRAMBufferSizeMB(128);

        try {
            // Add a few small documents
            for (int i = 0; i < 10; i++) {
                Engine.Index operation = indexForDoc(createParsedDoc(String.valueOf(i), null, DEFAULT_CRITERIA));
                compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
            }

            assertNull(compositeIndexWriter.getTragicException());
        } finally {
            IOUtils.close(compositeIndexWriter);
        }
    }

    public void testGetTragicExceptionWithOutOfMemoryError() throws Exception {
        AtomicBoolean shouldFail = new AtomicBoolean(false);
        AtomicReference<Error> thrownError = new AtomicReference<>();

        Directory dir = new FilterDirectory(newDirectory()) {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                IndexOutput out = super.createOutput(name, context);
                return new FilterIndexOutput("failing output", "test", out) {
                    @Override
                    public void writeBytes(byte[] b, int offset, int length) throws IOException {
                        if (shouldFail.get() && name.endsWith(".cfe")) {
                            Error ex = new OutOfMemoryError("Simulated write failure");
                            thrownError.set(ex);
                            throw ex;
                        }
                        super.writeBytes(b, offset, length);
                    }
                };
            }
        };

        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(dir),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        compositeIndexWriter.getConfig().setMaxBufferedDocs(2);
        // Add a document successfully
        Engine.Index operation = indexForDoc(createParsedDoc(String.valueOf("-1"), null, DEFAULT_CRITERIA));
        compositeIndexWriter.addDocuments(operation.docs(), operation.uid());

        // Enable failure
        shouldFail.set(true);

        boolean hitError = false;
        try {
            // This should trigger the failure
            for (int i = 0; i < 10; i++) {
                operation = indexForDoc(createParsedDoc(String.valueOf(i), null, DEFAULT_CRITERIA));
                compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
            }

            compositeIndexWriter.beforeRefresh();
            compositeIndexWriter.afterRefresh(true);
            compositeIndexWriter.commit();
        } catch (Error e) {
            hitError = true;
        }

        if (hitError && thrownError.get() != null) {
            Throwable tragic = compositeIndexWriter.getTragicException();
            if (tragic != null) {
                assertFalse("Writer should be closed after tragic exception", compositeIndexWriter.isOpen());
            }
        }

        IOUtils.closeWhileHandlingException(compositeIndexWriter, dir);
    }

    public void testRAMBytesUsedWithOldAndCurrentWriters() throws Exception {
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        try {
            // Create documents in first criteria group
            for (int i = 0; i < 10; i++) {
                Engine.Index operation = indexForDoc(createParsedDoc(String.valueOf(i), null, DEFAULT_CRITERIA));
                compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
            }

            long ramAfterGroup1 = compositeIndexWriter.ramBytesUsed();
            for (int i = 0; i < 10; i++) {
                Engine.Index operation = indexForDoc(createParsedDoc(String.valueOf(i), null, "testGroupingCriteria2"));
                compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
            }

            long ramAfterGroup2 = compositeIndexWriter.ramBytesUsed();
            // RAM should account for both groups
            assertTrue("RAM should account for multiple groups", ramAfterGroup2 >= ramAfterGroup1);
        } finally {
            IOUtils.close(compositeIndexWriter);
        }

    }

    public void testRAMBytesUsedWithTragicExceptionOnCurrent() throws Exception {
        Supplier<Directory> dirSupplier = () -> new FilterDirectory(newDirectory()) {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                IndexOutput out = super.createOutput(name, context);
                return new FilterIndexOutput("failing output", "test", out) {
                    @Override
                    public void writeBytes(byte[] b, int offset, int length) throws IOException {
                        throw new OutOfMemoryError("Simulated write failure");
                    }
                };
            }
        };

        FlushingIndexWriterFactory indexWriterFactory = new FlushingIndexWriterFactory(dirSupplier, new AtomicBoolean(true));

        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        compositeIndexWriter.getConfig().setMaxBufferedDocs(2);

        // Add a document successfully
        Engine.Index operation = indexForDoc(createParsedDoc(String.valueOf("-1"), null, DEFAULT_CRITERIA));
        try {
            compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
        } catch (Error ignored) {}

        assertThrows(AlreadyClosedException.class, compositeIndexWriter::ramBytesUsed);
        IOUtils.closeWhileHandlingException(compositeIndexWriter, indexWriterFactory);
    }

    public void testRAMBytesUsedWithTragicExceptionOnOld() throws Exception {
        Supplier<Directory> dirSupplier = () -> new FilterDirectory(newDirectory()) {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                IndexOutput out = super.createOutput(name, context);
                return new FilterIndexOutput("failing output", "test", out) {
                    @Override
                    public void writeBytes(byte[] b, int offset, int length) throws IOException {
                        throw new OutOfMemoryError("Simulated write failure");
                    }
                };
            }
        };

        FlushingIndexWriterFactory indexWriterFactory = new FlushingIndexWriterFactory(dirSupplier, new AtomicBoolean(true));

        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        compositeIndexWriter.getConfig().setMaxBufferedDocs(2);

        // Add a document successfully
        Engine.Index operation = indexForDoc(createParsedDoc(String.valueOf("-1"), null, DEFAULT_CRITERIA));
        try {
            compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
        } catch (Error ignored) {}

        ReleasableLock lock = compositeIndexWriter.getNewWriteLock().acquire();
        CountDownLatch latch = new CountDownLatch(1);
        Thread refresher = new Thread(() -> {
            latch.countDown();
            try {
                compositeIndexWriter.beforeRefresh();
            } catch (Exception ignored) {}
        });

        refresher.start();
        try {
            latch.await();
            assertThrows(AlreadyClosedException.class, compositeIndexWriter::ramBytesUsed);
        } finally {
            IOUtils.closeWhileHandlingException(compositeIndexWriter, indexWriterFactory, lock);
            refresher.join();
        }
    }

    public void testRAMBytesUsedWithWriterOnOld() throws Exception {
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        compositeIndexWriter.getConfig().setMaxBufferedDocs(2);

        // Add a document successfully
        Engine.Index operation = indexForDoc(createParsedDoc(String.valueOf("-1"), null, DEFAULT_CRITERIA));
        try {
            compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
        } catch (Error ignored) {}

        ReleasableLock lock = compositeIndexWriter.getNewWriteLock().acquire();
        CountDownLatch latch = new CountDownLatch(1);
        Thread refresher = new Thread(() -> {
            latch.countDown();
            try {
                compositeIndexWriter.beforeRefresh();
            } catch (Exception ignored) {}
        });

        refresher.start();
        try {
            latch.await();
            long ramBytesUsed = compositeIndexWriter.ramBytesUsed();
            assertTrue("RamBytesUsed bytes should be non-negative ", ramBytesUsed > 0);
        } finally {
            IOUtils.closeWhileHandlingException(compositeIndexWriter, lock);
            refresher.join();
        }
    }

    public void testFlushingBytesWithTragicExceptionOnCurrent() throws Exception {
        Supplier<Directory> dirSupplier = () -> new FilterDirectory(newDirectory()) {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                IndexOutput out = super.createOutput(name, context);
                return new FilterIndexOutput("failing output", "test", out) {
                    @Override
                    public void writeBytes(byte[] b, int offset, int length) throws IOException {
                        throw new OutOfMemoryError("Simulated write failure");
                    }
                };
            }
        };

        FlushingIndexWriterFactory indexWriterFactory = new FlushingIndexWriterFactory(dirSupplier, new AtomicBoolean(true));

        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        compositeIndexWriter.getConfig().setMaxBufferedDocs(2);

        // Add a document successfully
        Engine.Index operation = indexForDoc(createParsedDoc(String.valueOf("-1"), null, DEFAULT_CRITERIA));
        try {
            compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
        } catch (Error ignored) {}

        assertThrows(AlreadyClosedException.class, compositeIndexWriter::getFlushingBytes);
        IOUtils.closeWhileHandlingException(compositeIndexWriter, indexWriterFactory);
    }

    public void testFlushingBytesUsedWithTragicExceptionOnOld() throws Exception {
        Supplier<Directory> dirSupplier = () -> new FilterDirectory(newDirectory()) {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                IndexOutput out = super.createOutput(name, context);
                return new FilterIndexOutput("failing output", "test", out) {
                    @Override
                    public void writeBytes(byte[] b, int offset, int length) throws IOException {
                        throw new OutOfMemoryError("Simulated write failure");
                    }
                };
            }
        };

        FlushingIndexWriterFactory indexWriterFactory = new FlushingIndexWriterFactory(dirSupplier, new AtomicBoolean(true));

        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        compositeIndexWriter.getConfig().setMaxBufferedDocs(2);

        // Add a document successfully
        Engine.Index operation = indexForDoc(createParsedDoc(String.valueOf("-1"), null, DEFAULT_CRITERIA));
        try {
            compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
        } catch (Error ignored) {}

        ReleasableLock lock = compositeIndexWriter.getNewWriteLock().acquire();
        CountDownLatch latch = new CountDownLatch(1);
        Thread refresher = new Thread(() -> {
            latch.countDown();
            try {
                compositeIndexWriter.beforeRefresh();
            } catch (Exception ignored) {}
        });

        refresher.start();
        try {
            latch.await();
            assertThrows(AlreadyClosedException.class, compositeIndexWriter::getFlushingBytes);
        } finally {
            IOUtils.closeWhileHandlingException(compositeIndexWriter, indexWriterFactory, lock);
            refresher.join();
        }
    }

    public void testFlushingBytesUsedWithWriterOnOld() throws Exception {
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        compositeIndexWriter.getConfig().setMaxBufferedDocs(2);

        // Add a document successfully
        Engine.Index operation = indexForDoc(createParsedDoc(String.valueOf("-1"), null, DEFAULT_CRITERIA));
        try {
            compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
        } catch (Error ignored) {}

        ReleasableLock lock = compositeIndexWriter.getNewWriteLock().acquire();
        CountDownLatch latch = new CountDownLatch(1);
        Thread refresher = new Thread(() -> {
            latch.countDown();
            try {
                compositeIndexWriter.beforeRefresh();
            } catch (Exception ignored) {}
        });

        refresher.start();
        try {
            latch.await();
            long flushingBytes = compositeIndexWriter.getFlushingBytes();
            assertEquals("Flushing bytes should be non-negative", 0, flushingBytes);
        } finally {
            IOUtils.closeWhileHandlingException(compositeIndexWriter, lock);
            refresher.join();
        }
    }

    public void testTragicExceptionGetWithTragicExceptionOnCurrent() throws Exception {
        Supplier<Directory> dirSupplier = () -> new FilterDirectory(newDirectory()) {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                IndexOutput out = super.createOutput(name, context);
                return new FilterIndexOutput("failing output", "test", out) {
                    @Override
                    public void writeBytes(byte[] b, int offset, int length) throws IOException {
                        throw new OutOfMemoryError("Simulated write failure");
                    }
                };
            }
        };

        FlushingIndexWriterFactory indexWriterFactory = new FlushingIndexWriterFactory(dirSupplier, new AtomicBoolean(true));

        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        compositeIndexWriter.getConfig().setMaxBufferedDocs(2);

        // Add a document successfully
        Engine.Index operation = indexForDoc(createParsedDoc(String.valueOf("-1"), null, DEFAULT_CRITERIA));
        try {
            compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
        } catch (Error ignored) {}

        assertNotNull(compositeIndexWriter.getTragicException());
        IOUtils.closeWhileHandlingException(compositeIndexWriter, indexWriterFactory);
    }

    public void testTragicExceptionGetWithTragicExceptionOnOld() throws Exception {
        Supplier<Directory> dirSupplier = () -> new FilterDirectory(newDirectory()) {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                IndexOutput out = super.createOutput(name, context);
                return new FilterIndexOutput("failing output", "test", out) {
                    @Override
                    public void writeBytes(byte[] b, int offset, int length) throws IOException {
                        throw new OutOfMemoryError("Simulated write failure");
                    }
                };
            }
        };

        FlushingIndexWriterFactory indexWriterFactory = new FlushingIndexWriterFactory(dirSupplier, new AtomicBoolean(true));

        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        compositeIndexWriter.getConfig().setMaxBufferedDocs(2);

        // Add a document successfully
        Engine.Index operation = indexForDoc(createParsedDoc(String.valueOf("-1"), null, DEFAULT_CRITERIA));
        try {
            compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
        } catch (Error ignored) {}

        ReleasableLock lock = compositeIndexWriter.getNewWriteLock().acquire();
        CountDownLatch latch = new CountDownLatch(1);
        Thread refresher = new Thread(() -> {
            latch.countDown();
            try {
                compositeIndexWriter.beforeRefresh();
            } catch (Exception ignored) {}
        });

        refresher.start();
        try {
            latch.await();
            assertNotNull(compositeIndexWriter.getTragicException());
        } finally {
            IOUtils.closeWhileHandlingException(compositeIndexWriter, indexWriterFactory, lock);
            refresher.join();
        }
    }

    public void testSetLiveCommitDataWithRollback() throws Exception {
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        try {
            // Create documents in first criteria group
            for (int i = 0; i < 10; i++) {
                Engine.Index operation = indexForDoc(createParsedDoc(String.valueOf(i), null, DEFAULT_CRITERIA));
                compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
            }

            Map<String, String> data = new HashMap<>();
            data.put("status", "beforeCommit");
            compositeIndexWriter.setLiveCommitData(data.entrySet());
            compositeIndexWriter.commit();

            data = new HashMap<>();
            data.put("status", "beforeRollback");
            // Rollback without committing
            compositeIndexWriter.rollback();
            Engine.Index operation = indexForDoc(createParsedDoc(String.valueOf(13), null, "testGroupingCriteria1"));
            expectThrows(AlreadyClosedException.class, () -> compositeIndexWriter.addDocuments(operation.docs(), operation.uid()));

            // Reopen writer
            try (
                CompositeIndexWriter compositeIndexWriterForRollback = new CompositeIndexWriter(
                    config(),
                    createWriter(),
                    newSoftDeletesPolicy(),
                    softDeletesField,
                    indexWriterFactory
                )
            ) {
                for (Map.Entry<String, String> entry : compositeIndexWriterForRollback.getLiveCommitData()) {
                    if (entry.getKey().equals("status")) {
                        assertEquals("beforeCommit", entry.getValue());
                    }
                }
            }

        } finally {
            IOUtils.close(compositeIndexWriter);
        }
    }

    public void testRollbackWithWriterOnOld() throws Exception {
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        compositeIndexWriter.getConfig().setMaxBufferedDocs(2);

        // Add a document successfully
        Engine.Index operation = indexForDoc(createParsedDoc(String.valueOf("-1"), null, DEFAULT_CRITERIA));
        try {
            compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
        } catch (Error ignored) {}

        ReleasableLock lock = compositeIndexWriter.getNewWriteLock().acquire();
        CountDownLatch latch = new CountDownLatch(1);
        Thread refresher = new Thread(() -> {
            latch.countDown();
            try {
                compositeIndexWriter.beforeRefresh();
            } catch (Exception ignored) {}
        });

        refresher.start();
        try {
            latch.await();
            compositeIndexWriter.rollback();
        } catch (Exception ex) {
            fail(ex.getMessage());
        } finally {
            IOUtils.closeWhileHandlingException(compositeIndexWriter, lock);
            refresher.join();
        }
    }

    public void testObtainLock() throws Exception {
        try (
            CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
                config(),
                createWriter(),
                newSoftDeletesPolicy(),
                softDeletesField,
                indexWriterFactory
            )
        ) {
            try (Releasable lock = compositeIndexWriter.obtainWriteLockOnAllMap()) {
                assertTrue(compositeIndexWriter.isWriteLockedByCurrentThread());
            }
        }
    }

    public void testHasBlocksMergeFullyDelSegments() throws Exception {
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        try {
            Engine.Index operation = indexForDoc(createParsedDoc("foo", null, DEFAULT_CRITERIA));
            compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
            compositeIndexWriter.softUpdateDocuments(operation.uid(), operation.docs(), 2, 2, primaryTerm.get(), softDeletesField);
            compositeIndexWriter.beforeRefresh();
            compositeIndexWriter.afterRefresh(true);
            compositeIndexWriter.commit();
            compositeIndexWriter.softUpdateDocuments(operation.uid(), operation.docs(), 2, 2, primaryTerm.get(), softDeletesField);
            compositeIndexWriter.beforeRefresh();
            compositeIndexWriter.afterRefresh(true);
            compositeIndexWriter.forceMergeDeletes(true);
            compositeIndexWriter.commit();
            try (DirectoryReader directoryReader = DirectoryReader.open(compositeIndexWriter.getAccumulatingIndexWriter())) {
                assertEquals(1, directoryReader.leaves().size());
                assertFalse("hasBlocks should be cleared", directoryReader.leaves().get(0).reader().getMetaData().hasBlocks());
            }
        } finally {
            IOUtils.close(compositeIndexWriter);
        }
    }

    public Engine.Index appendOnlyPrimary(ParsedDocument doc, boolean retry, final long autoGeneratedIdTimestamp, boolean create) {
        return new Engine.Index(
            newUid(doc),
            doc,
            UNASSIGNED_SEQ_NO,
            1,
            create ? Versions.MATCH_DELETED : Versions.MATCH_ANY,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            autoGeneratedIdTimestamp,
            retry,
            UNASSIGNED_SEQ_NO,
            0
        );
    }
}
