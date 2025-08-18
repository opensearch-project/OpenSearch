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
import org.apache.lucene.store.AlreadyClosedException;
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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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

    public void testUnableToObtainLockOnActiveLookupWhenWriteLockDuringIndexing() throws IOException, InterruptedException {
        CompositeIndexWriter.LiveIndexWriterDeletesMap map = new CompositeIndexWriter.LiveIndexWriterDeletesMap();
        CountDownLatch writeLockAcquiredLatch = new CountDownLatch(1);
        CountDownLatch releaseWriteLockLatch = new CountDownLatch(1);
        Thread writer = new Thread(() -> {
            try (ReleasableLock ignore = map.acquireCurrentWriteLock()) {
                writeLockAcquiredLatch.countDown();
                releaseWriteLockLatch.await();
            } catch (InterruptedException ignored) {

            }
        });

        writer.start();
        writeLockAcquiredLatch.await(1, TimeUnit.SECONDS);

        expectThrows(
            LookupMapLockAcquisitionException.class,
            () -> map.computeIndexWriterIfAbsentForCriteria("200", this::createChildWriterFactory, new ShardId("foo", "_na_", 1))
        );
        releaseWriteLockLatch.countDown();
        writer.join();
    }

    public void testConcurrentIndexingDuringRefresh() throws IOException, InterruptedException {
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField
        );
        AtomicBoolean run = new AtomicBoolean(true);
        Thread indexer = new Thread(() -> {
            while (run.get()) {
                String id = Integer.toString(randomIntBetween(1, 100));
                try {
                    Engine.Index operation = indexForDoc(createParsedDoc(id, null));
                    try (Releasable ignore1 = compositeIndexWriter.acquireLock(operation.uid().bytes())) {
                        compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
                    }
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

        try {
            indexer.start();
            refresher.start();
        } finally {
            run.set(false);
            indexer.join();
            refresher.join();
            IOUtils.close(compositeIndexWriter);
        }
    }

    public void testConcurrentIndexAndDeleteDuringRefresh() throws IOException, InterruptedException {
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField
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
                    Engine.Index operation = indexForDoc(createParsedDoc(id, null));
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
            softDeletesField
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
        Engine.Index operation = indexForDoc(createParsedDoc(id, null));
        try (Releasable ignore1 = compositeIndexWriter.acquireLock(operation.uid().bytes())) {
            addDocException.set(new IOException("simulated"));
            expectThrows(IOException.class, () -> compositeIndexWriter.addDocuments(operation.docs(), operation.uid()));
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
