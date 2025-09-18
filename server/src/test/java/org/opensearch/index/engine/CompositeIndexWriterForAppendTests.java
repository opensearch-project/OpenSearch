/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.junit.After;
import org.junit.Before;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.common.CheckedBiFunction;
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.VersionType;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.mockito.Mockito.mock;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public class CompositeIndexWriterForAppendTests extends CriteriaBasedCompositeIndexWriterBaseTests {

    // For refresh
    public void testGetIndexWriterWithRotatingMapAlwaysPutWriterInCurrentMap() throws IOException, InterruptedException {
        AtomicReference<CompositeIndexWriter.LiveIndexWriterDeletesMap> liveIndexWriterDeletesMap =
            new AtomicReference<>(new CompositeIndexWriter.LiveIndexWriterDeletesMap());
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
                    .computeIndexWriterIfAbsentForCriteria("200", this::createChildWriterFactory);
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
        AtomicReference<CompositeIndexWriter.LiveIndexWriterDeletesMap> mapRef = new AtomicReference<>(new CompositeIndexWriter.LiveIndexWriterDeletesMap());
        CheckedBiFunction<String, CompositeIndexWriter.CriteriaBasedIndexWriterLookup, CompositeIndexWriter.DisposableIndexWriter, IOException> supplier =
            (crit, lookup) -> mock(CompositeIndexWriter.DisposableIndexWriter.class);

        // Compute thread
        Thread computeThread = new Thread(() -> {
            while (stopped.get() == false) {
                try {
                    CompositeIndexWriter.LiveIndexWriterDeletesMap currentMap = mapRef.get();
                    currentMap.computeIndexWriterIfAbsentForCriteria("test-criteria", supplier);
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

    public void testConcurrentIndexingDuringRefresh() throws IOException, InterruptedException {
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(config(), createWriter(), this::createChildWriterFactory, softDeletesField);
        AtomicBoolean run = new AtomicBoolean(true);
        Thread indexer = new Thread(() -> {
            while (run.get()) {
                String id = Integer.toString(randomIntBetween(1, 100));
                try {
                    Engine.Index operation = indexForDoc(createParsedDoc(id, null));
                    try(Releasable ignore1 = compositeIndexWriter.acquireLock(operation.uid().bytes())) {
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
                } catch (IOException e) {
                }
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

    public void testTreatDocumentFailureAsFatalErrorOnGroupSpecificIndexWriter() throws IOException {
        AtomicReference<IOException> addDocException = new AtomicReference<>();
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(config(), createWriter(), (criteria, lookup) -> new CompositeIndexWriter.DisposableIndexWriter(new IndexWriter(store.newTempDirectory("temp_" + criteria + "_" + UUID.randomUUID()),
            newIndexWriterConfig()) {
            @Override
            public long addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
                final IOException ex = addDocException.getAndSet(null);
                if (ex != null) {
                    throw ex;
                }
                return super.addDocuments(docs);
            }
        }, lookup), softDeletesField);


        String id = Integer.toString(randomIntBetween(1, 100));
        Engine.Index operation = indexForDoc(createParsedDoc(id, null));
        try(Releasable ignore1 = compositeIndexWriter.acquireLock(operation.uid().bytes())) {
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
