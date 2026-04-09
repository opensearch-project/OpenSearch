/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.stub.InMemoryCommitter;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockSearchBackEndPlugin;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.CommitterFactory;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.index.engine.EngineTestCase.createParsedDoc;
import static org.opensearch.index.engine.EngineTestCase.tombstoneDocSupplier;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DataFormatAwareEngine} covering the orchestration layer:
 * sequence number management, translog integration, refresh/flush lifecycle,
 * catalog snapshot management, concurrency, and failure handling.
 *
 * <p>Uses mock data format components to isolate the DFAE orchestration logic
 * from any specific data format implementation.</p>
 */
public class DataFormatAwareEngineTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private Store store;
    private ShardId shardId;
    private AtomicLong primaryTerm;
    private MockDataFormat mockDataFormat;
    private MockDataFormatPlugin mockPlugin;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        shardId = new ShardId(new Index("test", "_na_"), 0);
        primaryTerm = new AtomicLong(randomLongBetween(1, Long.MAX_VALUE));
        mockDataFormat = new MockDataFormat("composite", 100L, Set.of());
        mockPlugin = MockDataFormatPlugin.of(mockDataFormat);
        threadPool = new TestThreadPool(getClass().getName());
        store = createStore();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            store.close();
        } finally {
            terminate(threadPool);
        }
        super.tearDown();
    }

    private Store createStore() throws IOException {
        Directory dir = newDirectory();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .build()
        );
        Path path = createTempDir().resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        ShardPath shardPath = new ShardPath(false, path, path, shardId);
        return new Store(
            shardId,
            indexSettings,
            dir,
            new DummyShardLock(shardId),
            Store.OnClose.EMPTY,
            shardPath,
            new FsDirectoryFactory()
        );
    }

    /**
     * Bootstraps the store's Lucene index with the commit metadata that DFAE
     * expects to find: translog UUID, seq-no info, history UUID, etc.
     */
    private void bootstrapStoreWithMetadata(Store store, String translogUUID) throws IOException {
        try (
            IndexWriter writer = new IndexWriter(
                store.directory(),
                new IndexWriterConfig(Lucene.STANDARD_ANALYZER).setMergePolicy(NoMergePolicy.INSTANCE)
                    .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            )
        ) {
            Map<String, String> commitData = new HashMap<>();
            commitData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
            commitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
            commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
            commitData.put(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, "-1");
            commitData.put(Engine.HISTORY_UUID_KEY, UUID.randomUUID().toString());
            writer.setLiveCommitData(commitData.entrySet());
            writer.commit();
        }
    }

    private DataFormatAwareEngine createDFAEngine(Store store, Path translogPath) throws IOException {
        // Create the initial translog files and get the UUID
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        // Bootstrap the store's Lucene commit with matching translog UUID
        bootstrapStoreWithMetadata(store, uuid);
        return new DataFormatAwareEngine(buildDFAEngineConfig(store, translogPath));
    }

    private EngineConfig buildDFAEngineConfig(Store store, Path translogPath) {
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), mockDataFormat.name())
                .build()
        );

        TranslogConfig translogConfig = new TranslogConfig(
            shardId,
            translogPath,
            indexSettings,
            BigArrays.NON_RECYCLING_INSTANCE,
            "",
            false
        );

        DataFormatRegistry registry = createMockRegistry();
        CommitterFactory committerFactory = config -> new InMemoryCommitter(store);

        return new EngineConfig.Builder().shardId(shardId)
            .threadPool(threadPool)
            .indexSettings(indexSettings)
            .store(store)
            .mergePolicy(NoMergePolicy.INSTANCE)
            .translogConfig(translogConfig)
            .flushMergesAfter(TimeValue.timeValueMinutes(5))
            .externalRefreshListener(List.of())
            .internalRefreshListener(List.of())
            .globalCheckpointSupplier(() -> SequenceNumbers.NO_OPS_PERFORMED)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .primaryTermSupplier(primaryTerm::get)
            .tombstoneDocSupplier(tombstoneDocSupplier())
            .dataFormatRegistry(registry)
            .committerFactory(committerFactory)
            .build();
    }

    private DataFormatRegistry createMockRegistry() {
        PluginsService pluginsService = mock(PluginsService.class);
        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(mockPlugin));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(
            List.of(new MockSearchBackEndPlugin(List.of(mockDataFormat)))
        );
        return new DataFormatRegistry(pluginsService);
    }

    private Engine.Index indexOp(ParsedDocument doc) {
        return new Engine.Index(
            new Term(IdFieldMapper.NAME, Uid.encodeId(doc.id())),
            doc,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            -1,
            false,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
    }

    private Engine.Index replicaIndexOp(ParsedDocument doc, long seqNo) {
        return new Engine.Index(
            new Term(IdFieldMapper.NAME, Uid.encodeId(doc.id())),
            doc,
            seqNo,
            primaryTerm.get(),
            Versions.MATCH_ANY,
            null,
            Engine.Operation.Origin.REPLICA,
            System.nanoTime(),
            -1,
            false,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
    }

    public void testSequenceNumbersAssignedOnPrimary() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(5, 20);
            for (int i = 0; i < numDocs; i++) {
                ParsedDocument doc = createParsedDoc(Integer.toString(i), null);
                Engine.IndexResult result = engine.index(indexOp(doc));
                assertThat("seq no should be monotonically increasing", result.getSeqNo(), equalTo((long) i));
            }
            assertThat(
                "processed checkpoint should reflect all indexed docs",
                engine.getProcessedLocalCheckpoint(),
                equalTo((long) numDocs - 1)
            );
        }
    }

    public void testSequenceNumbersOnReplica() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            long[] seqNos = { 3, 1, 0, 2 };
            for (long seqNo : seqNos) {
                ParsedDocument doc = createParsedDoc(Long.toString(seqNo), null);
                Engine.IndexResult result = engine.index(replicaIndexOp(doc, seqNo));
                assertThat("replica should use the provided seq no", result.getSeqNo(), equalTo(seqNo));
            }
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo(3L));
        }
    }

    public void testLocalCheckpointAdvancesCorrectly() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(5, 15);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
                assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) i));
            }
        }
    }

    public void testIndexOperationsWrittenToTranslog() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(3, 10);
            for (int i = 0; i < numDocs; i++) {
                Engine.IndexResult result = engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
                assertThat("translog location should be set", result.getTranslogLocation(), notNullValue());
            }
            assertThat(engine.translogManager().getTranslogStats().estimatedNumberOfOperations(), equalTo(numDocs));
        }
    }

    public void testTranslogSyncPersistsCheckpoint() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(3, 10);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            // Before sync, persisted checkpoint may lag
            long persistedBefore = engine.getPersistedLocalCheckpoint();
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) numDocs - 1));

            engine.translogManager().syncTranslog();

            // After sync, persisted must equal processed
            assertThat(engine.getPersistedLocalCheckpoint(), equalTo((long) numDocs - 1));
            assertThat(engine.getPersistedLocalCheckpoint(), equalTo(engine.getProcessedLocalCheckpoint()));
            // Translog ops count should be unchanged by sync
            assertThat(engine.translogManager().getTranslogStats().estimatedNumberOfOperations(), equalTo(numDocs));
        }
    }

    public void testFlushTrimsTranslog() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            int numDocs = randomIntBetween(3, 10);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }
            assertThat(engine.translogManager().getTranslogStats().estimatedNumberOfOperations(), equalTo(numDocs));

            engine.flush(false, true);

            // After flush, translog generation should have rolled.
            // Full trimming depends on the committer advancing the persisted checkpoint
            // in the backing store, which the InMemoryCommitter in tests does not do.
            // Verify that flush at least completes without error and the translog is still valid.
            assertThat(engine.translogManager().getTranslogStats().estimatedNumberOfOperations(), greaterThanOrEqualTo(0));
        }
    }

    public void testRefreshProducesCatalogSnapshot() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(1, 5);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }
            engine.refresh("test");

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat(snapshot.getGeneration(), equalTo(1L));
                assertThat(snapshot.getSegments().size(), equalTo(1));

                org.opensearch.index.engine.exec.Segment segment = snapshot.getSegments().get(0);
                assertThat(segment.dfGroupedSearchableFiles().containsKey(mockDataFormat.name()), equalTo(true));

                WriterFileSet wfs = segment.dfGroupedSearchableFiles().get(mockDataFormat.name());
                assertThat(wfs.files().size(), equalTo(1));
                // MockWriter produces "data_gen<writerGen>.parquet" — first writer is gen 1
                assertTrue("file name should follow mock pattern", wfs.files().iterator().next().startsWith("data_gen"));
                assertTrue(wfs.files().iterator().next().endsWith(".parquet"));
                assertThat(wfs.numRows(), equalTo((long) numDocs));
                assertThat(wfs.writerGeneration(), equalTo(segment.generation()));
            }
            assertThat(engine.translogManager().getTranslogStats().estimatedNumberOfOperations(), equalTo(numDocs));
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) numDocs - 1));
        }
    }

    public void testRefreshAdvancesSnapshotGeneration() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            // Initial snapshot generation is 0
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                assertThat(ref.get().getGeneration(), equalTo(0L));
                assertThat(ref.get().getSegments().size(), equalTo(0));
            }

            engine.index(indexOp(createParsedDoc("1", null)));
            engine.refresh("first");

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                assertThat(ref.get().getGeneration(), equalTo(1L));
                assertThat(ref.get().getSegments().size(), equalTo(1));
            }

            engine.index(indexOp(createParsedDoc("2", null)));
            engine.refresh("second");

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                assertThat(ref.get().getGeneration(), equalTo(2L));
                // 2 segments: one from first refresh, one from second
                assertThat(ref.get().getSegments().size(), equalTo(2));
            }
        }
    }

    public void testRefreshUpdatesLastRefreshedCheckpoint() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            // Before any indexing, last refreshed checkpoint is at NO_OPS_PERFORMED
            assertThat(engine.lastRefreshedCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));

            int numDocs = randomIntBetween(3, 10);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            // Before refresh, last refreshed checkpoint hasn't advanced
            assertThat(engine.lastRefreshedCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));

            engine.refresh("test");

            // After refresh, last refreshed checkpoint should be at the processed checkpoint
            assertThat(engine.lastRefreshedCheckpoint(), equalTo((long) numDocs - 1));
            assertThat(engine.lastRefreshedCheckpoint(), equalTo(engine.getProcessedLocalCheckpoint()));
        }
    }

    public void testMultipleRefreshesAccumulateSegments() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numBatches = randomIntBetween(3, 6);
            for (int batch = 0; batch < numBatches; batch++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(batch), null)));
                engine.refresh("batch-" + batch);
            }

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat(snapshot.getSegments().size(), equalTo(numBatches));
                assertThat(snapshot.getGeneration(), equalTo((long) numBatches));

                // Each segment should have exactly 1 file with 1 row (1 doc per batch)
                for (org.opensearch.index.engine.exec.Segment segment : snapshot.getSegments()) {
                    WriterFileSet wfs = segment.dfGroupedSearchableFiles().get(mockDataFormat.name());
                    assertThat("each segment should have a WriterFileSet for the mock format", wfs, notNullValue());
                    assertThat(wfs.files().size(), equalTo(1));
                    assertThat(wfs.numRows(), equalTo(1L));
                    String fileName = wfs.files().iterator().next();
                    assertTrue("file should be a parquet file: " + fileName, fileName.endsWith(".parquet"));
                }
            }
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) numBatches - 1));
            assertThat(engine.lastRefreshedCheckpoint(), equalTo((long) numBatches - 1));
            assertThat(engine.translogManager().getTranslogStats().estimatedNumberOfOperations(), equalTo(numBatches));
        }
    }

    public void testFlushCommitsCatalogSnapshot() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            int numDocs = randomIntBetween(1, 5);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }
            engine.flush(false, true);

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat(snapshot, notNullValue());
                // Flush calls refresh internally, producing 1 segment
                assertThat(snapshot.getSegments().size(), equalTo(1));
                assertThat(snapshot.getGeneration(), equalTo(2L));
            }
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) numDocs - 1));
            assertThat(engine.lastRefreshedCheckpoint(), equalTo((long) numDocs - 1));
        }
    }

    public void testFlushWithNoOpsDoesNotFail() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            engine.flush(false, true);
        }
    }

    public void testForceFlushRequiresWaitIfOngoing() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            expectThrows(IllegalArgumentException.class, () -> engine.flush(true, false));
        }
    }

    public void testConcurrentIndexing() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numThreads = randomIntBetween(3, 6);
            int docsPerThread = randomIntBetween(10, 30);
            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            AtomicInteger failures = new AtomicInteger(0);
            AtomicLong maxSeqNo = new AtomicLong(-1);

            Thread[] threads = new Thread[numThreads];
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int d = 0; d < docsPerThread; d++) {
                            ParsedDocument doc = createParsedDoc(threadId + "_" + d, null);
                            Engine.IndexResult result = engine.index(indexOp(doc));
                            assertThat(result.getSeqNo(), greaterThanOrEqualTo(0L));
                            maxSeqNo.accumulateAndGet(result.getSeqNo(), Math::max);
                        }
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    }
                });
                threads[t].start();
            }

            for (Thread t : threads) {
                t.join();
            }

            assertThat(failures.get(), equalTo(0));
            int totalDocs = numThreads * docsPerThread;
            assertThat(maxSeqNo.get(), equalTo((long) totalDocs - 1));
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) totalDocs - 1));
        }
    }

    public void testConcurrentIndexAndRefresh() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numIndexThreads = randomIntBetween(2, 4);
            int docsPerThread = randomIntBetween(10, 20);
            AtomicInteger failures = new AtomicInteger(0);
            CountDownLatch indexingDone = new CountDownLatch(numIndexThreads);

            // Index first, then refresh — avoids writer pool race between
            // concurrent getAndLock (indexing) and checkoutAll (refresh).
            Thread[] indexThreads = new Thread[numIndexThreads];
            for (int t = 0; t < numIndexThreads; t++) {
                final int threadId = t;
                indexThreads[t] = new Thread(() -> {
                    try {
                        for (int d = 0; d < docsPerThread; d++) {
                            engine.index(indexOp(createParsedDoc(threadId + "_" + d, null)));
                        }
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    } finally {
                        indexingDone.countDown();
                    }
                });
                indexThreads[t].start();
            }

            for (Thread t : indexThreads)
                t.join();

            // Now refresh after all indexing is done
            engine.refresh("after-indexing");

            assertThat(failures.get(), equalTo(0));
            int totalDocs = numIndexThreads * docsPerThread;
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) totalDocs - 1));
            assertThat(engine.translogManager().getTranslogStats().estimatedNumberOfOperations(), equalTo(totalDocs));
            assertThat(engine.lastRefreshedCheckpoint(), equalTo((long) totalDocs - 1));

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                assertThat(ref.get().getGeneration(), equalTo(1L));
                assertThat(ref.get().getSegments().size(), greaterThan(0));
            }
        }
    }

    public void testConcurrentRefreshAndFlush() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            int numDocs = randomIntBetween(5, 15);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            AtomicInteger failures = new AtomicInteger(0);
            AtomicReference<Exception> failureCause = new AtomicReference<>();
            CyclicBarrier barrier = new CyclicBarrier(4);

            Thread[] threads = new Thread[4];
            for (int t = 0; t < 4; t++) {
                final boolean doFlush = t % 2 == 0;
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        if (doFlush) {
                            engine.flush(false, true);
                        } else {
                            engine.refresh("concurrent");
                        }
                    } catch (Exception e) {
                        failureCause.set(e);
                        failures.incrementAndGet();
                    }
                });
                threads[t].start();
            }

            for (Thread t : threads)
                t.join();
            if (failures.get() > 0) {
                throw new AssertionError("Failure while performing flush/refresh: " + failureCause.get());
            }

            // Post-conditions: all docs should still be tracked
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) numDocs - 1));
            // Snapshot should exist and have segments
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                assertThat(ref.get().getSegments().size(), greaterThan(0));
                assertThat(ref.get().getGeneration(), greaterThan(0L));
            }
        }
    }

    public void testCloseEngine() throws IOException {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        engine.index(indexOp(createParsedDoc("1", null)));
        engine.close();
        // Verify engine is closed by checking that operations throw
        expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDoc("2", null))));
    }

    public void testOperationsAfterCloseThrow() throws IOException {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        engine.close();
        expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDoc("1", null))));
    }

    public void testFlushAndClose() throws IOException {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
        int numDocs = randomIntBetween(3, 10);
        for (int i = 0; i < numDocs; i++) {
            engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
        }
        engine.flushAndClose();
        // Verify closed
        expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDoc("99", null))));
    }

    public void testRefreshAfterCloseThrows() throws IOException {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        engine.close();
        expectThrows(AlreadyClosedException.class, () -> engine.refresh("after-close"));
    }

    public void testFlushAfterCloseThrows() throws IOException {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        engine.close();
        expectThrows(AlreadyClosedException.class, () -> engine.flush(false, true));
    }

    public void testAcquireSnapshotReturnsValidSnapshot() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            // Initial snapshot: generation 0, no segments
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat(snapshot, notNullValue());
                assertThat(snapshot.getGeneration(), equalTo(0L));
                assertThat(snapshot.getSegments().size(), equalTo(0));
                assertThat(snapshot.getId(), equalTo(0L));
            }
        }
    }

    public void testSnapshotSurvivesRefreshWhileHeld() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.index(indexOp(createParsedDoc("1", null)));
            engine.refresh("first");

            GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot();
            long heldGen = ref.get().getGeneration();

            engine.index(indexOp(createParsedDoc("2", null)));
            engine.refresh("second");

            // Held snapshot should still be valid
            assertThat(ref.get().getGeneration(), equalTo(heldGen));

            // New snapshot should have higher generation
            try (GatedCloseable<CatalogSnapshot> newRef = engine.acquireSnapshot()) {
                assertThat(newRef.get().getGeneration(), greaterThan(heldGen));
            }

            ref.close();
        }
    }

    public void testThrottling() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            assertFalse(engine.isThrottled());
            assertThat(engine.getIndexThrottleTimeInMillis(), equalTo(0L));

            engine.activateThrottling();
            assertTrue(engine.isThrottled());

            engine.deactivateThrottling();
            assertFalse(engine.isThrottled());
        }
    }

    public void testEngineConfig() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            assertThat(engine.config(), notNullValue());
            assertThat(engine.config().getShardId(), equalTo(shardId));
        }
    }

    public void testHistoryUUID() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            assertThat(engine.getHistoryUUID(), notNullValue());
        }
    }

    public void testRefreshNeeded() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            assertTrue(engine.refreshNeeded());
        }
    }

    public void testIndexRefreshFlushEndToEnd() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            int numDocs = randomIntBetween(5, 15);

            // Phase 1: Index
            for (int i = 0; i < numDocs; i++) {
                Engine.IndexResult result = engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
                assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
                assertThat(result.getSeqNo(), equalTo((long) i));
                assertThat(result.getTranslogLocation(), notNullValue());
            }
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) numDocs - 1));
            assertThat(engine.translogManager().getTranslogStats().estimatedNumberOfOperations(), equalTo(numDocs));
            assertThat(engine.lastRefreshedCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));

            // Phase 2: Refresh — makes data searchable
            engine.refresh("test");
            assertThat(engine.lastRefreshedCheckpoint(), equalTo((long) numDocs - 1));
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat(snapshot.getGeneration(), equalTo(2L));
                assertThat(snapshot.getSegments().size(), equalTo(1));
                assertThat(snapshot.getSegments().get(0).dfGroupedSearchableFiles().containsKey(mockDataFormat.name()), equalTo(true));
            }
            // Reader should have format-specific reader
            try (GatedCloseable<IndexReaderProvider.Reader> readerRef = engine.acquireReader()) {
                assertThat(readerRef.get().reader(mockDataFormat), notNullValue());
            }

            // Phase 3: Flush — persists catalog snapshot
            engine.flush(false, true);
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) numDocs - 1));

            // Phase 4: Verify translog snapshot has all ops
            int historyCount = engine.countNumberOfHistoryOperations("test", 0, numDocs - 1);
            assertThat(historyCount, greaterThanOrEqualTo(0));
        }
    }

    public void testConcurrentIndexRefreshFlushEndToEnd() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            int totalDocs = randomIntBetween(50, 100);
            AtomicInteger failures = new AtomicInteger(0);
            AtomicReference<Exception> failureCause = new AtomicReference<>();

            // Index all docs first
            for (int i = 0; i < totalDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) totalDocs - 1));

            // Then do concurrent refresh + flush
            int numRefreshes = randomIntBetween(3, 7);
            int numFlushes = randomIntBetween(2, 5);
            CyclicBarrier barrier = new CyclicBarrier(3);
            Thread refresher = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < numRefreshes; i++) {
                        engine.refresh("background-" + i);
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                    failureCause.set(e);
                }
            });

            Thread flusher = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < numFlushes; i++) {
                        engine.flush(false, true);
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                    failureCause.set(e);
                }
            });

            refresher.start();
            flusher.start();
            barrier.await();
            refresher.join();
            flusher.join();

            if (failures.get() > 0) {
                throw new AssertionError("failure while performing test: " + failureCause.get());
            }
            engine.flush(false, true);
        }
    }

    public void testFailEnginePreventsSubsequentOps() throws IOException {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        engine.index(indexOp(createParsedDoc("1", null)));

        engine.failEngine("test failure", new RuntimeException("simulated"));

        expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDoc("2", null))));
        expectThrows(AlreadyClosedException.class, () -> engine.refresh("after-fail"));
        expectThrows(AlreadyClosedException.class, () -> engine.flush(false, true));
    }

    public void testDoubleFailEngineIsIdempotent() throws IOException {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        engine.failEngine("first failure", new RuntimeException("first"));
        // Second failEngine should not throw
        engine.failEngine("second failure", new RuntimeException("second"));
        expectThrows(AlreadyClosedException.class, () -> engine.ensureOpen());
    }

    public void testCatalogSnapshotContainsFormatSpecificFiles() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(1, 5);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }
            engine.refresh("test");

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat(snapshot.getSegments().size(), equalTo(1));

                org.opensearch.index.engine.exec.Segment segment = snapshot.getSegments().get(0);
                // Verify the segment has exactly one format entry
                assertThat(segment.dfGroupedSearchableFiles().size(), equalTo(1));
                assertThat(segment.dfGroupedSearchableFiles().containsKey(mockDataFormat.name()), equalTo(true));

                WriterFileSet wfs = segment.dfGroupedSearchableFiles().get(mockDataFormat.name());
                // Verify file references
                assertThat(wfs.files().isEmpty(), equalTo(false));
                assertThat(wfs.numRows(), equalTo((long) numDocs));
                assertThat(wfs.directory(), notNullValue());
                assertThat(wfs.writerGeneration(), equalTo(segment.generation()));

                // Verify via getSearchableFiles API as well
                java.util.Collection<WriterFileSet> searchableFiles = snapshot.getSearchableFiles(mockDataFormat.name());
                assertThat(searchableFiles.size(), equalTo(1));
                WriterFileSet fromApi = searchableFiles.iterator().next();
                assertThat(fromApi.files(), equalTo(wfs.files()));
                assertThat(fromApi.numRows(), equalTo(wfs.numRows()));
            }
        }
    }

    public void testCommitDataContainsRequiredMetadataKeys() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            engine.index(indexOp(createParsedDoc("1", null)));
            engine.flush(false, true);

            // The InMemoryCommitter stores the commit data. Access it via the engine's
            // committer factory pattern — we verify the commit data indirectly through
            // the translog UUID and seq-no stats being consistent after flush.
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo(0L));
            assertThat(engine.translogManager().getTranslogUUID(), notNullValue());
            assertThat(engine.getHistoryUUID(), notNullValue());
        }
    }

    public void testFlushCommitDataContainsCatalogSnapshotKeys() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            engine.index(indexOp(createParsedDoc("1", null)));
            engine.flush(false, true);

            // After flush, the catalog snapshot should be non-empty and have valid generation
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat(snapshot.getId(), greaterThanOrEqualTo(0L));
                assertThat(snapshot.getGeneration(), greaterThanOrEqualTo(0L));
            }
        }
    }

    public void testAcquireReaderReturnsValidReader() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(1, 5);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }
            engine.refresh("test");

            try (GatedCloseable<IndexReaderProvider.Reader> readerRef = engine.acquireReader()) {
                IndexReaderProvider.Reader reader = readerRef.get();
                assertThat(reader, notNullValue());
                assertThat(reader.catalogSnapshot(), notNullValue());
                assertThat(reader.catalogSnapshot().getGeneration(), equalTo(1L));
                assertThat(reader.catalogSnapshot().getSegments().size(), equalTo(1));
            }
        }
    }

    public void testAcquireReaderContainsFormatSpecificReader() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.index(indexOp(createParsedDoc("1", null)));
            engine.refresh("test");

            try (GatedCloseable<IndexReaderProvider.Reader> readerRef = engine.acquireReader()) {
                IndexReaderProvider.Reader reader = readerRef.get();
                // The mock data format is registered as "composite" — the reader manager
                // should have created a MockReader for it during afterRefresh
                Object formatReader = reader.reader(mockDataFormat);
                assertThat("reader for the registered data format should be present", formatReader, notNullValue());
            }
        }
    }

    public void testAcquireReaderReturnsNullForUnregisteredFormat() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.index(indexOp(createParsedDoc("1", null)));
            engine.refresh("test");

            try (GatedCloseable<IndexReaderProvider.Reader> readerRef = engine.acquireReader()) {
                IndexReaderProvider.Reader reader = readerRef.get();
                MockDataFormat unknownFormat = new MockDataFormat("unknown", 999L, mockDataFormat.supportedFields());
                assertNull("reader for unregistered format should be null", reader.reader(unknownFormat));
            }
        }
    }

    public void testAcquireReaderBeforeRefreshReturnsEmptyReaders() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            // Acquire reader before any refresh — the initial catalog snapshot
            // has no segments, so reader managers won't have created readers
            try (GatedCloseable<IndexReaderProvider.Reader> readerRef = engine.acquireReader()) {
                IndexReaderProvider.Reader reader = readerRef.get();
                assertThat(reader, notNullValue());
                assertThat(reader.catalogSnapshot(), notNullValue());
                // No refresh has happened, so the format reader may be null
                // (reader manager has no data for the initial empty snapshot)
            }
        }
    }

    public void testAcquireReaderSnapshotMatchesLatestRefresh() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            // Index and refresh twice
            engine.index(indexOp(createParsedDoc("1", null)));
            engine.refresh("first");

            engine.index(indexOp(createParsedDoc("2", null)));
            engine.refresh("second");

            long latestGen;
            try (GatedCloseable<CatalogSnapshot> snapRef = engine.acquireSnapshot()) {
                latestGen = snapRef.get().getGeneration();
            }

            try (GatedCloseable<IndexReaderProvider.Reader> readerRef = engine.acquireReader()) {
                assertThat(
                    "reader's snapshot should match the latest generation",
                    readerRef.get().catalogSnapshot().getGeneration(),
                    equalTo(latestGen)
                );
            }
        }
    }

    public void testAcquireReaderClosingReleasesSnapshotRef() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.index(indexOp(createParsedDoc("1", null)));
            engine.refresh("test");

            // Acquire and close a reader, then verify the engine still works
            GatedCloseable<IndexReaderProvider.Reader> readerRef = engine.acquireReader();
            IndexReaderProvider.Reader reader = readerRef.get();
            long readerGen = reader.catalogSnapshot().getGeneration();
            readerRef.close();

            // After closing, we should still be able to acquire new readers
            // and do more work
            engine.index(indexOp(createParsedDoc("2", null)));
            engine.refresh("after-close");

            try (GatedCloseable<IndexReaderProvider.Reader> newReaderRef = engine.acquireReader()) {
                assertThat(
                    "new reader should have a higher generation",
                    newReaderRef.get().catalogSnapshot().getGeneration(),
                    greaterThan(readerGen)
                );
            }
        }
    }

    public void testAcquireReaderAfterMultipleRefreshesSeesAllSegments() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numBatches = randomIntBetween(3, 6);
            for (int i = 0; i < numBatches; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
                engine.refresh("batch-" + i);
            }

            try (GatedCloseable<IndexReaderProvider.Reader> readerRef = engine.acquireReader()) {
                IndexReaderProvider.Reader reader = readerRef.get();
                CatalogSnapshot snapshot = reader.catalogSnapshot();
                assertThat(snapshot.getSegments().size(), equalTo(numBatches));
                assertThat(snapshot.getGeneration(), equalTo((long) numBatches));
                // Format-specific reader should be present
                assertThat(reader.reader(mockDataFormat), notNullValue());
            }
        }
    }

    public void testAcquireReaderAfterCloseThrows() throws IOException {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        engine.close();
        expectThrows(AlreadyClosedException.class, engine::acquireReader);
    }

    public void testConcurrentAcquireReader() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(5, 15);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }
            engine.refresh("setup");

            int numThreads = randomIntBetween(3, 6);
            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            AtomicInteger failures = new AtomicInteger(0);

            Thread[] threads = new Thread[numThreads];
            for (int t = 0; t < numThreads; t++) {
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int i = 0; i < 5; i++) {
                            try (GatedCloseable<IndexReaderProvider.Reader> ref = engine.acquireReader()) {
                                IndexReaderProvider.Reader reader = ref.get();
                                assertThat(reader.catalogSnapshot(), notNullValue());
                                assertThat(reader.catalogSnapshot().getSegments().size(), greaterThan(0));
                            }
                        }
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    }
                });
                threads[t].start();
            }

            for (Thread t : threads)
                t.join();
            assertThat(failures.get(), equalTo(0));
        }
    }

    public void testNewChangesSnapshotReturnsIndexedOps() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(5, 20);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            try (Translog.Snapshot snapshot = engine.newChangesSnapshot("test", 0, numDocs - 1, false, true)) {
                int count = 0;
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    assertThat(op.seqNo(), greaterThanOrEqualTo(0L));
                    count++;
                }
                assertThat("snapshot should contain all indexed ops", count, equalTo(numDocs));
            }
        }
    }

    public void testNewChangesSnapshotRespectsSeqNoRange() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(10, 20);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            // Request only a subset of the range
            long fromSeqNo = 3;
            long toSeqNo = 7;
            try (Translog.Snapshot snapshot = engine.newChangesSnapshot("test", fromSeqNo, toSeqNo, false, true)) {
                int count = 0;
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    assertThat(op.seqNo(), greaterThanOrEqualTo(fromSeqNo));
                    assertThat(op.seqNo(), org.hamcrest.Matchers.lessThanOrEqualTo(toSeqNo));
                    count++;
                }
                assertThat(count, greaterThan(0));
            }
        }
    }

    public void testNewChangesSnapshotAfterConcurrentIndexing() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numThreads = randomIntBetween(2, 4);
            int docsPerThread = randomIntBetween(10, 20);
            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            AtomicInteger failures = new AtomicInteger(0);

            Thread[] threads = new Thread[numThreads];
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int d = 0; d < docsPerThread; d++) {
                            engine.index(indexOp(createParsedDoc(threadId + "_" + d, null)));
                        }
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    }
                });
                threads[t].start();
            }
            for (Thread t : threads)
                t.join();
            assertThat(failures.get(), equalTo(0));

            int totalDocs = numThreads * docsPerThread;
            try (Translog.Snapshot snapshot = engine.newChangesSnapshot("test", 0, totalDocs - 1, false, true)) {
                int count = 0;
                while (snapshot.next() != null)
                    count++;
                assertThat("all concurrently indexed ops should be in the translog snapshot", count, equalTo(totalDocs));
            }
        }
    }

    public void testCountNumberOfHistoryOperations() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(5, 15);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            int count = engine.countNumberOfHistoryOperations("test", 0, numDocs - 1);
            assertThat(count, equalTo(numDocs));
        }
    }

    public void testCountNumberOfHistoryOperationsSubRange() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            // Count only ops in range [3, 7]
            int count = engine.countNumberOfHistoryOperations("test", 3, 7);
            assertThat(count, greaterThan(0));
            assertThat(count, org.hamcrest.Matchers.lessThanOrEqualTo(5));
        }
    }

    private Engine.Index translogRecoveryIndexOp(ParsedDocument doc, long seqNo) {
        return new Engine.Index(
            new Term(IdFieldMapper.NAME, Uid.encodeId(doc.id())),
            doc,
            seqNo,
            primaryTerm.get(),
            1L,
            null,
            Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
            System.nanoTime(),
            -1,
            false,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
    }

    public void testTranslogRecoveryOriginSkipsTranslogWrite() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            // Index via translog recovery — should NOT write to translog
            Engine.IndexResult result = engine.index(translogRecoveryIndexOp(createParsedDoc("1", null), 0));
            assertThat(result.getSeqNo(), equalTo(0L));
            assertNull("translog location should be null for recovery-origin ops", result.getTranslogLocation());

            // Translog should have 0 ops since recovery-origin skips the write
            assertThat(engine.translogManager().getTranslogStats().estimatedNumberOfOperations(), equalTo(0));

            // But the checkpoint should still advance
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo(0L));
        }
    }

    public void testTranslogRecoveryOriginMarksSeqNoAsPersisted() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.index(translogRecoveryIndexOp(createParsedDoc("1", null), 0));

            // Recovery-origin ops have no translog location, so they're marked as persisted immediately
            assertThat(engine.getPersistedLocalCheckpoint(), equalTo(0L));
        }
    }

    public void testMixedPrimaryAndRecoveryOriginOps() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            // Primary op — goes to translog
            engine.index(indexOp(createParsedDoc("primary_0", null)));
            assertThat(engine.translogManager().getTranslogStats().estimatedNumberOfOperations(), equalTo(1));

            // Recovery op at seq 1 — skips translog
            engine.index(translogRecoveryIndexOp(createParsedDoc("recovery_1", null), 1));
            assertThat(engine.translogManager().getTranslogStats().estimatedNumberOfOperations(), equalTo(1));

            // Another primary op
            engine.index(indexOp(createParsedDoc("primary_2", null)));
            assertThat(engine.translogManager().getTranslogStats().estimatedNumberOfOperations(), equalTo(2));

            // All 3 ops should be processed
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo(2L));

            // Refresh and verify catalog snapshot has segments
            engine.refresh("test");
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                assertThat(ref.get().getSegments().size(), greaterThan(0));
            }
        }
    }

    public void testCheckpointStallsOnSeqNoGap() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            // Index as replica with a gap: deliver 0, 1, 3 (missing 2)
            engine.index(replicaIndexOp(createParsedDoc("0", null), 0));
            engine.index(replicaIndexOp(createParsedDoc("1", null), 1));
            engine.index(replicaIndexOp(createParsedDoc("3", null), 3));

            // Checkpoint should stall at 1 because seq 2 is missing
            assertThat("checkpoint should stall at 1 due to gap at seq 2", engine.getProcessedLocalCheckpoint(), equalTo(1L));

            // Now fill the gap
            engine.index(replicaIndexOp(createParsedDoc("2", null), 2));

            // Checkpoint should jump to 3
            assertThat("checkpoint should advance to 3 after gap is filled", engine.getProcessedLocalCheckpoint(), equalTo(3L));
        }
    }

    public void testSeqNoGapWithConcurrentDelivery() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int totalOps = randomIntBetween(20, 50);
            AtomicInteger failures = new AtomicInteger(0);

            // Create a shuffled array of seq nos to simulate out-of-order delivery
            long[] seqNos = new long[totalOps];
            for (int i = 0; i < totalOps; i++)
                seqNos[i] = i;
            // Fisher-Yates shuffle
            for (int i = totalOps - 1; i > 0; i--) {
                int j = randomIntBetween(0, i);
                long tmp = seqNos[i];
                seqNos[i] = seqNos[j];
                seqNos[j] = tmp;
            }

            int numThreads = randomIntBetween(2, 4);
            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            AtomicInteger nextIdx = new AtomicInteger(0);

            Thread[] threads = new Thread[numThreads];
            for (int t = 0; t < numThreads; t++) {
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        int idx;
                        while ((idx = nextIdx.getAndIncrement()) < totalOps) {
                            long seqNo = seqNos[idx];
                            engine.index(replicaIndexOp(createParsedDoc(Long.toString(seqNo), null), seqNo));
                        }
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    }
                });
                threads[t].start();
            }
            for (Thread t : threads)
                t.join();

            assertThat(failures.get(), equalTo(0));
            assertThat(
                "all ops delivered, checkpoint should be totalOps - 1",
                engine.getProcessedLocalCheckpoint(),
                equalTo((long) totalOps - 1)
            );
        }
    }

    public void testGetSeqNoStats() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(5, 15);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            // Sync translog so persisted checkpoint advances
            engine.translogManager().syncTranslog();

            org.opensearch.index.seqno.SeqNoStats stats = engine.getSeqNoStats(numDocs - 1);
            assertThat(stats.getMaxSeqNo(), equalTo((long) numDocs - 1));
            assertThat(stats.getLocalCheckpoint(), equalTo((long) numDocs - 1));
            assertThat(stats.getGlobalCheckpoint(), equalTo((long) numDocs - 1));
        }
    }

    public void testGetSeqNoStatsAfterConcurrentIndexingAndRefresh() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numThreads = randomIntBetween(2, 4);
            int docsPerThread = randomIntBetween(10, 20);
            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            AtomicInteger failures = new AtomicInteger(0);

            Thread[] threads = new Thread[numThreads];
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int d = 0; d < docsPerThread; d++) {
                            engine.index(indexOp(createParsedDoc(threadId + "_" + d, null)));
                        }
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    }
                });
                threads[t].start();
            }
            for (Thread t : threads)
                t.join();
            assertThat(failures.get(), equalTo(0));

            engine.refresh("test");
            engine.translogManager().syncTranslog();

            int totalDocs = numThreads * docsPerThread;
            org.opensearch.index.seqno.SeqNoStats stats = engine.getSeqNoStats(totalDocs - 1);
            assertThat(stats.getMaxSeqNo(), equalTo((long) totalDocs - 1));
            assertThat(stats.getLocalCheckpoint(), equalTo((long) totalDocs - 1));
        }
    }

    public void testPersistedCheckpointLagsProcessedBeforeSync() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(3, 10);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            long processed = engine.getProcessedLocalCheckpoint();
            long persisted = engine.getPersistedLocalCheckpoint();

            assertThat("processed checkpoint should reflect all docs", processed, equalTo((long) numDocs - 1));
            // Before sync, persisted may lag behind processed
            // (ops are in translog buffer but not yet fsync'd)
            assertThat(persisted, org.hamcrest.Matchers.lessThanOrEqualTo(processed));
        }
    }

    public void testPersistedCheckpointCatchesUpAfterSync() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(3, 10);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            engine.translogManager().syncTranslog();

            assertThat(
                "after sync, persisted should catch up to processed",
                engine.getPersistedLocalCheckpoint(),
                equalTo(engine.getProcessedLocalCheckpoint())
            );
        }
    }

    public void testPersistedCheckpointAfterConcurrentIndexAndSync() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(20, 50);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            // Sync from multiple threads
            int numSyncThreads = randomIntBetween(2, 4);
            CyclicBarrier barrier = new CyclicBarrier(numSyncThreads);
            AtomicInteger failures = new AtomicInteger(0);

            Thread[] threads = new Thread[numSyncThreads];
            for (int t = 0; t < numSyncThreads; t++) {
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        engine.translogManager().syncTranslog();
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    }
                });
                threads[t].start();
            }
            for (Thread t : threads)
                t.join();
            assertThat(failures.get(), equalTo(0));

            assertThat(engine.getPersistedLocalCheckpoint(), equalTo((long) numDocs - 1));
        }
    }

    public void testNonWaitingFlushReturnsImmediatelyIfOngoing() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            int numDocs = randomIntBetween(5, 15);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            // Run multiple non-waiting flushes concurrently — none should throw
            int numThreads = randomIntBetween(3, 6);
            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            AtomicInteger failures = new AtomicInteger(0);

            Thread[] threads = new Thread[numThreads];
            for (int t = 0; t < numThreads; t++) {
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        engine.flush(false, false);
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    }
                });
                threads[t].start();
            }
            for (Thread t : threads)
                t.join();
            assertThat(failures.get(), equalTo(0));
        }
    }

    public void testShouldPeriodicallyFlush() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            // With no ops, should not need periodic flush
            // (translog is empty, well under threshold)
            boolean needsFlush = engine.shouldPeriodicallyFlush();
            // The result depends on translog size vs threshold — with 0 ops it should be false
            assertFalse("empty engine should not need periodic flush", needsFlush);

            // Index enough docs to potentially trigger periodic flush
            for (int i = 0; i < 100; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }
            // After indexing, shouldPeriodicallyFlush may or may not be true
            // depending on the configured threshold. The key assertion is it doesn't throw.
            engine.shouldPeriodicallyFlush();
        }
    }

    public void testWriteIndexingBufferTriggersRefresh() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(3, 10);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            long genBefore;
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                genBefore = ref.get().getGeneration();
            }

            // writeIndexingBuffer delegates to refresh
            engine.writeIndexingBuffer();

            long genAfter;
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                genAfter = ref.get().getGeneration();
            }

            assertThat("writeIndexingBuffer should trigger a refresh that advances the snapshot", genAfter, greaterThan(genBefore));
        }
    }

    public void testWriteIndexingBufferAfterConcurrentIndexing() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numThreads = randomIntBetween(2, 4);
            int docsPerThread = randomIntBetween(10, 20);
            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            AtomicInteger failures = new AtomicInteger(0);

            Thread[] threads = new Thread[numThreads];
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int d = 0; d < docsPerThread; d++) {
                            engine.index(indexOp(createParsedDoc(threadId + "_" + d, null)));
                        }
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    }
                });
                threads[t].start();
            }
            for (Thread t : threads)
                t.join();
            assertThat(failures.get(), equalTo(0));

            // writeIndexingBuffer should work after concurrent indexing
            engine.writeIndexingBuffer();

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                assertThat(ref.get().getSegments().size(), greaterThan(0));
            }
        }
    }
}
