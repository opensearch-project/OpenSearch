/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ReferenceManager;
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
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.dataformat.stub.InMemoryCommitter;
import org.opensearch.index.engine.dataformat.stub.MockDocumentInput;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.CommitterFactory;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SeqNoFieldMapper;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.index.engine.EngineTestCase.createParsedDoc;
import static org.opensearch.index.engine.EngineTestCase.tombstoneDocSupplier;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Abstract base class for testing {@link DataFormatAwareEngine} with any
 * {@link IndexingExecutionEngine} implementation.
 *
 * <p>Subclasses provide the engine-specific components via {@link #createDataFormatPlugin()}
 * and {@link #createSearchBackEndPlugin()}. The base class handles all DFAE lifecycle,
 * translog, store, and commit infrastructure.
 *
 * <p>Tests cover: indexing, refresh, flush, catalog snapshots, concurrency,
 * merge invariants, and failure handling — all driven through the real DFAE
 * orchestration layer with the injected engine.
 */
public abstract class AbstractDataFormatAwareEngineTestCase extends OpenSearchTestCase {

    protected ThreadPool threadPool;
    protected Store store;
    protected ShardId shardId;
    protected AtomicLong primaryTerm;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        shardId = new ShardId(new Index("test", "_na_"), 0);
        primaryTerm = new AtomicLong(randomLongBetween(1, Long.MAX_VALUE));
        threadPool = buildThreadPool();
        store = createStore();
    }

    /**
     * Override to provide a custom {@link ThreadPool} with plugin-specific executors
     * (e.g., the {@code parquet_native_write} executor for the Parquet engine).
     * Default creates a standard {@link TestThreadPool}.
     */
    protected ThreadPool buildThreadPool() {
        return new TestThreadPool(getClass().getName());
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

    // ── Extension points ──

    /** Returns the DataFormatPlugin that provides the IndexingExecutionEngine under test. */
    protected abstract DataFormatPlugin createDataFormatPlugin();

    /** Returns the SearchBackEndPlugin for reader manager creation. */
    protected abstract SearchBackEndPlugin<?> createSearchBackEndPlugin();

    /** Returns the data format name (e.g., "composite", "parquet"). */
    protected abstract String dataFormatName();

    /** Override to provide a custom CommitterFactory (e.g., LuceneCommitterFactory). Default uses InMemoryCommitter. */
    protected CommitterFactory createCommitterFactory(Store store) {
        return config -> new InMemoryCommitter(store);
    }

    /** Override to add plugin-specific index settings (e.g., sort config, parquet settings). */
    protected Settings additionalIndexSettings() {
        return Settings.EMPTY;
    }

    // ── Infrastructure ──

    /**
     * Creates a fresh {@link Store} backed by a temporary directory for each test.
     * Subclasses may override to provide a store with custom directory implementations.
     */
    protected Store createStore() throws IOException {
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
     * Bootstraps the store's Lucene commit with the metadata that {@link DataFormatAwareEngine}
     * expects: translog UUID, sequence number markers, and history UUID.
     */
    protected void bootstrapStoreWithMetadata(Store store, String translogUUID) throws IOException {
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

    /**
     * Creates a fully wired {@link DataFormatAwareEngine} with translog, store, and the
     * plugin-provided {@link IndexingExecutionEngine}. Each call creates a fresh translog
     * and bootstraps the store's commit metadata.
     */
    protected DataFormatAwareEngine createEngine() throws IOException {
        Path translogPath = createTempDir();
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid);
        return new DataFormatAwareEngine(buildEngineConfig(store, translogPath, List.of(), List.of()));
    }

    /**
     * Builds the {@link EngineConfig} wiring together the store, translog, data format registry,
     * committer factory, and refresh listeners. Subclasses may override to add engine-specific
     * configuration (e.g., {@link org.opensearch.index.codec.CodecService} for Lucene).
     */
    protected EngineConfig buildEngineConfig(
        Store store,
        Path translogPath,
        List<ReferenceManager.RefreshListener> externalListeners,
        List<ReferenceManager.RefreshListener> internalListeners
    ) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), dataFormatName())
            .put(additionalIndexSettings());
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settingsBuilder.build());
        TranslogConfig translogConfig = new TranslogConfig(
            shardId,
            translogPath,
            indexSettings,
            BigArrays.NON_RECYCLING_INSTANCE,
            "",
            false
        );
        DataFormatRegistry registry = createRegistry();
        CommitterFactory committerFactory = createCommitterFactory(store);

        return new EngineConfig.Builder().shardId(shardId)
            .threadPool(threadPool)
            .indexSettings(indexSettings)
            .store(store)
            .mergePolicy(NoMergePolicy.INSTANCE)
            .translogConfig(translogConfig)
            .flushMergesAfter(TimeValue.timeValueMinutes(5))
            .externalRefreshListener(externalListeners)
            .internalRefreshListener(internalListeners)
            .globalCheckpointSupplier(() -> SequenceNumbers.NO_OPS_PERFORMED)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .primaryTermSupplier(primaryTerm::get)
            .tombstoneDocSupplier(tombstoneDocSupplier())
            .dataFormatRegistry(registry)
            .committerFactory(committerFactory)
            .build();
    }

    @SuppressWarnings("unchecked")
    private DataFormatRegistry createRegistry() {
        PluginsService pluginsService = mock(PluginsService.class);
        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(createDataFormatPlugin()));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(createSearchBackEndPlugin()));
        return new DataFormatRegistry(pluginsService);
    }

    // ── Helpers ──

    protected Engine.Index indexOp(ParsedDocument doc) {
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

    /** Override to provide a custom DocumentInput for the engine under test. Default uses MockDocumentInput. */
    protected DocumentInput<?> createDocumentInput() {
        return new MockDocumentInput();
    }

    protected ParsedDocument createParsedDocWithInput(String id, String routing) {
        ParsedDocument base = createParsedDoc(id, routing);
        return new ParsedDocument(
            base.version(),
            SeqNoFieldMapper.SequenceIDFields.emptySeqID(),
            base.id(),
            base.routing(),
            base.docs(),
            base.source(),
            base.getMediaType(),
            null,
            createDocumentInput()
        );
    }

    // ── Common tests ──

    /** Indexes documents, refreshes, and verifies segments have unique generations and positive row counts. */
    public void testIndexAndRefreshProducesSegments() throws IOException {
        try (DataFormatAwareEngine engine = createEngine()) {
            int numDocs = randomIntBetween(3, 10);
            for (int i = 0; i < numDocs; i++) {
                Engine.IndexResult result = engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
                assertThat(result.getSeqNo(), equalTo((long) i));
            }
            engine.refresh("test");

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat(snapshot.getSegments().size(), greaterThan(0));
                // Verify segment generation uniqueness
                Set<Long> gens = new HashSet<>();
                for (Segment seg : snapshot.getSegments()) {
                    assertTrue("duplicate generation: " + seg.generation(), gens.add(seg.generation()));
                    assertThat(seg.dfGroupedSearchableFiles().isEmpty(), equalTo(false));
                    for (WriterFileSet wfs : seg.dfGroupedSearchableFiles().values()) {
                        assertThat(wfs.numRows(), greaterThan(0L));
                        assertThat(wfs.files().isEmpty(), equalTo(false));
                    }
                }
            }
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) numDocs - 1));
            assertThat(engine.lastRefreshedCheckpoint(), equalTo((long) numDocs - 1));
        }
    }

    /** Flushes after indexing and verifies the catalog snapshot preserves total row count. */
    public void testFlushCommitsAndPreservesData() throws IOException {
        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            int numDocs = randomIntBetween(3, 10);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
            }
            engine.flush(false, true);

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat(snapshot.getSegments().size(), greaterThan(0));
                long totalRows = snapshot.getSegments()
                    .stream()
                    .flatMap(s -> s.dfGroupedSearchableFiles().values().stream())
                    .mapToLong(WriterFileSet::numRows)
                    .sum();
                assertThat(totalRows, equalTo((long) numDocs));
            }
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) numDocs - 1));
        }
    }

    /** Multi-threaded indexing must produce a contiguous checkpoint with no gaps. */
    public void testConcurrentIndexingPreservesCheckpoint() throws Exception {
        try (DataFormatAwareEngine engine = createEngine()) {
            int numThreads = randomIntBetween(2, 4);
            int docsPerThread = randomIntBetween(10, 25);
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
                            Engine.IndexResult result = engine.index(indexOp(createParsedDocWithInput(threadId + "_" + d, null)));
                            maxSeqNo.accumulateAndGet(result.getSeqNo(), Math::max);
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
            assertThat(maxSeqNo.get(), equalTo((long) totalDocs - 1));
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) totalDocs - 1));
        }
    }

    /** Each refresh cycle produces a new segment; all generations must be unique. */
    public void testMultipleRefreshesAccumulateSegmentsWithUniqueGenerations() throws IOException {
        try (DataFormatAwareEngine engine = createEngine()) {
            int numBatches = randomIntBetween(3, 6);
            for (int batch = 0; batch < numBatches; batch++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(batch), null)));
                engine.refresh("batch-" + batch);
            }

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat(snapshot.getSegments().size(), equalTo(numBatches));
                // Verify all generations are unique
                Set<Long> gens = new HashSet<>();
                for (Segment seg : snapshot.getSegments()) {
                    assertTrue("duplicate generation: " + seg.generation(), gens.add(seg.generation()));
                }
                // Verify total row count
                long totalRows = snapshot.getSegments()
                    .stream()
                    .flatMap(s -> s.dfGroupedSearchableFiles().values().stream())
                    .mapToLong(WriterFileSet::numRows)
                    .sum();
                assertThat(totalRows, equalTo((long) numBatches));
            }
        }
    }

    /** Snapshot generation must strictly increase across refreshes. */
    public void testSnapshotGenerationAdvancesMonotonically() throws IOException {
        try (DataFormatAwareEngine engine = createEngine()) {
            long prevGen = -1;
            for (int i = 0; i < randomIntBetween(3, 6); i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
                engine.refresh("cycle-" + i);
                try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                    long gen = ref.get().getGeneration();
                    assertThat("generation must advance monotonically", gen, greaterThan(prevGen));
                    prevGen = gen;
                }
            }
        }
    }

    /** Every primary-origin index operation must produce a non-null translog location. */
    public void testTranslogRecordsAllPrimaryOps() throws IOException {
        try (DataFormatAwareEngine engine = createEngine()) {
            int numDocs = randomIntBetween(5, 15);
            for (int i = 0; i < numDocs; i++) {
                Engine.IndexResult result = engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
                assertThat(result.getTranslogLocation(), notNullValue());
            }
            assertThat(engine.translogManager().getTranslogStats().estimatedNumberOfOperations(), equalTo(numDocs));
        }
    }

    /** Concurrent indexing followed by refresh and flush must preserve total row count. */
    public void testConcurrentIndexThenRefreshAndFlush() throws Exception {
        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            int totalDocs = randomIntBetween(20, 50);
            AtomicInteger failures = new AtomicInteger(0);

            // Index concurrently
            int numThreads = randomIntBetween(2, 4);
            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            AtomicInteger docCounter = new AtomicInteger(0);
            Thread[] threads = new Thread[numThreads];
            for (int t = 0; t < numThreads; t++) {
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        int idx;
                        while ((idx = docCounter.getAndIncrement()) < totalDocs) {
                            engine.index(indexOp(createParsedDocWithInput(Integer.toString(idx), null)));
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

            // Refresh and flush
            engine.refresh("after-indexing");
            engine.flush(false, true);

            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) totalDocs - 1));
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                assertThat(ref.get().getSegments().size(), greaterThan(0));
                long totalRows = ref.get()
                    .getSegments()
                    .stream()
                    .flatMap(s -> s.dfGroupedSearchableFiles().values().stream())
                    .mapToLong(WriterFileSet::numRows)
                    .sum();
                assertThat("total rows must match indexed docs", totalRows, equalTo((long) totalDocs));
            }
        }
    }

    /** DocStats count must reflect the number of indexed documents after refresh. */
    public void testDocStatsReflectsIndexedDocuments() throws IOException {
        try (DataFormatAwareEngine engine = createEngine()) {
            int numDocs = randomIntBetween(3, 10);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
            }
            engine.refresh("test");

            var stats = engine.docStats();
            assertThat(stats.getCount(), equalTo((long) numDocs));
            assertThat(stats.getCount(), greaterThanOrEqualTo(0L));
        }
    }
}
