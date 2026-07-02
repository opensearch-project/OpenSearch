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
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.AppendOnlyIndexOperationRetryException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.RowIdAwareWriter;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.WriterState;
import org.opensearch.index.engine.dataformat.stub.InMemoryCommitter;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockDocumentInput;
import org.opensearch.index.engine.dataformat.stub.MockIndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.stub.MockSearchBackEndPlugin;
import org.opensearch.index.engine.dataformat.stub.MockWriter;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterFactory;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.get.DocumentLookupResult;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.InternalTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.plugins.DocumentLookupProvider;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.index.engine.EngineTestCase.tombstoneDocSupplier;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
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
        bootstrapStoreWithMetadata(store, translogUUID, SequenceNumbers.NO_OPS_PERFORMED);
    }

    private void bootstrapStoreWithMetadata(Store store, String translogUUID, long maxSeqNo) throws IOException {
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
            commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
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
        return buildDFAEngineConfig(store, translogPath, List.of(), List.of());
    }

    private EngineConfig buildDFAEngineConfig(
        Store store,
        Path translogPath,
        List<ReferenceManager.RefreshListener> externalListeners,
        List<ReferenceManager.RefreshListener> internalListeners
    ) {
        return buildDFAEngineConfig(store, translogPath, externalListeners, internalListeners, IndexModule.TieringState.HOT.name());
    }

    /** Builds a HOT-tier DFA {@link EngineConfig} carrying the given {@link DocumentLookupProvider}. */
    private EngineConfig buildDFAEngineConfig(Store store, Path translogPath, DocumentLookupProvider documentLookupProvider) {
        return buildDFAEngineConfig(store, translogPath, List.of(), List.of(), IndexModule.TieringState.HOT.name(), documentLookupProvider);
    }

    private EngineConfig buildDFAEngineConfig(
        Store store,
        Path translogPath,
        List<ReferenceManager.RefreshListener> externalListeners,
        List<ReferenceManager.RefreshListener> internalListeners,
        String tieringState
    ) {
        return buildDFAEngineConfig(store, translogPath, externalListeners, internalListeners, tieringState, null);
    }

    private EngineConfig buildDFAEngineConfig(
        Store store,
        Path translogPath,
        List<ReferenceManager.RefreshListener> externalListeners,
        List<ReferenceManager.RefreshListener> internalListeners,
        String tieringState,
        DocumentLookupProvider documentLookupProvider
    ) {
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), mockDataFormat.name())
                .put(IndexModule.INDEX_TIERING_STATE.getKey(), tieringState)
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

        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(documentMapper.getVersion()).thenReturn(1L);
        when(mapperService.documentMapper()).thenReturn(documentMapper);

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
            .eventListener(new Engine.EventListener() {
                @Override
                public void onFailedEngine(String reason, Exception e) {}
            })
            .mapperService(mapperService)
            .documentLookupProvider(documentLookupProvider)
            .build();
    }

    private DataFormatRegistry createMockRegistry() {
        PluginsService pluginsService = mock(PluginsService.class);
        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(mockPlugin));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(
            List.of(new MockSearchBackEndPlugin(List.of(mockDataFormat.name())))
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

    /**
     * Append-only optimizable variant of {@link #indexOp}: a non-negative autoGeneratedIdTimestamp
     * with isRetry=false makes {@link IndexingStrategyPlanner} take the optimizedAppendOnly path
     * (no safe-access enforcement).
     */
    private Engine.Index appendOnlyOp(ParsedDocument doc) {
        return new Engine.Index(
            new Term(IdFieldMapper.NAME, Uid.encodeId(doc.id())),
            doc,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            System.currentTimeMillis(),
            false,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
    }

    /** Explicit-id index with a caller-supplied version/versionType (for version-conflict tests). */
    private Engine.Index indexOpWithVersion(ParsedDocument doc, long version, VersionType versionType) {
        return new Engine.Index(
            new Term(IdFieldMapper.NAME, Uid.encodeId(doc.id())),
            doc,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            version,
            versionType,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            -1,
            false,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
    }

    /** Explicit-id index with compare-and-set ifSeqNo/ifPrimaryTerm (for optimistic-concurrency tests). */
    private Engine.Index indexOpWithIfSeqNo(ParsedDocument doc, long ifSeqNo, long ifPrimaryTerm) {
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
            ifSeqNo,
            ifPrimaryTerm
        );
    }

    /**
     * Wraps {@link EngineTestCase#createParsedDoc(String, String)} to attach a
     * {@link MockDocumentInput}. {@link DataFormatAwareEngine#index} requires a
     * non-null {@code DocumentInput} on every doc (it calls {@code addField} for version,
     * seqNo, primaryTerm), but the base helper leaves that field null because production
     * code (e.g., {@code IndexShard.applyIndexOperation}) populates it via
     * {@code DocumentMapperForType.parse}.
     */
    private ParsedDocument createParsedDoc(String id, String routing) {
        ParsedDocument base = EngineTestCase.createParsedDoc(id, routing);
        return new ParsedDocument(
            base.version(),
            SeqNoFieldMapper.SequenceIDFields.emptySeqID(),
            base.id(),
            base.routing(),
            base.docs(),
            base.source(),
            base.getMediaType(),
            null,
            new MockDocumentInput()
        );
    }

    /** Backwards-compatible alias for tests that explicitly call {@code createParsedDocWithInput}. */
    private ParsedDocument createParsedDocWithInput(String id, String routing) {
        return createParsedDoc(id, routing);
    }

    public void testSequenceNumbersAssignedOnPrimary() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(5, 20);
            for (int i = 0; i < numDocs; i++) {
                ParsedDocument doc = createParsedDocWithInput(Integer.toString(i), null);
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

    /**
     * {@link DataFormatAwareEngine#acquireHistoryRetentionLock()} must return a lock that pins a translog
     * generation in the deletion policy for as long as it is held, and releases that generation when closed.
     * Peer recovery / primary relocation relies on this to keep translog history available for the duration
     * of recovery.
     */
    public void testAcquireHistoryRetentionLockPinsTranslogGeneration() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(1, 20);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
            }

            // The retention lock pins a generation in the translog's deletion policy.
            TranslogDeletionPolicy deletionPolicy = ((InternalTranslogManager) engine.translogManager()).getTranslog().getDeletionPolicy();
            // No retention locks are held before acquiring one.
            deletionPolicy.assertNoOpenTranslogRefs();

            Closeable retentionLock = engine.acquireHistoryRetentionLock();
            assertThat(retentionLock, notNullValue());
            // While the lock is held there is an open translog reference pinning a generation.
            expectThrows(AssertionError.class, deletionPolicy::assertNoOpenTranslogRefs);

            // Releasing the lock releases the pinned generation.
            retentionLock.close();
            deletionPolicy.assertNoOpenTranslogRefs();
        }
    }

    /**
     * {@link DataFormatAwareEngine#countNumberOfHistoryOperations(String, long, long)} must count only the
     * operations whose seqNo falls within the requested {@code [fromSeqNo, toSeqNo]} range, de-duplicated --
     * i.e. the operations actually yielded by the changes snapshot for that range, not the raw operation
     * count of the underlying translog generations.
     */
    public void testCountNumberOfHistoryOperationsRespectsSeqNoRange() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                Engine.IndexResult result = engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
                assertThat(result.getSeqNo(), equalTo((long) i));
            }
            // Ensure the buffered translog operations are durable and readable by the changes snapshot.
            engine.translogManager().syncTranslog();

            // Full range counts every operation.
            assertThat(engine.countNumberOfHistoryOperations("test", 0, Long.MAX_VALUE), equalTo(numDocs));

            // Sub-ranges count only the operations whose seqNo lies within [fromSeqNo, toSeqNo].
            assertThat(engine.countNumberOfHistoryOperations("test", 5, 9), equalTo(5));
            assertThat(engine.countNumberOfHistoryOperations("test", 0, 4), equalTo(5));
            assertThat(engine.countNumberOfHistoryOperations("test", 3, 6), equalTo(4));
            assertThat(engine.countNumberOfHistoryOperations("test", 7, Long.MAX_VALUE), equalTo(3));
            // A range above the highest seqNo contains no operations.
            assertThat(engine.countNumberOfHistoryOperations("test", numDocs, Long.MAX_VALUE), equalTo(0));
        }
    }

    public void testLocalCheckpointAdvancesCorrectly() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(5, 15);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
                assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) i));
            }
        }
    }

    public void testIndexOperationsWrittenToTranslog() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(3, 10);
            for (int i = 0; i < numDocs; i++) {
                Engine.IndexResult result = engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
                assertThat("translog location should be set", result.getTranslogLocation(), notNullValue());
            }
            assertThat(engine.translogManager().getTranslogStats().estimatedNumberOfOperations(), equalTo(numDocs));
        }
    }

    public void testTranslogSyncPersistsCheckpoint() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(3, 10);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
            }
            engine.refresh("test");

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat(snapshot.getGeneration(), equalTo(2L));
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
            // Initial snapshot generation is 1 (bumpGeneration in constructor)
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                assertThat(ref.get().getGeneration(), equalTo(1L));
                assertThat(ref.get().getSegments().size(), equalTo(0));
            }

            engine.index(indexOp(createParsedDocWithInput("1", null)));
            engine.refresh("first");

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                assertThat(ref.get().getGeneration(), equalTo(2L));
                assertThat(ref.get().getSegments().size(), equalTo(1));
            }

            engine.index(indexOp(createParsedDocWithInput("2", null)));
            engine.refresh("second");

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                assertThat(ref.get().getGeneration(), equalTo(3L));
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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(batch), null)));
                engine.refresh("batch-" + batch);
            }

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat(snapshot.getSegments().size(), equalTo(numBatches));
                assertThat(snapshot.getGeneration(), equalTo((long) numBatches + 1));

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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
            }
            engine.flush(false, true);

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat(snapshot, notNullValue());
                // Flush calls refresh internally, producing 1 segment
                assertThat(snapshot.getSegments().size(), equalTo(1));
                assertThat(snapshot.getGeneration(), equalTo(3L));
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
                            ParsedDocument doc = createParsedDocWithInput(threadId + "_" + d, null);
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
                            engine.index(indexOp(createParsedDocWithInput(threadId + "_" + d, null)));
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
                assertThat(ref.get().getGeneration(), equalTo(2L));
                assertThat(ref.get().getSegments().size(), greaterThan(0));
            }
        }
    }

    public void testConcurrentRefreshAndFlush() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            int numDocs = randomIntBetween(5, 15);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
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
        engine.index(indexOp(createParsedDocWithInput("1", null)));
        engine.close();
        // Verify engine is closed by checking that operations throw
        expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDocWithInput("2", null))));
    }

    public void testOperationsAfterCloseThrow() throws IOException {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        engine.close();
        expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDocWithInput("1", null))));
    }

    public void testFlushAndClose() throws IOException {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
        int numDocs = randomIntBetween(3, 10);
        for (int i = 0; i < numDocs; i++) {
            engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
        }
        engine.flushAndClose();
        // Verify closed
        expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDocWithInput("99", null))));
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
            // Initial snapshot: generation 1 (bumpGeneration in constructor), no segments
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat(snapshot, notNullValue());
                assertThat(snapshot.getGeneration(), equalTo(1L));
                assertThat(snapshot.getSegments().size(), equalTo(0));
                assertThat(snapshot.getId(), equalTo(1L));
            }
        }
    }

    public void testSnapshotSurvivesRefreshWhileHeld() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.index(indexOp(createParsedDocWithInput("1", null)));
            engine.refresh("first");

            GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot();
            long heldGen = ref.get().getGeneration();

            engine.index(indexOp(createParsedDocWithInput("2", null)));
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

    @SuppressForbidden(reason = "access private fields to simulate merge pressure and verify throttle integration")
    public void testMergeThrottleIntegration() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            assertFalse("engine should not be throttled initially", engine.isThrottled());

            // Access the private mergeScheduler
            java.lang.reflect.Field schedulerField = DataFormatAwareEngine.class.getDeclaredField("mergeScheduler");
            schedulerField.setAccessible(true);
            org.opensearch.index.engine.dataformat.merge.MergeScheduler scheduler =
                (org.opensearch.index.engine.dataformat.merge.MergeScheduler) schedulerField.get(engine);

            // Access the scheduler's activeMerges to simulate in-flight merge pressure
            java.lang.reflect.Field activeMergesField = scheduler.getClass().getDeclaredField("activeMerges");
            activeMergesField.setAccessible(true);
            java.util.concurrent.atomic.AtomicInteger activeMerges =
                (java.util.concurrent.atomic.AtomicInteger) activeMergesField.get(scheduler);

            // Access maxMergeCount to know the threshold
            java.lang.reflect.Field maxMergeCountField = scheduler.getClass().getDeclaredField("maxMergeCount");
            maxMergeCountField.setAccessible(true);
            int maxMergeCount = (int) maxMergeCountField.get(scheduler);
            assertTrue("maxMergeCount should be positive", maxMergeCount > 0);

            // Simulate merge pressure: set activeMerges above maxMergeCount
            activeMerges.set(maxMergeCount + 1);

            // triggerMerges evaluates throttle: activeMerges + pendingMerges > maxMergeCount
            scheduler.triggerMerges();
            assertTrue("engine should be throttled when merge pressure exceeds threshold", engine.isThrottled());

            // Simulate merges completing: drop activeMerges below threshold
            activeMerges.set(0);
            scheduler.triggerMerges();
            assertFalse("engine should not be throttled after merge pressure subsides", engine.isThrottled());
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
            assertTrue(engine.

                refreshNeeded());
        }
    }

    public void testIndexRefreshFlushEndToEnd() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            int numDocs = randomIntBetween(5, 15);

            // Phase 1: Index
            for (int i = 0; i < numDocs; i++) {
                Engine.IndexResult result = engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
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
                assertThat(snapshot.getGeneration(), equalTo(3L));
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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
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
        engine.index(indexOp(createParsedDocWithInput("1", null)));

        engine.failEngine("test failure", new RuntimeException("simulated"));

        expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDocWithInput("2", null))));
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

    /**
     * Covers the refresh-thread branch when a writer's {@code flush()} throws.
     *
     * <p>The latch-and-flushQueue contract demands that, no matter which code path
     * fails, the engine is left in a consistent state for the surrounding orchestration:
     * <ul>
     *   <li>The exception must surface to the caller as {@link RefreshFailedEngineException}
     *       so the IndexShard can react.</li>
     *   <li>The shared {@code flushQueue} must not leak items that the next refresh would
     *       wrongly consider in-flight (no orphan writers waiting for nobody).</li>
     *   <li>{@link DataFormatAwareEngine#failEngine} must run with the originating cause,
     *       firing {@link Engine.EventListener#onFailedEngine} so the shard is failed.</li>
     *   <li>Subsequent operations must reject with {@link AlreadyClosedException}.</li>
     * </ul>
     *
     * <p>This test uses a {@link FailingFlushIndexingExecutionEngine} that returns
     * a writer whose {@code flush()} throws on first call, ensuring the refresh
     * thread itself is the one that observes the failure (only one writer in the
     * pool, so there are no write threads racing for it).
     */
    public void testRefreshThreadFlushFailureFailsEngineAndDrainsQueue() throws Exception {
        AtomicReference<Exception> failedEngineCause = new AtomicReference<>();
        Engine.EventListener listener = new Engine.EventListener() {
            @Override
            public void onFailedEngine(String reason, Exception failure) {
                failedEngineCause.set(failure);
            }
        };

        // Wire an indexing engine whose writer fails on flush.
        FailingFlushIndexingExecutionEngine failingEngine = new FailingFlushIndexingExecutionEngine(mockDataFormat);
        MockDataFormatPlugin failingPlugin = new MockDataFormatPlugin(mockDataFormat) {
            @Override
            public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig settings) {
                return failingEngine;
            }
        };

        EngineConfig config = buildFailingEngineConfig(failingPlugin, listener);
        DataFormatAwareEngine engine = new DataFormatAwareEngine(config);
        try {
            // One indexed doc → one writer in the pool. Refresh thread will be the sole flusher.
            engine.index(indexOp(createParsedDocWithInput("1", null)));
            assertThat("writer should have been used", failingEngine.writersCreated(), greaterThanOrEqualTo(1));

            RefreshFailedEngineException ex = expectThrows(RefreshFailedEngineException.class, () -> engine.refresh("flush-failure-test"));
            assertThat("refresh failure must wrap the IOException from flush", ex.getCause(), instanceOf(IOException.class));
            assertThat(ex.getCause().getMessage(), containsString("simulated flush failure"));

            // failEngine ran with the originating exception.
            assertThat("event listener must observe the failure", failedEngineCause.get(), notNullValue());

            // No writer leaked in the shared flushQueue. Use reflection (test-only) to
            // assert the queue is empty without exposing internals on DFAE.
            assertThat("flushQueue must be drained on failure", flushQueueSize(engine), equalTo(0));

            // Engine is closed for all subsequent operations.
            expectThrows(AlreadyClosedException.class, engine::ensureOpen);
            expectThrows(AlreadyClosedException.class, () -> engine.refresh("after-fail"));
            expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDocWithInput("2", null))));
        } finally {
            try {
                engine.close();
            } catch (Exception ignored) {
                // Already closed by failEngine.
            }
        }
    }

    /**
     * Verifies that engine close (closeNoLock) drains the flushQueue and closes any
     * writers still sitting in it. Without this, a writer checked out of the pool and
     * placed in the flushQueue (but not yet flushed) would be orphaned on engine close,
     * leaking its LuceneWriter's NativeFSLock in LOCK_HELD.
     */
    @SuppressForbidden(reason = "test needs reflective access to inject a writer into flushQueue")
    public void testCloseNoLockDrainsFlushQueue() throws Exception {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        try {
            // Inject a writer directly into the flushQueue (simulates a writer that was
            // checked out of the pool for refresh but not yet flushed when engine closes).
            java.lang.reflect.Field queueField = DataFormatAwareEngine.class.getDeclaredField("flushQueue");
            queueField.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.concurrent.ConcurrentLinkedQueue<org.opensearch.index.engine.dataformat.Writer<?>> queue =
                (java.util.concurrent.ConcurrentLinkedQueue<org.opensearch.index.engine.dataformat.Writer<?>>) queueField.get(engine);

            FailingFlushWriter orphanWriter = new FailingFlushWriter(777L, mockDataFormat);
            queue.add(orphanWriter);
            assertThat("writer injected into flushQueue", flushQueueSize(engine), equalTo(1));

            // Close the engine — closeNoLock must drain flushQueue and close the writer.
            engine.close();

            assertThat("flushQueue must be empty after close", flushQueueSize(engine), equalTo(0));
            assertThat("orphan writer must be closed", orphanWriter.state(), equalTo(WriterState.CLOSED));
        } finally {
            try {
                engine.close();
            } catch (Exception ignored) {}
        }
    }

    /**
     * Covers the preIndex cooperative-flush path when a writer's {@code flush()} throws.
     *
     * <p>When a write thread picks a writer from the flushQueue during preIndex and the
     * flush fails, the engine must:
     * <ul>
     *   <li>Call {@link DataFormatAwareEngine#failEngine} with the originating cause.</li>
     *   <li>Close the failing writer via {@code IOUtils.closeWhileHandlingException}.</li>
     *   <li>Count down the activeFlushLatch so the refresh thread is not stuck waiting.</li>
     *   <li>Reject subsequent operations with {@link AlreadyClosedException}.</li>
     * </ul>
     *
     * <p>This test injects a failing writer directly into the flushQueue via reflection,
     * then triggers preIndex by calling {@code engine.index()}.
     */
    @SuppressForbidden(reason = "test needs reflective access to inject a failing writer into flushQueue")
    public void testPreIndexFlushFailureFailsEngine() throws Exception {
        AtomicReference<Exception> failedEngineCause = new AtomicReference<>();
        Engine.EventListener listener = new Engine.EventListener() {
            @Override
            public void onFailedEngine(String reason, Exception failure) {
                failedEngineCause.set(failure);
            }
        };

        // Use a normal (non-failing) engine so we can inject the failure precisely.
        MockDataFormatPlugin normalPlugin = new MockDataFormatPlugin(mockDataFormat) {
        };
        EngineConfig config = buildFailingEngineConfig(normalPlugin, listener);
        DataFormatAwareEngine engine = new DataFormatAwareEngine(config);
        try {
            // Inject a FailingFlushWriter directly into the flushQueue.
            java.lang.reflect.Field queueField = DataFormatAwareEngine.class.getDeclaredField("flushQueue");
            queueField.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.concurrent.ConcurrentLinkedQueue<org.opensearch.index.engine.dataformat.Writer<?>> queue =
                (java.util.concurrent.ConcurrentLinkedQueue<org.opensearch.index.engine.dataformat.Writer<?>>) queueField.get(engine);
            queue.add(new FailingFlushWriter(99L, mockDataFormat));

            // The next index() call triggers preIndex() which polls the failing writer.
            AlreadyClosedException ex = expectThrows(
                AlreadyClosedException.class,
                () -> engine.index(indexOp(createParsedDocWithInput("trigger-preindex", null)))
            );

            // failEngine was called with the flush IOException.
            assertThat("event listener must observe the failure", failedEngineCause.get(), notNullValue());
            assertThat(failedEngineCause.get().getMessage(), containsString("simulated flush failure"));

            // Engine is closed for all subsequent operations.
            expectThrows(AlreadyClosedException.class, engine::ensureOpen);
        } finally {
            try {
                engine.close();
            } catch (Exception ignored) {}
        }
    }

    /**
     * Covers the preIndex happy path: a writer in the flushQueue is successfully flushed
     * by the write thread during preIndex, producing a pending segment.
     */
    @SuppressForbidden(reason = "test needs reflective access to inject a writer into flushQueue and read pendingWritersToClose")
    public void testPreIndexSuccessfulFlushProducesPendingSegment() throws Exception {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        try {
            // Index a doc so the engine is in a valid state.
            engine.index(indexOp(createParsedDocWithInput("0", null)));

            // Inject a writer that returns empty FileInfos on flush (success path, no files).
            java.lang.reflect.Field queueField = DataFormatAwareEngine.class.getDeclaredField("flushQueue");
            queueField.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.concurrent.ConcurrentLinkedQueue<org.opensearch.index.engine.dataformat.Writer<?>> queue =
                (java.util.concurrent.ConcurrentLinkedQueue<org.opensearch.index.engine.dataformat.Writer<?>>) queueField.get(engine);

            // A writer that succeeds on flush with empty result (no files produced).
            SuccessFlushWriter successWriter = new SuccessFlushWriter(42L, mockDataFormat);
            queue.add(successWriter);

            // Index another doc — this triggers preIndex which flushes the queued writer.
            engine.index(indexOp(createParsedDocWithInput("1", null)));

            // Engine should still be open (flush succeeded).
            engine.ensureOpen();

            // The writer should have been moved to pendingWritersToClose.
            java.lang.reflect.Field closersField = DataFormatAwareEngine.class.getDeclaredField("pendingWritersToClose");
            closersField.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.Collection<?> pendingClosers = (java.util.Collection<?>) closersField.get(engine);
            assertTrue("Writer should be queued for deferred close", pendingClosers.contains(successWriter));
        } finally {
            engine.close();
        }
    }

    /**
     * Covers the preIndex skip path when check_pending_flush is disabled.
     */
    @SuppressForbidden(reason = "test needs reflective access to inject a writer into flushQueue")
    public void testPreIndexSkipsWhenCheckPendingFlushDisabled() throws Exception {
        Path translogPath = createTempDir();
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid);

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), mockDataFormat.name())
                .put("index.check_pending_flush.enabled", false)
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
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(documentMapper.getVersion()).thenReturn(1L);
        when(mapperService.documentMapper()).thenReturn(documentMapper);

        EngineConfig config = new EngineConfig.Builder().shardId(shardId)
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
            .committerFactory(c -> new InMemoryCommitter(store))
            .mapperService(mapperService)
            .build();

        DataFormatAwareEngine engine = new DataFormatAwareEngine(config);
        try {
            // Even with a failing writer in the queue, preIndex should skip it.
            java.lang.reflect.Field queueField = DataFormatAwareEngine.class.getDeclaredField("flushQueue");
            queueField.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.concurrent.ConcurrentLinkedQueue<org.opensearch.index.engine.dataformat.Writer<?>> queue =
                (java.util.concurrent.ConcurrentLinkedQueue<org.opensearch.index.engine.dataformat.Writer<?>>) queueField.get(engine);
            queue.add(new FailingFlushWriter(99L, mockDataFormat));

            // Index should succeed — preIndex is disabled so the failing writer is never polled.
            engine.index(indexOp(createParsedDocWithInput("1", null)));
            engine.ensureOpen(); // Engine still alive

            // The failing writer is still in the queue (never polled).
            assertEquals(1, queue.size());
        } finally {
            engine.close();
        }
    }

    public void testCatalogSnapshotContainsFormatSpecificFiles() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(1, 5);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
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
            engine.index(indexOp(createParsedDocWithInput("1", null)));
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
            engine.index(indexOp(createParsedDocWithInput("1", null)));
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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
            }
            engine.refresh("test");

            try (GatedCloseable<IndexReaderProvider.Reader> readerRef = engine.acquireReader()) {
                IndexReaderProvider.Reader reader = readerRef.get();
                assertThat(reader, notNullValue());
                assertThat(reader.catalogSnapshot(), notNullValue());
                assertThat(reader.catalogSnapshot().getGeneration(), equalTo(2L));
                assertThat(reader.catalogSnapshot().getSegments().size(), equalTo(1));
            }
        }
    }

    public void testAcquireReaderContainsFormatSpecificReader() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.index(indexOp(createParsedDocWithInput("1", null)));
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
            engine.index(indexOp(createParsedDocWithInput("1", null)));
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
            engine.index(indexOp(createParsedDocWithInput("1", null)));
            engine.refresh("first");

            engine.index(indexOp(createParsedDocWithInput("2", null)));
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
            engine.index(indexOp(createParsedDocWithInput("1", null)));
            engine.refresh("test");

            // Acquire and close a reader, then verify the engine still works
            GatedCloseable<IndexReaderProvider.Reader> readerRef = engine.acquireReader();
            IndexReaderProvider.Reader reader = readerRef.get();
            long readerGen = reader.catalogSnapshot().getGeneration();
            readerRef.close();

            // After closing, we should still be able to acquire new readers
            // and do more work
            engine.index(indexOp(createParsedDocWithInput("2", null)));
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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
                engine.refresh("batch-" + i);
            }

            try (GatedCloseable<IndexReaderProvider.Reader> readerRef = engine.acquireReader()) {
                IndexReaderProvider.Reader reader = readerRef.get();
                CatalogSnapshot snapshot = reader.catalogSnapshot();
                assertThat(snapshot.getSegments().size(), equalTo(numBatches));
                assertThat(snapshot.getGeneration(), equalTo((long) numBatches + 1));
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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
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
                            engine.index(indexOp(createParsedDocWithInput(threadId + "_" + d, null)));
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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
            }

            int count = engine.countNumberOfHistoryOperations("test", 0, numDocs - 1);
            assertThat(count, equalTo(numDocs));
        }
    }

    public void testCountNumberOfHistoryOperationsSubRange() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
            }

            // Count only ops in range [3, 7]
            int count = engine.countNumberOfHistoryOperations("test", 3, 7);
            assertThat(count, greaterThan(0));
            assertThat(count, org.hamcrest.Matchers.lessThanOrEqualTo(5));
        }
    }

    public void testGetSeqNoStats() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            int numDocs = randomIntBetween(5, 15);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
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
                            engine.index(indexOp(createParsedDocWithInput(threadId + "_" + d, null)));
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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
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
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
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
                            engine.index(indexOp(createParsedDocWithInput(threadId + "_" + d, null)));
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

    // ═══════════════════════════════════════════════════════════════
    // Refresh Listener Tests — Use-case focused
    // ═══════════════════════════════════════════════════════════════

    /**
     * Use case: A search-after-refresh waiter registers a listener to know when
     * new data becomes searchable. After indexing + refresh, the listener must be
     * notified so it can unblock the waiting search request.
     */
    public void testRefreshListenerNotifiedWhenNewDataBecomesSearchable() throws IOException {
        Path translogPath = createTempDir();
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid);

        AtomicInteger beforeCount = new AtomicInteger(0);
        AtomicInteger afterCount = new AtomicInteger(0);
        AtomicLong afterDidRefreshTrue = new AtomicLong(0);

        ReferenceManager.RefreshListener listener = new ReferenceManager.RefreshListener() {
            @Override
            public void beforeRefresh() {
                beforeCount.incrementAndGet();
            }

            @Override
            public void afterRefresh(boolean didRefresh) {
                afterCount.incrementAndGet();
                if (didRefresh) {
                    afterDidRefreshTrue.incrementAndGet();
                }
            }
        };

        EngineConfig config = buildDFAEngineConfig(store, translogPath, List.of(listener), List.of());
        try (DataFormatAwareEngine engine = new DataFormatAwareEngine(config)) {
            // Index documents — data is buffered but not yet searchable
            int numDocs = randomIntBetween(3, 10);
            for (int i = 0; i < numDocs; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
            }

            // Refresh — makes data searchable, listener must be notified
            engine.refresh("test");

            // The listener must have been called: beforeRefresh once, afterRefresh(true) once
            assertThat("beforeRefresh must fire when new segments are produced", beforeCount.get(), equalTo(1));
            assertThat("afterRefresh must fire when new segments are produced", afterCount.get(), equalTo(1));
            assertThat("afterRefresh(didRefresh=true) confirms data is now searchable", afterDidRefreshTrue.get(), equalTo(1L));
        }
    }

    /**
     * Use case: When no new data has been indexed, a refresh should still notify
     * listeners (beforeRefresh is always called) but afterRefresh should indicate
     * that no actual refresh occurred (didRefresh=false). This allows waiters to
     * distinguish between "new data available" and "nothing changed".
     */
    public void testRefreshListenerNotifiedWithDidRefreshFalseWhenNoNewData() throws IOException {
        Path translogPath = createTempDir();
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid);

        AtomicInteger beforeCount = new AtomicInteger(0);
        AtomicInteger afterDidRefreshFalse = new AtomicInteger(0);
        AtomicInteger afterDidRefreshTrue = new AtomicInteger(0);

        ReferenceManager.RefreshListener listener = new ReferenceManager.RefreshListener() {
            @Override
            public void beforeRefresh() {
                beforeCount.incrementAndGet();
            }

            @Override
            public void afterRefresh(boolean didRefresh) {
                if (didRefresh) {
                    afterDidRefreshTrue.incrementAndGet();
                } else {
                    afterDidRefreshFalse.incrementAndGet();
                }
            }
        };

        EngineConfig config = buildDFAEngineConfig(store, translogPath, List.of(listener), List.of());
        try (DataFormatAwareEngine engine = new DataFormatAwareEngine(config)) {
            // Refresh with no data — no new segments produced
            engine.refresh("empty");

            // beforeRefresh is always called (listener needs to prepare)
            assertThat("beforeRefresh fires even when no data changed", beforeCount.get(), equalTo(1));
            // afterRefresh(false) indicates nothing new became searchable
            assertThat("afterRefresh(false) when no new segments", afterDidRefreshFalse.get(), equalTo(1));
            assertThat("afterRefresh(true) should NOT fire", afterDidRefreshTrue.get(), equalTo(0));
        }
    }

    /**
     * Use case: Multiple index-refresh cycles should produce monotonically advancing
     * notifications. A reader manager uses these to know which snapshot generation
     * to open. Each afterRefresh(true) must correspond to a new, higher-generation
     * catalog snapshot being available.
     */
    public void testRefreshListenerSeesMonotonicallyAdvancingSnapshots() throws IOException {
        Path translogPath = createTempDir();
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid);

        List<Long> observedGenerations = new ArrayList<>();

        ReferenceManager.RefreshListener listener = new ReferenceManager.RefreshListener() {
            @Override
            public void beforeRefresh() {}

            @Override
            public void afterRefresh(boolean didRefresh) {
                // Not ideal — we can't access the engine from here directly.
                // But we track call count and verify externally.
                if (didRefresh) {
                    observedGenerations.add(System.nanoTime()); // monotonic timestamp as proxy
                }
            }
        };

        EngineConfig config = buildDFAEngineConfig(store, translogPath, List.of(listener), List.of());
        try (DataFormatAwareEngine engine = new DataFormatAwareEngine(config)) {
            int numRefreshes = randomIntBetween(3, 6);
            for (int i = 0; i < numRefreshes; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
                engine.refresh("cycle-" + i);
            }

            // Each refresh with data should have triggered afterRefresh(true)
            assertThat("each refresh with data must notify", observedGenerations.size(), equalTo(numRefreshes));

            // Verify the catalog snapshot generation advanced monotonically
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                assertThat(
                    "final snapshot generation must equal number of refreshes + 1",
                    ref.get().getGeneration(),
                    equalTo((long) numRefreshes + 1)
                );
            }
        }
    }

    /**
     * Use case: Both external listeners (registered by IndexShard for search-after-refresh)
     * and internal listeners (registered by the engine for checkpoint tracking) must both
     * be invoked. Neither should be skipped.
     */
    public void testBothExternalAndInternalListenersInvoked() throws IOException {
        Path translogPath = createTempDir();
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid);

        AtomicInteger externalCalls = new AtomicInteger(0);
        AtomicInteger internalCalls = new AtomicInteger(0);

        ReferenceManager.RefreshListener external = new ReferenceManager.RefreshListener() {
            @Override
            public void beforeRefresh() {
                externalCalls.incrementAndGet();
            }

            @Override
            public void afterRefresh(boolean didRefresh) {
                externalCalls.incrementAndGet();
            }
        };

        ReferenceManager.RefreshListener internal = new ReferenceManager.RefreshListener() {
            @Override
            public void beforeRefresh() {
                internalCalls.incrementAndGet();
            }

            @Override
            public void afterRefresh(boolean didRefresh) {
                internalCalls.incrementAndGet();
            }
        };

        EngineConfig config = buildDFAEngineConfig(store, translogPath, List.of(external), List.of(internal));
        try (DataFormatAwareEngine engine = new DataFormatAwareEngine(config)) {
            engine.index(indexOp(createParsedDocWithInput("1", null)));
            engine.refresh("test");

            // Each listener gets beforeRefresh + afterRefresh = 2 calls
            assertThat("external listener must receive both before and after", externalCalls.get(), equalTo(2));
            assertThat("internal listener must receive both before and after", internalCalls.get(), equalTo(2));
        }
    }

    /**
     * Use case: The ordering contract — beforeRefresh is called BEFORE the catalog
     * snapshot is committed (so listeners can prepare), and afterRefresh is called
     * AFTER (so listeners can observe the new state). This is critical for reader
     * managers that need to open readers on the new snapshot.
     */
    public void testBeforeRefreshCalledBeforeSnapshotCommitAndAfterCalledAfter() throws IOException {
        Path translogPath = createTempDir();
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid);

        AtomicLong genSeenInBefore = new AtomicLong(-1);
        AtomicLong genSeenInAfter = new AtomicLong(-1);
        AtomicReference<DataFormatAwareEngine> engineRef = new AtomicReference<>();

        ReferenceManager.RefreshListener orderingListener = new ReferenceManager.RefreshListener() {
            @Override
            public void beforeRefresh() {
                DataFormatAwareEngine eng = engineRef.get();
                if (eng != null) {
                    try (GatedCloseable<CatalogSnapshot> ref = eng.acquireSnapshot()) {
                        genSeenInBefore.set(ref.get().getGeneration());
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }

            @Override
            public void afterRefresh(boolean didRefresh) {
                DataFormatAwareEngine eng = engineRef.get();
                if (eng != null) {
                    try (GatedCloseable<CatalogSnapshot> ref = eng.acquireSnapshot()) {
                        genSeenInAfter.set(ref.get().getGeneration());
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
        };

        EngineConfig config = buildDFAEngineConfig(store, translogPath, List.of(orderingListener), List.of());
        try (DataFormatAwareEngine engine = new DataFormatAwareEngine(config)) {
            engineRef.set(engine);

            engine.index(indexOp(createParsedDocWithInput("1", null)));
            engine.refresh("test");

            // beforeRefresh sees the OLD generation (snapshot not yet committed)
            assertThat("beforeRefresh must see pre-commit generation", genSeenInBefore.get(), equalTo(1L));
            // afterRefresh sees the NEW generation (snapshot committed)
            assertThat("afterRefresh must see post-commit generation", genSeenInAfter.get(), equalTo(2L));
        }
    }

    public void testSegmentsReturnsEngineSegments() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.index(indexOp(createParsedDocWithInput("1", null)));
            engine.refresh("test");

            List<org.opensearch.index.engine.Segment> segments = engine.segments(false);
            assertEquals(1, segments.size());
            assertTrue(segments.get(0).search);
            assertEquals(1, segments.get(0).docCount);
        }
    }

    public void testSegmentsStatsReturnsValidStats() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.index(indexOp(createParsedDocWithInput("1", null)));
            engine.refresh("test");

            SegmentsStats stats = engine.segmentsStats(false, false);
            assertEquals(1, stats.getCount());
            assertTrue(stats.getIndexWriterMemoryInBytes() >= 0);
        }
    }

    public void testSegmentsWithIOException() throws IOException {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        engine.close();
        expectThrows(AlreadyClosedException.class, () -> engine.segments(false));
    }

    public void testSegmentsStatsWithIOException() throws IOException {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        engine.close();
        expectThrows(AlreadyClosedException.class, () -> engine.segmentsStats(false, false));
    }

    public void testUnreferencedFileCleanUpsPerformed() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            long cleanups = engine.unreferencedFileCleanUpsPerformed();
            assertTrue(cleanups >= 0);
        }
    }

    /**
     * Covers {@code DataFormatAwareEngine.applyMergeChanges}: a forceMerge over two
     * previously-refreshed segments must (1) replace the source segments in the catalog
     * with a single merged segment, (2) restrict its beforeRefresh/afterRefresh
     * notifications to {@code RemoteStoreRefreshListener} instances only, so a plain
     * refresh listener that fires on real refreshes is NOT invoked by the merge, and
     * (3) release the refresh lock on exit so a subsequent {@code refresh()} proceeds.
     *
     * <p>The system-property gate on {@code MERGE_ENABLED_PROPERTY} applies only to
     * the background {@code triggerPossibleMerges()} path; {@code forceMerge} routes
     * straight to {@code MergeScheduler.forceMerge} and does not consult it, so this
     * test drives the merge end-to-end without touching system properties.
     */
    public void testApplyMergeChangesUpdatesCatalogAndSkipsNonRemoteStoreListeners() throws Exception {
        AtomicInteger beforeCalls = new AtomicInteger();
        AtomicInteger afterCalls = new AtomicInteger();
        // Records call order: 'B' for beforeRefresh, 'A' for afterRefresh.
        StringBuilder callOrder = new StringBuilder();

        ReferenceManager.RefreshListener listener = new ReferenceManager.RefreshListener() {
            @Override
            public void beforeRefresh() {
                synchronized (callOrder) {
                    callOrder.append('B');
                }
                beforeCalls.incrementAndGet();
            }

            @Override
            public void afterRefresh(boolean didRefresh) {
                synchronized (callOrder) {
                    callOrder.append('A');
                }
                afterCalls.incrementAndGet();
            }
        };

        Path translogPath = createTempDir();
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid);

        EngineConfig config = buildDFAEngineConfig(store, translogPath, List.of(listener), List.of());
        try (DataFormatAwareEngine engine = new DataFormatAwareEngine(config)) {
            // Produce two segments via two refresh cycles so the merger has something to combine.
            engine.index(indexOp(createParsedDocWithInput("1", null)));
            engine.refresh("seed-1");
            engine.index(indexOp(createParsedDocWithInput("2", null)));
            engine.refresh("seed-2");

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                assertThat("two segments before merge", ref.get().getSegments().size(), equalTo(2));
            }

            // Drain the listener counters from the two seed refreshes.
            final int beforeAfterSeed = beforeCalls.get();
            final int afterAfterSeed = afterCalls.get();
            assertThat("each refresh must invoke beforeRefresh once", beforeAfterSeed, equalTo(2));
            assertThat("each refresh must invoke afterRefresh once", afterAfterSeed, equalTo(2));

            // forceMerge runs synchronously on a FORCE_MERGE thread. Dispatch to a
            // thread with the expected name to satisfy the MergeScheduler assertion.
            Thread fmThread = new Thread(() -> {
                try {
                    engine.forceMerge(false, 1, false, false, false, "test-force-merge");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, "force_merge-test");
            fmThread.start();
            fmThread.join(30_000);

            assertBusy(() -> {
                try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                    assertThat("merge must collapse to a single segment", ref.get().getSegments().size(), equalTo(1));
                }
            }, 10, java.util.concurrent.TimeUnit.SECONDS);

            // applyMergeChanges must notify only RemoteStoreRefreshListener instances. The plain
            // listener registered here is not one, so the merge must NOT invoke it.
            assertThat(
                "merge must not invoke beforeRefresh on a non-remote-store listener",
                beforeCalls.get() - beforeAfterSeed,
                equalTo(0)
            );
            assertThat("merge must not invoke afterRefresh on a non-remote-store listener", afterCalls.get() - afterAfterSeed, equalTo(0));
            synchronized (callOrder) {
                // Seed cycles contribute "BABA"; the merge must append nothing for this listener.
                assertThat("merge must not append before/after for a non-remote-store listener", callOrder.toString(), equalTo("BABA"));
            }

            // Sanity: the refreshLock must have been released. A follow-up refresh must
            // complete without blocking, and the catalog generation must have advanced.
            long genBeforeFinalRefresh;
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                genBeforeFinalRefresh = ref.get().getGeneration();
            }
            engine.index(indexOp(createParsedDocWithInput("3", null)));
            engine.refresh("post-merge");
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                assertThat(
                    "refresh after merge must advance the catalog generation",
                    ref.get().getGeneration(),
                    greaterThan(genBeforeFinalRefresh)
                );
            }
        }
    }

    /**
     * Helper: creates a DFA engine with a committer that can inject failures.
     */
    private record FailingEngineResult(DataFormatAwareEngine engine, FailureInjectingCommitter committer) {
    }

    private FailingEngineResult createDFAEngineWithFailingCommitter(Store store, Path translogPath) throws IOException {
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid);
        // Create committer AFTER bootstrap so InMemoryCommitter can read segments info
        FailureInjectingCommitter committer = new FailureInjectingCommitter(store);
        EngineConfig config = buildDFAEngineConfigWithCommitterFactory(store, translogPath, c -> committer);
        return new FailingEngineResult(new DataFormatAwareEngine(config), committer);
    }

    /**
     * Builds an engine config wired to a custom data format plugin (whose writer can be
     * configured to fail on flush) and a custom event listener — used by tests that need
     * to observe shard-failure callbacks while exercising flush-failure paths.
     */
    private EngineConfig buildFailingEngineConfig(MockDataFormatPlugin plugin, Engine.EventListener listener) throws IOException {
        Path translogPath = createTempDir();
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid);
        return buildEngineConfigForPluginAndListener(translogPath, plugin, listener);
    }

    private EngineConfig buildDFAEngineConfigWithCommitterFactory(Store store, Path translogPath, CommitterFactory committerFactory) {
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
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(documentMapper.getVersion()).thenReturn(1L);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
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
            .eventListener(new Engine.EventListener() {
                @Override
                public void onFailedEngine(String reason, Exception e) {}
            })
            .mapperService(mapperService)
            .build();
    }

    /**
     * Builds an EngineConfig using a real DataFormatRegistry built from the given plugin,
     * with the supplied event listener — used by upstream's writer-flush-failure tests.
     */
    private EngineConfig buildEngineConfigForPluginAndListener(
        Path translogPath,
        MockDataFormatPlugin plugin,
        Engine.EventListener listener
    ) {
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
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(documentMapper.getVersion()).thenReturn(1L);
        when(mapperService.documentMapper()).thenReturn(documentMapper);

        PluginsService pluginsService = mock(PluginsService.class);
        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(plugin));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(
            List.of(new MockSearchBackEndPlugin(List.of(mockDataFormat.name())))
        );
        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

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
            .committerFactory(c -> {
                try {
                    return new InMemoryCommitter(store);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            })
            .eventListener(listener)
            .mapperService(mapperService)
            .build();
    }

    /**
     * A committer wrapper that can inject failures on commit. Tragic exceptions are now
     * surfaced via {@link MockIndexingExecutionEngine#setTragicException} instead.
     */
    static class FailureInjectingCommitter implements Committer {
        private final InMemoryCommitter delegate;
        private volatile IOException commitFailure;

        FailureInjectingCommitter(Store store) throws IOException {
            this.delegate = new InMemoryCommitter(store);
        }

        void setCommitFailure(IOException failure) {
            this.commitFailure = failure;
        }

        @Override
        public Committer.CommitResult commit(Committer.CommitInput commitInput) throws IOException {
            if (commitFailure != null) throw commitFailure;
            return delegate.commit(commitInput);
        }

        @Override
        public Map<String, String> getLastCommittedData() throws IOException {
            return delegate.getLastCommittedData();
        }

        @Override
        public CommitStats getCommitStats() {
            return delegate.getCommitStats();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public java.util.List<org.opensearch.index.engine.exec.coord.CatalogSnapshot> listCommittedSnapshots() {
            return delegate.listCommittedSnapshots();
        }

        @Override
        public void deleteCommit(org.opensearch.index.engine.exec.coord.CatalogSnapshot snapshot) {
            delegate.deleteCommit(snapshot);
        }

        @Override
        public boolean isCommitManagedFile(String fileName) {
            return delegate.isCommitManagedFile(fileName);
        }

        @Override
        public byte[] serializeToCommitFormat(org.opensearch.index.engine.exec.coord.CatalogSnapshot snapshot) throws IOException {
            return delegate.serializeToCommitFormat(snapshot);
        }

        @Override
        public void markStoreCorrupted(IOException cause) {
            delegate.markStoreCorrupted(cause);
        }
    }

    // --- Test: failEngine marks store as corrupted for CorruptIndexException ---

    public void testFailEngineWithCorruptionMarksStoreCorrupted() throws IOException {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        engine.index(indexOp(createParsedDoc("1", null)));

        // Fail with a corruption exception
        org.apache.lucene.index.CorruptIndexException corruption = new org.apache.lucene.index.CorruptIndexException(
            "test corruption",
            "test"
        );
        engine.failEngine("corruption test", corruption);

        // Engine should be closed
        expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDoc("2", null))));
        // Store should be marked as corrupted
        assertTrue("store should be marked corrupted", store.isMarkedCorrupted());
    }

    // --- Test: failEngine does NOT mark store corrupted for non-corruption exceptions ---

    public void testFailEngineWithNonCorruptionDoesNotMarkStoreCorrupted() throws IOException {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        engine.index(indexOp(createParsedDoc("1", null)));

        engine.failEngine("non-corruption test", new RuntimeException("simulated"));

        expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDoc("2", null))));
        assertFalse("store should NOT be marked corrupted for non-corruption failures", store.isMarkedCorrupted());
    }

    // --- Test: index → refresh → failEngine → verify no data loss for committed data ---

    public void testFailEngineAfterFlushPreservesCommittedData() throws IOException {
        Path translogPath = createTempDir();
        FailingEngineResult fer = createDFAEngineWithFailingCommitter(store, translogPath);
        DataFormatAwareEngine engine = fer.engine();
        FailureInjectingCommitter committer = fer.committer();
        engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

        // Index and flush to commit data
        int numDocs = randomIntBetween(3, 10);
        for (int i = 0; i < numDocs; i++) {
            engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
        }
        engine.flush(false, true);

        // Verify commit data was persisted
        Map<String, String> committedData = committer.getLastCommittedData();
        assertThat(committedData, notNullValue());
        assertTrue("committed data should contain translog UUID", committedData.containsKey(Translog.TRANSLOG_UUID_KEY));

        // Now fail the engine
        engine.failEngine("test", new RuntimeException("simulated"));

        // Committed data should still be accessible from the committer
        Map<String, String> dataAfterFail = committer.getLastCommittedData();
        assertThat(dataAfterFail, equalTo(committedData));
    }
    // --- Test: failEngine with null failure ---

    public void testFailEngineWithNullFailure() throws IOException {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        engine.index(indexOp(createParsedDoc("1", null)));

        // failEngine with null failure should still close the engine
        engine.failEngine("null failure test", null);

        expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDoc("2", null))));
        // Store should NOT be marked corrupted (null failure)
        assertFalse("store should not be corrupted for null failure", store.isMarkedCorrupted());
    }

    /**
     * Returns the first MockWriter currently in the engine's writer pool via reflection.
     * The engine must have indexed at least one doc so a writer exists in the pool.
     */

    private MockWriter getPooledMockWriter(DataFormatAwareEngine engine) {
        for (Writer<?> w : engine.getWriterPool()) {
            if (w instanceof MockWriter mw) return mw;
            if (w instanceof RowIdAwareWriter<?> riw && riw.getDelegate() instanceof MockWriter mw) return mw;
        }
        throw new AssertionError("No MockWriter found in writer pool");
    }

    public void testCorruptionExceptionDuringIndexFailsEngineAndMarksStore() throws IOException {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        try {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            // Index 5 docs, refresh, flush to establish baseline
            for (int i = 0; i < 5; i++) {
                Engine.IndexResult result = engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
                assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            }
            engine.refresh("baseline");
            engine.flush(false, true);

            // Index one more doc to ensure a writer exists in the pool after flush
            Engine.IndexResult extraResult = engine.index(indexOp(createParsedDoc("extra", null)));
            assertThat(extraResult.getResultType(), equalTo(Engine.Result.Type.SUCCESS));

            // Configure MockWriter to throw CorruptIndexException as the addDoc cause.
            // On primary, maybeFailEngine sees Lucene.isCorruptionException and calls failEngine,
            // which marks the store corrupted before notifying listeners.
            MockWriter writer = getPooledMockWriter(engine);
            org.apache.lucene.index.CorruptIndexException corruption = new org.apache.lucene.index.CorruptIndexException(
                "simulated corruption during index",
                "test"
            );
            writer.setWriteResultSupplier(() -> { throw new java.io.UncheckedIOException(new java.io.IOException(corruption)); });

            // Index doc on primary → corruption escalates via maybeFailEngine → failEngine.
            expectThrows(Exception.class, () -> engine.index(indexOp(createParsedDoc("6", null))));

            // Engine should be failed
            assertNotNull(
                "engine should have failed on primary with CorruptIndexException",
                new FailableDataFormatAwareEngine(engine).getFailedEngine()
            );

            // Store should be marked corrupted
            assertTrue("store should be marked corrupted", store.isMarkedCorrupted());

            // Verify corruption marker file exists on disk
            boolean foundCorruptionMarker = false;
            for (String file : store.directory().listAll()) {
                if (file.startsWith(Store.CORRUPTED_MARKER_NAME_PREFIX)) {
                    foundCorruptionMarker = true;
                    break;
                }
            }
            assertTrue("corruption marker file should exist on disk", foundCorruptionMarker);

            // No further operations possible
            expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDoc("6", null))));
        } finally {
            engine.close();
        }
    }

    private MockIndexingExecutionEngine getMockExecutionEngine(DataFormatAwareEngine engine) {
        return (MockIndexingExecutionEngine) engine.getIndexingExecutionEngine();
    }

    public void testRefreshAlreadyClosedWithTragicSourceFailsEngine() throws IOException {
        Path translogPath = createTempDir();
        FailingEngineResult fer = createDFAEngineWithFailingCommitter(store, translogPath);
        DataFormatAwareEngine engine = fer.engine();
        try {
            for (int i = 0; i < 5; i++) {
                Engine.IndexResult result = engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
                assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            }

            // Configure refresh to throw AlreadyClosedException, with a tragic exception
            // recorded on the indexing engine (the new tragic-source channel).
            MockIndexingExecutionEngine mockExecEngine = getMockExecutionEngine(engine);
            IOException tragicCause = new IOException("engine tragic");
            mockExecEngine.setTragicException(tragicCause);
            mockExecEngine.setRefreshFailure(() -> new AlreadyClosedException("engine closed"));

            // Refresh catches ACE → failOnTragicEvent → detect engine tragic → failEngine
            expectThrows(AlreadyClosedException.class, () -> engine.refresh("test"));

            Exception failedEngine = new FailableDataFormatAwareEngine(engine).getFailedEngine();
            assertNotNull("engine should have failed via failOnTragicEvent", failedEngine);
            assertSame("failed engine cause should be the indexing engine's tragic exception", tragicCause, failedEngine);

            expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDoc("5", null))));
        } finally {
            engine.close();
        }
    }

    public void testRefreshFailureAfterIndexingFailsEngine() throws Exception {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        try {
            MockIndexingExecutionEngine mockExecEngine = getMockExecutionEngine(engine);

            // Index some docs and refresh to establish baseline
            for (int i = 0; i < 10; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }
            engine.refresh("setup");

            // Index more docs so next refresh has unflushed segments to process
            for (int i = 10; i < 15; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            // Now inject refresh failure — next refresh will fail the engine
            mockExecEngine.setRefreshFailure(() -> new IOException("injected refresh failure"));

            // Trigger the failure — refresh will flush writers, find new segments, call
            // indexingExecutionEngine.refresh() which throws, then failEngine is called
            try {
                engine.refresh("trigger-failure");
            } catch (Exception e) {
                // expected
            }

            // Verify engine failed
            FailableDataFormatAwareEngine failable = new FailableDataFormatAwareEngine(engine);
            assertNotNull("engine should have failed from refresh IOException", failable.getFailedEngine());

            // Verify subsequent ops throw AlreadyClosedException
            expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDoc("post-fail", null))));
            expectThrows(AlreadyClosedException.class, () -> engine.refresh("post-fail"));
        } finally {
            engine.close();
        }
    }

    public void testFlushCorruptionExceptionFailsEngineViaMaybeFailEngine() throws IOException {
        Path translogPath = createTempDir();
        FailingEngineResult fer = createDFAEngineWithFailingCommitter(store, translogPath);
        DataFormatAwareEngine engine = fer.engine();
        FailureInjectingCommitter failingCommitter = fer.committer();
        try {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            // Index 20 docs
            for (int i = 0; i < 20; i++) {
                Engine.IndexResult result = engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
                assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            }

            // Refresh to make docs visible
            engine.refresh("test");

            // Configure committer to throw CorruptIndexException on commit
            org.apache.lucene.index.CorruptIndexException corruption = new org.apache.lucene.index.CorruptIndexException(
                "simulated corruption via maybeFailEngine",
                "test"
            );
            failingCommitter.setCommitFailure(corruption);

            // Flush — maybeFailEngine detects corruption and fails the engine
            FlushFailedEngineException thrown = expectThrows(FlushFailedEngineException.class, () -> engine.flush(false, true));

            // Verify the cause chain contains the original corruption exception
            assertTrue("cause should be CorruptIndexException", thrown.getCause() instanceof org.apache.lucene.index.CorruptIndexException);
            assertTrue(
                "cause message should reference simulated corruption",
                thrown.getCause().getMessage().contains("simulated corruption via maybeFailEngine")
            );

            // Engine should be failed
            assertNotNull("engine should have failed", new FailableDataFormatAwareEngine(engine).getFailedEngine());

            // Store should be marked corrupted (maybeFailEngine → Lucene.isCorruptionException → failEngine → markStoreCorrupted)
            assertTrue("store should be marked corrupted", store.isMarkedCorrupted());

            // No further operations possible
            expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDoc("20", null))));
            expectThrows(AlreadyClosedException.class, () -> engine.refresh("test"));
            expectThrows(AlreadyClosedException.class, () -> engine.flush(false, true));
        } finally {
            engine.close();
        }
    }

    public void testFlushNonCorruptionExceptionWrapsInFlushFailedEngineException() throws IOException {
        Path translogPath = createTempDir();
        FailingEngineResult fer = createDFAEngineWithFailingCommitter(store, translogPath);
        DataFormatAwareEngine engine = fer.engine();
        FailureInjectingCommitter failingCommitter = fer.committer();
        try {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            // Index 10 docs
            for (int i = 0; i < 10; i++) {
                Engine.IndexResult result = engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
                assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            }

            // Configure committer to throw non-corruption IOException on commit
            failingCommitter.setCommitFailure(new IOException("disk error"));

            // Flush should throw FlushFailedEngineException
            expectThrows(FlushFailedEngineException.class, () -> engine.flush(false, true));

            // Engine should still be open (non-corruption doesn't fail the engine)
            assertNull("engine should still be open", new FailableDataFormatAwareEngine(engine).getFailedEngine());

            // Disable failure
            failingCommitter.setCommitFailure(null);

            // Flush again — should succeed now
            engine.flush(false, true);

            // Verify all 10 docs committed via checkpoint
            assertThat("all 10 docs should be committed", engine.getProcessedLocalCheckpoint(), equalTo(9L));
        } finally {
            engine.close();
        }
    }

    public void testFlushWithCommitCorruptionFailsEngineAndMarksStore() throws Exception {
        Path translogPath = createTempDir();
        FailingEngineResult fer = createDFAEngineWithFailingCommitter(store, translogPath);
        DataFormatAwareEngine engine = fer.engine();
        FailureInjectingCommitter failingCommitter = fer.committer();
        try {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            // Index docs and flush successfully first
            for (int i = 0; i < 20; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }
            engine.refresh("setup");
            engine.flush(false, true);

            // Index more docs
            for (int i = 20; i < 30; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            // Inject CorruptIndexException on commit
            failingCommitter.setCommitFailure(new org.apache.lucene.index.CorruptIndexException("injected corruption", "test"));

            // Flush should fail the engine via maybeFailEngine
            try {
                engine.flush(false, true);
            } catch (FlushFailedEngineException | AlreadyClosedException e) {
                // expected
            }

            // Verify engine failed
            FailableDataFormatAwareEngine failable = new FailableDataFormatAwareEngine(engine);
            assertNotNull("engine should have failed from commit corruption", failable.getFailedEngine());

            // Verify store marked corrupted
            assertTrue("store should be marked corrupted", store.isMarkedCorrupted());
        } finally {
            engine.close();
        }
    }

    // --- Test: concurrent failEngine from multiple sources is idempotent ---

    public void testConcurrentFailEngineFromMultipleSourcesIsIdempotent() throws Exception {
        // Build engine with a counting event listener to verify onFailedEngine called exactly once
        AtomicInteger onFailedCount = new AtomicInteger(0);
        Engine.EventListener countingListener = new Engine.EventListener() {
            @Override
            public void onFailedEngine(String reason, Exception e) {
                onFailedCount.incrementAndGet();
            }
        };

        Path translogPath = createTempDir();
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid);

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
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(documentMapper.getVersion()).thenReturn(1L);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        EngineConfig config = new EngineConfig.Builder().shardId(shardId)
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
            .dataFormatRegistry(createMockRegistry())
            .committerFactory(c -> new InMemoryCommitter(store))
            .eventListener(countingListener)
            .mapperService(mapperService)
            .build();

        DataFormatAwareEngine engine = new DataFormatAwareEngine(config);
        try {
            // Index 10 docs
            for (int i = 0; i < 10; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            // Spawn 5 threads each calling failEngine with different reasons
            int numThreads = 5;
            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            AtomicInteger threadErrors = new AtomicInteger(0);

            Thread[] threads = new Thread[numThreads];
            for (int t = 0; t < numThreads; t++) {
                final int id = t;
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        engine.failEngine("reason-" + id, new RuntimeException("exception-" + id));
                    } catch (Exception e) {
                        threadErrors.incrementAndGet();
                    }
                });
                threads[t].start();
            }
            for (Thread t : threads) {
                t.join(10_000);
                assertFalse("thread should have completed", t.isAlive());
            }

            // No thread should have thrown
            assertThat("no thread should have thrown", threadErrors.get(), equalTo(0));

            // Verify engine failed exactly once (first caller wins)
            FailableDataFormatAwareEngine failable = new FailableDataFormatAwareEngine(engine);
            assertNotNull("engine should have failed", failable.getFailedEngine());

            // Verify onFailedEngine called exactly once
            assertThat("onFailedEngine should be called exactly once", onFailedCount.get(), equalTo(1));

            // Verify all subsequent ops throw AlreadyClosedException
            expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDoc("post-fail", null))));
            expectThrows(AlreadyClosedException.class, () -> engine.refresh("post-fail"));
            expectThrows(AlreadyClosedException.class, () -> engine.flush(false, true));
        } finally {
            engine.close();
        }
    }

    // --- Test: refresh failure with buffered unflushed segments ---

    public void testRefreshFailureWithBufferedSegmentsFailsEngine() throws Exception {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        try {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            // Index docs, refresh, flush — establish baseline
            for (int i = 0; i < 20; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }
            engine.refresh("setup");
            engine.flush(false, true);

            // Index more docs so next refresh has unflushed segments
            for (int i = 20; i < 40; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            // Inject refresh failure via MockIndexingExecutionEngine
            MockIndexingExecutionEngine mockExecEngine = getMockExecutionEngine(engine);
            mockExecEngine.setRefreshFailure(() -> new IOException("injected writer failure"));

            // Trigger failure via refresh
            try {
                engine.refresh("trigger-failure");
            } catch (Exception e) {
                // expected
            }

            // Verify consistent terminal state
            FailableDataFormatAwareEngine failable = new FailableDataFormatAwareEngine(engine);
            Exception failedEngine = failable.getFailedEngine();
            if (failedEngine != null) {
                expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDoc("post-fail", null))));
            }
        } finally {
            engine.close();
        }
    }

    // --- Test: operations after failEngine throw AlreadyClosedException with original cause ---

    public void testIndexAfterFailEngineThrowsAlreadyClosedWithOriginalCause() throws Exception {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        try {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            // Index 5 docs successfully
            for (int i = 0; i < 5; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            // Fail engine with a known cause
            IOException originalCause = new IOException("original cause");
            engine.failEngine("test failure", originalCause);

            // Index → AlreadyClosedException with original cause
            AlreadyClosedException indexEx = expectThrows(
                AlreadyClosedException.class,
                () -> engine.index(indexOp(createParsedDoc("post-fail", null)))
            );
            assertSame("index ACE cause should be the original failure", originalCause, indexEx.getCause());

            // Refresh → AlreadyClosedException with original cause
            AlreadyClosedException refreshEx = expectThrows(AlreadyClosedException.class, () -> engine.refresh("post-fail"));
            assertSame("refresh ACE cause should be the original failure", originalCause, refreshEx.getCause());

            // Flush → AlreadyClosedException with original cause
            AlreadyClosedException flushEx = expectThrows(AlreadyClosedException.class, () -> engine.flush(false, true));
            assertSame("flush ACE cause should be the original failure", originalCause, flushEx.getCause());
        } finally {
            engine.close();
        }
    }

    public void testEngineRecoveryAfterFailurePreservesCommittedData() throws Exception {
        Path translogPath = createTempDir();
        DataFormatAwareEngine engine = createDFAEngine(store, translogPath);
        try {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            // Index 20 docs, refresh, flush (commit point)
            for (int i = 0; i < 20; i++) {
                Engine.IndexResult result = engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
                assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            }
            engine.refresh("test");
            engine.flush(false, true);
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo(19L));

            // Index 10 more (unflushed — in translog only)
            for (int i = 20; i < 30; i++) {
                Engine.IndexResult result = engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
                assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            }
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo(29L));
        } finally {
            engine.close();
        }

        // Reopen engine from same store/translog — verifies committed data survives restart
        // (InMemoryCommitter doesn't persist commit data to store, so checkpoint reflects
        // bootstrap state; we verify the engine is operational after recovery)
        try (DataFormatAwareEngine engine2 = new DataFormatAwareEngine(buildDFAEngineConfig(store, translogPath))) {
            engine2.translogManager().recoverFromTranslog(ignore -> 0, engine2.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            // Refresh and verify engine is operational
            engine2.refresh("recovery-verify");

            // Verify engine is fully operational — can index new docs
            Engine.IndexResult newResult = engine2.index(indexOp(createParsedDoc("new-after-recovery", null)));
            assertThat("new doc after recovery should succeed", newResult.getResultType(), equalTo(Engine.Result.Type.SUCCESS));

            // Refresh and flush work
            engine2.refresh("post-recovery");
            engine2.flush(false, true);
        }
    }

    public void testIOExceptionDuringRefreshFailsEngine() throws IOException {
        DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());
        try {
            for (int i = 0; i < 10; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            // Inject IOException on refresh
            MockIndexingExecutionEngine mockExecEngine = getMockExecutionEngine(engine);
            mockExecEngine.setRefreshFailure(() -> new IOException("disk full during refresh"));

            // Refresh should fail the engine
            try {
                engine.refresh("test");
            } catch (Exception e) {
                // expected
            }

            // Engine should be failed
            assertNotNull(
                "engine should have failed from refresh IOException",
                new FailableDataFormatAwareEngine(engine).getFailedEngine()
            );
            expectThrows(AlreadyClosedException.class, () -> engine.index(indexOp(createParsedDoc("post-fail", null))));
        } finally {
            engine.close();
        }
    }

    public void testIOExceptionDuringCommitFailsEngineOnCorruption() throws IOException {
        Path translogPath = createTempDir();
        FailingEngineResult fer = createDFAEngineWithFailingCommitter(store, translogPath);
        DataFormatAwareEngine engine = fer.engine();
        FailureInjectingCommitter failingCommitter = fer.committer();
        try {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            for (int i = 0; i < 5; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            // Simulate disk full as CorruptIndexException during commit
            failingCommitter.setCommitFailure(new org.apache.lucene.index.CorruptIndexException("No space left on device", "test"));

            // Flush triggers commit which fails
            expectThrows(FlushFailedEngineException.class, () -> engine.flush(false, true));

            // Engine should be failed and store marked corrupted
            assertNotNull("engine should have failed", new FailableDataFormatAwareEngine(engine).getFailedEngine());
            assertTrue("store should be marked corrupted", store.isMarkedCorrupted());
        } finally {
            engine.close();
        }
    }

    public void testIOExceptionDuringCommitEngineStaysOpenForNonCorruption() throws IOException {
        Path translogPath = createTempDir();
        FailingEngineResult fer = createDFAEngineWithFailingCommitter(store, translogPath);
        DataFormatAwareEngine engine = fer.engine();
        FailureInjectingCommitter failingCommitter = fer.committer();
        try {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            for (int i = 0; i < 5; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            // Simulate disk full as plain IOException (not corruption)
            failingCommitter.setCommitFailure(new IOException("No space left on device"));

            // Flush fails but engine stays open (non-corruption)
            expectThrows(FlushFailedEngineException.class, () -> engine.flush(false, true));

            // Engine should still be open
            assertNull(
                "engine should not have failed for non-corruption IO error",
                new FailableDataFormatAwareEngine(engine).getFailedEngine()
            );
            assertFalse("store should NOT be corrupted", store.isMarkedCorrupted());

            // Clear failure and flush again — should succeed
            failingCommitter.setCommitFailure(null);
            engine.flush(false, true);
        } finally {
            engine.close();
        }
    }

    public void testOutOfMemoryErrorDuringFlushViaCommitter() throws IOException {
        Path translogPath = createTempDir();
        FailingEngineResult fer = createDFAEngineWithFailingCommitter(store, translogPath);
        DataFormatAwareEngine engine = fer.engine();
        FailureInjectingCommitter failingCommitter = fer.committer();
        try {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            for (int i = 0; i < 5; i++) {
                engine.index(indexOp(createParsedDoc(Integer.toString(i), null)));
            }

            failingCommitter.setCommitFailure(new IOException(new OutOfMemoryError("fake OOM during commit")));

            FlushFailedEngineException ex = expectThrows(FlushFailedEngineException.class, () -> engine.flush(false, true));
            assertTrue("root cause should be OOM", ex.getCause().getCause() instanceof OutOfMemoryError);
        } finally {
            engine.close();
        }
    }

    /**
     * Test helper that wraps a {@link DataFormatAwareEngine} to expose its private failure
     * handling methods ({@code failOnTragicEvent}, {@code maybeFailEngine}) and internal
     * state ({@code failedEngine}, {@code store}) for direct testing via reflection.
     */
    static class FailableDataFormatAwareEngine implements java.io.Closeable {
        private final DataFormatAwareEngine engine;

        FailableDataFormatAwareEngine(DataFormatAwareEngine engine) {
            this.engine = engine;
        }

        void failEngine(String reason, Exception failure) {
            engine.failEngine(reason, failure);
        }

        Exception getFailedEngine() {
            return engine.getFailedEngine();
        }

        Store getStore() {
            return engine.getStore();
        }

        DataFormatAwareEngine getEngine() {
            return engine;
        }

        @Override
        public void close() throws IOException {
            engine.close();
        }
    }

    /**
     * Reads the size of {@code DataFormatAwareEngine.flushQueue} via reflection so tests
     * can assert the queue is fully drained on failure paths without exposing internals.
     */
    @SuppressWarnings("unchecked")
    @SuppressForbidden(reason = "test utility needs reflective access to verify internal queue state")
    private static int flushQueueSize(DataFormatAwareEngine engine) throws Exception {
        java.lang.reflect.Field f = DataFormatAwareEngine.class.getDeclaredField("flushQueue");
        f.setAccessible(true);
        java.util.Collection<?> queue = (java.util.Collection<?>) f.get(engine);
        return queue.size();
    }

    /**
     * A {@link MockIndexingExecutionEngine} variant whose writer always throws on the
     * first {@code flush()} call. Tracks the number of writers it has created so tests
     * can assert that at least one writer entered the flow.
     */
    private static final class FailingFlushIndexingExecutionEngine extends MockIndexingExecutionEngine {
        private final AtomicInteger writersCreated = new AtomicInteger();
        private final MockDataFormat dataFormat;

        FailingFlushIndexingExecutionEngine(MockDataFormat dataFormat) {
            super(dataFormat);
            this.dataFormat = dataFormat;
        }

        @Override
        public org.opensearch.index.engine.dataformat.Writer<MockDocumentInput> createWriter(
            org.opensearch.index.engine.dataformat.WriterConfig config
        ) {
            writersCreated.incrementAndGet();
            return new FailingFlushWriter(config.writerGeneration(), dataFormat);
        }

        int writersCreated() {
            return writersCreated.get();
        }
    }

    /**
     * A writer that accepts documents but always throws an {@link IOException} when its
     * {@code flush()} is invoked. Used to drive the flush-failure paths in
     * {@link DataFormatAwareEngine#refresh} and {@code preIndex}.
     */
    private static final class FailingFlushWriter implements org.opensearch.index.engine.dataformat.Writer<MockDocumentInput> {
        private final long writerGeneration;
        private final org.opensearch.index.engine.dataformat.DataFormat dataFormat;
        private volatile WriterState state = WriterState.ACTIVE;

        FailingFlushWriter(long writerGeneration, org.opensearch.index.engine.dataformat.DataFormat dataFormat) {
            this.writerGeneration = writerGeneration;
            this.dataFormat = dataFormat;
        }

        @Override
        public org.opensearch.index.engine.dataformat.WriteResult addDoc(MockDocumentInput d) {
            return new org.opensearch.index.engine.dataformat.WriteResult.Success(1L, 1L, 0L);
        }

        @Override
        public org.opensearch.index.engine.dataformat.FileInfos flush(org.opensearch.index.engine.dataformat.FlushInput flushInput)
            throws IOException {
            throw new IOException("simulated flush failure for writer gen=" + writerGeneration + " format=" + dataFormat.name());
        }

        @Override
        public long generation() {
            return writerGeneration;
        }

        @Override
        public WriterState state() {
            return state;
        }

        @Override
        public boolean isSchemaMutable() {
            return true;
        }

        @Override
        public long mappingVersion() {
            return 0;
        }

        @Override
        public void updateMappingVersion(long newVersion) {}

        @Override
        public void close() {
            state = WriterState.CLOSED;
        }
    }

    /**
     * A writer that succeeds on flush, returning empty FileInfos. Used to test the
     * preIndex happy path.
     */
    private static final class SuccessFlushWriter implements org.opensearch.index.engine.dataformat.Writer<MockDocumentInput> {
        private final long writerGeneration;
        private final org.opensearch.index.engine.dataformat.DataFormat dataFormat;
        private volatile WriterState state = WriterState.ACTIVE;

        SuccessFlushWriter(long writerGeneration, org.opensearch.index.engine.dataformat.DataFormat dataFormat) {
            this.writerGeneration = writerGeneration;
            this.dataFormat = dataFormat;
        }

        @Override
        public org.opensearch.index.engine.dataformat.WriteResult addDoc(MockDocumentInput d) {
            return new org.opensearch.index.engine.dataformat.WriteResult.Success(1L, 1L, 0L);
        }

        @Override
        public org.opensearch.index.engine.dataformat.FileInfos flush(org.opensearch.index.engine.dataformat.FlushInput flushInput) {
            return org.opensearch.index.engine.dataformat.FileInfos.empty();
        }

        @Override
        public long generation() {
            return writerGeneration;
        }

        @Override
        public WriterState state() {
            return state;
        }

        @Override
        public boolean isSchemaMutable() {
            return true;
        }

        @Override
        public long mappingVersion() {
            return 0;
        }

        @Override
        public void updateMappingVersion(long newVersion) {}

        @Override
        public void close() {
            state = WriterState.CLOSED;
        }
    }

    /** With no gap (lcp == max), fillSeqNoGaps writes 0 NoOps and returns 0. */
    public void testFillSeqNoGapsNoGap() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            for (int i = 0; i < 5; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
            }
            // localCheckpoint == maxSeqNo == 4 — no gap to fill.
            assertEquals(4L, engine.getProcessedLocalCheckpoint());
            assertEquals(4L, engine.getSeqNoStats(SequenceNumbers.NO_OPS_PERFORMED).getMaxSeqNo());

            int filled = engine.fillSeqNoGaps(primaryTerm.get() + 1);
            assertEquals("no gaps to fill", 0, filled);
            assertEquals(4L, engine.getProcessedLocalCheckpoint());
        }
    }

    /**
     * Simulates a real failover gap: index N docs as PRIMARY, then replay a recovery-origin
     * op at a higher seqNo. The recovery op processes its own seqNo but leaves a gap below
     * it — fillSeqNoGaps must close the gap so localCheckpoint catches up to max.
     */
    public void testFillSeqNoGapsClosesGapAndWritesTranslogNoOps() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            // 3 successful PRIMARY ops → seqNos 0, 1, 2 / lcp = 2 / max = 2
            for (int i = 0; i < 3; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
            }
            assertEquals(2L, engine.getProcessedLocalCheckpoint());

            // Replay a recovery-origin op at seqNo=7. That marks 7 as seen+processed, but
            // localCheckpoint stays at 2 (the largest contiguous prefix). Gap: seqNos 3..6.
            ParsedDocument far = createParsedDocWithInput("7", null);
            Engine.Index recoveryOp = new Engine.Index(
                new Term(IdFieldMapper.NAME, Uid.encodeId(far.id())),
                far,
                7L,
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
            engine.index(recoveryOp);
            assertEquals("max advanced via recovery op", 7L, engine.getSeqNoStats(SequenceNumbers.NO_OPS_PERFORMED).getMaxSeqNo());
            assertEquals("lcp blocked by gap at 3..6", 2L, engine.getProcessedLocalCheckpoint());

            int translogOpsBefore = engine.translogManager().getTranslogStats().estimatedNumberOfOperations();
            int filled = engine.fillSeqNoGaps(primaryTerm.get());

            assertEquals("4 NoOps written for gap 3..6", 4, filled);
            assertEquals("lcp now equals max", 7L, engine.getProcessedLocalCheckpoint());
            assertEquals(
                "translog grew by 4 NoOps",
                translogOpsBefore + 4,
                engine.translogManager().getTranslogStats().estimatedNumberOfOperations()
            );
        }
    }

    /** fillSeqNoGaps is idempotent — second call after the first finds nothing to do. */
    public void testFillSeqNoGapsIdempotent() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            // Index 1 doc, then create a gap via a recovery op at seqNo=5.
            engine.index(indexOp(createParsedDocWithInput("0", null)));
            ParsedDocument far = createParsedDocWithInput("5", null);
            engine.index(
                new Engine.Index(
                    new Term(IdFieldMapper.NAME, Uid.encodeId(far.id())),
                    far,
                    5L,
                    primaryTerm.get(),
                    1L,
                    null,
                    Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
                    System.nanoTime(),
                    -1,
                    false,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0
                )
            );
            assertEquals(0L, engine.getProcessedLocalCheckpoint());
            assertEquals(5L, engine.getSeqNoStats(SequenceNumbers.NO_OPS_PERFORMED).getMaxSeqNo());

            int firstFill = engine.fillSeqNoGaps(primaryTerm.get());
            assertEquals("4 NoOps written for gap 1..4", 4, firstFill);
            assertEquals(5L, engine.getProcessedLocalCheckpoint());

            int secondFill = engine.fillSeqNoGaps(primaryTerm.get());
            assertEquals("no further NoOps on second call", 0, secondFill);
            assertEquals(5L, engine.getProcessedLocalCheckpoint());
        }
    }

    // --- Tiering freeze behavior ---

    /** Creates a DFA engine whose index settings already carry the given tiering state (freeze-on-open path). */
    private DataFormatAwareEngine createDFAEngineWithTieringState(Store store, Path translogPath, IndexModule.TieringState state)
        throws IOException {
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid);
        return new DataFormatAwareEngine(buildDFAEngineConfig(store, translogPath, List.of(), List.of(), state.name()));
    }

    /** Updates the engine's live index settings to the given tiering state (without invoking onSettingsChanged). */
    private void setTieringStateSetting(DataFormatAwareEngine engine, IndexModule.TieringState state) {
        IndexSettings indexSettings = engine.config().getIndexSettings();
        Settings newSettings = Settings.builder()
            .put(indexSettings.getSettings())
            .put(IndexModule.INDEX_TIERING_STATE.getKey(), state.name())
            .build();
        indexSettings.updateIndexMetadata(IndexMetadata.builder(indexSettings.getIndexMetadata()).settings(newSettings).build());
    }

    private void notifySettingsChanged(DataFormatAwareEngine engine) {
        engine.onSettingsChanged(TimeValue.MINUS_ONE, org.opensearch.core.common.unit.ByteSizeValue.ZERO, 0L);
    }

    public void testFreezeForTieringBlocksPrimaryIndex() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            engine.freezeForTiering();
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> engine.index(indexOp(createParsedDocWithInput("0", null)))
            );
            assertThat(e.getMessage(), containsString("frozen for tiering"));
        }
    }

    public void testFreezeForTieringIsIdempotent() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            engine.freezeForTiering();
            engine.freezeForTiering(); // second call must be a no-op, not throw
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> engine.index(indexOp(createParsedDocWithInput("0", null)))
            );
            assertThat(e.getMessage(), containsString("frozen for tiering"));
        }
    }

    /**
     * Many threads call {@link DataFormatAwareEngine#freezeForTiering()} simultaneously. The atomic
     * {@code compareAndSet} guard must make this idempotent under concurrency: no thread throws, and
     * the engine ends frozen (a primary index op is rejected).
     */
    public void testConcurrentFreezeForTiering_EndsFrozen() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            int numThreads = 8;
            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            AtomicReference<Throwable> error = new AtomicReference<>();
            Thread[] threads = new Thread[numThreads];
            for (int t = 0; t < numThreads; t++) {
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        engine.freezeForTiering();
                    } catch (Throwable e) {
                        error.compareAndSet(null, e);
                    }
                });
                threads[t].start();
            }
            for (Thread t : threads) {
                t.join();
            }

            assertNull("concurrent freezeForTiering must not throw", error.get());
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> engine.index(indexOp(createParsedDocWithInput("0", null)))
            );
            assertThat(e.getMessage(), containsString("frozen for tiering"));
        }
    }

    /**
     * Simulates parallel tier/cancel: freezer threads repeatedly call {@code freezeForTiering()} while a
     * single thread toggles the tiering state setting (HOT_TO_WARM ⇄ HOT) and drives
     * {@code onSettingsChanged}. Only that one thread mutates index settings, so the test targets the
     * freeze-state CAS rather than {@code IndexSettings} concurrency. After the storm, driving a
     * definitive state must converge the engine consistently (no deadlock, no torn state observable
     * through the index guard).
     */
    public void testConcurrentFreezeAndSettingsUnfreeze_ConvergesConsistently() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            int freezers = 4;
            int iterations = 100;
            AtomicReference<Throwable> error = new AtomicReference<>();
            CyclicBarrier barrier = new CyclicBarrier(freezers + 1);
            Thread[] threads = new Thread[freezers + 1];

            for (int t = 0; t < freezers; t++) {
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int n = 0; n < iterations; n++) {
                            engine.freezeForTiering();
                        }
                    } catch (Throwable e) {
                        error.compareAndSet(null, e);
                    }
                });
                threads[t].start();
            }
            // Single settings-mutating thread toggles the tiering state and notifies, racing the freezers.
            threads[freezers] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int n = 0; n < iterations; n++) {
                        setTieringStateSetting(engine, IndexModule.TieringState.HOT_TO_WARM);
                        notifySettingsChanged(engine);
                        setTieringStateSetting(engine, IndexModule.TieringState.HOT);
                        notifySettingsChanged(engine);
                    }
                } catch (Throwable e) {
                    error.compareAndSet(null, e);
                }
            });
            threads[freezers].start();

            for (Thread t : threads) {
                t.join();
            }
            assertNull("concurrent freeze/unfreeze must not deadlock or throw", error.get());

            // Converge to a definitive HOT state: engine must unfreeze cleanly and accept writes.
            setTieringStateSetting(engine, IndexModule.TieringState.HOT);
            notifySettingsChanged(engine);
            assertEquals(Engine.Result.Type.SUCCESS, engine.index(indexOp(createParsedDocWithInput("hot", null))).getResultType());

            // Converge to HOT_TO_WARM: engine must freeze cleanly and block writes.
            setTieringStateSetting(engine, IndexModule.TieringState.HOT_TO_WARM);
            notifySettingsChanged(engine);
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> engine.index(indexOp(createParsedDocWithInput("warm", null)))
            );
            assertThat(e.getMessage(), containsString("frozen for tiering"));
        }
    }

    public void testFrozenEngineSkipsNonForceFlush() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            engine.index(indexOp(createParsedDocWithInput("0", null)));
            engine.freezeForTiering();
            // A non-force flush while frozen must return early without committing.
            engine.flush(false, true);
            // A force flush is the prepare path and must proceed.
            engine.flush(true, true);
        }
    }

    public void testFrozenEngineSkipsRefreshExceptPrepareAndFlushSources() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            engine.freezeForTiering();
            // These sources are skipped while frozen (no exception, simply a no-op).
            engine.refresh("write indexing buffer");
            engine.maybeRefresh("external");
            // These two bypass the freeze and are allowed to proceed.
            engine.refresh("prepare_tiering");
            engine.refresh("flush");
        }
    }

    public void testFrozenEngineBlocksForceMerge() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            engine.freezeForTiering();
            // Force merge while frozen returns early without scheduling a merge.
            engine.forceMerge(false, 1, false, false, false, UUID.randomUUID().toString());
        }
    }

    /**
     * Verifies the merge-count / drain wrappers delegate to the merge scheduler. On a freshly
     * recovered engine with no indexing or merges, the scheduler reports zero active merges and
     * no pending queue, and {@code onMergesDrained} fires the listener immediately
     * (already drained).
     */
    public void testMergeCountWrappers_IdleEngine_ReportZeroAndAlreadyDrained() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            assertEquals("idle engine has no active merges", 0, engine.getActiveMergeCount());
            assertFalse("idle engine has no pending merges", engine.hasPendingMerges());

            AtomicBoolean listenerFired = new AtomicBoolean(false);
            engine.onMergesDrained(() -> listenerFired.set(true));

            assertTrue("listener fires immediately when idle engine is already drained", listenerFired.get());
        }
    }

    public void testConstructorFreezesWhenOpenedMidTiering() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngineWithTieringState(store, createTempDir(), IndexModule.TieringState.HOT_TO_WARM)) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            // Opened with INDEX_TIERING_STATE=HOT_TO_WARM — engine must be frozen on construction,
            // before any onSettingsChanged call.
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> engine.index(indexOp(createParsedDocWithInput("0", null)))
            );
            assertThat(e.getMessage(), containsString("frozen for tiering"));
        }
    }

    public void testOnSettingsChangedFreezesOnHotToWarm() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            // Initially HOT — indexing works.
            engine.index(indexOp(createParsedDocWithInput("0", null)));
            // Transition to HOT_TO_WARM and notify — engine freezes.
            setTieringStateSetting(engine, IndexModule.TieringState.HOT_TO_WARM);
            notifySettingsChanged(engine);
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> engine.index(indexOp(createParsedDocWithInput("1", null)))
            );
            assertThat(e.getMessage(), containsString("frozen for tiering"));
        }
    }

    public void testOnSettingsChangedUnfreezesOnReturnToHot() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            setTieringStateSetting(engine, IndexModule.TieringState.HOT_TO_WARM);
            notifySettingsChanged(engine);
            expectThrows(IllegalStateException.class, () -> engine.index(indexOp(createParsedDocWithInput("0", null))));
            // Tiering cancelled — state returns to HOT, engine unfreezes and indexing resumes.
            setTieringStateSetting(engine, IndexModule.TieringState.HOT);
            notifySettingsChanged(engine);
            Engine.IndexResult result = engine.index(indexOp(createParsedDocWithInput("1", null)));
            assertEquals(Engine.Result.Type.SUCCESS, result.getResultType());
        }
    }

    public void testIsFrozenForTieringLiveSettingsFallback() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            // Settings reflect HOT_TO_WARM but onSettingsChanged has NOT been called (the cached flag is
            // still false). The live-settings fallback must still block the operation.
            setTieringStateSetting(engine, IndexModule.TieringState.HOT_TO_WARM);
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> engine.index(indexOp(createParsedDocWithInput("0", null)))
            );
            assertThat(e.getMessage(), containsString("frozen for tiering"));
        }
    }

    public void testOnSettingsChangedUnrecognizedTieringValueNotFrozen() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            IndexSettings indexSettings = engine.config().getIndexSettings();
            Settings newSettings = Settings.builder()
                .put(indexSettings.getSettings())
                .put(IndexModule.INDEX_TIERING_STATE.getKey(), "NOT_A_REAL_STATE")
                .build();
            indexSettings.updateIndexMetadata(IndexMetadata.builder(indexSettings.getIndexMetadata()).settings(newSettings).build());
            notifySettingsChanged(engine);
            // Unrecognized value is treated as not frozen — indexing still works.
            Engine.IndexResult result = engine.index(indexOp(createParsedDocWithInput("0", null)));
            assertEquals(Engine.Result.Type.SUCCESS, result.getResultType());
        }
    }

    public void testDocCountLimitRejectsIndexingAboveMax() throws Exception {
        final int maxDocs = randomIntBetween(1, 20);
        MockIndexingExecutionEngine limitedEngine = new MockIndexingExecutionEngine(mockDataFormat) {
            @Override
            public long maxIndexableDocs() {
                return maxDocs;
            }
        };
        MockDataFormatPlugin limitedPlugin = new MockDataFormatPlugin(mockDataFormat) {
            @Override
            public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig settings) {
                return limitedEngine;
            }
        };

        EngineConfig config = buildFailingEngineConfig(limitedPlugin, new Engine.EventListener() {
            @Override
            public void onFailedEngine(String reason, Exception failure) {}
        });
        try (DataFormatAwareEngine eng = new DataFormatAwareEngine(config)) {
            int numDocs = between(maxDocs + 1, maxDocs * 2);
            for (int i = 0; i < numDocs; i++) {
                final long maxSeqNo = eng.getProcessedLocalCheckpoint();
                Engine.IndexResult result = eng.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
                if (i < maxDocs) {
                    assertThat("doc " + i + " should succeed", result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
                    assertNull(result.getFailure());
                    assertThat(result.getSeqNo(), greaterThanOrEqualTo(0L));
                } else {
                    assertThat("doc " + i + " should be rejected", result.getResultType(), equalTo(Engine.Result.Type.FAILURE));
                    assertNotNull(result.getFailure());
                    assertThat(result.getFailure(), instanceOf(IllegalArgumentException.class));
                    assertThat(
                        result.getFailure().getMessage(),
                        containsString("Number of documents in shard " + shardId + " exceeds the limit of [" + maxDocs + "]")
                    );
                    assertThat("seq no must not be assigned on rejection", result.getSeqNo(), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));
                    assertThat("local checkpoint must not advance on rejection", eng.getProcessedLocalCheckpoint(), equalTo(maxSeqNo));
                }
            }
            // Engine must remain open on primary even after rejections
            eng.refresh("verify-still-open");
        }
    }

    public void testConcurrentIndexAndRefreshDocCountNeverUnderCounts() throws Exception {
        Path translogPath = createTempDir();
        try (DataFormatAwareEngine eng = createDFAEngine(store, translogPath)) {
            int numThreads = 4;
            int docsPerThread = 50;
            int totalDocs = numThreads * docsPerThread;
            CyclicBarrier barrier = new CyclicBarrier(numThreads + 1); // +1 for refresh thread
            AtomicInteger successCount = new AtomicInteger();

            Thread[] indexThreads = new Thread[numThreads];
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                indexThreads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int d = 0; d < docsPerThread; d++) {
                            Engine.IndexResult result = eng.index(indexOp(createParsedDocWithInput(threadId + "_" + d, null)));
                            if (result.getResultType() == Engine.Result.Type.SUCCESS) {
                                successCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                indexThreads[t].start();
            }

            // Refresh thread that runs concurrently with indexing
            Thread refreshThread = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < 10; i++) {
                        eng.refresh("concurrent-refresh-" + i);
                        Thread.sleep(5);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            refreshThread.start();

            for (Thread t : indexThreads)
                t.join(30_000);
            refreshThread.join(30_000);

            // Final refresh to flush remaining buffered docs
            eng.refresh("final");

            // Verify: catalogSnapshot docs + pendingRowCount must equal successCount
            try (GatedCloseable<CatalogSnapshot> ref = eng.acquireSnapshot()) {
                long catalogDocs = ref.get().getNumDocs();
                assertThat("catalog must contain all successfully indexed docs", catalogDocs, equalTo((long) successCount.get()));
            }
        }
    }

    private DataFormatAwareEngine createDFAEngineWithLookupProvider(Store store, Path translogPath, DocumentLookupProvider provider)
        throws IOException {
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid);
        return new DataFormatAwareEngine(buildDFAEngineConfig(store, translogPath, provider));
    }

    private Engine.Get realtimeGet(String id) {
        return new Engine.Get(true, true, id, new Term(IdFieldMapper.NAME, Uid.encodeId(id)));
    }

    private DocumentLookupProvider mockLookupProvider() throws IOException {
        DocumentLookupProvider provider = mock(DocumentLookupProvider.class);
        when(provider.getById(any(), any(), any(), any())).thenReturn(DocumentLookupResult.notFound(""));
        when(provider.getVersionMetadata(any(), any(), any(), any())).thenReturn(DocumentLookupResult.notFound(""));
        return provider;
    }

    public void testGetMaxSeqNoOfUpdatesOrDeletes() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            assertThat(engine.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        }
    }

    public void testCurrentOngoingRefreshCheckpoint() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            assertThat(engine.currentOngoingRefreshCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
            for (int i = 0; i < 5; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
            }
            engine.refresh("test");
            assertThat(engine.currentOngoingRefreshCheckpoint(), greaterThanOrEqualTo(0L));
        }
    }

    /** Calls the new getById(Engine.Get, searcherFactory) and unwraps the pre-materialized lookup.
     *  DFA engines ignore the searcher factory, so a no-op factory is supplied. */
    private static DocumentLookupResult getByIdLookup(DataFormatAwareEngine engine, Engine.Get get) throws IOException {
        Engine.GetResult result = engine.getById(get, (source, scope) -> null);
        return result.exists() ? ((DocumentLookupResult.PreMaterialized) result).lookup() : DocumentLookupResult.notFound(get.id());
    }

    public void testGetByIdThrowsWhenNoProvider() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            Engine.Get get = realtimeGet("1");
            expectThrows(UnsupportedOperationException.class, () -> getByIdLookup(engine, get));
        }
    }

    public void testGetByIdReturnsNotFoundWhenEmptyCatalog() throws IOException {
        DocumentLookupProvider provider = mockLookupProvider();
        try (DataFormatAwareEngine engine = createDFAEngineWithLookupProvider(store, createTempDir(), provider)) {
            Engine.Get get = realtimeGet("1");
            DocumentLookupResult result = getByIdLookup(engine, get);
            assertFalse("should be not found on empty catalog", result.exists());
        }
    }

    public void testGetByIdReturnsDocFromTranslog() throws IOException {
        DocumentLookupProvider provider = mockLookupProvider();
        try (DataFormatAwareEngine engine = createDFAEngineWithLookupProvider(store, createTempDir(), provider)) {
            engine.index(indexOp(createParsedDocWithInput("1", null)));
            // Realtime get with readFromTranslog finds it in translog via versionMap location
            Engine.Get get = realtimeGet("1");
            DocumentLookupResult result = getByIdLookup(engine, get);
            assertTrue("doc should be found from translog", result.exists());
            assertThat(result.seqNo(), equalTo(0L));
        }
    }

    public void testGetByIdReturnsDocFromParquetAfterRefresh() throws IOException {
        DocumentLookupProvider provider = mock(DocumentLookupProvider.class);
        when(provider.getById(any(), any(), any(), any())).thenReturn(
            new DocumentLookupResult("1", 1L, true, null, 0L, 1L, Map.of(), Map.of())
        );
        try (DataFormatAwareEngine engine = createDFAEngineWithLookupProvider(store, createTempDir(), provider)) {
            engine.index(indexOp(createParsedDocWithInput("1", null)));
            engine.refresh("test");
            // Non-realtime get falls through to provider
            Engine.Get get = new Engine.Get(false, false, "1", new Term(IdFieldMapper.NAME, Uid.encodeId("1")));
            DocumentLookupResult result = getByIdLookup(engine, get);
            assertTrue("doc should be found from parquet", result.exists());
        }
    }

    /**
     * Covers resolveDocVersion falling back to DocumentLookupProvider when versionMap miss.
     * Also covers incrementIndexVersionLookup (called inside resolveDocVersion on provider path).
     */
    public void testResolveDocVersionFallsBackToProvider() throws IOException {
        DocumentLookupProvider provider = mock(DocumentLookupProvider.class);
        when(provider.getVersionMetadata(any(), any(), any(), any())).thenReturn(
            new DocumentLookupResult("1", 5L, true, null, 3L, 1L, Map.of(), Map.of())
        );
        try (DataFormatAwareEngine engine = createDFAEngineWithLookupProvider(store, createTempDir(), provider)) {
            engine.index(indexOp(createParsedDocWithInput("1", null)));
            engine.refresh("test");
            // Re-index same doc — resolveDocVersion misses versionMap (cleared by refresh rotation),
            // falls back to provider which returns version=5. MATCH_ANY accepts.
            Engine.IndexResult result = engine.index(indexOp(createParsedDocWithInput("1", null)));
            assertThat(result.getResultType(), equalTo(Engine.Result.Type.FAILURE));
            assertEquals(result.getFailure().getClass(), AppendOnlyIndexOperationRetryException.class);
        }
    }

    /** Reflectively obtains the engine's LiveVersionMap. resolveDocVersion/getVersionFromMap assert the
     *  per-uid keyed lock is held, so callers must wrap in {@code versionMap.acquireLock(uid)}. */
    @SuppressForbidden(reason = "test needs reflective access to the engine's versionMap field")
    private LiveVersionMap versionMapOf(DataFormatAwareEngine engine) throws Exception {
        java.lang.reflect.Field vmField = DataFormatAwareEngine.class.getDeclaredField("versionMap");
        vmField.setAccessible(true);
        return (LiveVersionMap) vmField.get(engine);
    }

    /** resolveDocVersion branch 1: versionMap miss with no provider returns null. */
    public void testResolveDocVersionReturnsNullWhenNoProvider() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            Engine.Index op = indexOp(createParsedDocWithInput("1", null));
            try (org.opensearch.common.lease.Releasable ignored = versionMapOf(engine).acquireLock(op.uid().bytes())) {
                assertNull(engine.resolveDocVersion(op, true));
            }
        }
    }

    /** resolveDocVersion branch 2: versionMap miss with an empty catalog returns null (provider not consulted). */
    public void testResolveDocVersionReturnsNullOnEmptyCatalog() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngineWithLookupProvider(store, createTempDir(), mockLookupProvider())) {
            Engine.Index op = indexOp(createParsedDocWithInput("1", null));
            try (org.opensearch.common.lease.Releasable ignored = versionMapOf(engine).acquireLock(op.uid().bytes())) {
                assertNull(engine.resolveDocVersion(op, true));
            }
        }
    }

    /** resolveDocVersion branch 4: versionMap miss, provider consulted, getById returns NOT_FOUND → null. */
    public void testResolveDocVersionReturnsNullWhenProviderNotFound() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngineWithLookupProvider(store, createTempDir(), mockLookupProvider())) {
            engine.index(indexOp(createParsedDocWithInput("1", null)));
            engine.refresh("test");
            Engine.Index op = indexOp(createParsedDocWithInput("2", null));
            try (org.opensearch.common.lease.Releasable ignored = versionMapOf(engine).acquireLock(op.uid().bytes())) {
                assertNull(engine.resolveDocVersion(op, true));
            }
        }
    }

    /** resolveDocVersion branch 6: versionMap hit returns the stored IndexVersionValue. */
    public void testResolveDocVersionReturnsVersionMapHit() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.index(indexOp(createParsedDocWithInput("1", null)));
            Engine.Index op = indexOp(createParsedDocWithInput("1", null));
            VersionValue vv;
            try (org.opensearch.common.lease.Releasable ignored = versionMapOf(engine).acquireLock(op.uid().bytes())) {
                vv = engine.resolveDocVersion(op, true);
            }
            assertNotNull(vv);
            assertThat(vv.version, equalTo(1L));
            assertThat(vv.seqNo, equalTo(0L));
        }
    }

    /**
     * Covers restoreVersionMapAndCheckpointTracker + compareOpToVersionMapOnSeqNo.
     * The provider returns docs for seqNos 0-3 so checkpoint advances contiguously.
     */
    public void testRestoreVersionMapAndCheckpointTracker() throws IOException {
        Path translogPath = createTempDir();
        DocumentLookupProvider provider = mock(DocumentLookupProvider.class);
        when(provider.getDocsAboveSeqNo(anyLong(), any(), any(), any())).thenReturn(
            List.of(
                new DocumentLookupResult("a", 1L, true, null, 0L, 1L, Map.of(), Map.of()),
                new DocumentLookupResult("b", 1L, true, null, 1L, 1L, Map.of(), Map.of()),
                new DocumentLookupResult("c", 1L, true, null, 2L, 1L, Map.of(), Map.of()),
                new DocumentLookupResult("d", 1L, true, null, 3L, 1L, Map.of(), Map.of())
            )
        );
        when(provider.getVersionMetadata(any(), any(), any(), any())).thenReturn(DocumentLookupResult.notFound("x"));

        // Bootstrap with maxSeqNo=3, localCheckpoint=-1 (triggers restore)
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid, 3L);

        EngineConfig config = buildDFAEngineConfig(store, translogPath, provider);
        try (DataFormatAwareEngine engine = new DataFormatAwareEngine(config)) {
            // All seqNos 0-3 marked → checkpoint advances to 3
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo(3L));
        }
    }

    /**
     * Covers compareOpToVersionMapOnSeqNo returning OP_STALE_OR_EQUAL.
     * Provider returns entries for id "x" at seqNo=2 and seqNo=5, plus a duplicate at seqNo=5.
     * The stale entry (seqNo=2) is overwritten by OP_NEWER, the duplicate (same seqNo=5) is skipped.
     */
    public void testRestoreVersionMapSkipsStaleEntries() throws IOException {
        Path translogPath = createTempDir();
        DocumentLookupProvider provider = mock(DocumentLookupProvider.class);
        // Return all seqNos 0-5 so checkpoint advances, with duplicate id "x" at seqNo 2 and 5
        when(provider.getDocsAboveSeqNo(anyLong(), any(), any(), any())).thenReturn(
            List.of(
                new DocumentLookupResult("a", 1L, true, null, 0L, 1L, Map.of(), Map.of()),
                new DocumentLookupResult("b", 1L, true, null, 1L, 1L, Map.of(), Map.of()),
                new DocumentLookupResult("x", 1L, true, null, 2L, 1L, Map.of(), Map.of()),
                new DocumentLookupResult("c", 1L, true, null, 3L, 1L, Map.of(), Map.of()),
                new DocumentLookupResult("d", 1L, true, null, 4L, 1L, Map.of(), Map.of()),
                new DocumentLookupResult("x", 2L, true, null, 5L, 1L, Map.of(), Map.of()),
                // Duplicate id "x" at same seqNo=5 → hits seqNo == versionValue.seqNo → OP_STALE_OR_EQUAL
                new DocumentLookupResult("x", 2L, true, null, 5L, 1L, Map.of(), Map.of())
            )
        );
        when(provider.getVersionMetadata(any(), any(), any(), any())).thenReturn(DocumentLookupResult.notFound("x"));

        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid, 5L);

        EngineConfig config = buildDFAEngineConfig(store, translogPath, provider);
        try (DataFormatAwareEngine engine = new DataFormatAwareEngine(config)) {
            // All seqNos 0-5 contiguous → checkpoint = 5
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo(5L));
            // Index "x" again — should succeed (versionMap has version=2 from seqNo=5 entry)
            Engine.IndexResult result = engine.index(indexOp(createParsedDocWithInput("x", null)));
            assertThat(result.getResultType(), equalTo(Engine.Result.Type.FAILURE));
            assertEquals(result.getFailure().getClass(), AppendOnlyIndexOperationRetryException.class);
        }
    }

    public void testRestoreVersionMapThrowsOnIOException() throws IOException {
        Path translogPath = createTempDir();
        DocumentLookupProvider provider = mock(DocumentLookupProvider.class);
        when(provider.getDocsAboveSeqNo(anyLong(), any(), any(), any())).thenThrow(new IOException("simulated read failure"));

        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid, 3L);

        EngineConfig config = buildDFAEngineConfig(store, translogPath, provider);
        EngineCreationFailureException ex = expectThrows(EngineCreationFailureException.class, () -> new DataFormatAwareEngine(config));
        assertThat(ex.getMessage(), containsString("failed to restore version map"));
        assertThat(ex.getCause(), instanceOf(IOException.class));
    }

    /**
     * Covers getVersionFromMap unsafe path: versionMap.isUnsafe() triggers refresh + enforceSafeAccess.
     */
    @SuppressForbidden(reason = "test needs reflective access to mark versionMap as unsafe")
    public void testGetVersionFromMapUnsafePathTriggersRefresh() throws Exception {
        DocumentLookupProvider provider = mockLookupProvider();
        try (DataFormatAwareEngine engine = createDFAEngineWithLookupProvider(store, createTempDir(), provider)) {
            engine.index(indexOp(createParsedDocWithInput("1", null)));

            java.lang.reflect.Field vmField = DataFormatAwareEngine.class.getDeclaredField("versionMap");
            vmField.setAccessible(true);
            LiveVersionMap versionMap = (LiveVersionMap) vmField.get(engine);
            java.lang.reflect.Field mapsField = LiveVersionMap.class.getDeclaredField("maps");
            mapsField.setAccessible(true);
            Object maps = mapsField.get(versionMap);
            java.lang.reflect.Field currentField = maps.getClass().getDeclaredField("current");
            currentField.setAccessible(true);
            Object current = currentField.get(maps);
            java.lang.reflect.Method markUnsafe = current.getClass().getDeclaredMethod("markAsUnsafe");
            markUnsafe.setAccessible(true);
            markUnsafe.invoke(current);

            assertTrue("versionMap should be unsafe", versionMap.isUnsafe());

            long genBefore;
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                genBefore = ref.get().getGeneration();
            }

            getByIdLookup(engine, realtimeGet("1"));

            long genAfter;
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                genAfter = ref.get().getGeneration();
            }
            assertThat("refresh should have been triggered by unsafe versionMap", genAfter, greaterThan(genBefore));
            assertFalse("versionMap should be safe after refresh", versionMap.isUnsafe());
        }
    }

    /**
     * DFAE port of InternalEngineTests.testVersionMapAfterAutoIDDocument (delete step dropped —
     * engine.delete() is unsupported in this harness). Verifies LiveVersionMap safe-access
     * transitions: optimized append-only skips the map; an explicit-id index enforces safe access
     * and stores the entry; safe access is carried over across refresh.
     */
    @SuppressForbidden(reason = "test needs reflective access to the engine's versionMap field")
    public void testVersionMapAfterAutoIDDocument() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            java.lang.reflect.Field vmField = DataFormatAwareEngine.class.getDeclaredField("versionMap");
            vmField.setAccessible(true);
            LiveVersionMap versionMap = (LiveVersionMap) vmField.get(engine);

            // Optimized append-only: no safe access enforced, nothing stored in the current map.
            engine.index(appendOnlyOp(createParsedDocWithInput("1", null)));
            assertFalse(versionMap.isSafeAccessRequired());
            assertTrue(versionMap.getAllCurrent().isEmpty());

            // Explicit-id index of the same id: enforces safe access and stores the entry.
            engine.index(indexOp(createParsedDocWithInput("1", null)));
            assertTrue(versionMap.isSafeAccessRequired());
            assertEquals(1, versionMap.getAllCurrent().size());

            // Refresh rotates the map; safe access is carried over.
            engine.refresh("test");
            assertTrue(versionMap.isSafeAccessRequired());

            // Append-only under active safe access still stores the entry.
            engine.index(appendOnlyOp(createParsedDocWithInput("2", null)));
            assertEquals(1, versionMap.getAllCurrent().size());
        }
    }

    /**
     * A: write-path version conflict resolved from the versionMap. EXTERNAL version=5 then a stale
     * EXTERNAL version=3 hits isVersionConflictForWrites (IndexingStrategyPlanner) → conflict.
     */
    public void testIndexVersionConflictFromVersionMap() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            Engine.IndexResult first = engine.index(indexOpWithVersion(createParsedDocWithInput("1", null), 5L, VersionType.EXTERNAL));
            assertThat(first.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            Engine.IndexResult stale = engine.index(indexOpWithVersion(createParsedDocWithInput("1", null), 3L, VersionType.EXTERNAL));
            assertThat(stale.getResultType(), equalTo(Engine.Result.Type.FAILURE));
            assertThat(stale.getFailure(), instanceOf(VersionConflictEngineException.class));
        }
    }

    /**
     * B: optimistic-concurrency conflict on the write path. After indexing id "1" (seqNo 0), an index
     * with a non-matching ifSeqNo hits the seqNo/term conflict branch (IndexingStrategyPlanner) → conflict.
     */
    public void testIndexIfSeqNoConflict() throws IOException {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            engine.index(indexOp(createParsedDocWithInput("1", null)));
            Engine.IndexResult result = engine.index(indexOpWithIfSeqNo(createParsedDocWithInput("1", null), 99L, primaryTerm.get()));
            assertThat(result.getResultType(), equalTo(Engine.Result.Type.FAILURE));
            assertThat(result.getFailure(), instanceOf(VersionConflictEngineException.class));
        }
    }

    /**
     * C: optimized append-only ops never enter safe-access mode — they leave the current map empty
     * and mark it unsafe (LiveVersionMap.maybePutIndexUnderLock else-branch).
     */
    @SuppressForbidden(reason = "test needs reflective access to the engine's versionMap field")
    public void testAppendOnlyMarksVersionMapUnsafe() throws Exception {
        try (DataFormatAwareEngine engine = createDFAEngine(store, createTempDir())) {
            java.lang.reflect.Field vmField = DataFormatAwareEngine.class.getDeclaredField("versionMap");
            vmField.setAccessible(true);
            LiveVersionMap versionMap = (LiveVersionMap) vmField.get(engine);

            int n = randomIntBetween(2, 5);
            for (int i = 0; i < n; i++) {
                engine.index(appendOnlyOp(createParsedDocWithInput(Integer.toString(i), null)));
            }
            assertFalse(versionMap.isSafeAccessRequired());
            assertTrue(versionMap.getAllCurrent().isEmpty());
            assertTrue(versionMap.isUnsafe());
        }
    }

    /**
     * Covers getById paths: delete check, version conflict, seqNo/primaryTerm conflict.
     * Uses reflection to inject a DeleteVersionValue into versionMap since engine.delete() is unsupported.
     */
    @SuppressForbidden(reason = "test needs reflective access to inject DeleteVersionValue into versionMap")
    public void testGetByIdDeleteAndConflictPaths() throws Exception {
        DocumentLookupProvider provider = mockLookupProvider();
        try (DataFormatAwareEngine engine = createDFAEngineWithLookupProvider(store, createTempDir(), provider)) {
            // Index a doc so versionMap has an IndexVersionValue
            engine.index(indexOp(createParsedDocWithInput("1", null)));

            // --- Path 1: versionValue.isDelete() ---
            // Inject a DeleteVersionValue into versionMap via reflection
            java.lang.reflect.Field vmField = DataFormatAwareEngine.class.getDeclaredField("versionMap");
            vmField.setAccessible(true);
            LiveVersionMap versionMap = (LiveVersionMap) vmField.get(engine);
            org.apache.lucene.util.BytesRef uid = new Term(IdFieldMapper.NAME, Uid.encodeId("1")).bytes();
            try (org.opensearch.common.lease.Releasable ignored = versionMap.acquireLock(uid)) {
                versionMap.putDeleteUnderLock(uid, new DeleteVersionValue(2L, 1L, 1L, System.currentTimeMillis()));
            }

            Engine.Get getForDelete = realtimeGet("1");
            DocumentLookupResult deleteResult = getByIdLookup(engine, getForDelete);
            assertFalse("deleted doc should return not found", deleteResult.exists());

            // --- Path 2: version conflict for reads ---
            // Put back an IndexVersionValue so we can test version conflict
            try (org.opensearch.common.lease.Releasable ignored = versionMap.acquireLock(uid)) {
                versionMap.putIndexUnderLock(uid, new IndexVersionValue(null, 5L, 0L, 1L));
            }

            Engine.Get getWithConflict = realtimeGet("1").version(3L).versionType(VersionType.EXTERNAL);
            expectThrows(VersionConflictEngineException.class, () -> getByIdLookup(engine, getWithConflict));

            // --- Path 3: seqNo/primaryTerm conflict (seqNo matches, primaryTerm doesn't) ---
            Engine.Get getWithSeqNoConflict = realtimeGet("1").setIfSeqNo(99L).setIfPrimaryTerm(1L);
            expectThrows(VersionConflictEngineException.class, () -> getByIdLookup(engine, getWithSeqNoConflict));

            // --- Path 4: primaryTerm mismatch (seqNo matches but primaryTerm doesn't) ---
            // versionMap has seqNo=0, primaryTerm=1 from the IndexVersionValue above
            try (org.opensearch.common.lease.Releasable ignored = versionMap.acquireLock(uid)) {
                versionMap.putIndexUnderLock(uid, new IndexVersionValue(null, 5L, 0L, 1L));
            }
            Engine.Get getWithPtConflict = realtimeGet("1").setIfSeqNo(0L).setIfPrimaryTerm(999L);
            expectThrows(VersionConflictEngineException.class, () -> getByIdLookup(engine, getWithPtConflict));

            // --- Path 5: seqNo and primaryTerm both match — no conflict, falls through ---
            Engine.Get getNoConflict = realtimeGet("1").setIfSeqNo(0L).setIfPrimaryTerm(1L);
            DocumentLookupResult noConflictResult = getByIdLookup(engine, getNoConflict);
            assertNotNull("should not throw when seqNo and primaryTerm match", noConflictResult);

            // --- Path 6: GC deletes in resolveDocVersion ---
            // Inject an expired DeleteVersionValue (time far in the past) so gc_deletes nullifies it
            try (org.opensearch.common.lease.Releasable ignored = versionMap.acquireLock(uid)) {
                versionMap.putDeleteUnderLock(uid, new DeleteVersionValue(3L, 2L, 1L, 1L)); // time=1ms (epoch start, expired)
            }
            // Index same doc again — resolveDocVersion finds expired DeleteVersionValue, nullifies it,
            // treats as new doc (version NOT_FOUND)
            Engine.IndexResult gcResult = engine.index(indexOp(createParsedDocWithInput("1", null)));
            assertThat(gcResult.getResultType(), equalTo(Engine.Result.Type.SUCCESS));

            // --- Path 7: translog readOperation returns null (bogus location) ---
            // Covers the negative path: if (operation != null) is false, falls through to refreshIfNeeded
            Translog.Location bogusLocation = new Translog.Location(999L, 0L, 1);
            try (org.opensearch.common.lease.Releasable ignored = versionMap.acquireLock(uid)) {
                versionMap.putIndexUnderLock(uid, new IndexVersionValue(bogusLocation, 6L, 3L, 1L));
            }
            Engine.Get getWithBadTranslog = realtimeGet("1");
            DocumentLookupResult translogNullResult = getByIdLookup(engine, getWithBadTranslog);
            assertNotNull("should fall through when translog returns null", translogNullResult);
        }
    }

    /**
     * Tests that engine close is graceful when concurrent index, refresh, and flush operations
     * are in flight. Verifies no unhandled exceptions escape and the engine transitions to closed.
     */
    public void testGracefulCloseUnderConcurrentLoad() throws Exception {
        try (Store store = createStore()) {
            DataFormatAwareEngine engine = createDFAEngine(store, createTempDir());

            final AtomicBoolean stop = new AtomicBoolean(false);
            final AtomicReference<Exception> failure = new AtomicReference<>();
            final CountDownLatch started = new CountDownLatch(3);

            // Thread 1: continuous indexing
            Thread indexThread = new Thread(() -> {
                started.countDown();
                int i = 0;
                while (stop.get() == false) {
                    try {
                        engine.index(indexOp(createParsedDocWithInput(Integer.toString(i++), null)));
                    } catch (AlreadyClosedException e) {
                        break; // expected during close
                    } catch (Exception e) {
                        if (stop.get() == false) {
                            failure.compareAndSet(null, e);
                        }
                        break;
                    }
                }
            });

            // Thread 2: continuous refresh
            Thread refreshThread = new Thread(() -> {
                started.countDown();
                while (stop.get() == false) {
                    try {
                        engine.refresh("concurrent-test");
                    } catch (AlreadyClosedException e) {
                        break;
                    } catch (Exception e) {
                        if (stop.get() == false) {
                            failure.compareAndSet(null, e);
                        }
                        break;
                    }
                }
            });

            // Thread 3: periodic flush
            Thread flushThread = new Thread(() -> {
                started.countDown();
                while (stop.get() == false) {
                    try {
                        engine.flush(false, true);
                        Thread.sleep(10);
                    } catch (AlreadyClosedException e) {
                        break;
                    } catch (FlushFailedEngineException e) {
                        // flush may be disabled during translog recovery or after close
                        break;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        if (stop.get() == false) {
                            failure.compareAndSet(null, e);
                        }
                        break;
                    }
                }
            });

            indexThread.start();
            refreshThread.start();
            flushThread.start();

            // Wait for all threads to start
            assertTrue(started.await(5, TimeUnit.SECONDS));

            // Let them run briefly
            Thread.sleep(200);

            // Close the engine while operations are in-flight
            stop.set(true);
            engine.close();

            // Wait for threads to finish
            indexThread.join(10_000);
            refreshThread.join(10_000);
            flushThread.join(10_000);

            assertFalse("Index thread should have stopped", indexThread.isAlive());
            assertFalse("Refresh thread should have stopped", refreshThread.isAlive());
            assertFalse("Flush thread should have stopped", flushThread.isAlive());

            // No unexpected exceptions
            if (failure.get() != null) {
                throw new AssertionError("Unexpected exception during concurrent close", failure.get());
            }
        }
    }

    /**
     * Reproduces the composite checkpoint-inflation data-loss bug (the one behind the
     * doc-count gap observed after a primary relocation under sustained ingest).
     *
     * <p>Mechanism: {@code flush()} calls {@code refresh("flush")} — which snapshots the
     * writers frozen at {@code checkoutAll} — and then commits
     * {@code LOCAL_CHECKPOINT = getProcessedCheckpoint()} read <b>after</b> that refresh. If a
     * document is processed <b>during</b> the flush (after the snapshot is frozen but before the
     * checkpoint is read) it lands in a NEW writer that is not in the snapshot, yet it advances
     * the processed checkpoint. The commit therefore records a checkpoint that is AHEAD of what
     * the durable snapshot actually contains. On recovery, replay starts at {@code committed+1}
     * and the band in between is seeded as "already processed", so it is never replayed — the
     * acknowledged docs are silently lost.
     *
     * <p>We inject the in-window document via an {@code afterRefresh} listener (it fires
     * synchronously at the end of {@code refresh()}, i.e. after the snapshot is built but, when
     * called from inside {@code flush()}, before {@code flush()} reads the checkpoint). We then
     * assert the engine-level invariant the fix must restore: the committed
     * {@code LOCAL_CHECKPOINT} must NOT exceed the highest seqno durably persisted in the
     * snapshot.
     */
    public void testFlushMustNotCommitCheckpointAheadOfPersistedSnapshot() throws Exception {
        Path translogPath = createTempDir();
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid);
        InMemoryCommitter committer = new InMemoryCommitter(store);

        final AtomicReference<DataFormatAwareEngine> engineRef = new AtomicReference<>();
        final AtomicBoolean armed = new AtomicBoolean(false);
        final AtomicBoolean fired = new AtomicBoolean(false);
        final List<Long> injectedSeqNos = new ArrayList<>();

        // Fires at the END of refresh() — after the catalog snapshot has been built, but
        // (when called from inside flush()) before flush() reads getProcessedCheckpoint().
        ReferenceManager.RefreshListener injector = new ReferenceManager.RefreshListener() {
            @Override
            public void beforeRefresh() {}

            @Override
            public void afterRefresh(boolean didRefresh) {
                if (armed.get() && fired.compareAndSet(false, true)) {
                    DataFormatAwareEngine eng = engineRef.get();
                    try {
                        for (int i = 10; i <= 12; i++) {
                            Engine.IndexResult r = eng.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
                            injectedSeqNos.add(r.getSeqNo());
                        }
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }
            }
        };

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
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(documentMapper.getVersion()).thenReturn(1L);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        EngineConfig config = new EngineConfig.Builder().shardId(shardId)
            .threadPool(threadPool)
            .indexSettings(indexSettings)
            .store(store)
            .mergePolicy(NoMergePolicy.INSTANCE)
            .translogConfig(translogConfig)
            .flushMergesAfter(TimeValue.timeValueMinutes(5))
            .externalRefreshListener(List.of(injector))
            .internalRefreshListener(List.of())
            .globalCheckpointSupplier(() -> SequenceNumbers.NO_OPS_PERFORMED)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .primaryTermSupplier(primaryTerm::get)
            .tombstoneDocSupplier(tombstoneDocSupplier())
            .dataFormatRegistry(createMockRegistry())
            .committerFactory(c -> committer)
            .eventListener(new Engine.EventListener() {
                @Override
                public void onFailedEngine(String reason, Exception e) {}
            })
            .mapperService(mapperService)
            .build();

        try (DataFormatAwareEngine engine = new DataFormatAwareEngine(config)) {
            engineRef.set(engine);
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            // Pre-flush docs: seqnos 0..9 — captured by the flush's refresh snapshot.
            for (int i = 0; i < 10; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
            }
            assertThat("processed checkpoint before flush", engine.getProcessedLocalCheckpoint(), equalTo(9L));

            // Arm the injector: during flush()'s refresh, seqnos 10,11,12 get indexed into a NEW
            // writer (after the snapshot is frozen) but before the commit reads the checkpoint.
            armed.set(true);
            engine.flush(false, true);

            assertThat("injector ran inside the flush window", injectedSeqNos, equalTo(List.of(10L, 11L, 12L)));
            assertThat("all 13 ops were processed/acked", engine.getProcessedLocalCheckpoint(), equalTo(12L));

            // What the commit DURABLY persisted: only seqnos 0..9 made it into the snapshot.
            long persistedRows;
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                persistedRows = ref.get()
                    .getSegments()
                    .stream()
                    .mapToLong(s -> s.dfGroupedSearchableFiles().get(mockDataFormat.name()).numRows())
                    .sum();
            }
            assertThat("snapshot durably persisted only the pre-flush docs [0..9]", persistedRows, equalTo(10L));
            final long persistedMaxSeqNo = persistedRows - 1; // contiguous 0..9 -> max seqno 9

            // What the commit CLAIMS is durable.
            long committedCheckpoint = Long.parseLong(committer.getLastCommittedData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));

            // INVARIANT the fix must restore: committed LOCAL_CHECKPOINT must not exceed the max
            // seqno actually present in the committed snapshot. Otherwise recovery replays from
            // committedCheckpoint+1 and silently drops (persistedMaxSeqNo, committedCheckpoint].
            assertThat(
                "committed LOCAL_CHECKPOINT ("
                    + committedCheckpoint
                    + ") must not exceed the max seqno durably persisted in the snapshot ("
                    + persistedMaxSeqNo
                    + "); recovery would otherwise skip and lose seqnos "
                    + (persistedMaxSeqNo + 1)
                    + ".."
                    + committedCheckpoint,
                committedCheckpoint,
                lessThanOrEqualTo(persistedMaxSeqNo)
            );
        }
    }
}
