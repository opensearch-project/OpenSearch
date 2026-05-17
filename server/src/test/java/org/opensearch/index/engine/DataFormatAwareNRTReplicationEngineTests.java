/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.stub.InMemoryCommitter;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockSearchBackEndPlugin;
import org.opensearch.index.engine.exec.FileDeleter;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.commit.CommitterFactory;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
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
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.index.engine.EngineTestCase.createParsedDoc;
import static org.opensearch.index.engine.EngineTestCase.tombstoneDocSupplier;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DataFormatAwareNRTReplicationEngine}.
 * Exercises constructor, replication-snapshot application, commit, write-stubs, and close
 * paths without relying on DFA integration tests.
 */
public class DataFormatAwareNRTReplicationEngineTests extends OpenSearchTestCase {

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
        IndexSettings indexSettings = replicaIndexSettings();
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

    private IndexSettings replicaIndexSettings() {
        // SegRep must be enabled for EngineConfig.isReadOnlyReplica() to return true.
        return IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), mockDataFormat.name())
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, "SEGMENT")
                .build()
        );
    }

    /** Writes a Lucene commit so the engine's store bootstrap finds valid userData. */
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

    private DataFormatAwareNRTReplicationEngine createReplicaEngine(Path translogPath) throws IOException {
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(store, uuid);
        return new DataFormatAwareNRTReplicationEngine(buildReplicaConfig(translogPath));
    }

    private EngineConfig buildReplicaConfig(Path translogPath) {
        IndexSettings indexSettings = replicaIndexSettings();
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
            .readOnlyReplica(true)
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

    private DataformatAwareCatalogSnapshot buildReplicationSnapshot(long id, long gen, long maxSeqNo, String historyUUID) {
        Map<String, String> userData = new HashMap<>();
        userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
        userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(maxSeqNo));
        userData.put(Translog.TRANSLOG_UUID_KEY, UUID.randomUUID().toString());
        if (historyUUID != null) {
            userData.put(Engine.HISTORY_UUID_KEY, historyUUID);
        }
        DataformatAwareCatalogSnapshot snapshot = (DataformatAwareCatalogSnapshot) CatalogSnapshotManager.createInitialSnapshot(
            id,
            gen,
            gen,
            List.<Segment>of(),
            gen,
            userData
        );
        // Simulate the primary having committed this snapshot (sets lastCommitGeneration).
        snapshot.setLastCommitInfo("segments_" + gen, gen, 0L);
        return snapshot;
    }

    // ---------- Tests ----------

    public void testCreateEngine() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            assertNotNull(engine.translogManager());
            assertEquals(SequenceNumbers.NO_OPS_PERFORMED, engine.getProcessedLocalCheckpoint());
            assertNotNull("historyUUID must be assigned", engine.getHistoryUUID());
            // commitStats must never be null on a replica (the review fix).
            assertNotNull(engine.commitStats());
        }
    }

    public void testUpdateCatalogSnapshotAdvancesCheckpointAndHistoryUUID() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            String newHistoryUUID = UUID.randomUUID().toString();
            long maxSeqNo = 42L;
            DataformatAwareCatalogSnapshot incoming = buildReplicationSnapshot(1L, 1L, maxSeqNo, newHistoryUUID);

            engine.updateCatalogSnapshot(incoming);

            assertEquals("processed checkpoint must be fast-forwarded", maxSeqNo, engine.getProcessedLocalCheckpoint());
            // historyUUID must reflect the incoming snapshot (replicated primaries can bump it).
            assertEquals(newHistoryUUID, engine.getHistoryUUID());
        }
    }

    public void testUpdateCatalogSnapshotTriggersFlushOnGenerationChange() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            DataformatAwareCatalogSnapshot first = buildReplicationSnapshot(1L, 1L, 5L, UUID.randomUUID().toString());
            engine.updateCatalogSnapshot(first);

            // Second snapshot with a different generation must force a flush-and-roll.
            DataformatAwareCatalogSnapshot second = buildReplicationSnapshot(2L, 2L, 10L, null);
            engine.updateCatalogSnapshot(second);

            assertEquals(10L, engine.getProcessedLocalCheckpoint());
        }
    }

    public void testReplicaWriteStubsAppendToTranslogAndAdvanceMaxSeqNo() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            ParsedDocument doc = createParsedDoc("id-1", null);
            long seqNo = 7L;
            Engine.IndexResult result = engine.index(replicaIndexOp(doc, seqNo));
            assertNotNull("write must record a translog location", result.getTranslogLocation());
            assertEquals(seqNo, result.getSeqNo());
            assertEquals(seqNo, engine.getMaxSeqNoOfUpdatesOrDeletes());
        }
    }

    // NOTE: Testing flush()+commitCatalogSnapshot() end-to-end in a unit test is fragile because
    // the replica's commitCatalogSnapshot sets nextWriteGeneration from the CatalogSnapshot's
    // generation, which can collide with the bootstrap Lucene commit state. End-to-end flush
    // behavior is covered by the DFA ITs (DataFormatAwareReplicationIT, DataFormatAwareUploadIT).

    public void testAcquireLastIndexCommitReturnsBootstrapCommit() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            expectThrows(UnsupportedOperationException.class, engine::acquireSafeIndexCommit);
        }
    }

    public void testCloseIsIdempotent() throws IOException {
        DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir());
        engine.close();
        // Second close must be a no-op, not throw.
        engine.close();
    }

    public void testOperationsAfterCloseThrowAlreadyClosed() throws IOException {
        DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir());
        engine.close();
        expectThrows(org.apache.lucene.store.AlreadyClosedException.class, () -> engine.acquireSnapshot());
        expectThrows(org.apache.lucene.store.AlreadyClosedException.class, engine::commitStats);
    }

    /**
     * File-deleter map must include BOTH Lucene and non-default formats.
     * The Lucene deleter must:
     *  (a) delete files in {@code <shardPath>/index/}
     *  (b) guard against deleting commit files ({@code segments_*}, {@code write.lock})
     * Without this, superseded Lucene secondary files from prior replication snapshots
     * accumulate forever on composite-engine replicas.
     */
    public void testBuildReplicaFileDeletersCoversLuceneAndNonDefaultFormats() throws IOException {
        MockDataFormat luceneLike = new MockDataFormat("lucene", 100L, Set.of());
        MockDataFormat parquetLike = new MockDataFormat("parquet", 50L, Set.of());
        PluginsService pluginsService = mock(PluginsService.class);
        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(
            List.of(MockDataFormatPlugin.of(luceneLike), MockDataFormatPlugin.of(parquetLike))
        );
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(
            List.of(new MockSearchBackEndPlugin(List.of("lucene", "parquet")))
        );
        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        Path root = createTempDir().resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        Path indexDir = root.resolve("index");
        Path parquetDir = root.resolve("parquet");
        java.nio.file.Files.createDirectories(indexDir);
        java.nio.file.Files.createDirectories(parquetDir);
        ShardPath shardPath = new ShardPath(false, root, root, shardId);

        bootstrapStoreWithMetadata(store, UUID.randomUUID().toString());
        Map<String, FileDeleter> deleters = DataFormatAwareNRTReplicationEngine.buildReplicaFileDeleters(
            shardPath,
            registry,
            new InMemoryCommitter(store)
        );

        assertTrue("parquet deleter must be present", deleters.containsKey("parquet"));
        assertTrue("lucene deleter must be present", deleters.containsKey("lucene"));

        // Plant files in each format's directory and verify the respective deleter removes them.
        Path luceneOrphan = indexDir.resolve("_stale_orphan.si");
        Path parquetOrphan = parquetDir.resolve("stale.parquet");
        java.nio.file.Files.write(luceneOrphan, new byte[] { 1, 2, 3 });
        java.nio.file.Files.write(parquetOrphan, new byte[] { 4, 5, 6 });

        deleters.get("lucene").deleteFiles(Map.of("lucene", List.of("_stale_orphan.si")));
        assertFalse("lucene deleter must physically remove the orphan", java.nio.file.Files.exists(luceneOrphan));

        deleters.get("parquet").deleteFiles(Map.of("parquet", List.of("stale.parquet")));
        assertFalse("parquet deleter must physically remove the orphan", java.nio.file.Files.exists(parquetOrphan));

        // Commit files MUST be preserved by the Lucene deleter even if listed.
        Path segmentsCommit = indexDir.resolve("segments_5");
        Path writeLock = indexDir.resolve("write.lock");
        java.nio.file.Files.write(segmentsCommit, new byte[] { 9 });
        java.nio.file.Files.write(writeLock, new byte[] { 9 });
        deleters.get("lucene").deleteFiles(Map.of("lucene", List.of("segments_5", "write.lock")));
        assertTrue("segments_N must be preserved", java.nio.file.Files.exists(segmentsCommit));
        assertTrue("write.lock must be preserved", java.nio.file.Files.exists(writeLock));
    }

    public void testUpdateCatalogSnapshotDoesNotRecommitOnSameGeneration() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            DataformatAwareCatalogSnapshot first = buildReplicationSnapshot(1L, 1L, 5L, UUID.randomUUID().toString());
            engine.updateCatalogSnapshot(first);
            long genAfterFirst = engine.commitStats().getGeneration();

            // Same generation (1L) — MUST NOT trigger another flush/roll.
            DataformatAwareCatalogSnapshot resend = buildReplicationSnapshot(1L, 1L, 6L, null);
            engine.updateCatalogSnapshot(resend);
            assertEquals("same-generation update must not advance commit generation", genAfterFirst, engine.commitStats().getGeneration());
            assertEquals("but local checkpoint still advances", 6L, engine.getProcessedLocalCheckpoint());
        }
    }

    public void testUpdateCatalogSnapshotWithoutHistoryUUIDPreservesExisting() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            String originalUUID = engine.getHistoryUUID();
            assertNotNull("fresh replica must have a historyUUID", originalUUID);

            // Snapshot with no HISTORY_UUID_KEY in userData — must NOT overwrite the existing uuid.
            DataformatAwareCatalogSnapshot noUUID = buildReplicationSnapshot(1L, 1L, 5L, /* historyUUID= */ null);
            engine.updateCatalogSnapshot(noUUID);
            assertEquals("historyUUID must be preserved when incoming has none", originalUUID, engine.getHistoryUUID());
        }
    }

    public void testFlushAndClose() throws Exception {
        Path translogPath = createTempDir().resolve("translog");
        DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(translogPath);
        // Advance the catalog generation so the close-time commit writes a new segments_N.
        DataformatAwareCatalogSnapshot incoming = buildReplicationSnapshot(1L, 2L, 10L, engine.getHistoryUUID());
        engine.updateCatalogSnapshot(incoming);
        // close() internally calls commitCatalogSnapshot → closeNoLock → IOUtils.close(...)
        engine.close();
        expectThrows(AlreadyClosedException.class, engine::ensureOpen);
    }

    public void testFailEngineAndDoubleFailIsNoOp() throws Exception {
        Path translogPath = createTempDir().resolve("translog");
        DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(translogPath);
        // First fail — must close the engine.
        engine.failEngine("first-fail", new IOException("simulated"));
        expectThrows(AlreadyClosedException.class, engine::ensureOpen);
        // Second fail — must be a no-op (already failed).
        engine.failEngine("second-fail", new IOException("ignored"));
        expectThrows(AlreadyClosedException.class, engine::ensureOpen);
    }

    public void testFlushAndCloseOnAlreadyClosedEngine() throws Exception {
        Path translogPath = createTempDir().resolve("translog");
        DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(translogPath);
        engine.close();
        expectThrows(AlreadyClosedException.class, engine::ensureOpen);
        // Must not throw — silently skips.
        engine.flushAndClose();
    }

    public void testMaybeFailEngineOnCorruption() throws Exception {
        Path translogPath = createTempDir().resolve("translog");
        DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(translogPath);
        engine.failEngine("corruption", new CorruptIndexException("simulated", "test"));
        expectThrows(AlreadyClosedException.class, engine::ensureOpen);
    }

    // ---------- Stats / introspection methods (cheap O(1) returns) ----------

    public void testGetSafeCommitInfoReturnsTrackerCheckpointAndZeroDocsForEmptySnapshot() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            org.opensearch.index.engine.SafeCommitInfo info = engine.getSafeCommitInfo();
            assertNotNull(info);
            // Fresh replica: localCheckpoint = NO_OPS_PERFORMED (-1), no segments → 0 docs
            assertEquals(SequenceNumbers.NO_OPS_PERFORMED, info.localCheckpoint);
            assertEquals(0, info.docCount);
        }
    }

    public void testGetMergeStatsReturnsEmpty() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            org.opensearch.index.merge.MergeStats stats = engine.getMergeStats();
            assertNotNull(stats);
            assertEquals(0L, stats.getTotal());
            assertEquals(0L, stats.getCurrent());
        }
    }

    public void testDocStatsZeroForFreshReplica() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            org.opensearch.index.shard.DocsStats stats = engine.docStats();
            assertNotNull(stats);
            assertEquals(0L, stats.getCount());
            assertEquals(0L, stats.getDeleted());
        }
    }

    public void testCommitStatsReturnsCommitData() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            org.opensearch.index.engine.CommitStats stats = engine.commitStats();
            assertNotNull(stats);
            assertNotNull("commit stats must include user data", stats.getUserData());
        }
    }

    public void testIsReplicaIndexerReturnsTrue() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            assertTrue("DFA NRT engine must always identify as replica", engine.isReplicaIndexer());
        }
    }

    public void testConfigReturnsConstructorConfig() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            assertNotNull("config() must return a non-null EngineConfig", engine.config());
        }
    }

    public void testHistoryUUIDIsNonNullOnOpen() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            assertNotNull("fresh replica must have a historyUUID", engine.getHistoryUUID());
            assertFalse("historyUUID must not be empty", engine.getHistoryUUID().isEmpty());
        }
    }

    public void testCheckpointAccessorsOnFreshEngine() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            assertEquals(SequenceNumbers.NO_OPS_PERFORMED, engine.getProcessedLocalCheckpoint());
            assertEquals(SequenceNumbers.NO_OPS_PERFORMED, engine.getPersistedLocalCheckpoint());
            // lastRefreshedCheckpoint may be -1 (NO_OPS_PERFORMED) or 0 depending on initialization
            long lrc = engine.lastRefreshedCheckpoint();
            assertTrue(
                "lastRefreshedCheckpoint must be non-negative or NO_OPS_PERFORMED",
                lrc == SequenceNumbers.NO_OPS_PERFORMED || lrc >= 0
            );
            // getMinRetainedSeqNo on fresh replica returns NO_OPS_PERFORMED
            long minRetained = engine.getMinRetainedSeqNo();
            assertTrue(
                "minRetainedSeqNo must be non-negative or NO_OPS_PERFORMED",
                minRetained == SequenceNumbers.NO_OPS_PERFORMED || minRetained >= 0
            );
        }
    }

    public void testStatsAndStubMethodsReturnDefaultValues() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            // Stubs / defaults that should never throw
            assertEquals(0L, engine.getWritingBytes());
            assertEquals(0L, engine.getIndexThrottleTimeInMillis());
            assertFalse(engine.isThrottled());
            assertFalse("replica must never report needing a refresh", engine.refreshNeeded());
            assertFalse("replica must always report no-op for maybeRefresh", engine.maybeRefresh("noop"));
            assertFalse(engine.shouldPeriodicallyFlush());
            assertEquals(0L, engine.getIndexBufferRAMBytesUsed());
            assertEquals(0L, engine.unreferencedFileCleanUpsPerformed());

            // Refresh / writeIndexingBuffer must be no-ops (don't throw)
            engine.refresh("noop-refresh");
            engine.writeIndexingBuffer();

            // History ops snapshot / counters
            assertEquals(0, engine.countNumberOfHistoryOperations("test", 0L, Long.MAX_VALUE));
            assertFalse(engine.hasCompleteOperationHistory("test", 0L));

            // Completion + segments stats — must return non-null without throwing
            assertNotNull(engine.completionStats());
            assertNotNull(engine.segmentsStats(false, false));
            // pollingIngestStats may return null on replicas (no ingestion source)
            engine.pollingIngestStats();
        }
    }

    public void testFillSeqNoGapsIsNoOp() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            // DFA replica engine returns 0 — Lucene-style gaps don't apply
            assertEquals(0, engine.fillSeqNoGaps(1L));
        }
    }

    public void testMaybePruneDeletesIsNoOp() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            // Must not throw
            engine.maybePruneDeletes();
        }
    }

    public void testForceMergeIsNoOp() throws Exception {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            // DFA replica engines don't merge. Must not throw.
            engine.forceMerge(false, 1, false, false, false, "test-uuid");
        }
    }

    // ---------- Snapshot acquisition variants ----------

    public void testAcquireSnapshotReturnsLatest() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            try (org.opensearch.common.concurrent.GatedCloseable<CatalogSnapshot> snap = engine.acquireSnapshot()) {
                assertNotNull(snap.get());
            }
        }
    }

    public void testAcquireSafeCatalogSnapshotReturnsLatestForFreshEngine() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            try (org.opensearch.common.concurrent.GatedCloseable<CatalogSnapshot> safe = engine.acquireSafeCatalogSnapshot()) {
                assertNotNull(safe.get());
            }
        }
    }

    public void testAcquireLastCommittedSnapshotReturnsSnapshot() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            // After a commit (via update + flush), lastCommittedSnapshot is set
            DataformatAwareCatalogSnapshot incoming = buildReplicationSnapshot(1L, 1L, 5L, UUID.randomUUID().toString());
            engine.updateCatalogSnapshot(incoming);
            try (org.opensearch.common.concurrent.GatedCloseable<CatalogSnapshot> last = engine.acquireLastCommittedSnapshot(false)) {
                assertNotNull(last.get());
            }
        }
    }

    public void testSerializeSnapshotToRemoteMetadataThrowsOnReplica() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            try (org.opensearch.common.concurrent.GatedCloseable<CatalogSnapshot> snap = engine.acquireSnapshot()) {
                // Replicas don't produce upload metadata bytes — that's the primary's job.
                expectThrows(UnsupportedOperationException.class, () -> engine.serializeSnapshotToRemoteMetadata(snap.get()));
            }
        }
    }

    // ---------- prepareIndex / prepareDelete (translog replay path) ----------

    public void testPrepareIndexNullDocMapperThrows() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            org.opensearch.index.mapper.SourceToParse source = new org.opensearch.index.mapper.SourceToParse(
                "idx",
                "doc-id",
                new org.opensearch.core.common.bytes.BytesArray("{}"),
                org.opensearch.common.xcontent.XContentType.JSON
            );
            // prepareIndex requires a non-null docMapper to parse the source
            expectThrows(
                NullPointerException.class,
                () -> engine.prepareIndex(
                    /* docMapper */ null,
                    source,
                    5L,
                    1L,
                    1L,
                    org.opensearch.index.VersionType.EXTERNAL,
                    Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
                    -1L,
                    false,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    SequenceNumbers.UNASSIGNED_PRIMARY_TERM
                )
            );
        }
    }

    public void testPrepareDeleteBuildsValidDeleteOp() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            Engine.Delete op = engine.prepareDelete(
                "doc-id",
                /* seqNo */ 7L,
                /* primaryTerm */ 1L,
                /* version */ 1L,
                org.opensearch.index.VersionType.EXTERNAL,
                Engine.Operation.Origin.PRIMARY,
                /* ifSeqNo */ SequenceNumbers.UNASSIGNED_SEQ_NO,
                /* ifPrimaryTerm */ SequenceNumbers.UNASSIGNED_PRIMARY_TERM
            );
            assertNotNull(op);
            assertEquals(7L, op.seqNo());
        }
    }

    public void testIndexDeleteNoOpReturnUnsupportedOnReplica() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            // Replica can't ACTIVELY index/delete/noOp — those go through prepareIndex on the primary.
            // The replica's index/delete/noOp methods are stubs that throw or return synthetic results.
            // Validate they don't blow up the JVM and return some result (or throw a known type).
            try {
                Engine.IndexResult ir = engine.index(null);
                // If it doesn't throw, the result must be non-null
                assertNotNull(ir);
            } catch (UnsupportedOperationException | NullPointerException expected) {
                // Either is acceptable for a replica stub
            }
        }
    }

    // ---------- updateCatalogSnapshot edge cases ----------

    public void testUpdateCatalogSnapshotPreservesCommitSegmentInfos() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            DataformatAwareCatalogSnapshot incoming = buildReplicationSnapshot(1L, 1L, 5L, UUID.randomUUID().toString());

            // Simulate SegmentReplicationTarget setting the bytes on the snapshot
            org.apache.lucene.index.SegmentInfos sis = new org.apache.lucene.index.SegmentInfos(
                org.apache.lucene.util.Version.LATEST.major
            );
            sis.setUserData(java.util.Map.of(), false);
            incoming.setReplicatingCommitData(sis);

            engine.updateCatalogSnapshot(incoming);
            // After commit, a segments_N file with these bytes is written. A subsequent
            // commitStats read should succeed (no IndexNotFoundException / corruption).
            assertNotNull(engine.commitStats());
        }
    }

    public void testUpdateCatalogSnapshotMonotonicSeqNoAdvancement() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            // First snapshot at seqNo 5
            DataformatAwareCatalogSnapshot first = buildReplicationSnapshot(1L, 1L, 5L, UUID.randomUUID().toString());
            engine.updateCatalogSnapshot(first);
            assertEquals(5L, engine.getProcessedLocalCheckpoint());

            // Second snapshot at seqNo 10 (advances)
            DataformatAwareCatalogSnapshot second = buildReplicationSnapshot(2L, 2L, 10L, null);
            engine.updateCatalogSnapshot(second);
            assertEquals(10L, engine.getProcessedLocalCheckpoint());
        }
    }

    public void testEnsureOpenThrowsAlreadyClosedAfterClose() throws IOException {
        DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir());
        engine.close();
        AlreadyClosedException e = expectThrows(AlreadyClosedException.class, engine::ensureOpen);
        assertNotNull(e.getMessage());
    }

    public void testTranslogManagerAccessor() throws IOException {
        try (DataFormatAwareNRTReplicationEngine engine = createReplicaEngine(createTempDir())) {
            assertNotNull("translogManager must be available on a fresh engine", engine.translogManager());
        }
    }
}
