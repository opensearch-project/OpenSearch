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
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockDocumentInput;
import org.opensearch.index.engine.dataformat.stub.MockSearchBackEndPlugin;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterFactory;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
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
import org.opensearch.index.translog.TranslogStats;
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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.index.engine.EngineTestCase.createParsedDoc;
import static org.opensearch.index.engine.EngineTestCase.tombstoneDocSupplier;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Recovery tests for {@link DataFormatAwareEngine}.
 * Mirrors the recovery test patterns from {@code InternalEngineTests} (L1024-1133)
 * to verify translog replay, checkpoint advancement, and segment creation
 * after engine close/reopen.
 */
public class DataFormatAwareEngineRecoveryTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private Store store;
    private ShardId shardId;
    private AtomicLong primaryTerm;
    private AtomicLong globalCheckpoint;
    private MockDataFormat mockDataFormat;
    private MockDataFormatPlugin mockPlugin;
    private Path translogPath;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        shardId = new ShardId(new Index("test", "_na_"), 0);
        primaryTerm = new AtomicLong(1);
        globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        mockDataFormat = new MockDataFormat("composite", 100L, Set.of());
        mockPlugin = MockDataFormatPlugin.of(mockDataFormat);
        threadPool = new TestThreadPool(getClass().getName());
        store = createStore();
        translogPath = createTempDir("translog");
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

    // ---- Committer that persists to Lucene (survives engine reopen) ----

    /**
     * A committer that writes commit data back to the Lucene IndexWriter,
     * so it survives engine close/reopen. This simulates the production
     * LuceneCommitter behavior for recovery tests.
     */
    static class PersistentCommitter implements Committer {
        private final Store store;
        private volatile Map<String, String> committedData;
        private volatile CatalogSnapshot lastCommittedSnapshot;

        PersistentCommitter(Store store) throws IOException {
            this.store = store;
            this.committedData = Map.copyOf(store.readLastCommittedSegmentsInfo().getUserData());
            // Deserialize existing catalog snapshot if present
            String serialized = committedData.get(CatalogSnapshot.CATALOG_SNAPSHOT_KEY);
            if (serialized != null) {
                try {
                    this.lastCommittedSnapshot = DataformatAwareCatalogSnapshot.deserializeFromString(serialized, dir -> dir);
                } catch (IOException e) {
                    // Deserialization failed — start without committed snapshot
                }
            }
        }

        @Override
        public void commit(Map<String, String> commitData) throws IOException {
            try (
                IndexWriter writer = new IndexWriter(
                    store.directory(),
                    new IndexWriterConfig(Lucene.STANDARD_ANALYZER).setMergePolicy(NoMergePolicy.INSTANCE)
                        .setOpenMode(IndexWriterConfig.OpenMode.APPEND)
                )
            ) {
                writer.setLiveCommitData(commitData.entrySet());
                writer.commit();
            }
            this.committedData = Map.copyOf(commitData);
            // Store the catalog snapshot if present in commit data
            String serialized = commitData.get(CatalogSnapshot.CATALOG_SNAPSHOT_KEY);
            if (serialized != null) {
                try {
                    this.lastCommittedSnapshot = DataformatAwareCatalogSnapshot.deserializeFromString(serialized, dir -> dir);
                } catch (IOException e) {
                    // If deserialization fails, keep the previous snapshot
                }
            }
        }

        @Override
        public Map<String, String> getLastCommittedData() {
            return committedData;
        }

        @Override
        public CommitStats getCommitStats() {
            return null;
        }

        @Override
        public SafeCommitInfo getSafeCommitInfo() {
            return SafeCommitInfo.EMPTY;
        }

        @Override
        public List<CatalogSnapshot> listCommittedSnapshots() {
            if (lastCommittedSnapshot != null) {
                return List.of(lastCommittedSnapshot);
            }
            return List.of();
        }

        @Override
        public void deleteCommit(CatalogSnapshot snapshot) {}

        @Override
        public boolean isCommitManagedFile(String fileName) {
            return false;
        }

        @Override
        public void close() {}
    }

    // ---- Store and engine creation helpers ----

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

    private void bootstrapStore(Store store, String translogUUID) throws IOException {
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

    private DataFormatAwareEngine createEngine() throws IOException {
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStore(store, uuid);
        return new DataFormatAwareEngine(buildEngineConfig());
    }

    private DataFormatAwareEngine reopenEngine() throws IOException {
        return new DataFormatAwareEngine(buildEngineConfig());
    }

    private EngineConfig buildEngineConfig() {
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
        CommitterFactory committerFactory = config -> new PersistentCommitter(store);

        return new EngineConfig.Builder().shardId(shardId)
            .threadPool(threadPool)
            .indexSettings(indexSettings)
            .store(store)
            .mergePolicy(NoMergePolicy.INSTANCE)
            .translogConfig(translogConfig)
            .flushMergesAfter(TimeValue.timeValueMinutes(5))
            .externalRefreshListener(List.of())
            .internalRefreshListener(List.of())
            .globalCheckpointSupplier(globalCheckpoint::get)
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
            List.of(new MockSearchBackEndPlugin(List.of(mockDataFormat.name())))
        );
        return new DataFormatRegistry(pluginsService);
    }

    private Engine.Index indexOp(String id) {
        ParsedDocument doc = createParsedDocWithInput(id);
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

    private Engine.Index replayOp(Translog.Index translogOp) {
        ParsedDocument doc = createParsedDocWithInput(translogOp.id());
        return new Engine.Index(
            new Term(IdFieldMapper.NAME, Uid.encodeId(doc.id())),
            doc,
            translogOp.seqNo(),
            translogOp.primaryTerm(),
            translogOp.version(),
            null,
            Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
            System.nanoTime(),
            translogOp.getAutoGeneratedIdTimestamp(),
            false,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
    }

    private ParsedDocument createParsedDocWithInput(String id) {
        ParsedDocument base = createParsedDoc(id, null);
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

    private int recoverFromTranslog(DataFormatAwareEngine engine) throws IOException {
        int recovered = engine.translogManager().recoverFromTranslog(snapshot -> {
            int ops = 0;
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                if (op instanceof Translog.Index) {
                    engine.index(replayOp((Translog.Index) op));
                    ops++;
                }
            }
            return ops;
        }, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
        // Advance global checkpoint after recovery (simulates single-node cluster)
        globalCheckpoint.set(engine.getProcessedLocalCheckpoint());
        return recovered;
    }

    // ---- Recovery Tests ----

    /**
     * Mirrors InternalEngineTests.testTranslogRecoveryDoesNotReplayIntoTranslog (L1024).
     *
     * Index docs, close engine (without flush — all ops are in translog only),
     * reopen, recover. Verify:
     * - Translog has uncommitted ops before recovery
     * - After recovery, ops are committed (flush triggered by onAfterTranslogRecovery)
     * - Replayed ops are NOT double-written to translog
     */
    public void testTranslogRecoveryDoesNotReplayIntoTranslog() throws IOException {
        final int docs = randomIntBetween(1, 32);

        // Index docs and close WITHOUT flush — all ops stay in translog
        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            for (int i = 0; i < docs; i++) {
                engine.index(indexOp(Integer.toString(i)));
            }
            // Advance global checkpoint to match local (simulates single-node replication)
            globalCheckpoint.set(engine.getProcessedLocalCheckpoint());
            // DO NOT flush — close with data only in translog
        }

        // Reopen — translog should have all ops as uncommitted
        try (DataFormatAwareEngine engine = reopenEngine()) {
            TranslogStats stats = engine.translogManager().getTranslogStats();
            assertThat("all ops should be uncommitted in translog", stats.getUncommittedOperations(), equalTo(docs));

            // Recover — this replays ops and triggers flush via onAfterTranslogRecovery
            int recovered = recoverFromTranslog(engine);
            assertThat("should recover all docs", recovered, equalTo(docs));

            // After recovery, checkpoint should advance
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) docs - 1));

            // Translog should be trimmed after recovery flush
            globalCheckpoint.set(engine.getProcessedLocalCheckpoint());
            TranslogStats afterStats = engine.translogManager().getTranslogStats();
            assertThat("translog should be trimmed after recovery flush", afterStats.getUncommittedOperations(), equalTo(0));
        }
    }

    /**
     * Mirrors InternalEngineTests.testTranslogRecoveryWithMultipleGenerations (L1059).
     *
     * Index docs with random flushes and translog rolls, close, reopen, recover.
     * Verify all docs are recovered and checkpoint is correct.
     */
    public void testTranslogRecoveryWithMultipleGenerations() throws IOException {
        final int docs = randomIntBetween(10, 100);

        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            for (int i = 0; i < docs; i++) {
                engine.index(indexOp(Integer.toString(i)));
                if (rarely()) {
                    engine.translogManager().rollTranslogGeneration();
                } else if (rarely()) {
                    engine.flush(randomBoolean(), true);
                }
            }
        }

        // Reopen and recover
        try (DataFormatAwareEngine engine = reopenEngine()) {
            int recovered = recoverFromTranslog(engine);

            // Checkpoint must reflect all docs
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) docs - 1));
            assertThat(engine.getSeqNoStats(-1).getMaxSeqNo(), equalTo((long) docs - 1));

            // After recovery + refresh, segments should exist in the catalog snapshot
            engine.refresh("test");
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat("snapshot should have segments after recovery", snapshot.getSegments().size(), greaterThan(0));
            }
        }
    }

    /**
     * Mirrors InternalEngineTests.testRecoveryFromTranslogUpToSeqNo (L1099).
     *
     * Index docs, recover up to a partial seqNo (simulating recovery up to globalCheckpoint).
     * Verify checkpoint advances only to the recovered seqNo.
     */
    public void testRecoveryFromTranslogUpToSeqNo() throws IOException {
        final int docs = randomIntBetween(10, 50);

        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            for (int i = 0; i < docs; i++) {
                engine.index(indexOp(Integer.toString(i)));
            }
            // No flush — close with everything in translog
        }

        // Recover up to a partial seqNo
        final long recoverUpTo = randomLongBetween(0, docs - 1);

        try (DataFormatAwareEngine engine = reopenEngine()) {
            int recovered = engine.translogManager().recoverFromTranslog(snapshot -> {
                int ops = 0;
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    if (op instanceof Translog.Index) {
                        engine.index(replayOp((Translog.Index) op));
                        ops++;
                    }
                }
                return ops;
            }, engine.getProcessedLocalCheckpoint(), recoverUpTo);

            assertThat(engine.getProcessedLocalCheckpoint(), equalTo(recoverUpTo));
            assertThat(recovered, equalTo((int) (recoverUpTo + 1)));
        }
    }

    /**
     * DFA-specific: Index docs, flush (commit), index more without flush, close, reopen.
     * Only the unflushed docs should be replayed from translog.
     */
    public void testRecoveryOnlyReplaysUnflushedOps() throws IOException {
        final int flushedDocs = randomIntBetween(5, 20);
        final int unflushedDocs = randomIntBetween(5, 20);

        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            // Index and flush — these are committed
            for (int i = 0; i < flushedDocs; i++) {
                engine.index(indexOp(Integer.toString(i)));
            }
            engine.refresh("before-flush");
            engine.flush(false, true);

            // Index more WITHOUT flush — these are only in translog
            for (int i = flushedDocs; i < flushedDocs + unflushedDocs; i++) {
                engine.index(indexOp(Integer.toString(i)));
            }
        }

        // Reopen — should only need to replay unflushedDocs
        try (DataFormatAwareEngine engine = reopenEngine()) {
            TranslogStats stats = engine.translogManager().getTranslogStats();
            assertThat("only unflushed ops should be uncommitted", stats.getUncommittedOperations(), equalTo(unflushedDocs));

            // The committed local checkpoint should be at flushedDocs - 1
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) flushedDocs - 1));

            // Recover
            int recovered = recoverFromTranslog(engine);
            assertThat("should recover only unflushed docs", recovered, equalTo(unflushedDocs));

            // After recovery, checkpoint should cover all docs
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) (flushedDocs + unflushedDocs - 1)));
        }
    }

    /**
     * DFA-specific: Verify that after recovery and refresh, the catalog snapshot
     * contains segments with the correct structure.
     */
    public void testRecoveryProducesValidCatalogSnapshot() throws IOException {
        final int docs = randomIntBetween(5, 30);

        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            for (int i = 0; i < docs; i++) {
                engine.index(indexOp(Integer.toString(i)));
            }
        }

        try (DataFormatAwareEngine engine = reopenEngine()) {
            recoverFromTranslog(engine);
            engine.refresh("test");

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat(snapshot, org.hamcrest.Matchers.notNullValue());
                assertThat(snapshot.getSegments().size(), greaterThan(0));

                // Snapshot should have files for our format
                Map<String, java.util.Collection<String>> filesByFormat = snapshot.getFilesByFormat();
                assertThat("snapshot should have files for the data format", filesByFormat.size(), greaterThanOrEqualTo(1));
            }
        }
    }

    /**
     * DFA-specific: After recovery, verify the engine is fully functional —
     * can index new docs, refresh, and flush without errors.
     */
    public void testEngineIsFunctionalAfterRecovery() throws IOException {
        final int initialDocs = randomIntBetween(5, 20);

        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            for (int i = 0; i < initialDocs; i++) {
                engine.index(indexOp(Integer.toString(i)));
            }
        }

        try (DataFormatAwareEngine engine = reopenEngine()) {
            recoverFromTranslog(engine);

            // Engine should accept new writes after recovery
            int newDocs = randomIntBetween(5, 20);
            for (int i = initialDocs; i < initialDocs + newDocs; i++) {
                Engine.IndexResult result = engine.index(indexOp(Integer.toString(i)));
                assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            }

            // Refresh should produce segments
            engine.refresh("after-new-writes");
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                assertThat(ref.get().getSegments().size(), greaterThan(0));
            }

            // Flush should succeed
            engine.flush(false, true);

            // Final checkpoint should reflect all docs
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) (initialDocs + newDocs - 1)));
        }
    }

    /**
     * DFA-specific: Verify that the committed data after recovery contains
     * all required metadata keys for subsequent recovery.
     */
    public void testRecoveryFlushWritesCorrectCommitData() throws IOException {
        final int docs = randomIntBetween(5, 20);

        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            for (int i = 0; i < docs; i++) {
                engine.index(indexOp(Integer.toString(i)));
            }
        }

        try (DataFormatAwareEngine engine = reopenEngine()) {
            recoverFromTranslog(engine);
            // onAfterTranslogRecovery triggers flush, which writes commit data

            // Read back the committed data (written by the PersistentCommitter to Lucene)
            Map<String, String> commitData = store.readLastCommittedSegmentsInfo().getUserData();

            assertThat(commitData.get(Translog.TRANSLOG_UUID_KEY), org.hamcrest.Matchers.notNullValue());
            assertThat(commitData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY), equalTo(Long.toString(docs - 1)));
            assertThat(commitData.get(SequenceNumbers.MAX_SEQ_NO), equalTo(Long.toString(docs - 1)));
            assertThat(commitData.get(Engine.HISTORY_UUID_KEY), org.hamcrest.Matchers.notNullValue());
        }
    }

    /**
     * Verify double-restart works: index → close → recover → close → recover.
     * The second recovery should have nothing to replay (since the first recovery flushed).
     */
    public void testDoubleRecovery() throws IOException {
        final int docs = randomIntBetween(5, 20);

        // First lifecycle: index docs, close without flush
        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            for (int i = 0; i < docs; i++) {
                engine.index(indexOp(Integer.toString(i)));
            }
        }

        // Second lifecycle: recover (flush happens), close
        try (DataFormatAwareEngine engine = reopenEngine()) {
            recoverFromTranslog(engine);
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) docs - 1));
        }

        // Third lifecycle: should have nothing to recover
        try (DataFormatAwareEngine engine = reopenEngine()) {
            TranslogStats stats = engine.translogManager().getTranslogStats();
            assertThat("nothing to recover after prior recovery flushed", stats.getUncommittedOperations(), equalTo(0));

            int recovered = recoverFromTranslog(engine);
            assertThat("no ops should be recovered", recovered, equalTo(0));
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) docs - 1));
        }
    }

    /**
     * Regression test: restart with committed data and empty translog.
     * The reader managers must be notified about existing committed segments
     * even though no translog replay or refresh occurs.
     *
     * Without the fix in CatalogSnapshotManager, the afterRefresh callback
     * would never fire and reader managers would have no open readers.
     */
    public void testReaderManagersInitializedOnRestartWithCommittedData() throws IOException {
        final int docs = randomIntBetween(5, 20);

        // First lifecycle: index docs, flush (commit), close cleanly
        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            for (int i = 0; i < docs; i++) {
                engine.index(indexOp(Integer.toString(i)));
            }
            globalCheckpoint.set(engine.getProcessedLocalCheckpoint());
            engine.refresh("before-flush");
            engine.flush(false, true);
            // Everything is committed — translog is empty after flush
        }

        // Second lifecycle: reopen with committed data, empty translog
        try (DataFormatAwareEngine engine = reopenEngine()) {
            // No translog ops to replay
            int recovered = recoverFromTranslog(engine);
            assertThat("nothing to recover — all committed", recovered, equalTo(0));

            // The catalog snapshot should have segments from the committed data
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat("committed snapshot must have segments", snapshot.getSegments().size(), greaterThan(0));
                // Verify files exist in the snapshot
                Map<String, java.util.Collection<String>> filesByFormat = snapshot.getFilesByFormat();
                assertThat("committed snapshot must have format files", filesByFormat.isEmpty(), equalTo(false));
            }

            // Engine should be fully functional for new writes
            Engine.IndexResult result = engine.index(indexOp(Integer.toString(docs)));
            assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) docs));
        }
    }

    /**
     * Multi-threaded indexing before close, then recovery. Verifies no sequence
     * gaps exist after recovery — the checkpoint must be contiguous.
     */
    public void testConcurrentIndexingThenRecoveryPreservesCheckpoint() throws Exception {
        final int numThreads = randomIntBetween(2, 4);
        final int docsPerThread = randomIntBetween(10, 25);
        final int totalDocs = numThreads * docsPerThread;

        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            AtomicInteger failures = new AtomicInteger(0);

            Thread[] threads = new Thread[numThreads];
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int d = 0; d < docsPerThread; d++) {
                            engine.index(indexOp(threadId + "_" + d));
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
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) totalDocs - 1));
        }

        // Reopen and recover — checkpoint must be contiguous with no gaps
        try (DataFormatAwareEngine engine = reopenEngine()) {
            int recovered = recoverFromTranslog(engine);
            assertThat(recovered, equalTo(totalDocs));
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) totalDocs - 1));

            // Verify seq-no stats are consistent
            assertThat(engine.getSeqNoStats(-1).getMaxSeqNo(), equalTo((long) totalDocs - 1));
        }
    }

    /**
     * After recovery, subsequent refreshes must produce monotonically increasing
     * snapshot generations — no reuse or regression.
     */
    public void testSnapshotGenerationAdvancesMonotonicallyAfterRecovery() throws IOException {
        final int docs = randomIntBetween(5, 20);

        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            for (int i = 0; i < docs; i++) {
                engine.index(indexOp(Integer.toString(i)));
            }
        }

        try (DataFormatAwareEngine engine = reopenEngine()) {
            recoverFromTranslog(engine);

            long prevGen = -1;
            int numRefreshes = randomIntBetween(3, 6);
            for (int r = 0; r < numRefreshes; r++) {
                engine.index(indexOp("post_recovery_" + r));
                engine.refresh("refresh-" + r);

                try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                    long currentGen = ref.get().getGeneration();
                    assertThat("generation must strictly increase", currentGen, greaterThan(prevGen));
                    prevGen = currentGen;
                }
            }
        }
    }

    /**
     * After recovery and refresh, docStats().getCount() must equal the total number
     * of indexed documents (committed + recovered).
     */
    public void testDocStatsAfterRecoveryReflectsAllDocs() throws IOException {
        final int flushedDocs = randomIntBetween(5, 15);
        final int unflushedDocs = randomIntBetween(5, 15);
        final int totalDocs = flushedDocs + unflushedDocs;

        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            for (int i = 0; i < flushedDocs; i++) {
                engine.index(indexOp(Integer.toString(i)));
            }
            engine.refresh("before-flush");
            engine.flush(false, true);

            for (int i = flushedDocs; i < totalDocs; i++) {
                engine.index(indexOp(Integer.toString(i)));
            }
        }

        try (DataFormatAwareEngine engine = reopenEngine()) {
            recoverFromTranslog(engine);
            engine.refresh("post-recovery");

            assertThat(engine.docStats().getCount(), equalTo((long) totalDocs));
        }
    }

    /**
     * Interleave indexing with random refreshes and flushes, then close and recover.
     * Verifies no data loss under the mixed workload.
     */
    public void testInterleavedIndexRefreshFlushThenRecover() throws IOException {
        final int docs = randomIntBetween(20, 60);

        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            for (int i = 0; i < docs; i++) {
                engine.index(indexOp(Integer.toString(i)));
                if (randomIntBetween(1, 10) == 1) {
                    engine.refresh("interleaved-refresh-" + i);
                }
                if (randomIntBetween(1, 15) == 1) {
                    engine.flush(false, true);
                }
            }
            globalCheckpoint.set(engine.getProcessedLocalCheckpoint());
        }

        try (DataFormatAwareEngine engine = reopenEngine()) {
            int recovered = recoverFromTranslog(engine);
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) docs - 1));

            engine.refresh("post-recovery");
            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat(snapshot.getSegments().size(), greaterThan(0));

                long totalRows = snapshot.getSegments()
                    .stream()
                    .flatMap(s -> s.dfGroupedSearchableFiles().values().stream())
                    .mapToLong(org.opensearch.index.engine.exec.WriterFileSet::numRows)
                    .sum();
                assertThat("total rows must equal total docs indexed", totalRows, equalTo((long) docs));
            }
        }
    }

    /**
     * After recovery, verify that a newChangesSnapshot contains all ops for the
     * full seq-no range — confirms no operations were lost during the recovery cycle.
     */
    public void testTranslogSnapshotContainsAllOpsAfterRecovery() throws IOException {
        final int docs = randomIntBetween(10, 40);

        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            for (int i = 0; i < docs; i++) {
                engine.index(indexOp(Integer.toString(i)));
            }
        }

        try (DataFormatAwareEngine engine = reopenEngine()) {
            recoverFromTranslog(engine);

            try (Translog.Snapshot snapshot = engine.newChangesSnapshot("test", 0, docs - 1, false, true)) {
                int count = 0;
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    assertThat(op.seqNo(), greaterThanOrEqualTo(0L));
                    count++;
                }
                assertThat("all ops must be present in translog snapshot after recovery", count, equalTo(docs));
            }
        }
    }

    /**
     * Concurrent indexing followed by flush, then recovery. Verifies the committed
     * catalog snapshot survives and is correctly restored.
     */
    public void testConcurrentIndexThenFlushAndRecover() throws Exception {
        final int numThreads = randomIntBetween(2, 4);
        final int docsPerThread = randomIntBetween(10, 20);
        final int totalDocs = numThreads * docsPerThread;

        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            AtomicInteger failures = new AtomicInteger(0);

            Thread[] threads = new Thread[numThreads];
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                threads[t] = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int d = 0; d < docsPerThread; d++) {
                            engine.index(indexOp(threadId + "_" + d));
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

            engine.refresh("before-flush");
            engine.flush(false, true);
            globalCheckpoint.set(engine.getProcessedLocalCheckpoint());
        }

        // Reopen — committed data, empty translog
        try (DataFormatAwareEngine engine = reopenEngine()) {
            int recovered = recoverFromTranslog(engine);
            assertThat("all committed — nothing to recover from translog", recovered, equalTo(0));
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) totalDocs - 1));

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat("committed snapshot must have segments", snapshot.getSegments().size(), greaterThan(0));

                long totalRows = snapshot.getSegments()
                    .stream()
                    .flatMap(s -> s.dfGroupedSearchableFiles().values().stream())
                    .mapToLong(org.opensearch.index.engine.exec.WriterFileSet::numRows)
                    .sum();
                assertThat("total rows must match total docs", totalRows, equalTo((long) totalDocs));
            }
        }
    }

    /**
     * Regression test for the CatalogSnapshotManager fix: multiple restart cycles
     * with flush each time must correctly propagate the catalog snapshot.
     *
     * Lifecycle: index→flush→close→reopen→index→flush→close→reopen. Each reopen
     * must see the accumulated data from all prior lifecycles.
     */
    public void testMultipleFlushRestartCyclesAccumulateData() throws IOException {
        int accumulatedDocs = 0;

        // First lifecycle
        int firstBatch = randomIntBetween(5, 15);
        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            for (int i = 0; i < firstBatch; i++) {
                engine.index(indexOp(Integer.toString(accumulatedDocs + i)));
            }
            accumulatedDocs += firstBatch;
            globalCheckpoint.set(engine.getProcessedLocalCheckpoint());
            engine.refresh("flush-1");
            engine.flush(false, true);
        }

        // Second lifecycle: recover, add more, flush
        int secondBatch = randomIntBetween(5, 15);
        try (DataFormatAwareEngine engine = reopenEngine()) {
            recoverFromTranslog(engine);
            for (int i = 0; i < secondBatch; i++) {
                engine.index(indexOp(Integer.toString(accumulatedDocs + i)));
            }
            accumulatedDocs += secondBatch;
            globalCheckpoint.set(engine.getProcessedLocalCheckpoint());
            engine.refresh("flush-2");
            engine.flush(false, true);
        }

        // Third lifecycle: verify accumulated snapshot
        final int expectedTotal = accumulatedDocs;
        try (DataFormatAwareEngine engine = reopenEngine()) {
            int recovered = recoverFromTranslog(engine);
            assertThat("all committed — nothing to recover", recovered, equalTo(0));
            assertThat(engine.getProcessedLocalCheckpoint(), equalTo((long) expectedTotal - 1));

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                assertThat(snapshot.getSegments().size(), greaterThan(0));

                long totalRows = snapshot.getSegments()
                    .stream()
                    .flatMap(s -> s.dfGroupedSearchableFiles().values().stream())
                    .mapToLong(org.opensearch.index.engine.exec.WriterFileSet::numRows)
                    .sum();
                assertThat("snapshot must reflect all docs across lifecycles", totalRows, equalTo((long) expectedTotal));
            }
        }
    }

    /**
     * After recovery, each refresh must produce segments with unique generations.
     * No two segments in a snapshot should share the same generation.
     */
    public void testSegmentGenerationsAreUniqueAfterRecovery() throws IOException {
        final int docs = randomIntBetween(10, 30);

        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager().recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            for (int i = 0; i < docs; i++) {
                engine.index(indexOp(Integer.toString(i)));
            }
        }

        try (DataFormatAwareEngine engine = reopenEngine()) {
            recoverFromTranslog(engine);

            // Do multiple refresh cycles
            int numCycles = randomIntBetween(3, 5);
            for (int c = 0; c < numCycles; c++) {
                engine.index(indexOp("post_" + c));
                engine.refresh("cycle-" + c);
            }

            try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
                CatalogSnapshot snapshot = ref.get();
                List<org.opensearch.index.engine.exec.Segment> segments = snapshot.getSegments();
                long distinctGenerations = segments.stream().map(org.opensearch.index.engine.exec.Segment::generation).distinct().count();
                assertThat("all segment generations must be unique", distinctGenerations, equalTo((long) segments.size()));
            }
        }
    }
}
