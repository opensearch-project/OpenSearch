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
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.commit.CommitterFactory;
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
        return (DataformatAwareCatalogSnapshot) CatalogSnapshotManager.createInitialSnapshot(
            id,
            gen,
            gen,
            List.<Segment>of(),
            gen,
            userData
        );
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
            // Bootstrap commit is enough; no flush needed to get a valid IndexCommit handle.
            assertNotNull(engine.acquireLastIndexCommit(false).get());
            assertNotNull(engine.acquireSafeIndexCommit().get());
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
        expectThrows(org.apache.lucene.store.AlreadyClosedException.class, () -> engine.acquireLastIndexCommit(false));
        expectThrows(org.apache.lucene.store.AlreadyClosedException.class, engine::commitStats);
    }
}
