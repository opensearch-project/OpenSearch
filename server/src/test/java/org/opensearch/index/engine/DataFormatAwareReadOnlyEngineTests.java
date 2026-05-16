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
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.stub.InMemoryCommitter;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockSearchBackEndPlugin;
import org.opensearch.index.engine.exec.DataFormatAwareIndexerFactory;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.commit.CommitterFactory;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.instanceOf;
import static org.opensearch.index.engine.EngineTestCase.tombstoneDocSupplier;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DataFormatAwareReadOnlyEngine}.
 * Exercises constructor, write rejection, no-op translog, flush, close, failEngine,
 * and constant-return methods for the warm read-only primary engine.
 */
public class DataFormatAwareReadOnlyEngineTests extends OpenSearchTestCase {

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
        store = createStore(warmIndexSettings());
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

    private Store createStore(IndexSettings indexSettings) throws IOException {
        Directory dir = newDirectory();
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

    private IndexSettings warmIndexSettings() {
        return IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), mockDataFormat.name())
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, "SEGMENT")
                .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true)
                .build()
        );
    }

    private IndexSettings nonWarmIndexSettings() {
        return IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), mockDataFormat.name())
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, "SEGMENT")
                .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), false)
                .build()
        );
    }

    /** Writes a Lucene commit so the engine's store bootstrap finds valid userData. */
    private void bootstrapStoreWithMetadata(Store store, String translogUUID, String historyUUID) throws IOException {
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
            if (historyUUID != null) {
                commitData.put(Engine.HISTORY_UUID_KEY, historyUUID);
            }
            writer.setLiveCommitData(commitData.entrySet());
            writer.commit();
        }
    }

    private DataFormatAwareReadOnlyEngine createReadOnlyEngine() throws IOException {
        return createReadOnlyEngine(store, warmIndexSettings(), null);
    }

    private DataFormatAwareReadOnlyEngine createReadOnlyEngine(Store store, IndexSettings indexSettings, Engine.EventListener listener)
        throws IOException {
        String translogUUID = UUID.randomUUID().toString();
        String historyUUID = UUID.randomUUID().toString();
        bootstrapStoreWithMetadata(store, translogUUID, historyUUID);
        return new DataFormatAwareReadOnlyEngine(buildConfig(store, indexSettings, listener));
    }

    private DataFormatAwareReadOnlyEngine createReadOnlyEngineWithoutHistoryUUID() throws IOException {
        String translogUUID = UUID.randomUUID().toString();
        bootstrapStoreWithMetadata(store, translogUUID, null);
        return new DataFormatAwareReadOnlyEngine(buildConfig(store, warmIndexSettings(), null));
    }

    private EngineConfig buildConfig(Store store, IndexSettings indexSettings, Engine.EventListener listener) {
        Path translogPath = createTempDir().resolve("translog");
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
        EngineConfig.Builder builder = new EngineConfig.Builder().shardId(shardId)
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
            .readOnlyReplica(false);
        if (listener != null) {
            builder.eventListener(listener);
        }
        return builder.build();
    }

    private DataFormatRegistry createMockRegistry() {
        PluginsService pluginsService = mock(PluginsService.class);
        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(mockPlugin));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(
            List.of(new MockSearchBackEndPlugin(List.of(mockDataFormat.name())))
        );
        return new DataFormatRegistry(pluginsService);
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
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine()) {
            assertNotNull(engine.translogManager());
            assertEquals(SequenceNumbers.NO_OPS_PERFORMED, engine.getProcessedLocalCheckpoint());
            assertNotNull("historyUUID must be assigned", engine.getHistoryUUID());
            assertNotNull(engine.config());
        }
    }

    public void testCreateEngineWithHistoryUUID() throws IOException {
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine()) {
            String historyUUID = engine.getHistoryUUID();
            assertNotNull("historyUUID must be read from commit", historyUUID);
            assertFalse("historyUUID must not be empty", historyUUID.isEmpty());
        }
    }

    public void testCreateEngineWithoutHistoryUUID() throws IOException {
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngineWithoutHistoryUUID()) {
            // Engine should fabricate a historyUUID when not in commit (same as NRT engine)
            assertNotNull("historyUUID must be fabricated when not in commit", engine.getHistoryUUID());
            assertFalse("fabricated historyUUID must not be empty", engine.getHistoryUUID().isEmpty());
        }
    }

    public void testWriteRejection() throws IOException {
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine()) {
            expectThrows(UnsupportedOperationException.class, () -> engine.index(null));
            expectThrows(UnsupportedOperationException.class, () -> engine.delete(null));
            expectThrows(UnsupportedOperationException.class, () -> engine.noOp(null));
        }
    }

    public void testNoOpTranslog() throws IOException {
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine()) {
            // UUID is valid (non-null, non-empty)
            String translogUUID = engine.translogManager().getTranslogUUID();
            assertNotNull("translog UUID must be non-null", translogUUID);
            assertFalse("translog UUID must not be empty", translogUUID.isEmpty());

            // Zero uncommitted operations
            assertEquals(0, engine.translogManager().getTranslogStats().getUncommittedOperations());
            assertEquals(0L, engine.translogManager().getTranslogStats().getUncommittedSizeInBytes());

            // Sync is a no-op (does not throw)
            engine.translogManager().syncTranslog();
        }
    }

    public void testFlushIsNoOp() throws IOException {
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine()) {
            // flush is always a no-op on read-only engine (does not throw, does not commit)
            engine.flush(false, true);
            engine.flush(true, false);
            engine.flush();
            // Engine should still be open
            engine.ensureOpen();
        }
    }

    public void testCloseReleasesResources() throws IOException {
        DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine();
        engine.close();
        expectThrows(AlreadyClosedException.class, engine::ensureOpen);
    }

    public void testCloseIsIdempotent() throws IOException {
        DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine();
        engine.close();
        // Second close must be a no-op, not throw.
        engine.close();
    }

    public void testFlushAndClose() throws IOException {
        DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine();
        engine.flushAndClose();
        expectThrows(AlreadyClosedException.class, engine::ensureOpen);
    }

    public void testFailEngine() throws IOException {
        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        Engine.EventListener listener = new Engine.EventListener() {
            @Override
            public void onFailedEngine(String reason, Exception e) {
                listenerCalled.set(true);
            }
        };
        DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine(store, warmIndexSettings(), listener);
        engine.failEngine("test-failure", new IOException("simulated"));
        expectThrows(AlreadyClosedException.class, engine::ensureOpen);
        assertTrue("event listener must be notified on failEngine", listenerCalled.get());
    }

    public void testOperationsAfterCloseThrowAlreadyClosed() throws IOException {
        DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine();
        engine.close();
        expectThrows(AlreadyClosedException.class, engine::acquireReader);
        expectThrows(AlreadyClosedException.class, engine::acquireSafeCatalogSnapshot);
    }

    public void testConstantReturns() throws IOException {
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine()) {
            assertEquals(0L, engine.getWritingBytes());
            assertEquals(0L, engine.getIndexBufferRAMBytesUsed());
            assertFalse(engine.shouldPeriodicallyFlush());
        }
    }

    public void testIsReplicaIndexerReturnsFalse() throws IOException {
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine()) {
            assertFalse("read-only primary engine must not identify as replica", engine.isReplicaIndexer());
        }
    }

    public void testCheckpointAccessorsOnFreshEngine() throws IOException {
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine()) {
            assertEquals(SequenceNumbers.NO_OPS_PERFORMED, engine.getProcessedLocalCheckpoint());
            assertEquals(SequenceNumbers.NO_OPS_PERFORMED, engine.getPersistedLocalCheckpoint());
            // Persisted == processed for this engine
            assertEquals(engine.getProcessedLocalCheckpoint(), engine.getPersistedLocalCheckpoint());
        }
    }

    public void testAcquireReaderReturnsNonNull() throws IOException {
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine()) {
            try (
                org.opensearch.common.concurrent.GatedCloseable<org.opensearch.index.engine.exec.Indexer.Reader> reader = engine
                    .acquireReader()
            ) {
                assertNotNull("acquireReader must return a non-null reader", reader.get());
            }
        }
    }

    public void testAcquireSafeCatalogSnapshotReturnsNonNull() throws IOException {
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine()) {
            try (org.opensearch.common.concurrent.GatedCloseable<CatalogSnapshot> snapshot = engine.acquireSafeCatalogSnapshot()) {
                assertNotNull("acquireSafeCatalogSnapshot must return a non-null snapshot", snapshot.get());
            }
        }
    }

    // ---------- Replication and Checkpoint Tests ----------

    public void testFinalizeReplicationAdvancesCheckpointAndHistoryUUID() throws IOException {
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine()) {
            String newHistoryUUID = UUID.randomUUID().toString();
            long maxSeqNo = randomLongBetween(1, 100_000);
            DataformatAwareCatalogSnapshot incoming = buildReplicationSnapshot(1L, 1L, maxSeqNo, newHistoryUUID);

            engine.finalizeReplication(incoming);

            assertEquals("processed checkpoint must be fast-forwarded", maxSeqNo, engine.getProcessedLocalCheckpoint());
            assertEquals("historyUUID must reflect the incoming snapshot", newHistoryUUID, engine.getHistoryUUID());
        }
    }

    public void testFinalizeReplicationWithoutHistoryUUIDPreservesExisting() throws IOException {
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine()) {
            String originalUUID = engine.getHistoryUUID();
            assertNotNull("fresh engine must have a historyUUID", originalUUID);

            DataformatAwareCatalogSnapshot noUUID = buildReplicationSnapshot(1L, 1L, 5L, null);
            engine.finalizeReplication(noUUID);

            assertEquals("historyUUID must be preserved when incoming has none", originalUUID, engine.getHistoryUUID());
        }
    }

    public void testFinalizeReplicationUpdatesSnapshotManager() throws IOException {
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine()) {
            long maxSeqNo = randomLongBetween(1, 100_000);
            DataformatAwareCatalogSnapshot incoming = buildReplicationSnapshot(1L, 1L, maxSeqNo, UUID.randomUUID().toString());

            engine.finalizeReplication(incoming);

            try (org.opensearch.common.concurrent.GatedCloseable<CatalogSnapshot> ref = engine.getCatalogSnapshotManager()
                .acquireSnapshot()) {
                assertEquals("latest snapshot ID must match incoming", 1L, ref.get().getId());
            }
        }
    }

    public void testFinalizeReplicationOnClosedEngineThrows() throws IOException {
        DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine();
        engine.close();
        DataformatAwareCatalogSnapshot incoming = buildReplicationSnapshot(1L, 1L, 5L, UUID.randomUUID().toString());
        expectThrows(AlreadyClosedException.class, () -> engine.finalizeReplication(incoming));
    }

    public void testCheckpointConsistencyAfterMultipleReplications() throws IOException {
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine()) {
            int numReplications = randomIntBetween(2, 10);
            long gen = 0;
            for (int i = 0; i < numReplications; i++) {
                gen++;
                long maxSeqNo = randomLongBetween(0, Long.MAX_VALUE / 2);
                DataformatAwareCatalogSnapshot incoming = buildReplicationSnapshot(gen, gen, maxSeqNo, UUID.randomUUID().toString());
                engine.finalizeReplication(incoming);

                // Invariant: persisted == processed
                assertEquals(
                    "persisted must equal processed after replication",
                    engine.getProcessedLocalCheckpoint(),
                    engine.getPersistedLocalCheckpoint()
                );
                // Invariant: maxSeqNo >= localCheckpoint
                long globalCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;
                assertTrue(
                    "maxSeqNo must be >= localCheckpoint",
                    engine.getSeqNoStats(globalCheckpoint).getMaxSeqNo() >= engine.getProcessedLocalCheckpoint()
                );
            }
        }
    }

    // ---------- Translog Tests ----------

    public void testTranslogSnapshotIsAlwaysEmpty() throws IOException {
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine()) {
            assertEquals(0, engine.translogManager().getTranslogStats().getUncommittedOperations());
            assertEquals(0L, engine.translogManager().getTranslogStats().getUncommittedSizeInBytes());

            // Even after replication, translog remains empty
            DataformatAwareCatalogSnapshot incoming = buildReplicationSnapshot(1L, 1L, randomLongBetween(1, 100_000), null);
            engine.finalizeReplication(incoming);

            assertEquals(0, engine.translogManager().getTranslogStats().getUncommittedOperations());
            assertEquals(0L, engine.translogManager().getTranslogStats().getUncommittedSizeInBytes());
        }
    }

    // ---------- Snapshot Ref-Counting Tests ----------

    public void testAcquireSafeCatalogSnapshotPreventsCleanupWhileHeld() throws IOException {
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine()) {
            // Acquire a snapshot reference
            org.opensearch.common.concurrent.GatedCloseable<CatalogSnapshot> heldRef = engine.acquireSafeCatalogSnapshot();
            CatalogSnapshot heldSnapshot = heldRef.get();
            assertNotNull(heldSnapshot);
            long heldId = heldSnapshot.getId();

            // Apply a new replication snapshot (advancing the engine state)
            DataformatAwareCatalogSnapshot incoming = buildReplicationSnapshot(1L, 1L, 10L, UUID.randomUUID().toString());
            engine.finalizeReplication(incoming);

            // Held snapshot is still accessible
            assertEquals("held snapshot ID must remain unchanged", heldId, heldRef.get().getId());

            // Engine's current snapshot is the new one
            try (org.opensearch.common.concurrent.GatedCloseable<CatalogSnapshot> currentRef = engine.acquireSafeCatalogSnapshot()) {
                assertEquals("engine's current snapshot must be the new one", 1L, currentRef.get().getId());
            }

            // Release the held reference
            heldRef.close();

            // Engine's current snapshot is still the new one
            try (org.opensearch.common.concurrent.GatedCloseable<CatalogSnapshot> afterRef = engine.acquireSafeCatalogSnapshot()) {
                assertEquals("after release, engine snapshot must still be the new one", 1L, afterRef.get().getId());
            }
        }
    }

    // ---------- Concurrent Close Tests ----------

    public void testConcurrentCloseExecutesExactlyOnce() throws Exception {
        DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine();
        int threadCount = randomIntBetween(2, 10);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        List<Exception> exceptions = new ArrayList<>();

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        try {
            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        engine.close();
                    } catch (Exception e) {
                        synchronized (exceptions) {
                            exceptions.add(e);
                        }
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            assertTrue("all threads must complete within timeout", doneLatch.await(30, TimeUnit.SECONDS));
            assertTrue("no exceptions expected from concurrent close, got: " + exceptions, exceptions.isEmpty());
            expectThrows(AlreadyClosedException.class, engine::ensureOpen);
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    // ---------- Commit UserData Tests ----------

    // ---------- Read Path Tests ----------

    public void testAcquireReaderAfterReplication() throws IOException {
        try (DataFormatAwareReadOnlyEngine engine = createReadOnlyEngine()) {
            // Apply a replication snapshot
            DataformatAwareCatalogSnapshot incoming = buildReplicationSnapshot(1L, 1L, 10L, UUID.randomUUID().toString());
            engine.finalizeReplication(incoming);

            // acquireReader should still work after replication
            try (
                org.opensearch.common.concurrent.GatedCloseable<org.opensearch.index.engine.exec.Indexer.Reader> reader = engine
                    .acquireReader()
            ) {
                assertNotNull("acquireReader must return a non-null reader after replication", reader.get());
            }
        }
    }

    // ---------- Engine Flip Integration Tests (DataFormatAwareIndexerFactory) ----------

    private EngineConfig buildWarmPrimaryConfig() throws IOException {
        String translogUUID = UUID.randomUUID().toString();
        String historyUUID = UUID.randomUUID().toString();
        bootstrapStoreWithMetadata(store, translogUUID, historyUUID);
        return buildConfig(store, warmIndexSettings(), null);
    }

    private EngineConfig buildReplicaConfig() throws IOException {
        IndexSettings replicaSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), mockDataFormat.name())
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, "SEGMENT")
                .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true)
                .build()
        );
        Store replicaStore = createStore(replicaSettings);
        Path translogPath = createTempDir().resolve("translog");
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(replicaStore, uuid, UUID.randomUUID().toString());
        TranslogConfig translogConfig = new TranslogConfig(
            shardId,
            translogPath,
            replicaSettings,
            BigArrays.NON_RECYCLING_INSTANCE,
            "",
            false
        );
        DataFormatRegistry registry = createMockRegistry();
        CommitterFactory committerFactory = config -> new InMemoryCommitter(replicaStore);
        return new EngineConfig.Builder().shardId(shardId)
            .threadPool(threadPool)
            .indexSettings(replicaSettings)
            .store(replicaStore)
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

    private EngineConfig buildHotPrimaryConfig() throws IOException {
        IndexSettings hotSettings = nonWarmIndexSettings();
        Store hotStore = createStore(hotSettings);
        Path translogPath = createTempDir().resolve("translog");
        String translogUUID = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        String historyUUID = UUID.randomUUID().toString();
        bootstrapStoreWithMetadata(hotStore, translogUUID, historyUUID);
        TranslogConfig translogConfig = new TranslogConfig(
            shardId,
            translogPath,
            hotSettings,
            BigArrays.NON_RECYCLING_INSTANCE,
            "",
            false
        );
        DataFormatRegistry registry = createMockRegistry();
        CommitterFactory committerFactory = config -> new InMemoryCommitter(hotStore);
        return new EngineConfig.Builder().shardId(shardId)
            .threadPool(threadPool)
            .indexSettings(hotSettings)
            .store(hotStore)
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
            .readOnlyReplica(false)
            .build();
    }

    public void testFactoryCreatesReadOnlyEngineForWarmPrimary() throws IOException {
        EngineConfig config = buildWarmPrimaryConfig();
        DataFormatAwareIndexerFactory factory = new DataFormatAwareIndexerFactory();
        Indexer engine = factory.createIndexer(config);
        try {
            assertThat(engine, instanceOf(DataFormatAwareReadOnlyEngine.class));
        } finally {
            engine.close();
        }
    }

    public void testFactoryCreatesNRTEngineForReplica() throws IOException {
        EngineConfig config = buildReplicaConfig();
        DataFormatAwareIndexerFactory factory = new DataFormatAwareIndexerFactory();
        Indexer engine = factory.createIndexer(config);
        try {
            assertThat(engine, instanceOf(DataFormatAwareNRTReplicationEngine.class));
        } finally {
            engine.close();
            config.getStore().close();
        }
    }

    public void testFactoryCreatesWritableEngineForHotPrimary() throws IOException {
        EngineConfig config = buildHotPrimaryConfig();
        DataFormatAwareIndexerFactory factory = new DataFormatAwareIndexerFactory();
        Indexer engine = factory.createIndexer(config);
        try {
            assertThat(engine, instanceOf(DataFormatAwareEngine.class));
        } finally {
            engine.close();
            config.getStore().close();
        }
    }
}
