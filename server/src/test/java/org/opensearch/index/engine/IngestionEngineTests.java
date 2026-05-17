/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.NoMergePolicy;
import org.opensearch.action.admin.indices.streamingingestion.state.ShardIngestionState;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.pollingingest.IngestionSettings;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.indices.pollingingest.StreamPoller;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.ingest.IngestService;
import org.opensearch.test.IndexSettingsModule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.mockito.Mockito;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class IngestionEngineTests extends EngineTestCase {

    private IndexSettings indexSettings;
    private Store ingestionEngineStore;
    private IngestionEngine ingestionEngine;
    // the messages of the stream to ingest from
    private List<byte[]> messages;
    private EngineConfig engineConfig;
    private ClusterApplierService clusterApplierService;

    @Override
    @Before
    public void setUp() throws Exception {
        indexSettings = newIndexSettings();
        super.setUp();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        ingestionEngineStore = createStore(indexSettings, newDirectory());
        // create some initial messages
        messages = new ArrayList<>();
        publishData("{\"_id\":\"2\",\"_source\":{\"name\":\"bob\", \"age\": 24}}");
        publishData("{\"_id\":\"1\",\"_source\":{\"name\":\"alice\", \"age\": 20}}");
        clusterApplierService = mock(ClusterApplierService.class);
        when(clusterApplierService.state()).thenReturn(ClusterState.EMPTY_STATE);
        ingestionEngine = buildIngestionEngine(globalCheckpoint, ingestionEngineStore, indexSettings, clusterApplierService);
    }

    private void publishData(String message) {
        messages.add(message.getBytes(StandardCharsets.UTF_8));
    }

    protected IndexSettings newIndexSettings() {
        return IndexSettingsModule.newIndexSettings(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .put(IndexMetadata.SETTING_INGESTION_SOURCE_TYPE, "fake")
                .put(IndexMetadata.SETTING_INGESTION_SOURCE_POINTER_INIT_RESET, "earliest")
                .build()
        );
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (ingestionEngine != null) {
            ingestionEngine.close();
        }
        if (ingestionEngineStore != null) {
            ingestionEngineStore.close();
        }
        super.tearDown();
        engineConfig = null;
    }

    public void testCreateEngine() throws IOException {
        // wait for the engine to ingest all messages
        waitForResults(ingestionEngine, 2);
        // flush
        ingestionEngine.flush(false, true);
        Map<String, String> commitData = ingestionEngine.commitDataAsMap();
        // verify the commit data
        Assert.assertEquals(7, commitData.size());
        // the commiit data is the start of the current batch
        Assert.assertEquals("1", commitData.get(StreamPoller.BATCH_START));

        // validate ingestion state on successful engine creation
        ShardIngestionState ingestionState = ingestionEngine.getIngestionState();
        assertEquals("test", ingestionState.getIndex());
        assertEquals("DROP", ingestionState.getErrorPolicy());
        assertFalse(ingestionState.isPollerPaused());
        assertFalse(ingestionState.isWriteBlockEnabled());
    }

    public void testRecovery() throws IOException {
        // wait for the engine to ingest all messages
        waitForResults(ingestionEngine, 2);
        // flush
        ingestionEngine.flush(false, true);

        // ingest some new messages
        publishData("{\"_id\":\"3\",\"_source\":{\"name\":\"john\", \"age\": 30}}");
        publishData("{\"_id\":\"4\",\"_source\":{\"name\":\"jane\", \"age\": 25}}");
        ingestionEngine.close();
        ingestionEngine = buildIngestionEngine(new AtomicLong(0), ingestionEngineStore, indexSettings, clusterApplierService);
        waitForResults(ingestionEngine, 4);
    }

    public void testPushAPIFailures() {
        Engine.Index indexMock = Mockito.mock(Engine.Index.class);
        assertThrows(IngestionEngineException.class, () -> ingestionEngine.index(indexMock));
        Engine.Delete deleteMock = Mockito.mock(Engine.Delete.class);
        assertThrows(IngestionEngineException.class, () -> ingestionEngine.delete(deleteMock));
    }

    public void testCreationFailure() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        FakeIngestionSource.FakeIngestionConsumerFactory consumerFactory = new FakeIngestionSource.FakeIngestionConsumerFactory(messages);
        Store mockStore = spy(store);
        doThrow(new IOException("Simulated IOException")).when(mockStore).trimUnsafeCommits(any());

        EngineConfig engineConfig = config(
            indexSettings,
            mockStore,
            createTempDir(),
            NoMergePolicy.INSTANCE,
            null,
            null,
            globalCheckpoint::get
        );
        // overwrite the config with ingestion engine settings
        String mapping = "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}";
        MapperService mapperService = createMapperService(mapping);
        engineConfig = config(engineConfig, () -> new DocumentMapperForType(mapperService.documentMapper(), null), clusterApplierService);
        try {
            new IngestionEngine(engineConfig, consumerFactory, mock(IngestService.class));
            fail("Expected EngineException to be thrown");
        } catch (EngineException e) {
            assertEquals("failed to create engine", e.getMessage());
            assertTrue(e.getCause() instanceof IOException);
        }
    }

    public void testIngestionStateUpdate() {
        publishData("{\"_id\":\"3\",\"_source\":{\"name\":\"john\", \"age\": 30}}");
        publishData("{\"_id\":\"4\",\"_source\":{\"name\":\"jane\", \"age\": 25}}");
        waitForResults(ingestionEngine, 4);
        // flush
        ingestionEngine.flush(false, true);

        ShardIngestionState initialIngestionState = ingestionEngine.getIngestionState();
        assertEquals(false, initialIngestionState.isPollerPaused());

        publishData("{\"_id\":\"5\",\"_source\":{\"name\":\"john\", \"age\": 30}}");
        publishData("{\"_id\":\"6\",\"_source\":{\"name\":\"jane\", \"age\": 25}}");
        waitForResults(ingestionEngine, 6);

        // pause ingestion
        ingestionEngine.updateIngestionSettings(IngestionSettings.builder().setIsPaused(true).build());
        // resume ingestion with offset reset
        ingestionEngine.updateIngestionSettings(
            IngestionSettings.builder().setIsPaused(false).setResetState(StreamPoller.ResetState.RESET_BY_OFFSET).setResetValue("1").build()
        );
        ShardIngestionState resumedIngestionState = ingestionEngine.getIngestionState();
        assertEquals(false, resumedIngestionState.isPollerPaused());

        publishData("{\"_id\":\"7\",\"_source\":{\"name\":\"jane\", \"age\": 27}}");
        waitForResults(ingestionEngine, 7);
        PollingIngestStats stats = ingestionEngine.pollingIngestStats();
        assertEquals(6, stats.getConsumerStats().totalPolledCount());
    }

    public void testShouldPeriodicallyFlush() throws IOException {
        // Wait for messages to be ingested first so batchStartPointer is set
        waitForResults(ingestionEngine, 2);

        // Should flush because lastCommittedBatchStartPointer is null (no commit yet)
        assertTrue(ingestionEngine.shouldPeriodicallyFlush());

        // After first flush, lastCommittedBatchStartPointer is set
        ingestionEngine.flush(false, true);

        // Should not flush immediately after commit since pointer hasn't changed
        assertFalse(ingestionEngine.shouldPeriodicallyFlush());

        // Publish new messages, which will advance the batch start pointer
        publishData("{\"_id\":\"3\",\"_source\":{\"name\":\"john\", \"age\": 30}}");
        publishData("{\"_id\":\"4\",\"_source\":{\"name\":\"jane\", \"age\": 25}}");
        waitForResults(ingestionEngine, 4);

        // Should flush because batchStartPointer has changed since last commit
        assertTrue(ingestionEngine.shouldPeriodicallyFlush());

        // Flush again
        ingestionEngine.flush(false, true);

        // Should not flush immediately after commit
        assertFalse(ingestionEngine.shouldPeriodicallyFlush());
    }

    private IngestionEngine buildIngestionEngine(
        AtomicLong globalCheckpoint,
        Store store,
        IndexSettings settings,
        ClusterApplierService clusterApplierService
    ) throws IOException {
        FakeIngestionSource.FakeIngestionConsumerFactory consumerFactory = new FakeIngestionSource.FakeIngestionConsumerFactory(messages);
        if (engineConfig == null) {
            engineConfig = config(settings, store, createTempDir(), NoMergePolicy.INSTANCE, null, null, globalCheckpoint::get);
        }
        // overwrite the config with ingestion engine settings
        String mapping = "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}";
        MapperService mapperService = createMapperService(mapping);
        engineConfig = config(engineConfig, () -> new DocumentMapperForType(mapperService.documentMapper(), null), clusterApplierService);
        if (!Lucene.indexExists(store.directory())) {
            store.createEmpty(engineConfig.getIndexSettings().getIndexVersionCreated().luceneVersion);
            final String translogUuid = Translog.createEmptyTranslog(
                engineConfig.getTranslogConfig().getTranslogPath(),
                SequenceNumbers.NO_OPS_PERFORMED,
                shardId,
                primaryTerm.get()
            );
            store.associateIndexWithNewTranslog(translogUuid);
        }
        IngestionEngine ingestionEngine = new IngestionEngine(engineConfig, consumerFactory, mock(IngestService.class));
        ingestionEngine.start();
        return ingestionEngine;
    }

    private void waitForResults(Engine engine, int numDocs) {
        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> { Assert.assertTrue(resultsFound(engine, numDocs)); });
    }

    private boolean resultsFound(Engine engine, int numDocs) {
        engine.refresh("index");
        try (Engine.Searcher searcher = engine.acquireSearcher("index")) {
            return searcher.getIndexReader().numDocs() == numDocs;
        }
    }

    public void testConstructorWithNonNullIngestService() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        Store testStore = createStore(indexSettings, newDirectory());
        FakeIngestionSource.FakeIngestionConsumerFactory consumerFactory = new FakeIngestionSource.FakeIngestionConsumerFactory(messages);

        EngineConfig config = config(indexSettings, testStore, createTempDir(), NoMergePolicy.INSTANCE, null, null, globalCheckpoint::get);
        String mapping = "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}";
        MapperService mapperService = createMapperService(mapping);
        config = config(config, () -> new DocumentMapperForType(mapperService.documentMapper(), null), clusterApplierService);

        testStore.createEmpty(config.getIndexSettings().getIndexVersionCreated().luceneVersion);
        final String translogUuid = Translog.createEmptyTranslog(
            config.getTranslogConfig().getTranslogPath(),
            SequenceNumbers.NO_OPS_PERFORMED,
            shardId,
            primaryTerm.get()
        );
        testStore.associateIndexWithNewTranslog(translogUuid);

        // non-null IngestService — engine should start with pipeline support available
        IngestService ingestService = mock(IngestService.class);
        IngestionEngine engine = new IngestionEngine(config, consumerFactory, ingestService);
        engine.start();
        waitForResults(engine, 2);
        engine.close();
        testStore.close();
    }

    // ==================== Per-Partition Recovery Tests (PR5) ====================

    public void testParsePerPartitionStartPointersEmpty() {
        FakeIngestionSource.FakeIngestionConsumerFactory factory = new FakeIngestionSource.FakeIngestionConsumerFactory(messages);
        Map<Integer, org.opensearch.index.IngestionShardPointer> result = IngestionEngine.parsePerPartitionStartPointers(
            java.util.Collections.emptyMap(),
            factory
        );
        assertTrue("empty commit data yields empty map", result.isEmpty());
    }

    public void testParsePerPartitionStartPointersLegacyOnly() {
        // Only the legacy batch_start key is present — no per-partition keys should be recovered.
        FakeIngestionSource.FakeIngestionConsumerFactory factory = new FakeIngestionSource.FakeIngestionConsumerFactory(messages);
        Map<String, String> commitData = new java.util.HashMap<>();
        commitData.put(StreamPoller.BATCH_START, "42");
        Map<Integer, org.opensearch.index.IngestionShardPointer> result = IngestionEngine.parsePerPartitionStartPointers(
            commitData,
            factory
        );
        assertTrue("only legacy key yields empty per-partition map", result.isEmpty());
    }

    public void testParsePerPartitionStartPointersPerPartitionKeys() {
        FakeIngestionSource.FakeIngestionConsumerFactory factory = new FakeIngestionSource.FakeIngestionConsumerFactory(messages);
        Map<String, String> commitData = new java.util.HashMap<>();
        commitData.put(StreamPoller.BATCH_START, "42");          // legacy MIN, should be ignored by helper
        commitData.put(StreamPoller.BATCH_START_PREFIX + "0", "42");
        commitData.put(StreamPoller.BATCH_START_PREFIX + "4", "100");
        commitData.put(StreamPoller.BATCH_START_PREFIX + "8", "200");
        commitData.put("unrelated_key", "ignored");

        Map<Integer, org.opensearch.index.IngestionShardPointer> result = IngestionEngine.parsePerPartitionStartPointers(
            commitData,
            factory
        );

        assertEquals("three partitions recovered", 3, result.size());
        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(42), result.get(0));
        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(100), result.get(4));
        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(200), result.get(8));
    }

    public void testParsePerPartitionStartPointersIgnoresMalformedAndEmptySuffix() {
        // Malformed (non-numeric) and empty-suffix keys must be silently dropped without failing.
        FakeIngestionSource.FakeIngestionConsumerFactory factory = new FakeIngestionSource.FakeIngestionConsumerFactory(messages);
        Map<String, String> commitData = new java.util.HashMap<>();
        commitData.put(StreamPoller.BATCH_START_PREFIX + "0", "10");      // valid
        commitData.put(StreamPoller.BATCH_START_PREFIX + "X", "20");      // non-numeric suffix
        commitData.put(StreamPoller.BATCH_START_PREFIX, "30");            // empty suffix
        commitData.put(StreamPoller.BATCH_START_PREFIX + "3xyz", "40");   // partially numeric
        commitData.put(StreamPoller.BATCH_START_PREFIX + "7", "50");      // valid

        Map<Integer, org.opensearch.index.IngestionShardPointer> result = IngestionEngine.parsePerPartitionStartPointers(
            commitData,
            factory
        );

        assertEquals("only valid keys are recovered", 2, result.size());
        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(10), result.get(0));
        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(50), result.get(7));
    }

    public void testParsePerPartitionStartPointersSwallowsParserRuntimeException() {
        // A consumer factory whose parsePointerFromString throws (e.g. malformed value, IllegalArgumentException
        // from a strict source-specific parser) must not fail the entire recovery — the entry is skipped and the
        // remaining keys are still recovered.
        FakeIngestionSource.FakeIngestionConsumerFactory throwingFactory = new FakeIngestionSource.FakeIngestionConsumerFactory(messages) {
            @Override
            public FakeIngestionSource.FakeIngestionShardPointer parsePointerFromString(String pointer) {
                if ("BAD".equals(pointer)) {
                    throw new IllegalArgumentException("simulated parse failure for [" + pointer + "]");
                }
                return super.parsePointerFromString(pointer);
            }
        };
        Map<String, String> commitData = new java.util.HashMap<>();
        commitData.put(StreamPoller.BATCH_START_PREFIX + "0", "10");
        commitData.put(StreamPoller.BATCH_START_PREFIX + "1", "BAD");
        commitData.put(StreamPoller.BATCH_START_PREFIX + "2", "30");

        Map<Integer, org.opensearch.index.IngestionShardPointer> result = IngestionEngine.parsePerPartitionStartPointers(
            commitData,
            throwingFactory
        );

        assertEquals("the throwing entry is dropped; the other two are recovered", 2, result.size());
        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(10), result.get(0));
        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(30), result.get(2));
        assertFalse(result.containsKey(1));
    }

    public void testParsePerPartitionStartPointersSwallowsParserNullReturn() {
        // A plugin that returns null from parsePointerFromString (instead of throwing) must not produce a null
        // entry in the recovery map — a downstream cast to a concrete pointer type would NPE otherwise.
        FakeIngestionSource.FakeIngestionConsumerFactory nullReturningFactory = new FakeIngestionSource.FakeIngestionConsumerFactory(
            messages
        ) {
            @Override
            public FakeIngestionSource.FakeIngestionShardPointer parsePointerFromString(String pointer) {
                if ("NULL".equals(pointer)) {
                    return null;
                }
                return super.parsePointerFromString(pointer);
            }
        };
        Map<String, String> commitData = new java.util.HashMap<>();
        commitData.put(StreamPoller.BATCH_START_PREFIX + "0", "10");
        commitData.put(StreamPoller.BATCH_START_PREFIX + "1", "NULL");

        Map<Integer, org.opensearch.index.IngestionShardPointer> result = IngestionEngine.parsePerPartitionStartPointers(
            commitData,
            nullReturningFactory
        );

        assertEquals("the null-returning entry is dropped", 1, result.size());
        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(10), result.get(0));
        assertFalse(result.containsKey(1));
    }

    public void testParsePerPartitionStartPointersDoesNotMatchBatchStartItself() {
        // BATCH_START ("batch_start") is a substring prefix of BATCH_START_PREFIX ("batch_start_p"). Defensive guard
        // ensures the legacy key is never misinterpreted as a per-partition key.
        FakeIngestionSource.FakeIngestionConsumerFactory factory = new FakeIngestionSource.FakeIngestionConsumerFactory(messages);
        Map<String, String> commitData = new java.util.HashMap<>();
        commitData.put(StreamPoller.BATCH_START, "999");  // ONLY the legacy key, looks similar to the prefix

        Map<Integer, org.opensearch.index.IngestionShardPointer> result = IngestionEngine.parsePerPartitionStartPointers(
            commitData,
            factory
        );

        assertTrue("BATCH_START must not be parsed as a per-partition key", result.isEmpty());
    }

    public void testCommitDataDoesNotIncludePerPartitionKeysForSinglePartitionConsumer() throws IOException {
        // Regression: FakeIngestionConsumerFactory returns -1 from getSourcePartitionCount (legacy default),
        // so assignedPartitions stays empty, the legacy createShardConsumer path is taken, and the poller's
        // getBatchStartPointers() returns empty. The commit must therefore NOT contain any batch_start_p* keys.
        waitForResults(ingestionEngine, 2);
        ingestionEngine.flush(false, true);
        Map<String, String> commitData = ingestionEngine.commitDataAsMap();

        assertTrue("legacy batch_start must be present", commitData.containsKey(StreamPoller.BATCH_START));
        for (String key : commitData.keySet()) {
            assertFalse("no batch_start_p* keys in single-partition mode, found: " + key, key.startsWith(StreamPoller.BATCH_START_PREFIX));
        }
    }

    // ---- maybeMigrateLegacyCheckpoint (Bug N1) ----

    public void testMigrateLegacyCheckpointOnSingleToMultiPartitionUpgrade() {
        // The migration case: previous commits wrote only legacy batch_start (size==1 era), but now the source has
        // expanded and assignedPartitions has size > 1. The legacy pointer must be attributed to partition shardId.
        Map<Integer, org.opensearch.index.IngestionShardPointer> perPartitionStart = new java.util.HashMap<>();
        Map<String, String> commitData = new java.util.HashMap<>();
        commitData.put(StreamPoller.BATCH_START, "42");
        FakeIngestionSource.FakeIngestionShardPointer legacy = new FakeIngestionSource.FakeIngestionShardPointer(42);

        IngestionEngine.maybeMigrateLegacyCheckpoint(perPartitionStart, commitData, legacy, java.util.Arrays.asList(0, 1), 0, false);

        assertEquals("legacy checkpoint attributed to partition shardId", 1, perPartitionStart.size());
        assertEquals(legacy, perPartitionStart.get(0));
    }

    public void testMigrateLegacyCheckpointSkipsWhenPerPartitionAlreadyPresent() {
        Map<Integer, org.opensearch.index.IngestionShardPointer> perPartitionStart = new java.util.HashMap<>();
        perPartitionStart.put(0, new FakeIngestionSource.FakeIngestionShardPointer(100));
        Map<String, String> commitData = new java.util.HashMap<>();
        commitData.put(StreamPoller.BATCH_START, "42");
        FakeIngestionSource.FakeIngestionShardPointer legacy = new FakeIngestionSource.FakeIngestionShardPointer(42);

        IngestionEngine.maybeMigrateLegacyCheckpoint(perPartitionStart, commitData, legacy, java.util.Arrays.asList(0, 1), 0, false);

        // per-partition map already has authoritative recovery; legacy migration must not clobber it
        assertEquals(1, perPartitionStart.size());
        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(100), perPartitionStart.get(0));
    }

    public void testMigrateLegacyCheckpointSkipsInSinglePartitionMode() {
        // size == 1: legacy single-partition path handles seeking via forcedShardPointer; no migration needed.
        Map<Integer, org.opensearch.index.IngestionShardPointer> perPartitionStart = new java.util.HashMap<>();
        Map<String, String> commitData = new java.util.HashMap<>();
        commitData.put(StreamPoller.BATCH_START, "42");
        FakeIngestionSource.FakeIngestionShardPointer legacy = new FakeIngestionSource.FakeIngestionShardPointer(42);

        IngestionEngine.maybeMigrateLegacyCheckpoint(
            perPartitionStart,
            commitData,
            legacy,
            java.util.Collections.singletonList(0),
            0,
            false
        );

        assertTrue("no migration in single-partition mode", perPartitionStart.isEmpty());
    }

    public void testMigrateLegacyCheckpointSkipsOnForceReset() {
        // forceResetPoller overrides recovery; the user explicitly chose a reset point, do not migrate.
        Map<Integer, org.opensearch.index.IngestionShardPointer> perPartitionStart = new java.util.HashMap<>();
        Map<String, String> commitData = new java.util.HashMap<>();
        commitData.put(StreamPoller.BATCH_START, "42");
        FakeIngestionSource.FakeIngestionShardPointer override = new FakeIngestionSource.FakeIngestionShardPointer(999);

        IngestionEngine.maybeMigrateLegacyCheckpoint(perPartitionStart, commitData, override, java.util.Arrays.asList(0, 1), 0, true);

        assertTrue("force reset skips migration", perPartitionStart.isEmpty());
    }

    public void testMigrateLegacyCheckpointSkipsWhenLegacyKeyMissing() {
        // Fresh-start case: no commit data, nothing to migrate.
        Map<Integer, org.opensearch.index.IngestionShardPointer> perPartitionStart = new java.util.HashMap<>();
        IngestionEngine.maybeMigrateLegacyCheckpoint(
            perPartitionStart,
            java.util.Collections.emptyMap(),
            null,
            java.util.Arrays.asList(0, 1),
            0,
            false
        );
        assertTrue(perPartitionStart.isEmpty());
    }

    public void testMigrateLegacyCheckpointRejectsPartitionAwareLegacyPointer() {
        // Adversarial / corruption case: BATCH_START somehow holds a partition-aware value (e.g. hand-edited
        // commit data, a future format change, a misbehaving plugin's parsePointerFromString). Attribution would
        // silently seek the wrong partition because the seek code uses the map's KEY for the TopicPartition and
        // only the pointer's offset. The migration must detect this and refuse — defense in depth.
        org.opensearch.index.IngestionShardPointer partitionAwareLegacy = mock(
            org.opensearch.index.IngestionShardPointer.class,
            withSettings().extraInterfaces(org.opensearch.index.SourcePartitionAwarePointer.class)
        );
        when(partitionAwareLegacy.asString()).thenReturn("3:42");

        Map<Integer, org.opensearch.index.IngestionShardPointer> perPartitionStart = new java.util.HashMap<>();
        Map<String, String> commitData = new java.util.HashMap<>();
        commitData.put(StreamPoller.BATCH_START, "3:42");

        IngestionEngine.maybeMigrateLegacyCheckpoint(
            perPartitionStart,
            commitData,
            partitionAwareLegacy,
            java.util.Arrays.asList(0, 1),
            0,
            false
        );

        assertTrue(
            "partition-aware legacy pointer must NOT be migrated (would attribute partition 3's offset to key 0 → wrong-partition seek)",
            perPartitionStart.isEmpty()
        );
    }
}
