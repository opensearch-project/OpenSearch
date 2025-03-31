/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.NoMergePolicy;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.pollingingest.StreamPoller;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.IndexSettingsModule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class IngestionEngineTests extends EngineTestCase {

    private IndexSettings indexSettings;
    private Store ingestionEngineStore;
    private IngestionEngine ingestionEngine;
    // the messages of the stream to ingest from
    private List<byte[]> messages;
    private EngineConfig engineConfig;

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
        ingestionEngine = buildIngestionEngine(globalCheckpoint, ingestionEngineStore, indexSettings);
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
        Assert.assertEquals("0", commitData.get(StreamPoller.BATCH_START));

        // verify the stored offsets
        var offset = new FakeIngestionSource.FakeIngestionShardPointer(0);
        ingestionEngine.refresh("read_offset");
        try (Engine.Searcher searcher = ingestionEngine.acquireSearcher("read_offset")) {
            Set<IngestionShardPointer> persistedPointers = ingestionEngine.fetchPersistedOffsets(
                Lucene.wrapAllDocsLive(searcher.getDirectoryReader()),
                offset
            );
            Assert.assertEquals(2, persistedPointers.size());
        }
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
        ingestionEngine = buildIngestionEngine(new AtomicLong(0), ingestionEngineStore, indexSettings);
        waitForResults(ingestionEngine, 4);
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
        engineConfig = config(engineConfig, () -> new DocumentMapperForType(mapperService.documentMapper(), null));
        try {
            new IngestionEngine(engineConfig, consumerFactory);
            fail("Expected EngineException to be thrown");
        } catch (EngineException e) {
            assertEquals("failed to create engine", e.getMessage());
            assertTrue(e.getCause() instanceof IOException);
        }
    }

    private IngestionEngine buildIngestionEngine(AtomicLong globalCheckpoint, Store store, IndexSettings settings) throws IOException {
        FakeIngestionSource.FakeIngestionConsumerFactory consumerFactory = new FakeIngestionSource.FakeIngestionConsumerFactory(messages);
        if (engineConfig == null) {
            engineConfig = config(settings, store, createTempDir(), NoMergePolicy.INSTANCE, null, null, globalCheckpoint::get);
        }
        // overwrite the config with ingestion engine settings
        String mapping = "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}";
        MapperService mapperService = createMapperService(mapping);
        engineConfig = config(engineConfig, () -> new DocumentMapperForType(mapperService.documentMapper(), null));
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
        IngestionEngine ingestionEngine = new IngestionEngine(engineConfig, consumerFactory);
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
}
