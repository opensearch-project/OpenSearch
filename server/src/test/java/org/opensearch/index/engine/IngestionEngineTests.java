/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.NoMergePolicy;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
import org.opensearch.indices.ingest.StreamPoller;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;

public class IngestionEngineTests extends EngineTestCase {

    private IndexSettings indexSettings;
    private Store ingestionEngineStore;
    private IngestionEngine ingestionEngine;
    // the messages of the stream to ingest from
    private List<byte[]> messages;

    @Override
    @Before
    public void setUp() throws Exception {
        indexSettings = newIndexSettings();
        super.setUp();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        ingestionEngineStore = createStore(indexSettings, newDirectory());
        // create some initial messages
        messages = new ArrayList<>();
        publishData("{\"name\":\"bob\", \"age\": 24}");
        publishData("{\"name\":\"alice\", \"age\": 20}");
        ingestionEngine = buildIngestionEngine(globalCheckpoint, ingestionEngineStore, indexSettings);
    }

    private void publishData(String message) {
        messages.add(message.getBytes());
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
    }

    public void testCreateEngine() throws IOException {
        // wait for the engine to ingest all messages
        waitForResults(ingestionEngine, 2);
        // flush
        ingestionEngine.flush(false, true);
        Map<String, String> commitData = ingestionEngine.commitDataAsMap();
        // verify the commit data
        Assert.assertEquals(1, commitData.size());
        Assert.assertEquals("2", commitData.get(StreamPoller.BATCH_START));

        // verify the stored offsets
        var offset = new IngestionEngineUtils.FakeIngestionShardPointer(0);
        ingestionEngine.refresh("read_offset");
        try (Engine.Searcher searcher = ingestionEngine.acquireSearcher("read_offset")) {
            Set<IngestionShardPointer> persistedPointers = ingestionEngine.fetchPersistedOffsets(Lucene.wrapAllDocsLive(searcher.getDirectoryReader()), offset);
            Assert.assertEquals(2, persistedPointers.size());
        }
    }

    public void testRecovery() throws IOException {
        // wait for the engine to ingest all messages
        waitForResults(ingestionEngine, 2);
        // flush
        ingestionEngine.flush(false, true);

        // ingest some new messages
        publishData("{\"name\":\"john\", \"age\": 30}");
        publishData("{\"name\":\"jane\", \"age\": 25}");
        ingestionEngine.close();
        ingestionEngine = buildIngestionEngine(new AtomicLong(2), ingestionEngineStore, indexSettings);
        waitForResults(ingestionEngine, 4);
    }

    private IngestionEngine buildIngestionEngine(AtomicLong globalCheckpoint, Store store, IndexSettings settings) throws IOException {
        IngestionEngineUtils.FakeIngestionConsumerFactory consumerFactory = new IngestionEngineUtils.FakeIngestionConsumerFactory(messages);
        EngineConfig engineConfig = config(settings, store, createTempDir(), NoMergePolicy.INSTANCE, null, null, globalCheckpoint::get);
        // overwrite the config with ingestion engine settings
        String mapping = "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}";
        MapperService mapperService = createMapperService(mapping);
        engineConfig = config(engineConfig, () -> new DocumentMapperForType(mapperService.documentMapper(), null), consumerFactory);
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
        return new IngestionEngine(engineConfig);
    }

    private void waitForResults(Engine engine, int numDocs) {
        await()
            .atMost(3, TimeUnit.SECONDS)
            .untilAsserted(
                () -> {
                    Assert.assertTrue(resultsFound(engine, numDocs));
                });
    }

    private boolean resultsFound(Engine engine, int numDocs) {
        engine.refresh("index");
        try (Engine.Searcher searcher = engine.acquireSearcher("index")) {
            return searcher.getIndexReader().numDocs() == numDocs;
        }
    }
}
