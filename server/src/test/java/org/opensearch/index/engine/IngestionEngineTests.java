/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.ingest.KafkaUtils;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.IndexSettingsModule;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

public class IngestionEngineTests extends EngineTestCase {
    static final String topicName = "test";

    private IndexSettings indexSettings;
    private Store ingestionEngineStore;
    private IngestionEngine ingestionEngine;

    public KafkaContainer kafka;
    public String bootstrapServers;

    @Override
    @Before
    public void setUp() throws Exception {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
        kafka.start();
        bootstrapServers = kafka.getBootstrapServers();
        indexSettings = newIndexSettings();
        prepareKafkaData();
        super.setUp();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        ingestionEngineStore = createStore(indexSettings, newDirectory());
        ingestionEngine = buildIngestionEngine(globalCheckpoint, ingestionEngineStore, indexSettings);
    }

    protected IndexSettings newIndexSettings() {
        return IndexSettingsModule.newIndexSettings(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .put("bootstrap.servers", bootstrapServers)
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
        if (kafka != null) {
            kafka.stop();
        }
    }

    private void prepareKafkaData() {
        String boostrapServers = kafka.getBootstrapServers();
        KafkaUtils.createTopic(topicName, 1, boostrapServers);
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer
            <String, String>(props);
        producer.send(new ProducerRecord<String, String>(topicName, "1", "value1"));
        producer.send(new ProducerRecord<String, String>(topicName, "2", "value2"));
        producer.send(new ProducerRecord<String, String>(topicName, "3", "value3"));
        producer.close();
    }

    public void testCreateEngine() throws IOException {
        await()
            .atMost(300, TimeUnit.SECONDS)
            .untilAsserted(
                () -> {
                    Assert.assertTrue(resultsFound(ingestionEngine));
                });
    }

    private IngestionEngine buildIngestionEngine(AtomicLong globalCheckpoint, Store store, IndexSettings settings) throws IOException {
        Lucene.cleanLuceneIndex(store.directory());
        final Path translogDir = createTempDir();
        final EngineConfig engineConfig = config(settings, store, translogDir, NoMergePolicy.INSTANCE, null, null, globalCheckpoint::get);
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

    private boolean resultsFound(Engine engine) {
        engine.refresh("index");
        try (Engine.Searcher searcher = engine.acquireSearcher("index")) {
            return searcher.getIndexReader().numDocs() == 3;
        }
    }
}
