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
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.ingest.KafkaUtils;
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

    public KafkaContainer kafka;
    public String bootstrapServers;

    @Override
    @Before
    public void setUp() throws Exception {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
        kafka.start();
        bootstrapServers = kafka.getBootstrapServers();
        prepareKafkaData();
        super.setUp();
    }

    private void prepareKafkaData() {
        String boostrapServers = kafka.getBootstrapServers();
        KafkaUtils.createTopic(topicName,1, boostrapServers);
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

    public void createEngine() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (
            final Store ingestionEngineStore = createStore(INDEX_SETTINGS, newDirectory());
            final IngestionEngine ingestionEngine = buildIngestionEngine(globalCheckpoint, ingestionEngineStore, defaultSettings);
        ){

        }
    }

    private IngestionEngine buildIngestionEngine(AtomicLong globalCheckpoint, Store store, IndexSettings settings) throws IOException {
        Lucene.cleanLuceneIndex(store.directory());
        final Path translogDir = createTempDir();
        final EngineConfig engineConfig = config(settings, store, translogDir, NoMergePolicy.INSTANCE, null, null, globalCheckpoint::get);
        if (Lucene.indexExists(store.directory())) {
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

    public void testIngestionEngineStart() {
        InternalEngine internalEngine = engine;
        EngineConfig engineConfig = internalEngine.config();
        engineConfig.getIndexSettings().getIndexMetadata().getIngestionSourceConfig();
        await()
            .atMost(3, TimeUnit.SECONDS)
            .untilAsserted(
                () -> {
                    Assert.assertTrue(resultsFound());
                });
    }

    private boolean resultsFound() {
        engine.refresh("warm_up");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            return searcher.getIndexReader().numDocs() == 3;
        }
    }
}
