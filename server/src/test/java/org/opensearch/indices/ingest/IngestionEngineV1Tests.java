/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.ingest;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.engine.InternalEngine;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.containers.KafkaContainer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class IngestionEngineV1Tests extends EngineTestCase
{
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

    @Override
    protected Settings indexSettings() {
        return Settings.builder()
            .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), "1h") // make sure this doesn't kick in on us
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), codecName)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(
                IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.getKey(),
                between(10, 10 * IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.get(Settings.EMPTY))
            )
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), between(0, 1000))
            .put("bootstrap.servers", bootstrapServers)
            .build();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if(kafka!=null) {
            kafka.stop();
        }
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
