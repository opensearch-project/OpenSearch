/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base test class for Kafka ingestion tests.
 */
@ThreadLeakFilters(filters = TestContainerThreadLeakFilter.class)
public class KafkaIngestionBaseIT extends OpenSearchIntegTestCase {
    static final String topicName = "test";
    static final String indexName = "testindex";
    static final String mapping = "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}";
    static final long defaultMessageTimestamp = 1739459500000L;

    protected KafkaContainer kafka;
    protected Producer<String, String> producer;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(KafkaPlugin.class);
    }

    @Before
    private void setup() {
        setupKafka();
    }

    @After
    private void cleanup() {
        stopKafka();
    }

    private void setupKafka() {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
            // disable topic auto creation
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        kafka.start();

        // setup producer
        String boostrapServers = kafka.getBootstrapServers();
        KafkaUtils.createTopic(topicName, 1, boostrapServers);
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    private void stopKafka() {
        if (producer != null) {
            producer.close();
        }

        if (kafka != null) {
            kafka.stop();
        }
    }

    protected void produceData(String id, String name, String age) {
        produceData(id, name, age, defaultMessageTimestamp);
    }

    protected void produceData(String id, String name, String age, long timestamp) {
        String payload = String.format(
            Locale.ROOT,
            "{\"_id\":\"%s\", \"_op_type:\":\"index\",\"_source\":{\"name\":\"%s\", \"age\": %s}}",
            id,
            name,
            age
        );
        producer.send(new ProducerRecord<>(topicName, null, timestamp, "null", payload));
    }

    protected void waitForSearchableDocs(long docCount, List<String> nodes) throws Exception {
        assertBusy(() -> {
            for (String node : nodes) {
                final SearchResponse response = client(node).prepareSearch(indexName).setSize(0).setPreference("_only_local").get();
                final long hits = response.getHits().getTotalHits().value();
                if (hits < docCount) {
                    fail("Expected search hits on node: " + node + " to be at least " + docCount + " but was: " + hits);
                }
            }
        }, 1, TimeUnit.MINUTES);
    }

    protected void waitForState(Callable<Boolean> checkState) throws Exception {
        assertBusy(() -> {
            if (checkState.call() == false) {
                fail("Provided state requirements not met");
            }
        }, 1, TimeUnit.MINUTES);
    }

    protected String getSettings(String indexName, String setting) {
        return client().admin().indices().prepareGetSettings(indexName).get().getSetting(indexName, setting);
    }

    protected void createIndexWithDefaultSettings(int numShards, int numReplicas) {
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("index.replication.type", "SEGMENT")
                // set custom kafka consumer properties
                .put("ingestion_source.param.fetch.min.bytes", 30000)
                .put("ingestion_source.param.enable.auto.commit", false)
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );
    }
}
