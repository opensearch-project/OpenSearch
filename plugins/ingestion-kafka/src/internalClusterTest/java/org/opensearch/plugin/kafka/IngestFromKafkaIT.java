/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * Integration test for Kafka ingestion
 */
public class IngestFromKafkaIT extends OpenSearchIntegTestCase {
    static final String topicName = "test";

    private KafkaContainer kafka;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(KafkaPlugin.class);
    }

    public void testKafkaIngestion() {
        setupKafka();
        // create an index with ingestion source from kafka
        createIndex("test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.topic", "test")
                .put("ingestion_source.param.bootstrapServers", kafka.getBootstrapServers())
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}");

        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(21);
        await()
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(
                () -> {
                    refresh("test");
                    SearchResponse response = client().prepareSearch("test").setQuery(query).get();
                    assertThat(response.getHits().getTotalHits().value, is(1L));
                });

        stopKafka();
    }

    private void setupKafka() {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
            // disable topic auto creation
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        kafka.start();
        prepareKafkaData();
    }

    private void stopKafka() {
        kafka.stop();
    }

    private void prepareKafkaData() {
        String boostrapServers = kafka.getBootstrapServers();
        KafkaUtils.createTopic(topicName, 1, boostrapServers);
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        producer.send(new ProducerRecord<>(topicName, "null", "{\"name\":\"bob\", \"age\": 24}"));
        producer.send(new ProducerRecord<>(topicName, "null", "{\"name\":\"alice\", \"age\": 20}"));
        producer.close();
    }
}
