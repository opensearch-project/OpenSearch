/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionRequest;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionResponse;
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.transport.client.Requests;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@ThreadLeakFilters(filters = KafkaSingleNodeTests.TestContainerThreadLeakFilter.class)
public class KafkaSingleNodeTests extends OpenSearchSingleNodeTestCase {
    private KafkaContainer kafka;
    private Producer<String, String> producer;
    private final String topicName = "test";
    private final String indexName = "testindex";
    private final String mappings = "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(KafkaPlugin.class);
    }

    @Before
    public void setup() {
        Assume.assumeTrue("Docker is not available", DockerClientFactory.instance().isDockerAvailable());
        setupKafka();
    }

    @After
    public void cleanup() {
        stopKafka();
    }

    public void testPauseAndResumeAPIs() throws Exception {
        produceData("{\"_id\":\"1\",\"_version\":\"1\",\"_op_type\":\"index\",\"_source\":{\"name\":\"name\", \"age\": 25}}");
        produceData("{\"_id\":\"2\",\"_version\":\"1\",\"_op_type\":\"index\",\"_source\":{\"name\":\"name\", \"age\": 25}}");

        createIndexWithMappingSource(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("index.replication.type", "SEGMENT")
                .put("ingestion_source.pointer_based_lag_update_interval", "0")
                .build(),
            mappings
        );
        ensureGreen(indexName);

        waitForState(() -> {
            RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            return response.getHits().getTotalHits().value() == 2;
        });

        ResumeIngestionResponse resumeResponse = client().admin()
            .indices()
            .resumeIngestion(Requests.resumeIngestionRequest(indexName, 0, ResumeIngestionRequest.ResetSettings.ResetMode.OFFSET, "0"))
            .get();
        assertTrue(resumeResponse.isAcknowledged());
        assertFalse(resumeResponse.isShardsAcknowledged());
        assertEquals(1, resumeResponse.getShardFailures().length);

        // pause ingestion
        client().admin().indices().pauseIngestion(Requests.pauseIngestionRequest(indexName)).get();
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState(indexName);
            return ingestionState.getFailedShards() == 0
                && Arrays.stream(ingestionState.getShardStates())
                    .allMatch(state -> state.isPollerPaused() && state.getPollerState().equalsIgnoreCase("paused"));
        });

        produceData("{\"_id\":\"1\",\"_version\":\"2\",\"_op_type\":\"index\",\"_source\":{\"name\":\"name\", \"age\": 30}}");
        produceData("{\"_id\":\"2\",\"_version\":\"2\",\"_op_type\":\"index\",\"_source\":{\"name\":\"name\", \"age\": 30}}");

        // resume ingestion with offset reset
        client().admin()
            .indices()
            .resumeIngestion(Requests.resumeIngestionRequest(indexName, 0, ResumeIngestionRequest.ResetSettings.ResetMode.OFFSET, "0"))
            .get();
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState(indexName);
            return Arrays.stream(ingestionState.getShardStates())
                .allMatch(
                    state -> state.isPollerPaused() == false
                        && (state.getPollerState().equalsIgnoreCase("polling") || state.getPollerState().equalsIgnoreCase("processing"))
                );
        });

        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(29);
        waitForState(() -> {
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            PollingIngestStats stats = client().admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
                .getPollingIngestStats();

            return response.getHits().getTotalHits().value() == 2
                && stats != null
                && stats.getConsumerStats().totalPolledCount() == 4
                && stats.getConsumerStats().totalPollerMessageFailureCount() == 0;
        });
    }

    // This test validates shard initialization does not fail due to kafka connection errors.
    public void testShardInitializationUsingUnknownTopic() throws Exception {
        createIndexWithMappingSource(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.topic", "unknownTopic")
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("index.replication.type", "SEGMENT")
                .build(),
            mappings
        );
        ensureGreen(indexName);
    }

    public void testConsumerSettingUpdateWithMultipleProcessorThreads() throws Exception {
        // Create index with num_processor_threads = 5, pointer.init.reset to offset 100, and auto.offset.reset = none
        // This should cause consumer initialization to fail
        createIndexWithMappingSource(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "reset_by_offset")
                .put("ingestion_source.pointer.init.reset.value", "100")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.param.auto.offset.reset", "none")
                .put("ingestion_source.num_processor_threads", 5)
                .put("index.replication.type", "SEGMENT")
                .build(),
            mappings
        );

        ensureGreen(indexName);

        // Wait for paused status due to consumer error
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState(indexName);
            PollingIngestStats stats = client().admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
                .getPollingIngestStats();

            return ingestionState.getShardStates().length == 1
                && ingestionState.getShardStates()[0].isPollerPaused()
                && stats != null
                && stats.getConsumerStats().totalConsumerErrorCount() >= 1L;
        });

        // Publish 5 messages
        for (int i = 0; i < 5; i++) {
            produceData(
                "{\"_id\":\"" + i + "\",\"_version\":\"1\",\"_op_type\":\"index\",\"_source\":{\"name\":\"name" + i + "\", \"age\": 25}}"
            );
        }

        // Update auto.offset.reset to earliest
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("ingestion_source.param.auto.offset.reset", "earliest"))
            .get();

        // Resume ingestion
        client().admin().indices().resumeIngestion(Requests.resumeIngestionRequest(indexName)).get();

        // Wait for 5 searchable docs
        waitForState(() -> {
            RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            return response.getHits().getTotalHits().value() == 5;
        });
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

    private void produceData(String payload) {
        producer.send(new ProducerRecord<>(topicName, null, 1739459500000L, "null", payload));
    }

    protected GetIngestionStateResponse getIngestionState(String indexName) throws ExecutionException, InterruptedException {
        return client().admin().indices().getIngestionState(Requests.getIngestionStateRequest(indexName)).get();
    }

    protected void waitForState(Callable<Boolean> checkState) throws Exception {
        assertBusy(() -> {
            if (checkState.call() == false) {
                fail("Provided state requirements not met");
            }
        }, 1, TimeUnit.MINUTES);
    }

    public static final class TestContainerThreadLeakFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            return t.getName().startsWith("testcontainers-pull-watchdog-")
                || t.getName().startsWith("testcontainers-ryuk")
                || t.getName().startsWith("stream-poller-consumer");
        }
    }
}
