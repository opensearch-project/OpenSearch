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
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
                    .allMatch(state -> state.isPollerPaused() && state.pollerState().equalsIgnoreCase("paused"));
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
                        && (state.pollerState().equalsIgnoreCase("polling") || state.pollerState().equalsIgnoreCase("processing"))
                );
        });

        // validate duplicate messages are skipped
        waitForState(() -> {
            PollingIngestStats stats = client().admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
                .getPollingIngestStats();
            return stats.getConsumerStats().totalDuplicateMessageSkippedCount() == 2;
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
