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
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.hamcrest.Matchers.is;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for Kafka ingestion
 */
@ThreadLeakFilters(filters = TestContainerWatchdogThreadLeakFilter.class)
public class IngestFromKafkaIT extends OpenSearchIntegTestCase {
    static final String topicName = "test";

    private KafkaContainer kafka;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(KafkaPlugin.class);
    }

    /**
     * test ingestion-kafka-plugin is installed
     */
    public void testPluginsAreInstalled() {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.addMetric(NodesInfoRequest.Metric.PLUGINS.metricName());
        NodesInfoResponse nodesInfoResponse = OpenSearchIntegTestCase.client().admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        List<PluginInfo> pluginInfos = nodesInfoResponse.getNodes()
            .stream()
            .flatMap(
                (Function<NodeInfo, Stream<PluginInfo>>) nodeInfo -> nodeInfo.getInfo(PluginsAndModules.class).getPluginInfos().stream()
            )
            .collect(Collectors.toList());
        Assert.assertTrue(
            pluginInfos.stream().anyMatch(pluginInfo -> pluginInfo.getName().equals("org.opensearch.plugin.kafka.KafkaPlugin"))
        );
    }

    public void testKafkaIngestion() {
        try {
            setupKafka();
            // create an index with ingestion source from kafka
            createIndex(
                "test",
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("ingestion_source.type", "kafka")
                    .put("ingestion_source.pointer.init.reset", "earliest")
                    .put("ingestion_source.param.topic", "test")
                    .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                    .build(),
                "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
            );

            RangeQueryBuilder query = new RangeQueryBuilder("age").gte(21);
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                refresh("test");
                SearchResponse response = client().prepareSearch("test").setQuery(query).get();
                assertThat(response.getHits().getTotalHits().value(), is(1L));
            });
        } finally {
            stopKafka();
        }
    }

    private void setupKafka() {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
            // disable topic auto creation
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        kafka.start();
        prepareKafkaData();
    }

    private void stopKafka() {
        if (kafka != null) {
            kafka.stop();
        }
    }

    private void prepareKafkaData() {
        String boostrapServers = kafka.getBootstrapServers();
        KafkaUtils.createTopic(topicName, 1, boostrapServers);
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        producer.send(new ProducerRecord<>(topicName, "null", "{\"_id\":\"1\",\"_source\":{\"name\":\"bob\", \"age\": 24}}"));
        producer.send(
            new ProducerRecord<>(
                topicName,
                "null",
                "{\"_id\":\"2\", \"_op_type:\":\"index\",\"_source\":{\"name\":\"alice\", \"age\": 20}}"
            )
        );
        producer.close();
    }
}
