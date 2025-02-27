/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Assert;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for Kafka ingestion
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IngestFromKafkaIT extends KafkaIngestionBaseIT {
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
        produceData("1", "name1", "24");
        produceData("2", "name2", "20");

        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.topic", "test")
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("index.replication.type", "SEGMENT")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(21);
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            refresh("test");
            SearchResponse response = client().prepareSearch("test").setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
        });
    }

    public void testKafkaIngestion_RewindByTimeStamp() {
        produceData("1", "name1", "24", 1739459500000L);
        produceData("2", "name2", "20", 1739459800000L);

        // create an index with ingestion source from kafka
        createIndex(
            "test_rewind_by_timestamp",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "rewind_by_timestamp")
                // 1739459500000 is the timestamp of the first message
                // 1739459800000 is the timestamp of the second message
                // by resetting to 1739459600000, only the second message will be ingested
                .put("ingestion_source.pointer.init.reset.value", "1739459600000")
                .put("ingestion_source.param.topic", "test")
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.param.auto.offset.reset", "latest")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            refresh("test_rewind_by_timestamp");
            SearchResponse response = client().prepareSearch("test_rewind_by_timestamp").setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
        });
    }

    public void testKafkaIngestion_RewindByOffset() {
        produceData("1", "name1", "24");
        produceData("2", "name2", "20");
        // create an index with ingestion source from kafka
        createIndex(
            "test_rewind_by_offset",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "rewind_by_offset")
                .put("ingestion_source.pointer.init.reset.value", "1")
                .put("ingestion_source.param.topic", "test")
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.param.auto.offset.reset", "latest")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            refresh("test_rewind_by_offset");
            SearchResponse response = client().prepareSearch("test_rewind_by_offset").setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
        });
    }
}
