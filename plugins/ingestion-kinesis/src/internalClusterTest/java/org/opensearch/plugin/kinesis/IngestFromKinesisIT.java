/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Assert;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testcontainers.containers.localstack.LocalStackContainer;

import static org.hamcrest.Matchers.is;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for Kinesis ingestion
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IngestFromKinesisIT extends KinesisIngestionBaseIT {
    /**
     * test ingestion-kinesis-plugin is installed
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
            pluginInfos.stream().anyMatch(pluginInfo -> pluginInfo.getName().equals("org.opensearch.plugin.kinesis.KinesisPlugin"))
        );
    }

    public void testKinesisIngestion() {
        produceData("1", "name1", "24");
        produceData("2", "name2", "20");

        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kinesis")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.stream", "test")
                .put("ingestion_source.param.region", localstack.getRegion())
                .put("ingestion_source.param.access_key", localstack.getAccessKey())
                .put("ingestion_source.param.secret_key", localstack.getSecretKey())
                .put(
                    "ingestion_source.param.endpoint_override",
                    localstack.getEndpointOverride(LocalStackContainer.Service.KINESIS).toString()
                )
                .put("index.replication.type", "SEGMENT")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(21);
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            refresh("test");
            SearchResponse response = client().prepareSearch("test").setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            PollingIngestStats stats = client().admin().indices().prepareStats("test").get().getIndex("test").getShards()[0]
                .getPollingIngestStats();
            assertNotNull(stats);
            assertThat(stats.getMessageProcessorStats().totalProcessedCount(), is(2L));
            assertThat(stats.getConsumerStats().totalPolledCount(), is(2L));
        });
    }

    public void testKinesisIngestion_RewindByOffset() throws InterruptedException {
        produceData("1", "name1", "24");
        produceData("2", "name2", "24");
        String sequenceNumber = produceData("3", "name3", "20");
        logger.info("Produced message with sequence number: {}", sequenceNumber);
        produceData("4", "name4", "21");

        await().atMost(5, TimeUnit.SECONDS).until(() -> isRewinded(sequenceNumber));

        // create an index with ingestion source from kinesis
        createIndex(
            "test_rewind_by_offset",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kinesis")
                .put("ingestion_source.pointer.init.reset", "rewind_by_offset")
                .put("ingestion_source.pointer.init.reset.value", sequenceNumber)
                .put("ingestion_source.param.stream", "test")
                .put("ingestion_source.param.region", localstack.getRegion())
                .put("ingestion_source.param.access_key", localstack.getAccessKey())
                .put("ingestion_source.param.secret_key", localstack.getSecretKey())
                .put(
                    "ingestion_source.param.endpoint_override",
                    localstack.getEndpointOverride(LocalStackContainer.Service.KINESIS).toString()
                )
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            refresh("test_rewind_by_offset");
            SearchResponse response = client().prepareSearch("test_rewind_by_offset").setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(2L));
        });
    }

    private boolean isRewinded(String sequenceNumber) {
        DescribeStreamResponse describeStreamResponse = kinesisClient.describeStream(
            DescribeStreamRequest.builder().streamName(streamName).build()
        );

        String shardId = describeStreamResponse.streamDescription().shards().get(0).shardId();

        GetShardIteratorRequest iteratorRequest = GetShardIteratorRequest.builder()
            .streamName(streamName)
            .shardId(shardId)
            .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
            .startingSequenceNumber(sequenceNumber)
            .build();

        GetShardIteratorResponse iteratorResponse = kinesisClient.getShardIterator(iteratorRequest);
        String shardIterator = iteratorResponse.shardIterator();

        // Use the iterator to read the record
        GetRecordsRequest recordsRequest = GetRecordsRequest.builder()
            .shardIterator(shardIterator)
            .limit(1)  // Adjust as needed
            .build();

        GetRecordsResponse recordsResponse = kinesisClient.getRecords(recordsRequest);
        List<Record> records = recordsResponse.records();
        if (records.size() != 1) {
            return false;
        }
        return records.get(0).partitionKey().equals("3");
    }
}
