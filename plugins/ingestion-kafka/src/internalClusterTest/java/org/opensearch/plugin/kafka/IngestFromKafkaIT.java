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
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionRequest;
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Requests;
import org.junit.Assert;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for Kafka ingestion.
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
        createIndexWithDefaultSettings(1, 0);

        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(21);
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            PollingIngestStats stats = client().admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
                .getPollingIngestStats();
            assertNotNull(stats);
            assertThat(stats.getMessageProcessorStats().totalProcessedCount(), is(2L));
            assertThat(stats.getConsumerStats().totalPolledCount(), is(2L));
        });
    }

    public void testKafkaIngestion_RewindByTimeStamp() {
        produceData("1", "name1", "24", 1739459500000L, "index");
        produceData("2", "name2", "20", 1739459800000L, "index");

        // create an index with ingestion source from kafka
        createIndex(
            "test_rewind_by_timestamp",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "reset_by_timestamp")
                // 1739459500000 is the timestamp of the first message
                // 1739459800000 is the timestamp of the second message
                // by resetting to 1739459600000, only the second message will be ingested
                .put("ingestion_source.pointer.init.reset.value", "1739459600000")
                .put("ingestion_source.param.topic", "test")
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.param.auto.offset.reset", "latest")
                .put("ingestion_source.all_active", true)
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
                .put("ingestion_source.pointer.init.reset", "reset_by_offset")
                .put("ingestion_source.pointer.init.reset.value", "1")
                .put("ingestion_source.param.topic", "test")
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.param.auto.offset.reset", "latest")
                .put("ingestion_source.all_active", true)
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
        await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
            refresh("test_rewind_by_offset");
            SearchResponse response = client().prepareSearch("test_rewind_by_offset").setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
        });
    }

    public void testCloseIndex() throws Exception {
        createIndexWithDefaultSettings(1, 0);
        ensureGreen(indexName);
        client().admin().indices().close(Requests.closeIndexRequest(indexName)).get();
    }

    public void testMessageOperationTypes() throws Exception {
        // Step 1: Produce message and wait for it to be searchable

        produceData("1", "name", "25", defaultMessageTimestamp, "index");
        createIndexWithDefaultSettings(1, 0);
        ensureGreen(indexName);
        waitForState(() -> {
            BoolQueryBuilder query = new BoolQueryBuilder().must(new TermQueryBuilder("_id", "1"));
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            return 25 == (Integer) response.getHits().getHits()[0].getSourceAsMap().get("age");
        });

        // Step 2: Update age field from 25 to 30 and validate

        produceData("1", "name", "30", defaultMessageTimestamp, "index");
        waitForState(() -> {
            BoolQueryBuilder query = new BoolQueryBuilder().must(new TermQueryBuilder("_id", "1"));
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            return 30 == (Integer) response.getHits().getHits()[0].getSourceAsMap().get("age");
        });

        // Step 3: Delete the document and validate
        produceData("1", "name", "30", defaultMessageTimestamp, "delete");
        waitForState(() -> {
            BoolQueryBuilder query = new BoolQueryBuilder().must(new TermQueryBuilder("_id", "1"));
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            return response.getHits().getTotalHits().value() == 0;
        });

        // Step 4: Validate create operation
        produceData("2", "name", "30", defaultMessageTimestamp, "create");
        waitForState(() -> {
            BoolQueryBuilder query = new BoolQueryBuilder().must(new TermQueryBuilder("_id", "2"));
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            return 30 == (Integer) response.getHits().getHits()[0].getSourceAsMap().get("age");
        });
    }

    public void testUpdateWithoutIDField() throws Exception {
        // Step 1: Produce message without ID
        String payload = "{\"_op_type\":\"index\",\"_source\":{\"name\":\"name\", \"age\": 25}}";
        produceData(payload);

        createIndexWithDefaultSettings(1, 0);
        ensureGreen(indexName);

        waitForState(() -> {
            BoolQueryBuilder query = new BoolQueryBuilder().must(new TermQueryBuilder("age", "25"));
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            return 25 == (Integer) response.getHits().getHits()[0].getSourceAsMap().get("age");
        });

        SearchResponse searchableDocsResponse = client().prepareSearch(indexName).setSize(10).setPreference("_only_local").get();
        assertThat(searchableDocsResponse.getHits().getTotalHits().value(), is(1L));
        assertEquals(25, searchableDocsResponse.getHits().getHits()[0].getSourceAsMap().get("age"));
        String id = searchableDocsResponse.getHits().getHits()[0].getId();

        // Step 2: Produce an update message using retrieved ID and validate

        produceData(id, "name", "30", defaultMessageTimestamp, "index");
        waitForState(() -> {
            BoolQueryBuilder query = new BoolQueryBuilder().must(new TermQueryBuilder("_id", id));
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            return 30 == (Integer) response.getHits().getHits()[0].getSourceAsMap().get("age");
        });
    }

    public void testMultiThreadedWrites() throws Exception {
        // create index with 5 writer threads
        createIndexWithDefaultSettings(indexName, 1, 0, 5);
        ensureGreen(indexName);

        // Step 1: Produce messages
        for (int i = 0; i < 1000; i++) {
            produceData(Integer.toString(i), "name" + i, "25");
        }

        waitForState(() -> {
            SearchResponse searchableDocsResponse = client().prepareSearch(indexName).setSize(2000).setPreference("_only_local").get();
            return searchableDocsResponse.getHits().getTotalHits().value() == 1000;
        });

        // Step 2: Produce an update message and validate
        for (int i = 0; i < 1000; i++) {
            produceData(Integer.toString(i), "name" + i, "30");
        }

        waitForState(() -> {
            RangeQueryBuilder query = new RangeQueryBuilder("age").gte(28);
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            return response.getHits().getTotalHits().value() == 1000;
        });
    }

    public void testAllActiveIngestion() throws Exception {
        // Create all-active pull-based index
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        for (int i = 0; i < 10; i++) {
            produceData(Integer.toString(i), "name" + i, "30");
        }

        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.all_active", true)
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        ensureYellowAndNoInitializingShards(indexName);
        waitForSearchableDocs(10, List.of(nodeA));
        flush(indexName);

        // add a second node and verify the replica ingests the data
        final String nodeB = internalCluster().startDataOnlyNode();
        ensureGreen(indexName);
        assertTrue(nodeA.equals(primaryNodeName(indexName)));
        assertTrue(nodeB.equals(replicaNodeName(indexName)));
        waitForSearchableDocs(10, List.of(nodeB));

        // verify pause and resume functionality on replica

        // pause ingestion
        pauseIngestionAndWait(indexName, 2);

        for (int i = 10; i < 20; i++) {
            produceData(Integer.toString(i), "name" + i, "30");
        }

        // replica must not ingest when paused
        Thread.sleep(1000);
        assertEquals(10, getSearchableDocCount(nodeB));

        // resume ingestion
        resumeIngestionAndWait(indexName, 2);

        // verify replica ingests data after resuming ingestion
        waitForSearchableDocs(20, List.of(nodeA, nodeB));

        // produce 10 more messages
        for (int i = 20; i < 30; i++) {
            produceData(Integer.toString(i), "name" + i, "30");
        }

        // Add new node and wait for new node to join cluster
        final String nodeC = internalCluster().startDataOnlyNode();
        assertBusy(() -> {
            assertEquals(
                "Should have 4 nodes total (1 cluster manager + 3 data)",
                4,
                internalCluster().clusterService().state().nodes().getSize()
            );
        }, 30, TimeUnit.SECONDS);

        // move replica from nodeB to nodeC
        ensureGreen(indexName);
        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(indexName, 0, nodeB, nodeC)).get();
        ensureGreen(indexName);

        // confirm replica ingests messages after moving to new node
        waitForSearchableDocs(30, List.of(nodeA, nodeC));

        for (int i = 30; i < 40; i++) {
            produceData(Integer.toString(i), "name" + i, "30");
        }

        // restart replica node and verify ingestion
        internalCluster().restartNode(nodeC);
        ensureGreen(indexName);
        waitForSearchableDocs(40, List.of(nodeA, nodeC));

        // Verify both primary and replica do not have failed messages
        Map<String, PollingIngestStats> shardTypeToStats = getPollingIngestStatsForPrimaryAndReplica(indexName);
        assertNotNull(shardTypeToStats.get("primary"));
        assertNotNull(shardTypeToStats.get("replica"));
        assertThat(shardTypeToStats.get("primary").getConsumerStats().totalPollerMessageDroppedCount(), is(0L));
        assertThat(shardTypeToStats.get("primary").getConsumerStats().totalPollerMessageFailureCount(), is(0L));
        // replica consumes only 10 messages after it has been restarted
        assertThat(shardTypeToStats.get("replica").getConsumerStats().totalPollerMessageDroppedCount(), is(0L));
        assertThat(shardTypeToStats.get("replica").getConsumerStats().totalPollerMessageFailureCount(), is(0L));

        GetIngestionStateResponse ingestionState = getIngestionState(indexName);
        assertEquals(2, ingestionState.getShardStates().length);
        assertEquals(0, ingestionState.getFailedShards());
    }

    public void testReplicaPromotionOnAllActiveIngestion() throws Exception {
        // Create all-active pull-based index
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        for (int i = 0; i < 10; i++) {
            produceData(Integer.toString(i), "name" + i, "30");
        }

        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.all_active", true)
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        ensureYellowAndNoInitializingShards(indexName);
        waitForSearchableDocs(10, List.of(nodeA));

        // add second node
        final String nodeB = internalCluster().startDataOnlyNode();
        ensureGreen(indexName);
        assertTrue(nodeA.equals(primaryNodeName(indexName)));
        assertTrue(nodeB.equals(replicaNodeName(indexName)));
        waitForSearchableDocs(10, List.of(nodeB));

        // Validate replica promotion
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeA));
        ensureYellowAndNoInitializingShards(indexName);
        assertTrue(nodeB.equals(primaryNodeName(indexName)));
        for (int i = 10; i < 20; i++) {
            produceData(Integer.toString(i), "name" + i, "30");
        }

        waitForSearchableDocs(20, List.of(nodeB));

        // add third node and allocate the replica once the node joins the cluster
        final String nodeC = internalCluster().startDataOnlyNode();
        assertBusy(() -> { assertEquals(3, internalCluster().clusterService().state().nodes().getSize()); }, 30, TimeUnit.SECONDS);
        client().admin().cluster().prepareReroute().add(new AllocateReplicaAllocationCommand(indexName, 0, nodeC)).get();
        ensureGreen(indexName);
        waitForSearchableDocs(20, List.of(nodeC));

    }

    public void testSnapshotRestoreOnAllActiveIngestion() throws Exception {
        // Create all-active pull-based index
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();
        for (int i = 0; i < 20; i++) {
            produceData(Integer.toString(i), "name" + i, "30");
        }

        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.all_active", true)
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );
        ensureGreen(indexName);
        waitForSearchableDocs(20, List.of(nodeA, nodeB));

        // Register snapshot repository
        String snapshotRepositoryName = "test-snapshot-repo";
        String snapshotName = "snapshot-1";
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(snapshotRepositoryName)
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath()).put("compress", false))
        );

        // Take snapshot
        flush(indexName);
        CreateSnapshotResponse snapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepositoryName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertTrue(snapshotResponse.getSnapshotInfo().successfulShards() > 0);

        // Delete Index
        assertAcked(client().admin().indices().prepareDelete(indexName));
        waitForState(() -> {
            ClusterState state = client().admin().cluster().prepareState().setIndices(indexName).get().getState();
            return state.getRoutingTable().hasIndex(indexName) == false && state.getMetadata().hasIndex(indexName) == false;
        });

        for (int i = 20; i < 40; i++) {
            produceData(Integer.toString(i), "name" + i, "30");
        }

        // Restore Index from Snapshot
        client().admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepositoryName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        ensureGreen(indexName);

        refresh(indexName);
        waitForSearchableDocs(40, List.of(nodeA, nodeB));

        // Verify both primary and replica have indexed remaining messages
        Map<String, PollingIngestStats> shardTypeToStats = getPollingIngestStatsForPrimaryAndReplica(indexName);
        assertNotNull(shardTypeToStats.get("primary"));
        assertNotNull(shardTypeToStats.get("replica"));
        assertThat(shardTypeToStats.get("primary").getConsumerStats().totalPolledCount(), is(21L));
        assertThat(shardTypeToStats.get("primary").getConsumerStats().totalPollerMessageDroppedCount(), is(0L));
        assertThat(shardTypeToStats.get("primary").getConsumerStats().totalPollerMessageFailureCount(), is(0L));
        assertThat(shardTypeToStats.get("replica").getConsumerStats().totalPolledCount(), is(21L));
        assertThat(shardTypeToStats.get("replica").getConsumerStats().totalPollerMessageDroppedCount(), is(0L));
        assertThat(shardTypeToStats.get("replica").getConsumerStats().totalPollerMessageFailureCount(), is(0L));
    }

    public void testResetPollerInAllActiveIngestion() throws Exception {
        // Create all-active pull-based index
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();
        for (int i = 0; i < 10; i++) {
            produceData(Integer.toString(i), "name" + i, "30");
        }

        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.all_active", true)
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        ensureGreen(indexName);
        waitForSearchableDocs(10, List.of(nodeA, nodeB));

        // pause ingestion
        pauseIngestionAndWait(indexName, 2);

        // reset to offset=2 and resume ingestion
        resumeIngestionWithResetAndWait(indexName, 0, ResumeIngestionRequest.ResetSettings.ResetMode.OFFSET, "2", 2);

        // validate there are 8 messages polled after reset
        waitForState(() -> {
            Map<String, PollingIngestStats> shardTypeToStats = getPollingIngestStatsForPrimaryAndReplica(indexName);
            assertNotNull(shardTypeToStats.get("primary"));
            assertNotNull(shardTypeToStats.get("replica"));
            return shardTypeToStats.get("primary").getConsumerStats().totalPolledCount() == 8
                && shardTypeToStats.get("replica").getConsumerStats().totalPolledCount() == 8;
        });
    }

    public void testAllActiveOffsetBasedLag() throws Exception {
        // Create all-active pull-based index
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();

        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.pointer_based_lag_update_interval", "3s")
                .put("ingestion_source.all_active", true)
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        ensureGreen(indexName);
        // no messages published, expect 0 lag
        assertTrue(validateOffsetBasedLagForPrimaryAndReplica(0));

        // pause ingestion
        pauseIngestionAndWait(indexName, 2);

        // produce 10 messages in paused state and validate lag
        for (int i = 0; i < 10; i++) {
            produceData(Integer.toString(i), "name" + i, "30");
        }
        waitForState(() -> validateOffsetBasedLagForPrimaryAndReplica(10));

        // resume ingestion
        resumeIngestionAndWait(indexName, 2);
        waitForSearchableDocs(10, List.of(nodeA, nodeB));
        waitForState(() -> validateOffsetBasedLagForPrimaryAndReplica(0));
    }

    // returns PollingIngestStats for single primary and single replica
    private Map<String, PollingIngestStats> getPollingIngestStatsForPrimaryAndReplica(String indexName) {
        IndexStats indexStats = client().admin().indices().prepareStats(indexName).get().getIndex(indexName);
        ShardStats[] shards = indexStats.getShards();
        assertEquals(2, shards.length);
        Map<String, PollingIngestStats> shardTypeToStats = new HashMap<>();
        for (ShardStats shardStats : shards) {
            if (shardStats.getShardRouting().primary()) {
                shardTypeToStats.put("primary", shardStats.getPollingIngestStats());
            } else {
                shardTypeToStats.put("replica", shardStats.getPollingIngestStats());
            }
        }

        return shardTypeToStats;
    }

    private boolean validateOffsetBasedLagForPrimaryAndReplica(long expectedLag) {
        boolean valid = true;
        Map<String, PollingIngestStats> shardTypeToStats = getPollingIngestStatsForPrimaryAndReplica(indexName);
        valid &= shardTypeToStats.get("primary") != null
            && shardTypeToStats.get("primary").getConsumerStats().pointerBasedLag() == expectedLag;
        valid &= shardTypeToStats.get("replica") != null
            && shardTypeToStats.get("replica").getConsumerStats().pointerBasedLag() == expectedLag;
        return valid;
    }

    public void testAllActiveIngestionBatchStartPointerOnReplicaPromotion() throws Exception {
        // Step 1: Publish 10 messages
        for (int i = 1; i <= 10; i++) {
            produceDataWithExternalVersion(String.valueOf(i), 1, "name" + i, "25", defaultMessageTimestamp, "index");
        }

        // Step 2: Start nodes
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();

        // Step 3: Create all-active index
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.all_active", true)
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        ensureGreen(indexName);

        // Step 4: Wait for 10 messages to be searchable on nodeA
        waitForSearchableDocs(10, Arrays.asList(nodeA));

        // Step 5: Flush to persist data
        flush(indexName);

        // Step 6: Add second node
        final String nodeB = internalCluster().startDataOnlyNode();

        // Step 7: Relocate shard from nodeA to nodeB
        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(indexName, 0, nodeA, nodeB)).get();
        ensureGreen(indexName);
        assertTrue(nodeB.equals(primaryNodeName(indexName)));

        // Step 8: Publish 1 new message
        produceDataWithExternalVersion("11", 1, "name11", "25", defaultMessageTimestamp, "index");

        // Step 9: Wait for 11 messages to be visible on nodeB
        waitForSearchableDocs(11, Arrays.asList(nodeB));

        // Step 10: Flush to persist data
        flush(indexName);

        // Step 11: Validate processed messages and version conflict count on nodeB
        PollingIngestStats nodeBStats = client(nodeB).admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
            .getPollingIngestStats();
        assertNotNull(nodeBStats);
        assertEquals(2L, nodeBStats.getMessageProcessorStats().totalProcessedCount());
        assertEquals(1L, nodeBStats.getMessageProcessorStats().totalVersionConflictsCount());

        // Step 12: Add third node
        final String nodeC = internalCluster().startDataOnlyNode();

        // Step 13: Bring down nodeA so the new replica will be allocated to nodeC
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeA));

        // Step 14: Add a replica (will be allocated to nodeC since only nodeB and nodeC are available)
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            .get();
        ensureGreen(indexName);

        // Step 15: Wait for 11 messages to be searchable on nodeC (replica)
        waitForSearchableDocs(11, Arrays.asList(nodeC));

        // Step 16: Bring down nodeB (primary) and wait for nodeC to become primary
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeB));
        ensureYellowAndNoInitializingShards(indexName);
        assertTrue(nodeC.equals(primaryNodeName(indexName)));

        // Step 17: Publish 1 more message
        produceDataWithExternalVersion("12", 1, "name12", "25", defaultMessageTimestamp, "index");

        // Step 18: Wait for 12 messages to be visible on nodeC
        waitForSearchableDocs(12, Arrays.asList(nodeC));

        // Step 19: Validate processed messages and version conflict count on nodeC
        PollingIngestStats nodeCStats = client(nodeC).admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
            .getPollingIngestStats();
        assertNotNull(nodeCStats);

        assertEquals(2L, nodeCStats.getMessageProcessorStats().totalProcessedCount());
        assertEquals(1L, nodeCStats.getMessageProcessorStats().totalVersionConflictsCount());
    }

    public void testAllActiveIngestionPeriodicFlush() throws Exception {
        // Publish 10 messages
        for (int i = 1; i <= 10; i++) {
            produceData(String.valueOf(i), "name" + i, "25");
        }

        // Start nodes
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();

        // Create all-active index with 5 second periodic flush interval
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.all_active", true)
                .put("index.periodic_flush_interval", "5s")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        ensureGreen(indexName);

        waitForSearchableDocs(10, Arrays.asList(nodeA));
        waitForState(() -> getPeriodicFlushCount(nodeA, indexName) >= 1);
    }

    public void testRawPayloadMapperIngestion() throws Exception {
        // Start cluster
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();

        // Publish 2 valid messages
        String validMessage1 = "{\"name\":\"alice\",\"age\":30}";
        String validMessage2 = "{\"name\":\"bob\",\"age\":25}";
        produceData(validMessage1);
        produceData(validMessage2);

        // Create index with raw_payload mapper
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.mapper_type", "raw_payload")
                .put("ingestion_source.error_strategy", "drop")
                .put("ingestion_source.all_active", true)
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        ensureGreen(indexName);

        // Wait for both messages to be indexed
        waitForSearchableDocs(2, List.of(nodeA));

        // Verify stats show 2 processed messages
        waitForState(() -> {
            PollingIngestStats stats = client().admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
                .getPollingIngestStats();
            return stats != null
                && stats.getMessageProcessorStats().totalProcessedCount() == 2L
                && stats.getConsumerStats().totalPolledCount() == 2L
                && stats.getConsumerStats().totalPollerMessageFailureCount() == 0L
                && stats.getConsumerStats().totalPollerMessageDroppedCount() == 0L
                && stats.getMessageProcessorStats().totalInvalidMessageCount() == 0L;
        });

        // Validate document content
        SearchResponse searchResponse = client().prepareSearch(indexName).get();
        assertEquals(2, searchResponse.getHits().getHits().length);
        for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
            Map<String, Object> source = searchResponse.getHits().getHits()[i].getSourceAsMap();
            assertTrue(source.containsKey("name"));
            assertTrue(source.containsKey("age"));
        }

        // Publish invalid JSON message
        String invalidJsonMessage = "{ invalid json";
        produceData(invalidJsonMessage);

        // Wait for consumer to encounter the error and drop it
        waitForState(() -> {
            PollingIngestStats stats = client().admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
                .getPollingIngestStats();
            return stats != null
                && stats.getConsumerStats().totalPolledCount() == 3L
                && stats.getConsumerStats().totalPollerMessageFailureCount() == 1L
                && stats.getConsumerStats().totalPollerMessageDroppedCount() == 1L
                && stats.getMessageProcessorStats().totalProcessedCount() == 2L;
        });

        // Publish message with invalid content that will fail at processor level
        String invalidFieldTypeMessage = "{\"name\":123,\"age\":\"not a number\"}";
        produceData(invalidFieldTypeMessage);

        // Wait for processor to encounter the error
        waitForState(() -> {
            PollingIngestStats stats = client().admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
                .getPollingIngestStats();
            return stats != null
                && stats.getConsumerStats().totalPolledCount() == 4L
                && stats.getConsumerStats().totalPollerMessageFailureCount() == 1L
                && stats.getMessageProcessorStats().totalProcessedCount() == 3L
                && stats.getMessageProcessorStats().totalFailedCount() == 1L
                && stats.getMessageProcessorStats().totalFailuresDroppedCount() == 1L;
        });

        // Pause ingestion, reset to offset 0, and resume
        pauseIngestion(indexName);
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState(indexName);
            return ingestionState.getShardStates().length == 1
                && ingestionState.getFailedShards() == 0
                && ingestionState.getShardStates()[0].isPollerPaused()
                && ingestionState.getShardStates()[0].getPollerState().equalsIgnoreCase("paused");
        });

        // Resume with reset to offset 0 (will re-process the 2 valid messages)
        resumeIngestion(indexName, 0, ResumeIngestionRequest.ResetSettings.ResetMode.OFFSET, "0");
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState(indexName);
            return ingestionState.getShardStates().length == 1
                && ingestionState.getShardStates()[0].isPollerPaused() == false
                && (ingestionState.getShardStates()[0].getPollerState().equalsIgnoreCase("polling")
                    || ingestionState.getShardStates()[0].getPollerState().equalsIgnoreCase("processing"));
        });

        // Wait for the 3 messages to be processed by the processor after reset (1 will be dropped by the poller)
        waitForState(() -> {
            PollingIngestStats stats = client().admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
                .getPollingIngestStats();
            return stats != null && stats.getMessageProcessorStats().totalProcessedCount() == 3L;
        });

        // Verify still only 2 documents (no duplicates must be indexed)
        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
        SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
        assertThat(response.getHits().getTotalHits().value(), is(2L));
    }

    public void testDynamicUpdateKafkaParams() throws Exception {
        // Step 1: Publish 10 messages with versioning
        for (int i = 0; i < 10; i++) {
            produceDataWithExternalVersion(String.valueOf(i), 1, "name" + i, "25", defaultMessageTimestamp, "index");
        }

        // Step 2: Create index with pointer.init.reset to offset 1 and auto.offset.reset=latest
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "reset_by_offset")
                .put("ingestion_source.pointer.init.reset.value", "1")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.param.auto.offset.reset", "latest")
                .put("ingestion_source.param.max.poll.records", "100")
                .put("ingestion_source.all_active", true)
                .build(),
            mapping
        );

        ensureGreen(indexName);

        // Step 3: Wait for 9 messages to be visible on both nodes
        waitForSearchableDocs(9, Arrays.asList(nodeA, nodeB));

        // Step 4: Update the index settings to set auto.offset.reset to earliest and max.poll.records to 200
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(
                Settings.builder()
                    .put("ingestion_source.param.auto.offset.reset", "earliest")
                    .put("ingestion_source.param.max.poll.records", "200")
            )
            .get();

        // Verify the setting was updated
        String autoOffsetReset = getSettings(indexName, "index.ingestion_source.param.auto.offset.reset");
        assertEquals("earliest", autoOffsetReset);

        // Step 5: Pause and resume ingestion, setting offset to 100 (out-of-range, hence expect auto.offset.reset to be used)
        pauseIngestionAndWait(indexName, 2);
        resumeIngestionWithResetAndWait(indexName, 0, ResumeIngestionRequest.ResetSettings.ResetMode.OFFSET, "100", 2);

        // Step 6: Wait for version conflict count to be 9 and total messages processed to be 10 on both shards.
        // Since offset 100 doesn't exist, it will fall back to earliest (offset 0).
        waitForState(() -> {
            Map<String, PollingIngestStats> shardTypeToStats = getPollingIngestStatsForPrimaryAndReplica(indexName);
            PollingIngestStats primaryStats = shardTypeToStats.get("primary");
            PollingIngestStats replicaStats = shardTypeToStats.get("replica");

            return primaryStats != null
                && primaryStats.getMessageProcessorStats().totalProcessedCount() == 10L
                && primaryStats.getMessageProcessorStats().totalVersionConflictsCount() == 9L
                && replicaStats != null
                && replicaStats.getMessageProcessorStats().totalProcessedCount() == 10L
                && replicaStats.getMessageProcessorStats().totalVersionConflictsCount() == 9L;
        });

        waitForSearchableDocs(10, Arrays.asList(nodeA, nodeB));

        // Step 7: Pause ingestion
        pauseIngestionAndWait(indexName, 2);

        // Step 8: Update auto.offset.reset back to "latest". This is surrounded by pause/resume to indirectly infer
        // the config change has been applied.
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("ingestion_source.param.auto.offset.reset", "latest"))
            .get();

        // Verify the setting was updated
        autoOffsetReset = getSettings(indexName, "index.ingestion_source.param.auto.offset.reset");
        assertEquals("latest", autoOffsetReset);

        // Step 9: Verify processed count is still 10 on both shards
        Map<String, PollingIngestStats> shardTypeToStats = getPollingIngestStatsForPrimaryAndReplica(indexName);
        assertEquals(10L, shardTypeToStats.get("primary").getMessageProcessorStats().totalProcessedCount());
        assertEquals(9L, shardTypeToStats.get("primary").getMessageProcessorStats().totalVersionConflictsCount());
        assertEquals(10L, shardTypeToStats.get("replica").getMessageProcessorStats().totalProcessedCount());
        assertEquals(9L, shardTypeToStats.get("replica").getMessageProcessorStats().totalVersionConflictsCount());

        // Step 10: Resume ingestion. This does not recreate the poller as consumer is not reset.
        resumeIngestionAndWait(indexName, 2);

        // Step 11: Publish 10 more messages
        for (int i = 10; i < 20; i++) {
            produceDataWithExternalVersion(String.valueOf(i), 1, "name" + i, "25", defaultMessageTimestamp, "index");
        }

        // Step 12: Wait for processed count to be 21 and version conflict to be 10 on both shards. On updating the Kafka settings, the
        // last message (offset=9) is reprocessed resulting in additional processed message and version conflict.
        waitForState(() -> {
            Map<String, PollingIngestStats> updatedStats = getPollingIngestStatsForPrimaryAndReplica(indexName);
            PollingIngestStats primaryStats = updatedStats.get("primary");
            PollingIngestStats replicaStats = updatedStats.get("replica");

            return primaryStats != null
                && primaryStats.getMessageProcessorStats().totalProcessedCount() == 21L
                && primaryStats.getMessageProcessorStats().totalVersionConflictsCount() == 10L
                && replicaStats != null
                && replicaStats.getMessageProcessorStats().totalProcessedCount() == 21L
                && replicaStats.getMessageProcessorStats().totalVersionConflictsCount() == 10L;
        });

        waitForSearchableDocs(20, Arrays.asList(nodeA, nodeB));
    }

    public void testConsumerInitializationFailureAndRecovery() throws Exception {
        // Step 1: Create index with auto.offset.reset=none and pointer.init.reset to offset 100 (invalid offset)
        // This should cause consumer initialization to fail
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        createIndex(
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
                .put("ingestion_source.all_active", true)
                .build(),
            mapping
        );

        ensureGreen(indexName);

        // Step 2: Wait for consumer error and paused status
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState(indexName);
            PollingIngestStats stats = client(nodeA).admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
                .getPollingIngestStats();

            return ingestionState.getShardStates().length == 1
                && ingestionState.getShardStates()[0].isPollerPaused()
                && stats != null
                && stats.getConsumerStats().totalConsumerErrorCount() >= 1L;
        });

        // Step 3: Publish 10 messages
        for (int i = 0; i < 10; i++) {
            produceData(Integer.toString(i), "name" + i, "25");
        }

        // Step 4: Update auto.offset.reset to earliest
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("ingestion_source.param.auto.offset.reset", "earliest"))
            .get();

        // Verify the setting was updated
        String autoOffsetReset = getSettings(indexName, "index.ingestion_source.param.auto.offset.reset");
        assertEquals("earliest", autoOffsetReset);

        // Step 5: Resume ingestion and wait for 10 searchable docs
        resumeIngestionAndWait(indexName, 1);

        waitForSearchableDocs(10, Arrays.asList(nodeA));

        // Verify all 10 messages were processed
        PollingIngestStats stats = client(nodeA).admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
            .getPollingIngestStats();
        assertEquals(10L, stats.getMessageProcessorStats().totalProcessedCount());

        // Step 6: Update auto.offset.reset to earliest again. Consumer must not be reinitialized again as no config change.
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("ingestion_source.param.auto.offset.reset", "earliest"))
            .get();

        // Step 7: Publish 1 more message
        produceData("10", "name10", "30");

        // Step 8: Wait for 11 searchable docs
        waitForSearchableDocs(11, Arrays.asList(nodeA));

        // Step 9: Verify total processed message count is 11
        PollingIngestStats finalStats = client(nodeA).admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
            .getPollingIngestStats();
        assertEquals(11L, finalStats.getMessageProcessorStats().totalProcessedCount());
    }

    public void testDynamicConfigUpdateOnNoMessages() throws Exception {
        // Step 1: Create index with pointer.init.reset to offset 100 and auto.offset.reset to earliest
        // Since offset 100 doesn't exist, it will fall back to earliest (offset 0)
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "reset_by_offset")
                .put("ingestion_source.pointer.init.reset.value", "100")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.param.auto.offset.reset", "earliest")
                .put("ingestion_source.all_active", true)
                .build(),
            mapping
        );

        ensureGreen(indexName);

        // Step 2: Wait for poller state to be polling
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState(indexName);
            return ingestionState.getShardStates().length == 1
                && ingestionState.getShardStates()[0].getPollerState().equalsIgnoreCase("polling");
        });

        // Step 3: Pause ingestion
        pauseIngestionAndWait(indexName, 1);

        // Step 4: Update auto.offset.reset to latest. This is surrounded by pause/resume to indirectly infer the config change has been
        // applied.
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("ingestion_source.param.auto.offset.reset", "latest"))
            .get();

        // Verify the setting was updated
        String autoOffsetReset = getSettings(indexName, "index.ingestion_source.param.auto.offset.reset");
        assertEquals("latest", autoOffsetReset);

        // Step 5: Resume ingestion
        resumeIngestionAndWait(indexName, 1);

        // Step 6: Publish 1 message and wait for it to be searchable
        produceData("1", "name1", "25");

        waitForSearchableDocs(1, Arrays.asList(nodeA));

        // Verify 1 message was processed
        PollingIngestStats stats = client(nodeA).admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
            .getPollingIngestStats();
        assertEquals(1L, stats.getMessageProcessorStats().totalProcessedCount());
    }
}
