/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionRequest;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionResponse;
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateResponse;
import org.opensearch.action.pagination.PageParams;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Requests;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for segment replication with remote store using kafka as ingestion source.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
@ThreadLeakFilters(filters = TestContainerThreadLeakFilter.class)
public class RemoteStoreKafkaIT extends KafkaIngestionBaseIT {
    private static final String REPOSITORY_NAME = "test-remote-store-repo";
    private Path absolutePath;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (absolutePath == null) {
            absolutePath = randomRepoPath().toAbsolutePath();
        }
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, absolutePath))
            .build();
    }

    public void testSegmentReplicationWithRemoteStore() throws Exception {
        // Step 1: Create primary and replica nodes. Create index with 1 replica and kafka as ingestion source.

        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        createIndexWithDefaultSettings(1, 1);
        ensureYellowAndNoInitializingShards(indexName);
        final String nodeB = internalCluster().startDataOnlyNode();
        ensureGreen(indexName);
        assertTrue(nodeA.equals(primaryNodeName(indexName)));
        assertTrue(nodeB.equals(replicaNodeName(indexName)));
        verifyRemoteStoreEnabled(nodeA);
        verifyRemoteStoreEnabled(nodeB);

        // Step 2: Produce update messages and validate segment replication

        produceData("1", "name1", "24");
        produceData("2", "name2", "20");
        refresh(indexName);
        waitForSearchableDocs(2, Arrays.asList(nodeA, nodeB));

        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(21);
        SearchResponse primaryResponse = client(nodeA).prepareSearch(indexName).setQuery(query).setPreference("_only_local").get();
        assertThat(primaryResponse.getHits().getTotalHits().value(), is(1L));
        SearchResponse replicaResponse = client(nodeB).prepareSearch(indexName).setQuery(query).setPreference("_only_local").get();
        assertThat(replicaResponse.getHits().getTotalHits().value(), is(1L));

        // Step 3: Stop current primary node and validate replica promotion.

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeA));
        ensureYellowAndNoInitializingShards(indexName);
        assertTrue(nodeB.equals(primaryNodeName(indexName)));

        // Step 4: Verify new primary node is able to index documents

        produceData("3", "name3", "30");
        produceData("4", "name4", "31");
        refresh(indexName);
        waitForSearchableDocs(4, Arrays.asList(nodeB));

        SearchResponse newPrimaryResponse = client(nodeB).prepareSearch(indexName).setQuery(query).setPreference("_only_local").get();
        assertThat(newPrimaryResponse.getHits().getTotalHits().value(), is(3L));

        // Step 5: Add a new node and assign the replica shard. Verify node recovery works.

        final String nodeC = internalCluster().startDataOnlyNode();
        client().admin().cluster().prepareReroute().add(new AllocateReplicaAllocationCommand(indexName, 0, nodeC)).get();
        ensureGreen(indexName);
        assertTrue(nodeC.equals(replicaNodeName(indexName)));
        verifyRemoteStoreEnabled(nodeC);

        waitForSearchableDocs(4, Arrays.asList(nodeC));
        SearchResponse newReplicaResponse = client(nodeC).prepareSearch(indexName).setQuery(query).setPreference("_only_local").get();
        assertThat(newReplicaResponse.getHits().getTotalHits().value(), is(3L));

        // Step 6: Produce new updates and verify segment replication works when primary and replica index are not empty.
        produceData("5", "name5", "40");
        produceData("6", "name6", "41");
        refresh(indexName);
        waitForSearchableDocs(6, Arrays.asList(nodeB, nodeC));
    }

    public void testCloseIndex() throws Exception {
        produceData("1", "name1", "24");
        produceData("2", "name2", "20");
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();

        createIndexWithDefaultSettings(1, 1);
        ensureGreen(indexName);
        waitForSearchableDocs(2, Arrays.asList(nodeA, nodeB));
        client().admin().indices().close(Requests.closeIndexRequest(indexName)).get();
    }

    public void testErrorStrategy() throws Exception {
        produceData("1", "name1", "25");
        // malformed message
        produceData("2", "", "");
        produceData("3", "name3", "25");
        produceData("{\"_op_type\":\"invalid\",\"_source\":{\"name\":\"name4\", \"age\": 25}}");
        produceData("5", "name5", "25");

        internalCluster().startClusterManagerOnlyNode();
        final String node = internalCluster().startDataOnlyNode();

        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.error_strategy", "block")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.internal_queue_size", "1000")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("index.replication.type", "SEGMENT")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        ensureGreen(indexName);
        waitForState(() -> "block".equalsIgnoreCase(getSettings(indexName, "index.ingestion_source.error_strategy")));
        waitForSearchableDocs(1, Arrays.asList(node));

        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("ingestion_source.error_strategy", "drop"))
            .get();
        waitForState(() -> "drop".equalsIgnoreCase(getSettings(indexName, "index.ingestion_source.error_strategy")));
        resumeIngestion(indexName);
        waitForSearchableDocs(3, Arrays.asList(node));

        PollingIngestStats stats = client().admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
            .getPollingIngestStats();
        assertNotNull(stats);
        assertThat(stats.getMessageProcessorStats().totalFailedCount(), is(1L));
        assertThat(stats.getMessageProcessorStats().totalFailuresDroppedCount(), is(1L));
        assertThat(stats.getConsumerStats().totalConsumerErrorCount(), is(0L));
        assertThat(stats.getConsumerStats().totalPollerMessageDroppedCount(), is(1L));
    }

    public void testPauseAndResumeIngestion() throws Exception {
        // setup nodes and index
        produceData("1", "name1", "24");
        produceData("2", "name2", "20");
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();

        createIndexWithDefaultSettings(1, 1);
        ensureYellowAndNoInitializingShards(indexName);
        waitForSearchableDocs(2, Arrays.asList(nodeA));
        final String nodeB = internalCluster().startDataOnlyNode();
        ensureGreen(indexName);
        assertTrue(nodeA.equals(primaryNodeName(indexName)));

        // pause ingestion
        pauseIngestionAndWait(indexName, 1);

        // verify ingestion state is persisted
        produceData("3", "name3", "30");
        produceData("4", "name4", "31");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeA));
        ensureYellowAndNoInitializingShards(indexName);
        assertTrue(nodeB.equals(primaryNodeName(indexName)));

        final String nodeC = internalCluster().startDataOnlyNode();
        client().admin().cluster().prepareReroute().add(new AllocateReplicaAllocationCommand(indexName, 0, nodeC)).get();
        ensureGreen(indexName);
        assertTrue(nodeC.equals(replicaNodeName(indexName)));
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState(indexName);
            return ingestionState.getFailedShards() == 0
                && Arrays.stream(ingestionState.getShardStates())
                    .allMatch(state -> state.isPollerPaused() && state.getPollerState().equalsIgnoreCase("paused"));
        });
        assertEquals(2, getSearchableDocCount(nodeB));

        // resume ingestion
        resumeIngestionAndWait(indexName, 1);
        waitForSearchableDocs(4, Arrays.asList(nodeB, nodeC));
    }

    public void testDefaultGetIngestionState() throws ExecutionException, InterruptedException {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        createIndexWithDefaultSettings(1, 1);
        ensureGreen(indexName);

        GetIngestionStateResponse ingestionState = getIngestionState(new String[] { indexName }, new int[] { 0 });
        assertEquals(0, ingestionState.getFailedShards());
        assertEquals(1, ingestionState.getSuccessfulShards());
        assertEquals(1, ingestionState.getTotalShards());
        assertEquals(1, ingestionState.getShardStates().length);
        assertEquals(0, ingestionState.getShardStates()[0].getShardId());
        assertEquals("POLLING", ingestionState.getShardStates()[0].getPollerState());
        assertEquals("DROP", ingestionState.getShardStates()[0].getErrorPolicy());
        assertFalse(ingestionState.getShardStates()[0].isPollerPaused());

        GetIngestionStateResponse ingestionStateForInvalidShard = getIngestionState(new String[] { indexName }, new int[] { 1 });
        assertEquals(0, ingestionStateForInvalidShard.getTotalShards());
    }

    public void testPaginatedGetIngestionState() throws ExecutionException, InterruptedException {
        recreateKafkaTopics(5);
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        createIndexWithDefaultSettings("index1", 5, 0, 1);
        createIndexWithDefaultSettings("index2", 5, 0, 1);
        ensureGreen("index1");
        ensureGreen("index2");

        List<GetIngestionStateResponse> ingestionStateResponseList = new ArrayList<>();
        GetIngestionStateResponse ingestionStatePage = null;
        while (ingestionStatePage == null || ingestionStatePage.getNextPageToken() != null) {
            String nextToken = ingestionStatePage == null ? null : ingestionStatePage.getNextPageToken();
            PageParams pageParams = new PageParams(nextToken, "asc", 3);
            ingestionStatePage = getIngestionState(new String[] { "index1", "index2" }, new int[] { 0, 1, 2, 3, 4 }, pageParams);
            ingestionStateResponseList.add(ingestionStatePage);
        }

        // we have 2 index, each with 5 shards, total of 10 shards
        // for page size of 3, we expect 4 pages in total
        assertEquals(4, ingestionStateResponseList.size());

        // validate page 1
        GetIngestionStateResponse responsePage1 = ingestionStateResponseList.get(0);
        assertEquals(3, responsePage1.getTotalShards());
        assertEquals(3, responsePage1.getSuccessfulShards());
        assertEquals(3, responsePage1.getShardStates().length);
        assertTrue(Arrays.stream(responsePage1.getShardStates()).allMatch(shardIngestionState -> {
            boolean shardsMatch = Set.of(0, 1, 2).contains(shardIngestionState.getShardId());
            boolean indexMatch = "index1".equalsIgnoreCase(shardIngestionState.getIndex());
            return indexMatch && shardsMatch;
        }));

        // validate page 2
        GetIngestionStateResponse responsePage2 = ingestionStateResponseList.get(1);
        assertEquals(3, responsePage2.getTotalShards());
        assertEquals(3, responsePage2.getSuccessfulShards());
        assertEquals(3, responsePage2.getShardStates().length);
        assertTrue(Arrays.stream(responsePage2.getShardStates()).allMatch(shardIngestionState -> {
            boolean matchIndex1 = Set.of(3, 4).contains(shardIngestionState.getShardId())
                && "index1".equalsIgnoreCase(shardIngestionState.getIndex());
            boolean matchIndex2 = shardIngestionState.getShardId() == 0 && "index2".equalsIgnoreCase(shardIngestionState.getIndex());
            return matchIndex1 || matchIndex2;
        }));

        // validate page 3
        GetIngestionStateResponse responsePage3 = ingestionStateResponseList.get(2);
        assertEquals(3, responsePage3.getTotalShards());
        assertEquals(3, responsePage3.getSuccessfulShards());
        assertEquals(3, responsePage3.getShardStates().length);
        assertTrue(Arrays.stream(responsePage3.getShardStates()).allMatch(shardIngestionState -> {
            boolean shardsMatch = Set.of(1, 2, 3).contains(shardIngestionState.getShardId());
            boolean indexMatch = "index2".equalsIgnoreCase(shardIngestionState.getIndex());
            return indexMatch && shardsMatch;
        }));

        // validate page 4
        GetIngestionStateResponse responsePage4 = ingestionStateResponseList.get(3);
        assertEquals(1, responsePage4.getTotalShards());
        assertEquals(1, responsePage4.getSuccessfulShards());
        assertEquals(1, responsePage4.getShardStates().length);
        assertTrue(Arrays.stream(responsePage4.getShardStates()).allMatch(shardIngestionState -> {
            boolean shardsMatch = shardIngestionState.getShardId() == 4;
            boolean indexMatch = "index2".equalsIgnoreCase(shardIngestionState.getIndex());
            return indexMatch && shardsMatch;
        }));
    }

    public void testExternalVersioning() throws Exception {
        // setup nodes and index
        produceDataWithExternalVersion("1", 1, "name1", "25", defaultMessageTimestamp, "index");
        produceDataWithExternalVersion("2", 1, "name2", "25", defaultMessageTimestamp, "index");
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();

        createIndexWithDefaultSettings(1, 1);
        ensureGreen(indexName);
        waitForSearchableDocs(2, Arrays.asList(nodeA, nodeB));

        // validate next version docs get indexed
        produceDataWithExternalVersion("1", 2, "name1", "30", defaultMessageTimestamp, "index");
        produceDataWithExternalVersion("2", 2, "name2", "30", defaultMessageTimestamp, "index");
        waitForState(() -> {
            BoolQueryBuilder query1 = new BoolQueryBuilder().must(new TermQueryBuilder("_id", 1));
            SearchResponse response1 = client().prepareSearch(indexName).setQuery(query1).get();
            assertThat(response1.getHits().getTotalHits().value(), is(1L));
            BoolQueryBuilder query2 = new BoolQueryBuilder().must(new TermQueryBuilder("_id", 2));
            SearchResponse response2 = client().prepareSearch(indexName).setQuery(query2).get();
            assertThat(response2.getHits().getTotalHits().value(), is(1L));
            return 30 == (Integer) response1.getHits().getHits()[0].getSourceAsMap().get("age")
                && 30 == (Integer) response2.getHits().getHits()[0].getSourceAsMap().get("age");
        });

        // test out-of-order updates
        produceDataWithExternalVersion("1", 1, "name1", "25", defaultMessageTimestamp, "index");
        produceDataWithExternalVersion("2", 1, "name2", "25", defaultMessageTimestamp, "index");
        produceDataWithExternalVersion("3", 1, "name3", "25", defaultMessageTimestamp, "index");
        waitForSearchableDocs(3, Arrays.asList(nodeA, nodeB));

        BoolQueryBuilder query1 = new BoolQueryBuilder().must(new TermQueryBuilder("_id", 1));
        SearchResponse response1 = client().prepareSearch(indexName).setQuery(query1).get();
        assertThat(response1.getHits().getTotalHits().value(), is(1L));
        assertEquals(30, response1.getHits().getHits()[0].getSourceAsMap().get("age"));

        BoolQueryBuilder query2 = new BoolQueryBuilder().must(new TermQueryBuilder("_id", 2));
        SearchResponse response2 = client().prepareSearch(indexName).setQuery(query2).get();
        assertThat(response2.getHits().getTotalHits().value(), is(1L));
        assertEquals(30, response2.getHits().getHits()[0].getSourceAsMap().get("age"));

        // test deletes with smaller version
        produceDataWithExternalVersion("1", 1, "name1", "25", defaultMessageTimestamp, "delete");
        produceDataWithExternalVersion("4", 1, "name4", "25", defaultMessageTimestamp, "index");
        waitForSearchableDocs(4, Arrays.asList(nodeA, nodeB));
        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(23);
        SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
        assertThat(response.getHits().getTotalHits().value(), is(4L));

        // test deletes with correct version
        produceDataWithExternalVersion("1", 3, "name1", "30", defaultMessageTimestamp, "delete");
        produceDataWithExternalVersion("2", 3, "name2", "30", defaultMessageTimestamp, "delete");
        waitForState(() -> {
            RangeQueryBuilder rangeQuery = new RangeQueryBuilder("age").gte(23);
            SearchResponse rangeQueryResponse = client().prepareSearch(indexName).setQuery(rangeQuery).get();
            assertThat(rangeQueryResponse.getHits().getTotalHits().value(), is(2L));
            return true;
        });

        // validate processor stats
        PollingIngestStats stats = client().admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
            .getPollingIngestStats();
        assertNotNull(stats);
        assertThat(stats.getMessageProcessorStats().totalProcessedCount(), is(11L));
        assertThat(stats.getMessageProcessorStats().totalVersionConflictsCount(), is(3L));
    }

    public void testExternalVersioningWithDisabledGCDeletes() throws Exception {
        // setup nodes and index
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();

        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("index.replication.type", "SEGMENT")
                .put("index.gc_deletes", "0")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        // insert documents
        produceDataWithExternalVersion("1", 1, "name1", "25", defaultMessageTimestamp, "index");
        produceDataWithExternalVersion("2", 1, "name2", "25", defaultMessageTimestamp, "index");
        waitForState(() -> {
            RangeQueryBuilder rangeQuery = new RangeQueryBuilder("age").gte(23);
            SearchResponse rangeQueryResponse = client().prepareSearch(indexName).setQuery(rangeQuery).get();
            assertThat(rangeQueryResponse.getHits().getTotalHits().value(), is(2L));
            return true;
        });

        // delete documents 1 and 2
        produceDataWithExternalVersion("1", 2, "name1", "25", defaultMessageTimestamp, "delete");
        produceDataWithExternalVersion("2", 2, "name2", "25", defaultMessageTimestamp, "delete");
        produceDataWithExternalVersion("3", 1, "name3", "25", defaultMessageTimestamp, "index");
        waitForState(() -> {
            BoolQueryBuilder query = new BoolQueryBuilder().must(new TermQueryBuilder("_id", 3));
            SearchResponse response = client().prepareSearch(indexName).setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            return 25 == (Integer) response.getHits().getHits()[0].getSourceAsMap().get("age");
        });
        waitForSearchableDocs(1, Arrays.asList(nodeA, nodeB));

        // validate index operation with lower version creates new document
        produceDataWithExternalVersion("1", 1, "name1", "35", defaultMessageTimestamp, "index");
        produceDataWithExternalVersion("4", 1, "name4", "35", defaultMessageTimestamp, "index");
        waitForState(() -> {
            RangeQueryBuilder rangeQuery = new RangeQueryBuilder("age").gte(34);
            SearchResponse rangeQueryResponse = client().prepareSearch(indexName).setQuery(rangeQuery).get();
            assertThat(rangeQueryResponse.getHits().getTotalHits().value(), is(2L));
            return true;
        });

    }

    public void testClusterWriteBlock() throws Exception {
        // setup nodes and index
        produceData("1", "name1", "24");
        produceData("2", "name2", "20");
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();

        createIndexWithDefaultSettings(1, 1);
        ensureGreen(indexName);
        waitForSearchableDocs(2, Arrays.asList(nodeA, nodeB));

        // create a write block
        setWriteBlock(indexName, true);
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState(indexName);
            return ingestionState.getFailedShards() == 0
                && Arrays.stream(ingestionState.getShardStates())
                    .allMatch(state -> state.isWriteBlockEnabled() && state.getPollerState().equalsIgnoreCase("paused"));
        });

        // verify write block state in poller is persisted
        produceData("3", "name3", "30");
        produceData("4", "name4", "31");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeA));
        ensureYellowAndNoInitializingShards(indexName);
        assertTrue(nodeB.equals(primaryNodeName(indexName)));

        final String nodeC = internalCluster().startDataOnlyNode();
        client().admin().cluster().prepareReroute().add(new AllocateReplicaAllocationCommand(indexName, 0, nodeC)).get();
        ensureGreen(indexName);
        assertTrue(nodeC.equals(replicaNodeName(indexName)));
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState(indexName);
            return Arrays.stream(ingestionState.getShardStates())
                .allMatch(state -> state.isWriteBlockEnabled() && state.getPollerState().equalsIgnoreCase("paused"));
        });
        assertEquals(2, getSearchableDocCount(nodeB));

        // remove write block
        setWriteBlock(indexName, false);
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState(indexName);
            return ingestionState.getFailedShards() == 0
                && Arrays.stream(ingestionState.getShardStates()).allMatch(state -> state.isWriteBlockEnabled() == false);
        });
        waitForSearchableDocs(4, Arrays.asList(nodeB, nodeC));
    }

    public void testOffsetUpdateOnBlockErrorPolicy() throws Exception {
        // setup nodes and index using block strategy
        // produce one invalid message to block the processor
        produceData("1", "name1", "21");
        produceData("{\"_op_type\":\"invalid\",\"_source\":{\"name\":\"name4\", \"age\": 25}}");
        produceData("2", "name2", "22");
        produceData("3", "name3", "24");
        produceData("4", "name4", "24");
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();

        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.error_strategy", "block")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.internal_queue_size", "1000")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("index.replication.type", "SEGMENT")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        ensureGreen(indexName);
        // expect only 1 document to be successfully indexed
        waitForSearchableDocs(1, Arrays.asList(nodeA, nodeB));

        // pause ingestion
        pauseIngestionAndWait(indexName, 1);
        // revalidate that only 1 document is visible
        waitForSearchableDocs(1, Arrays.asList(nodeA, nodeB));

        // update offset to skip past the invalid message
        resumeIngestionWithResetAndWait(indexName, 0, ResumeIngestionRequest.ResetSettings.ResetMode.OFFSET, "2", 1);

        // validate remaining messages are successfully indexed
        waitForSearchableDocs(4, Arrays.asList(nodeA, nodeB));
        PollingIngestStats stats = client().admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
            .getPollingIngestStats();
        assertThat(stats.getConsumerStats().totalPolledCount(), is(3L));
        assertThat(stats.getConsumerStats().totalPollerMessageFailureCount(), is(0L));
    }

    public void testConsumerResetByTimestamp() throws Exception {
        produceData("1", "name1", "21", 100, "index");
        produceData("2", "name2", "22", 105, "index");
        produceData("3", "name3", "24", 110, "index");
        produceData("4", "name4", "24", 120, "index");
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();

        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.error_strategy", "drop")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.internal_queue_size", "1000")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("index.replication.type", "SEGMENT")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        ensureGreen(indexName);
        waitForSearchableDocs(4, Arrays.asList(nodeA, nodeB));

        // expect error response since ingestion not yet paused
        ResumeIngestionResponse resumeResponse = resumeIngestion(
            indexName,
            0,
            ResumeIngestionRequest.ResetSettings.ResetMode.TIMESTAMP,
            "100"
        );
        assertTrue(resumeResponse.isAcknowledged());
        assertFalse(resumeResponse.isShardsAcknowledged());
        assertEquals(1, resumeResponse.getShardFailures().length);

        // pause ingestion
        pauseIngestionAndWait(indexName, 1);

        // reset consumer by a timestamp after first message was produced
        resumeIngestionWithResetAndWait(indexName, 0, ResumeIngestionRequest.ResetSettings.ResetMode.TIMESTAMP, "102", 1);

        waitForSearchableDocs(4, Arrays.asList(nodeA, nodeB));
        waitForState(() -> {
            PollingIngestStats stats = client().admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
                .getPollingIngestStats();
            return stats.getConsumerStats().totalPolledCount() == 3;
        });
    }

    public void testRemoteSnapshotRestore() throws Exception {
        String snapshotRepositoryName = "test-snapshot-repo";
        String snapshotName = "snapshot-1";

        // Step 1: Setup index and data
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();
        createIndexWithDefaultSettings(1, 1);
        ensureGreen(indexName);
        produceData("1", "name1", "24");
        produceData("2", "name2", "20");
        refresh(indexName);
        waitForSearchableDocs(2, Arrays.asList(nodeA, nodeB));

        // Step 2: Register snapshot repository
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(snapshotRepositoryName)
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath()).put("compress", false))
        );

        // Step 3: Take snapshot
        flush(indexName);
        CreateSnapshotResponse snapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepositoryName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertTrue(snapshotResponse.getSnapshotInfo().successfulShards() > 0);

        // validate total polled count is 2
        PollingIngestStats stats1 = client().admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
            .getPollingIngestStats();
        assertEquals(2, stats1.getConsumerStats().totalPolledCount());

        // Step 4: Delete Index
        assertAcked(client().admin().indices().prepareDelete(indexName));
        waitForState(() -> {
            ClusterState state = client().admin().cluster().prepareState().setIndices(indexName).get().getState();
            return state.getRoutingTable().hasIndex(indexName) == false && state.getMetadata().hasIndex(indexName) == false;
        });
        produceData("3", "name3", "24");
        produceData("4", "name4", "20");

        // Step 5: Restore Index from Snapshot
        client().admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepositoryName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        ensureGreen(indexName);

        // Step 6: Verify index is restored and resumes ingestion from the 3rd message
        refresh(indexName);
        waitForSearchableDocs(4, Arrays.asList(nodeA, nodeB));
        assertHitCount(client().prepareSearch(indexName).get(), 4);

        // after index is restored, it should resume ingestion from batchStartPointer available in the latest commit
        PollingIngestStats stats2 = client().admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
            .getPollingIngestStats();
        assertEquals(3, stats2.getConsumerStats().totalPolledCount());
        assertEquals(0, stats2.getMessageProcessorStats().totalVersionConflictsCount());
    }

    public void testIndexRelocation() throws Exception {
        // Step 1: Create 2 nodes
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();

        // Step 2: Create index on nodeA
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.param.auto.offset.reset", "earliest")
                .put("index.routing.allocation.require._name", nodeA)
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );
        ensureGreen(indexName);
        assertTrue(nodeA.equals(primaryNodeName(indexName)));

        // Step 3: Write documents and verify
        produceData("1", "name1", "24");
        produceData("2", "name2", "20");
        refresh(indexName);
        waitForSearchableDocs(2, List.of(nodeA));
        flush(indexName);

        // Step 4: Relocate index to nodeB
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put("index.routing.allocation.require._name", nodeB))
                .get()
        );

        // Step 5: Wait for relocation to complete
        waitForState(() -> nodeB.equals(primaryNodeName(indexName)));

        // Step 6: Ensure index is searchable on nodeB and can resume ingestion
        ensureGreen(indexName);
        refresh(indexName);
        waitForSearchableDocs(2, List.of(nodeB));
        produceData("3", "name3", "24");
        produceData("4", "name4", "20");
        waitForSearchableDocs(4, List.of(nodeB));
    }

    public void testKafkaConnectionLost() throws Exception {
        // Step 1: Create 2 nodes
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();

        // Step 2: Create index
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("ingestion_source.param.auto.offset.reset", "earliest")
                .put("index.routing.allocation.require._name", nodeA)
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );
        ensureGreen(indexName);
        assertTrue(nodeA.equals(primaryNodeName(indexName)));

        // Step 3: Write documents and verify
        produceData("1", "name1", "24");
        produceData("2", "name2", "20");
        refresh(indexName);
        waitForSearchableDocs(2, List.of(nodeA));
        flush(indexName);

        // Step 4: Stop kafka and relocate index to nodeB
        kafka.stop();
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put("index.routing.allocation.require._name", nodeB))
                .get()
        );

        // Step 5: Wait for relocation to complete
        waitForState(() -> nodeB.equals(primaryNodeName(indexName)));

        // Step 6: Ensure index is searchable on nodeB even though kafka is down
        ensureGreen(indexName);
        waitForSearchableDocs(2, List.of(nodeB));
        waitForState(() -> {
            PollingIngestStats stats = client().admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
                .getPollingIngestStats();
            return stats.getConsumerStats().totalConsumerErrorCount() > 0;
        });
    }

    private void verifyRemoteStoreEnabled(String node) {
        GetSettingsResponse settingsResponse = client(node).admin().indices().prepareGetSettings(indexName).get();
        String remoteStoreEnabled = settingsResponse.getIndexToSettings().get(indexName).get("index.remote_store.enabled");
        assertEquals("Remote store should be enabled", "true", remoteStoreEnabled);
    }

    public void testBatchStartPointerOnReplicaPromotion() throws Exception {
        // Step 1: Publish 10 messages
        for (int i = 1; i <= 10; i++) {
            produceDataWithExternalVersion(String.valueOf(i), 1, "name" + i, "25", defaultMessageTimestamp, "index");
        }

        // Step 2: Start nodes
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();

        // Step 3: Create index with 1 replica
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("index.replication.type", "SEGMENT")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        ensureYellowAndNoInitializingShards(indexName);

        // Step 4: Add second node and verify green status
        final String nodeB = internalCluster().startDataOnlyNode();
        ensureGreen(indexName);

        // Step 5: Verify nodeA has the primary shard
        assertTrue(nodeA.equals(primaryNodeName(indexName)));
        assertTrue(nodeB.equals(replicaNodeName(indexName)));
        verifyRemoteStoreEnabled(nodeA);
        verifyRemoteStoreEnabled(nodeB);

        // Step 6: Wait for 10 messages to be searchable on both nodes
        waitForSearchableDocs(10, Arrays.asList(nodeA, nodeB));

        // Step 7: Flush to persist data
        flush(indexName);

        // Step 8: Bring down nodeA (primary) and wait for nodeB to become primary
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeA));
        ensureYellowAndNoInitializingShards(indexName);
        assertTrue(nodeB.equals(primaryNodeName(indexName)));

        // Step 9: Publish 1 new message
        produceDataWithExternalVersion("11", 1, "name11", "25", defaultMessageTimestamp, "index");

        // Step 10: Wait for 11 messages to be visible on nodeB
        waitForSearchableDocs(11, Arrays.asList(nodeB));

        // Step 11: Validate version conflict count is exactly 1
        PollingIngestStats finalStats = client(nodeB).admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
            .getPollingIngestStats();
        assertNotNull(finalStats);

        assertEquals(1L, finalStats.getMessageProcessorStats().totalVersionConflictsCount());
        assertEquals(2L, finalStats.getMessageProcessorStats().totalProcessedCount());
    }

    public void testPeriodicFlush() throws Exception {
        // Publish 10 messages
        for (int i = 1; i <= 10; i++) {
            produceData(String.valueOf(i), "name" + i, "25");
        }

        // Start nodes
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();

        // Create index with 5 second periodic flush interval
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("index.replication.type", "SEGMENT")
                .put("index.periodic_flush_interval", "5s")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}"
        );

        ensureGreen(indexName);
        verifyRemoteStoreEnabled(nodeA);

        waitForSearchableDocs(10, Arrays.asList(nodeA));
        waitForState(() -> getPeriodicFlushCount(nodeA, indexName) >= 1);
    }
}
