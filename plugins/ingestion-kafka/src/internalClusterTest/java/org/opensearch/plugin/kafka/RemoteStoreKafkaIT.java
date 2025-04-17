/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionResponse;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionResponse;
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateResponse;
import org.opensearch.action.pagination.PageParams;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Requests;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

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
        waitForSearchableDocs(2, Arrays.asList(node));
    }

    public void testPauseAndResumeIngestion() throws Exception {
        // setup nodes and index
        produceData("1", "name1", "24");
        produceData("2", "name2", "20");
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();

        createIndexWithDefaultSettings(1, 1);
        ensureGreen(indexName);
        waitForSearchableDocs(2, Arrays.asList(nodeA, nodeB));

        // pause ingestion
        PauseIngestionResponse pauseResponse = pauseIngestion(indexName);
        assertTrue(pauseResponse.isAcknowledged());
        assertTrue(pauseResponse.isShardsAcknowledged());
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState(indexName);
            return Arrays.stream(ingestionState.getShardStates())
                .allMatch(state -> state.isPollerPaused() && state.pollerState().equalsIgnoreCase("paused"));
        });

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
        assertEquals(2, getSearchableDocCount(nodeB));
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState(indexName);
            return Arrays.stream(ingestionState.getShardStates())
                .allMatch(state -> state.isPollerPaused() && state.pollerState().equalsIgnoreCase("paused"));
        });

        // resume ingestion
        ResumeIngestionResponse resumeResponse = resumeIngestion(indexName);
        assertTrue(resumeResponse.isAcknowledged());
        assertTrue(resumeResponse.isShardsAcknowledged());
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = getIngestionState(indexName);
            return Arrays.stream(ingestionState.getShardStates())
                .allMatch(
                    state -> state.isPollerPaused() == false
                        && (state.pollerState().equalsIgnoreCase("polling") || state.pollerState().equalsIgnoreCase("processing"))
                );
        });
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
        assertEquals(0, ingestionState.getShardStates()[0].shardId());
        assertEquals("POLLING", ingestionState.getShardStates()[0].pollerState());
        assertEquals("DROP", ingestionState.getShardStates()[0].errorPolicy());
        assertFalse(ingestionState.getShardStates()[0].isPollerPaused());

        GetIngestionStateResponse ingestionStateForInvalidShard = getIngestionState(new String[] { indexName }, new int[] { 1 });
        assertEquals(0, ingestionStateForInvalidShard.getTotalShards());
    }

    public void testPaginatedGetIngestionState() throws ExecutionException, InterruptedException {
        recreateKafkaTopics(5);
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        createIndexWithDefaultSettings("index1", 5, 0);
        createIndexWithDefaultSettings("index2", 5, 0);
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
            boolean shardsMatch = Set.of(0, 1, 2).contains(shardIngestionState.shardId());
            boolean indexMatch = "index1".equalsIgnoreCase(shardIngestionState.index());
            return indexMatch && shardsMatch;
        }));

        // validate page 2
        GetIngestionStateResponse responsePage2 = ingestionStateResponseList.get(1);
        assertEquals(3, responsePage2.getTotalShards());
        assertEquals(3, responsePage2.getSuccessfulShards());
        assertEquals(3, responsePage2.getShardStates().length);
        assertTrue(Arrays.stream(responsePage2.getShardStates()).allMatch(shardIngestionState -> {
            boolean matchIndex1 = Set.of(3, 4).contains(shardIngestionState.shardId())
                && "index1".equalsIgnoreCase(shardIngestionState.index());
            boolean matchIndex2 = shardIngestionState.shardId() == 0 && "index2".equalsIgnoreCase(shardIngestionState.index());
            return matchIndex1 || matchIndex2;
        }));

        // validate page 3
        GetIngestionStateResponse responsePage3 = ingestionStateResponseList.get(2);
        assertEquals(3, responsePage3.getTotalShards());
        assertEquals(3, responsePage3.getSuccessfulShards());
        assertEquals(3, responsePage3.getShardStates().length);
        assertTrue(Arrays.stream(responsePage3.getShardStates()).allMatch(shardIngestionState -> {
            boolean shardsMatch = Set.of(1, 2, 3).contains(shardIngestionState.shardId());
            boolean indexMatch = "index2".equalsIgnoreCase(shardIngestionState.index());
            return indexMatch && shardsMatch;
        }));

        // validate page 4
        GetIngestionStateResponse responsePage4 = ingestionStateResponseList.get(3);
        assertEquals(1, responsePage4.getTotalShards());
        assertEquals(1, responsePage4.getSuccessfulShards());
        assertEquals(1, responsePage4.getShardStates().length);
        assertTrue(Arrays.stream(responsePage4.getShardStates()).allMatch(shardIngestionState -> {
            boolean shardsMatch = shardIngestionState.shardId() == 4;
            boolean indexMatch = "index2".equalsIgnoreCase(shardIngestionState.index());
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

    private void verifyRemoteStoreEnabled(String node) {
        GetSettingsResponse settingsResponse = client(node).admin().indices().prepareGetSettings(indexName).get();
        String remoteStoreEnabled = settingsResponse.getIndexToSettings().get(indexName).get("index.remote_store.enabled");
        assertEquals("Remote store should be enabled", "true", remoteStoreEnabled);
    }
}
