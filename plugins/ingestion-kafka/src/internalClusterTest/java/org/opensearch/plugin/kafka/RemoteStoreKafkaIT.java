/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Requests;

import java.nio.file.Path;
import java.util.Arrays;

import static org.hamcrest.Matchers.is;

/**
 * Integration tests for segment replication with remote store using kafka as ingestion source.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
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

    private void verifyRemoteStoreEnabled(String node) {
        GetSettingsResponse settingsResponse = client(node).admin().indices().prepareGetSettings(indexName).get();
        String remoteStoreEnabled = settingsResponse.getIndexToSettings().get(indexName).get("index.remote_store.enabled");
        assertEquals("Remote store should be enabled", "true", remoteStoreEnabled);
    }
}
