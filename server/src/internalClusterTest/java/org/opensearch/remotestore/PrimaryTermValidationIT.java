/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.coordination.FollowersChecker;
import org.opensearch.cluster.coordination.LeaderChecker;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)

public class PrimaryTermValidationIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "remote-store-test-idx-1";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    public void testPrimaryTermValidation() throws Exception {
        // Follower checker interval is lower compared to leader checker so that the cluster manager can remove the node
        // with network partition faster. The follower check retry count is also kept 1.
        Settings clusterSettings = Settings.builder()
            .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
            .put(LeaderChecker.LEADER_CHECK_INTERVAL_SETTING.getKey(), "20s")
            .put(LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), 4)
            .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
            .put(FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "1s")
            .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, REPOSITORY_2_NAME, true))
            .build();
        internalCluster().startClusterManagerOnlyNode(clusterSettings);

        // Create repository
        absolutePath = randomRepoPath().toAbsolutePath();
        assertAcked(
            clusterAdmin().preparePutRepository(REPOSITORY_NAME).setType("fs").setSettings(Settings.builder().put("location", absolutePath))
        );
        absolutePath2 = randomRepoPath().toAbsolutePath();
        putRepository(absolutePath2, REPOSITORY_2_NAME);

        // Start data nodes and create index
        internalCluster().startDataOnlyNodes(2, clusterSettings);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        // Get the names of nodes to create network disruption
        String primaryNode = primaryNodeName(INDEX_NAME);
        String replicaNode = replicaNodeName(INDEX_NAME);
        String clusterManagerNode = internalCluster().getClusterManagerName();
        logger.info("Node names : clusterManager={} primary={} replica={}", clusterManagerNode, primaryNode, replicaNode);

        // Index some docs and validate that both primary and replica node has it. Refresh is triggered to trigger segment replication
        // to ensure replica is also upto date.
        int numOfDocs = randomIntBetween(5, 10);
        for (int i = 0; i < numOfDocs; i++) {
            indexSameDoc(clusterManagerNode, INDEX_NAME);
        }
        refresh(INDEX_NAME);
        assertBusy(
            () -> assertHitCount(client(primaryNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), numOfDocs)
        );
        assertBusy(
            () -> assertHitCount(client(replicaNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), numOfDocs)
        );

        // Start network disruption - primary node will be isolated
        Set<String> nodesInOneSide = Stream.of(clusterManagerNode, replicaNode).collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInOtherSide = Stream.of(primaryNode).collect(Collectors.toCollection(HashSet::new));
        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(nodesInOneSide, nodesInOtherSide),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(networkDisruption);
        logger.info("--> network disruption is started");
        networkDisruption.startDisrupting();

        // Ensure the node which is partitioned is removed from the cluster
        assertBusy(() -> {
            NodesInfoResponse response = client(clusterManagerNode).admin().cluster().prepareNodesInfo().get();
            assertThat(response.getNodes().size(), equalTo(2));
        });

        // Ensure that the cluster manager has latest information about the index
        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = client(clusterManagerNode).admin()
                .cluster()
                .health(new ClusterHealthRequest())
                .actionGet(TimeValue.timeValueSeconds(1));
            assertTrue(clusterHealthResponse.getIndices().containsKey(INDEX_NAME));
            ClusterIndexHealth clusterIndexHealth = clusterHealthResponse.getIndices().get(INDEX_NAME);
            assertEquals(ClusterHealthStatus.YELLOW, clusterHealthResponse.getStatus());
            assertEquals(1, clusterIndexHealth.getNumberOfShards());
            assertEquals(1, clusterIndexHealth.getActiveShards());
            assertEquals(1, clusterIndexHealth.getUnassignedShards());
            assertEquals(1, clusterIndexHealth.getUnassignedShards());
            assertEquals(1, clusterIndexHealth.getActivePrimaryShards());
            assertEquals(ClusterHealthStatus.YELLOW, clusterIndexHealth.getStatus());
        });

        // Index data to the newly promoted primary
        indexSameDoc(clusterManagerNode, INDEX_NAME);
        RefreshResponse refreshResponse = client(clusterManagerNode).admin()
            .indices()
            .prepareRefresh(INDEX_NAME)
            .setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_HIDDEN_FORBID_CLOSED)
            .execute()
            .actionGet();
        assertNoFailures(refreshResponse);
        assertEquals(1, refreshResponse.getSuccessfulShards());
        assertHitCount(client(replicaNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), numOfDocs + 1);

        // At this point we stop the disruption. Since the follower checker has already failed and cluster manager has removed the node
        // from cluster, failed node needs to start discovery process by leader checker call. We stop the disruption to allow the failed
        // node to
        // communicate with the other node which it assumes has replica.
        networkDisruption.stopDisrupting();

        // When the index call is made to the stale primary, it makes the primary term validation call to the other node (which
        // it assumes has the replica node). At this moment, the stale primary realises that it is no more the primary and the caller
        // received the following exception.
        ShardNotFoundException exception = assertThrows(ShardNotFoundException.class, () -> indexSameDoc(primaryNode, INDEX_NAME));
        assertTrue(exception.getMessage().contains("no such shard"));
        ensureStableCluster(3);
        ensureGreen(INDEX_NAME);
    }

    private IndexResponse indexSameDoc(String nodeName, String indexName) {
        return client(nodeName).prepareIndex(indexName)
            .setId(UUIDs.randomBase64UUID())
            .setSource("{\"foo\" : \"bar\"}", XContentType.JSON)
            .get();
    }
}
