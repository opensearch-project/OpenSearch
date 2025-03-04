/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.admin.indices.recovery.RecoveryRequest;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.SegmentReplicationPerGroupStats;
import org.opensearch.index.SegmentReplicationShardStats;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS;
import static org.opensearch.cluster.routing.RecoverySource.Type.EMPTY_STORE;
import static org.opensearch.cluster.routing.RecoverySource.Type.EXISTING_STORE;
import static org.opensearch.cluster.routing.allocation.decider.SearchReplicaAllocationDecider.SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchReplicaReplicationAndRecoveryIT extends SegmentReplicationBaseIT {

    private static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected Path absolutePath;

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

    @After
    public void teardown() {
        clusterAdmin().prepareCleanupRepository(REPOSITORY_NAME).get();

    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.READER_WRITER_SPLIT_EXPERIMENTAL, true).build();
    }

    public void testReplication() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String primary = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startDataOnlyNode();

        // set search only role on node
        setSearchDedicatedNodeSettings(replica);

        ensureGreen(INDEX_NAME);

        final int docCount = 10;
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh(INDEX_NAME);
        waitForSearchableDocs(docCount, primary, replica);
    }

    public void testSegmentReplicationStatsResponseWithSearchReplica() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final List<String> nodes = internalCluster().startDataOnlyNodes(2);
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .put("number_of_search_only_replicas", 1)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()
        );

        // set search only role on node
        setSearchDedicatedNodeSettings(nodes.get(0));

        ensureGreen(INDEX_NAME);

        final int docCount = 5;
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh(INDEX_NAME);
        waitForSearchableDocs(docCount, nodes);

        SegmentReplicationStatsResponse segmentReplicationStatsResponse = dataNodeClient().admin()
            .indices()
            .prepareSegmentReplicationStats(INDEX_NAME)
            .setDetailed(true)
            .execute()
            .actionGet();

        // Verify the number of indices
        assertEquals(1, segmentReplicationStatsResponse.getReplicationStats().size());
        // Verify total shards
        assertEquals(2, segmentReplicationStatsResponse.getTotalShards());
        // Verify the number of primary shards
        assertEquals(1, segmentReplicationStatsResponse.getReplicationStats().get(INDEX_NAME).size());

        SegmentReplicationPerGroupStats perGroupStats = segmentReplicationStatsResponse.getReplicationStats().get(INDEX_NAME).get(0);
        Set<SegmentReplicationShardStats> replicaStats = perGroupStats.getReplicaStats();
        // Verify the number of replica stats
        assertEquals(1, replicaStats.size());
        for (SegmentReplicationShardStats replicaStat : replicaStats) {
            assertNotNull(replicaStat.getCurrentReplicationState());
        }
    }

    public void testSearchReplicaRecovery() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String primary = internalCluster().startDataOnlyNode();
        final String replica = internalCluster().startDataOnlyNode();

        // ensure search replicas are only allocated to "replica" node.
        setSearchDedicatedNodeSettings(replica);

        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        assertRecoverySourceType(replica, EMPTY_STORE);

        final int docCount = 10;
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh(INDEX_NAME);
        flush(INDEX_NAME);
        waitForSearchableDocs(10, primary, replica);

        // Node stats should show remote download stats as nonzero, use this as a precondition to compare
        // post restart.
        assertDownloadStats(replica, true);

        internalCluster().restartNode(replica);
        ensureGreen(INDEX_NAME);
        assertDocCounts(10, replica);

        assertRecoverySourceType(replica, EXISTING_STORE);
        assertDownloadStats(replica, false);
    }

    public void testRecoveryAfterDocsIndexed() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String primary = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final int docCount = 10;
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh(INDEX_NAME);

        final String replica = internalCluster().startDataOnlyNode();

        // search node setting
        setSearchDedicatedNodeSettings(replica);

        ensureGreen(INDEX_NAME);
        assertDocCounts(10, replica);

        assertRecoverySourceType(replica, EMPTY_STORE);
        // replica should have downloaded from remote
        assertDownloadStats(replica, true);

        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 0))
            .get();

        ensureGreen(INDEX_NAME);

        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1))
            .get();
        ensureGreen(INDEX_NAME);
        assertDocCounts(10, replica);

        internalCluster().restartNode(replica);

        ensureGreen(INDEX_NAME);
        assertDocCounts(10, replica);
        assertRecoverySourceType(replica, EXISTING_STORE);
        assertDownloadStats(replica, false);
    }

    private static void assertRecoverySourceType(String replica, RecoverySource.Type recoveryType) throws InterruptedException,
        ExecutionException {
        RecoveryResponse recoveryResponse = client().admin().indices().recoveries(new RecoveryRequest(INDEX_NAME)).get();
        for (RecoveryState recoveryState : recoveryResponse.shardRecoveryStates().get(INDEX_NAME)) {
            if (recoveryState.getPrimary() == false) {
                assertEquals("All SR should be of expected recovery type", recoveryType, recoveryState.getRecoverySource().getType());
                assertEquals("All SR should be on the specified node", replica, recoveryState.getTargetNode().getName());
            }
        }
    }

    private static void assertDownloadStats(String replica, boolean expectBytesDownloaded) throws InterruptedException, ExecutionException {
        NodesStatsResponse nodesStatsResponse = client().admin().cluster().nodesStats(new NodesStatsRequest(replica)).get();
        assertEquals(1, nodesStatsResponse.getNodes().size());
        NodeStats nodeStats = nodesStatsResponse.getNodes().get(0);
        assertEquals(replica, nodeStats.getNode().getName());
        if (expectBytesDownloaded) {
            assertTrue(nodeStats.getIndices().getSegments().getRemoteSegmentStats().getDownloadBytesStarted() > 0);
        } else {
            assertEquals(0, nodeStats.getIndices().getSegments().getRemoteSegmentStats().getDownloadBytesStarted());
        }
    }

    public void testStopPrimary_RestoreOnNewNode() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String primary = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final int docCount = 10;
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh(INDEX_NAME);
        assertDocCounts(docCount, primary);

        final String replica = internalCluster().startDataOnlyNode();

        // search node setting
        setSearchDedicatedNodeSettings(replica);

        ensureGreen(INDEX_NAME);
        assertDocCounts(docCount, replica);
        // stop the primary
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));

        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = clusterAdmin().prepareHealth(INDEX_NAME).get();
            assertEquals(ClusterHealthStatus.RED, clusterHealthResponse.getStatus());
        });
        assertDocCounts(docCount, replica);

        String restoredPrimary = internalCluster().startDataOnlyNode();

        client().admin().cluster().restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME), PlainActionFuture.newFuture());
        ensureGreen(INDEX_NAME);
        assertDocCounts(docCount, replica, restoredPrimary);

        for (int i = docCount; i < docCount * 2; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh(INDEX_NAME);
        assertBusy(() -> assertDocCounts(20, replica, restoredPrimary));
    }

    public void testFailoverToNewPrimaryWithPollingReplication() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String primary = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final int docCount = 10;
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh(INDEX_NAME);

        final String replica = internalCluster().startDataOnlyNode();

        // search node setting
        setSearchDedicatedNodeSettings(replica);

        ensureGreen(INDEX_NAME);
        assertDocCounts(10, replica);

        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, 1))
            .get();
        final String writer_replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        // stop the primary
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));

        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = clusterAdmin().prepareHealth(INDEX_NAME).get();
            assertEquals(ClusterHealthStatus.YELLOW, clusterHealthResponse.getStatus());
        });
        ClusterHealthResponse clusterHealthResponse = clusterAdmin().prepareHealth(INDEX_NAME).get();
        assertEquals(ClusterHealthStatus.YELLOW, clusterHealthResponse.getStatus());
        assertDocCounts(10, replica);

        for (int i = docCount; i < docCount * 2; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh(INDEX_NAME);
        assertBusy(() -> assertDocCounts(20, replica, writer_replica));
    }

    private void setSearchDedicatedNodeSettings(String nodeName) {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name", nodeName))
            .execute()
            .actionGet();
    }
}
