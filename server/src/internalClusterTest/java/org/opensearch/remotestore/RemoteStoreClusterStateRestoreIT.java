/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreClusterStateRestoreIT extends BaseRemoteStoreRestoreIT {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true).build();
    }

    private void addNewNodes(int dataNodeCount, int clusterManagerNodeCount) {
        internalCluster().startNodes(dataNodeCount + clusterManagerNodeCount);
    }

    private Map<String, Long> initialTestSetup(int shardCount, int replicaCount, int dataNodeCount, int clusterManagerNodeCount) {
        prepareCluster(clusterManagerNodeCount, dataNodeCount, INDEX_NAME, replicaCount, shardCount);
        Map<String, Long> indexStats = indexData(1, false, INDEX_NAME);
        assertEquals(shardCount * (replicaCount + 1), getNumShards(INDEX_NAME).totalNumShards);
        ensureGreen(INDEX_NAME);
        return indexStats;
    }

    private void resetCluster(int dataNodeCount, int clusterManagerNodeCount) {
        internalCluster().stopAllNodes();
        internalCluster().startClusterManagerOnlyNodes(clusterManagerNodeCount);
        internalCluster().startDataOnlyNodes(dataNodeCount);
    }

    @Override
    protected void verifyRestoredData(Map<String, Long> indexStats, String indexName) throws Exception {
        ensureRed(indexName);
        restore(false, indexName);
        super.verifyRestoredData(indexStats, indexName);
    }

    public void testFullClusterRestore() throws Exception {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        // Step - 1 index some data to generate files in remote directory
        Map<String, Long> indexStats = initialTestSetup(shardCount, replicaCount, dataNodeCount, 1);
        String prevClusterUUID = clusterService().state().metadata().clusterUUID();

        // Step - 2 Replace all nodes in the cluster with new nodes. This ensures new cluster state doesn't have previous index metadata
        resetCluster(dataNodeCount, clusterManagerNodeCount);

        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        // Step - 3 Trigger full cluster restore and validate
        validateMetadata(List.of(INDEX_NAME));
        verifyRestoredData(indexStats, INDEX_NAME);
    }

    public void testFullClusterRestoreMultipleIndices() throws Exception {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        // Step - 1 index some data to generate files in remote directory
        Map<String, Long> indexStats = initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);

        String secondIndexName = INDEX_NAME + "-2";
        createIndex(secondIndexName, remoteStoreIndexSettings(replicaCount, shardCount + 1));
        Map<String, Long> indexStats2 = indexData(1, false, secondIndexName);
        assertEquals((shardCount + 1) * (replicaCount + 1), getNumShards(secondIndexName).totalNumShards);
        ensureGreen(secondIndexName);

        String prevClusterUUID = clusterService().state().metadata().clusterUUID();

        // Step - 2 Replace all nodes in the cluster with new nodes. This ensures new cluster state doesn't have previous index metadata
        resetCluster(dataNodeCount, clusterManagerNodeCount);

        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        // Step - 3 Trigger full cluster restore
        validateMetadata(List.of(INDEX_NAME, secondIndexName));
        verifyRestoredData(indexStats, INDEX_NAME);
    }

    public void testFullClusterRestoreManifestFilePointsToInvalidIndexMetadataPathThrowsException() throws Exception {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        // Step - 1 index some data to generate files in remote directory
        initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);

        String prevClusterUUID = clusterService().state().metadata().clusterUUID();
        String clusterName = clusterService().state().getClusterName().value();

        // Step - 2 Replace all nodes in the cluster with new nodes. This ensures new cluster state doesn't have previous index metadata
        internalCluster().stopAllNodes();
        // Step - 3 Delete index metadata file in remote
        try {
            Files.move(
                segmentRepoPath.resolve(
                    RemoteClusterStateService.encodeString(clusterName) + "/cluster-state/" + prevClusterUUID + "/index"
                ),
                segmentRepoPath.resolve("cluster-state/")
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        assertThrows(IllegalStateException.class, () -> addNewNodes(dataNodeCount, clusterManagerNodeCount));
        // Test is complete

        // Starting a node without remote state to ensure test cleanup
        internalCluster().startNode(Settings.builder().put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), false).build());
    }

    public void testRemoteStateFullRestart() throws Exception {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 3;

        Map<String, Long> indexStats = initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);
        String prevClusterUUID = clusterService().state().metadata().clusterUUID();
        // Delete index metadata file in remote
        try {
            Files.move(
                segmentRepoPath.resolve(
                    RemoteClusterStateService.encodeString(clusterService().state().getClusterName().value())
                        + "/cluster-state/"
                        + prevClusterUUID
                        + "/manifest"
                ),
                segmentRepoPath.resolve("cluster-state/")
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);
        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert Objects.equals(newClusterUUID, prevClusterUUID) : "Full restart not successful. cluster uuid has changed";
        validateCurrentMetadata();
        verifyRestoredData(indexStats, INDEX_NAME);
    }

    private void validateMetadata(List<String> indexNames) {
        assertEquals(clusterService().state().metadata().indices().size(), indexNames.size());
        for (String indexName : indexNames) {
            assertTrue(clusterService().state().metadata().hasIndex(indexName));
        }
    }

    private void validateCurrentMetadata() throws Exception {
        RemoteClusterStateService remoteClusterStateService = internalCluster().getInstance(
            RemoteClusterStateService.class,
            internalCluster().getClusterManagerName()
        );
        assertBusy(() -> {
            ClusterMetadataManifest manifest = remoteClusterStateService.getLatestClusterMetadataManifest(
                getClusterState().getClusterName().value(),
                getClusterState().metadata().clusterUUID()
            ).get();
            ClusterState clusterState = getClusterState();
            Metadata currentMetadata = clusterState.metadata();
            assertEquals(currentMetadata.indices().size(), manifest.getIndices().size());
            assertEquals(currentMetadata.coordinationMetadata().term(), manifest.getClusterTerm());
            assertEquals(clusterState.version(), manifest.getStateVersion());
            assertEquals(clusterState.stateUUID(), manifest.getStateUUID());
            assertEquals(currentMetadata.clusterUUIDCommitted(), manifest.isClusterUUIDCommitted());
            for (UploadedIndexMetadata uploadedIndexMetadata : manifest.getIndices()) {
                IndexMetadata currentIndexMetadata = currentMetadata.index(uploadedIndexMetadata.getIndexName());
                assertEquals(currentIndexMetadata.getIndex().getUUID(), uploadedIndexMetadata.getIndexUUID());
            }
        });
    }
}
