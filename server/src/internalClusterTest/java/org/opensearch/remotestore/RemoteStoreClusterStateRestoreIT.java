/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING;
import static org.opensearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.opensearch.indices.ShardLimitValidator.SETTING_MAX_SHARDS_PER_CLUSTER_KEY;

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

    private void validateMetadataAfterRestore(Metadata prevMetadata) throws Exception {
        assertBusy(() -> {
            ClusterState clusterState = getClusterState();
            Metadata currentMetadata = clusterState.metadata();
            assertEquals(prevMetadata.indices().size(), currentMetadata.indices().size());
            for (IndexMetadata currentIndexMetadata : currentMetadata.indices().values()) {
                IndexMetadata prevIndexMetadata = prevMetadata.index(currentIndexMetadata.getIndex().getName());
                assertEquals(prevIndexMetadata.getIndex().getName(), currentIndexMetadata.getIndex().getName());
                assertEquals(prevIndexMetadata.getSettings(), currentIndexMetadata.getSettings());
                assertEquals(prevIndexMetadata.mapping(), currentIndexMetadata.mapping());
            }
        });
    }

    private void restoreAndValidateFails(
        Metadata clusterUUID,
        PlainActionFuture<RestoreRemoteStoreResponse> actionListener,
        Class<? extends Throwable> clazz,
        String errorSubString
    ) {
        try {
            validateMetadataAfterRestore(clusterUUID);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // try {
        // validateMetadataAfterRestore(clusterUUID);
        // } catch (Exception e) {
        // assertTrue(
        // String.format(Locale.ROOT, "%s %s", clazz, e),
        // clazz.isAssignableFrom(e.getClass())
        // || clazz.isAssignableFrom(e.getCause().getClass())
        // || (e.getCause().getCause() != null && clazz.isAssignableFrom(e.getCause().getCause().getClass()))
        // );
        // assertTrue(
        // String.format(Locale.ROOT, "Error message mismatch. Expected: [%s]. Actual: [%s]", errorSubString, e.getMessage()),
        // e.getMessage().contains(errorSubString)
        // );
        // }
    }

    private void reduceShardLimits(int maxShardsPerNode, int maxShardsPerCluster) {
        // Step 3 - Reduce shard limits to hit shard limit with less no of shards
        try {
            client().admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest().transientSettings(
                        Settings.builder()
                            .put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), maxShardsPerNode)
                            .put(SETTING_MAX_SHARDS_PER_CLUSTER_KEY, maxShardsPerCluster)
                    )
                )
                .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void resetShardLimits() {
        // Step - 5 Reset the cluster settings
        ClusterUpdateSettingsRequest resetRequest = new ClusterUpdateSettingsRequest();
        resetRequest.transientSettings(
            Settings.builder().putNull(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey()).putNull(SETTING_MAX_SHARDS_PER_CLUSTER_KEY)
        );

        try {
            client().admin().cluster().updateSettings(resetRequest).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void performOperation(int iteration) throws Exception {
        if (randomBoolean()) {
            // create index
            createIndex(INDEX_NAME + iteration, remoteStoreIndexSettings(1, 1));
        }
        if (randomBoolean()) {
            // update settings
            randomIndexSettings();
        }
        if (randomBoolean()) {
            // add mappings
            randomIndexMappings();
        }
        if (randomIntBetween(1, 5) == 5) { // Deleting index with 20% probability
            // delete index
            deleteRandomIndex();
        }
        if (randomBoolean()) {
            internalCluster().stopCurrentClusterManagerNode();
            internalCluster().startClusterManagerOnlyNode();
            internalCluster().validateClusterFormed();
        }
    }

    private String randomIndex() {
        if (clusterService().state().metadata().indices().size() == 0) {
            return null;
        }
        return randomFrom(clusterService().state().metadata().indices().keySet());
    }

    private void randomIndexSettings() {
        String indexName = randomIndex();
        if (indexName == null) {
            return;
        }
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), randomTimeValue(1, 60, new String[] { "s" }));
        client().admin().indices().updateSettings(new UpdateSettingsRequest(indexName).settings(settingsBuilder.build())).actionGet();
    }

    private void randomIndexMappings() {
        String indexName = randomIndex();
        if (indexName == null) {
            return;
        }
        String fieldName = "field-" + randomAlphaOfLength(10);

        // The updated index mapping as a JSON string
        String updatedMapping = String.format(Locale.ROOT, "{\"properties\": {\"%s\": {\"type\": \"text\"}}}", fieldName);

        PutMappingRequest request = new PutMappingRequest(indexName);
        request.source(updatedMapping, XContentType.JSON);

        client().admin().indices().putMapping(request).actionGet();
    }

    private void deleteRandomIndex() {
        String indexName = randomIndex();
        if (indexName == null) {
            return;
        }
        client().admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
    }

}
