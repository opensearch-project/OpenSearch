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
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_READ_ONLY_SETTING;
import static org.opensearch.cluster.metadata.Metadata.CLUSTER_READ_ONLY_BLOCK;
import static org.opensearch.cluster.metadata.Metadata.SETTING_READ_ONLY_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.opensearch.indices.ShardLimitValidator.SETTING_MAX_SHARDS_PER_CLUSTER_KEY;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.SYSTEM_REPOSITORY_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

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
        updateIndexBlock(true, secondIndexName);

        String prevClusterUUID = clusterService().state().metadata().clusterUUID();

        // Step - 2 Replace all nodes in the cluster with new nodes. This ensures new cluster state doesn't have previous index metadata
        resetCluster(dataNodeCount, clusterManagerNodeCount);

        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        // Step - 3 Trigger full cluster restore
        // validateMetadata(List.of(INDEX_NAME, secondIndexName));
        verifyRestoredData(indexStats, INDEX_NAME);
        verifyRestoredData(indexStats2, secondIndexName, false);
        assertTrue(INDEX_READ_ONLY_SETTING.get(clusterService().state().metadata().index(secondIndexName).getSettings()));
        assertThrows(ClusterBlockException.class, () -> indexSingleDoc(secondIndexName));
        // Test is complete

        // Remove the block to ensure proper cleanup
        updateIndexBlock(false, secondIndexName);
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

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/9834")
    public void testFullClusterRestoreGlobalMetadata() throws Exception {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        // Step - 1 index some data to generate files in remote directory
        Map<String, Long> indexStats = initialTestSetup(shardCount, replicaCount, dataNodeCount, 1);
        String prevClusterUUID = clusterService().state().metadata().clusterUUID();

        // Create global metadata - register a custom repo
        // TODO - uncomment after customs is also uploaded correctly
        // registerCustomRepository();

        // Create global metadata - persistent settings
        updatePersistentSettings(Settings.builder().put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), 34).build());

        // Create global metadata - index template
        putIndexTemplate();

        // Create global metadata - Put cluster block
        addClusterLevelReadOnlyBlock();

        // Step - 2 Replace all nodes in the cluster with new nodes. This ensures new cluster state doesn't have previous index metadata
        resetCluster(dataNodeCount, clusterManagerNodeCount);

        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        // Step - 3 Trigger full cluster restore and validate
        // validateCurrentMetadata();
        verifyRestoredData(indexStats, INDEX_NAME, false);

        // validate global metadata restored
        verifyRestoredRepositories();
        verifyRestoredIndexTemplate();
        assertEquals(Integer.valueOf(34), SETTING_CLUSTER_MAX_SHARDS_PER_NODE.get(clusterService().state().metadata().settings()));
        assertEquals(true, SETTING_READ_ONLY_SETTING.get(clusterService().state().metadata().settings()));
        assertTrue(clusterService().state().blocks().hasGlobalBlock(CLUSTER_READ_ONLY_BLOCK));
        // Test is complete

        // Remote the cluster read only block to ensure proper cleanup
        updatePersistentSettings(Settings.builder().put(SETTING_READ_ONLY_SETTING.getKey(), false).build());
        assertFalse(clusterService().state().blocks().hasGlobalBlock(CLUSTER_READ_ONLY_BLOCK));
    }

    private void registerCustomRepository() {
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository("custom-repo")
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath()).put("compress", false))
                .get()
        );
    }

    private void verifyRestoredRepositories() {
        RepositoriesMetadata repositoriesMetadata = clusterService().state().metadata().custom(RepositoriesMetadata.TYPE);
        assertEquals(2, repositoriesMetadata.repositories().size()); // includes remote store repo as well
        assertTrue(SYSTEM_REPOSITORY_SETTING.get(repositoriesMetadata.repository(REPOSITORY_NAME).settings()));
        assertTrue(SYSTEM_REPOSITORY_SETTING.get(repositoriesMetadata.repository(REPOSITORY_2_NAME).settings()));
        // assertEquals("fs", repositoriesMetadata.repository("custom-repo").type());
        // assertEquals(Settings.builder().put("location", randomRepoPath()).put("compress", false).build(),
        // repositoriesMetadata.repository("custom-repo").settings());
    }

    private void addClusterLevelReadOnlyBlock() throws InterruptedException, ExecutionException {
        updatePersistentSettings(Settings.builder().put(SETTING_READ_ONLY_SETTING.getKey(), true).build());
        assertTrue(clusterService().state().blocks().hasGlobalBlock(CLUSTER_READ_ONLY_BLOCK));
    }

    private void updatePersistentSettings(Settings settings) throws ExecutionException, InterruptedException {
        ClusterUpdateSettingsRequest resetRequest = new ClusterUpdateSettingsRequest();
        resetRequest.persistentSettings(settings);
        assertAcked(client().admin().cluster().updateSettings(resetRequest).get());
    }

    private void verifyRestoredIndexTemplate() {
        Map<String, IndexTemplateMetadata> indexTemplateMetadataMap = clusterService().state().metadata().templates();
        assertEquals(1, indexTemplateMetadataMap.size());
        assertEquals(Arrays.asList("pattern-1", "log-*"), indexTemplateMetadataMap.get("my-template").patterns());
        assertEquals(
            Settings.builder() // <1>
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 1)
                .build(),
            indexTemplateMetadataMap.get("my-template").settings()
        );
    }

    private static void putIndexTemplate() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("my-template"); // <1>
        request.patterns(Arrays.asList("pattern-1", "log-*")); // <2>

        request.settings(
            Settings.builder() // <1>
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 1)
        );
        assertTrue(client().admin().indices().putTemplate(request).actionGet().isAcknowledged());
    }

    private static void updateIndexBlock(boolean value, String secondIndexName) throws InterruptedException, ExecutionException {
        assertAcked(
            client().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(Settings.builder().put(INDEX_READ_ONLY_SETTING.getKey(), value).build(), secondIndexName)
                )
                .get()
        );
    }
}
