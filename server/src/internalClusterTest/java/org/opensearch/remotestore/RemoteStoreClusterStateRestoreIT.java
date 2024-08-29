/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.datastream.DataStreamRolloverIT;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.opensearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.ComponentTemplate;
import org.opensearch.cluster.metadata.ComponentTemplateMetadata;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.ComposableIndexTemplateMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.Template;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.opensearch.cluster.coordination.ClusterBootstrapService.INITIAL_CLUSTER_MANAGER_NODES_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_READ_ONLY_SETTING;
import static org.opensearch.cluster.metadata.Metadata.CLUSTER_READ_ONLY_BLOCK;
import static org.opensearch.cluster.metadata.Metadata.SETTING_READ_ONLY_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.encodeString;
import static org.opensearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.SYSTEM_REPOSITORY_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreClusterStateRestoreIT extends BaseRemoteStoreRestoreIT {
    static final String TEMPLATE_NAME = "remote-store-test-template";
    static final String COMPONENT_TEMPLATE_NAME = "remote-component-template1";
    static final String COMPOSABLE_TEMPLATE_NAME = "remote-composable-template1";
    static final Setting<String> MOCK_SETTING = Setting.simpleString("mock-setting");
    static final String[] EXCLUDED_NODES = { "ex-1", "ex-2" };

    @Before
    public void setup() {
        asyncUploadMockFsRepo = false;
    }

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

    protected void verifyRedIndicesAndTriggerRestore(Map<String, Long> indexStats, String indexName, boolean indexMoreDocs)
        throws Exception {
        ensureRed(indexName);
        restore(false, indexName);
        verifyRestoredData(indexStats, indexName, indexMoreDocs);
    }

    public void testFullClusterRestore() throws Exception {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        // Step - 1 index some data to generate files in remote directory
        Map<String, Long> indexStats = initialTestSetup(shardCount, replicaCount, dataNodeCount, 1);
        String prevClusterUUID = clusterService().state().metadata().clusterUUID();
        long prevClusterStateVersion = clusterService().state().version();
        // Step - 1.1 Add some cluster state elements
        ActionFuture<AcknowledgedResponse> response = client().admin()
            .indices()
            .preparePutTemplate(TEMPLATE_NAME)
            .addAlias(new Alias(INDEX_NAME))
            .setPatterns(Arrays.stream(INDEX_NAMES_WILDCARD.split(",")).collect(Collectors.toList()))
            .execute();
        assertTrue(response.get().isAcknowledged());
        ActionFuture<ClusterUpdateSettingsResponse> clusterUpdateSettingsResponse = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(SETTING_READ_ONLY_SETTING.getKey(), false).build())
            .execute();
        assertTrue(clusterUpdateSettingsResponse.get().isAcknowledged());
        // update coordination metadata
        client().execute(AddVotingConfigExclusionsAction.INSTANCE, new AddVotingConfigExclusionsRequest(EXCLUDED_NODES));
        // Add a custom metadata as component index template
        ActionFuture<AcknowledgedResponse> componentTemplateResponse = client().execute(
            PutComponentTemplateAction.INSTANCE,
            new PutComponentTemplateAction.Request(COMPONENT_TEMPLATE_NAME).componentTemplate(
                new ComponentTemplate(new Template(Settings.EMPTY, null, Collections.emptyMap()), 1L, Collections.emptyMap())
            )
        );
        assertTrue(componentTemplateResponse.get().isAcknowledged());
        ActionFuture<AcknowledgedResponse> composableTemplateResponse = client().execute(
            PutComposableIndexTemplateAction.INSTANCE,
            new PutComposableIndexTemplateAction.Request(COMPOSABLE_TEMPLATE_NAME).indexTemplate(
                new ComposableIndexTemplate(
                    Arrays.stream(INDEX_NAMES_WILDCARD.split(",")).collect(Collectors.toList()),
                    new Template(Settings.EMPTY, null, Collections.emptyMap()),
                    Collections.singletonList(COMPONENT_TEMPLATE_NAME),
                    1L,
                    1L,
                    Collections.emptyMap(),
                    null
                )
            )
        );
        assertTrue(composableTemplateResponse.get().isAcknowledged());

        // Step - 2 Replace all nodes in the cluster with new nodes. This ensures new cluster state doesn't have previous index metadata
        resetCluster(dataNodeCount, clusterManagerNodeCount);

        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        // Step - 3 validate cluster state restored
        long newClusterStateVersion = clusterService().state().version();
        assert prevClusterStateVersion < newClusterStateVersion : String.format(
            Locale.ROOT,
            "ClusterState version is not restored. previousClusterVersion: [%s] is greater than current [%s]",
            prevClusterStateVersion,
            newClusterStateVersion
        );
        validateMetadata(List.of(INDEX_NAME));
        verifyRedIndicesAndTriggerRestore(indexStats, INDEX_NAME, true);
        clusterService().state()
            .metadata()
            .coordinationMetadata()
            .getVotingConfigExclusions()
            .stream()
            .forEach(config -> assertTrue(Arrays.stream(EXCLUDED_NODES).anyMatch(node -> node.equals(config.getNodeId()))));
        assertFalse(clusterService().state().metadata().templates().isEmpty());
        assertTrue(clusterService().state().metadata().templates().containsKey(TEMPLATE_NAME));
        assertFalse(clusterService().state().metadata().settings().isEmpty());
        assertFalse(clusterService().state().metadata().settings().getAsBoolean(SETTING_READ_ONLY_SETTING.getKey(), true));
        assertNotNull(clusterService().state().metadata().custom("component_template"));
        ComponentTemplateMetadata componentTemplateMetadata = clusterService().state().metadata().custom("component_template");
        assertFalse(componentTemplateMetadata.componentTemplates().isEmpty());
        assertTrue(componentTemplateMetadata.componentTemplates().containsKey(COMPONENT_TEMPLATE_NAME));
        assertNotNull(clusterService().state().metadata().custom("index_template"));
        ComposableIndexTemplateMetadata composableIndexTemplate = clusterService().state().metadata().custom("index_template");
        assertFalse(composableIndexTemplate.indexTemplates().isEmpty());
        assertTrue(composableIndexTemplate.indexTemplates().containsKey(COMPOSABLE_TEMPLATE_NAME));
    }

    /**
     * This test scenario covers the case where right after remote state restore and persisting it to disk via LucenePersistedState, full cluster restarts.
     * This is a special case for remote state as at this point cluster uuid in the restored state is still ClusterState.UNKNOWN_UUID as we persist it disk.
     * After restart the local disk state will be read but should be again overridden with remote state.
     *
     * 1. Form a cluster and index few docs
     * 2. Replace all nodes to remove all local disk state
     * 3. Start cluster manager node without correct seeding to ensure local disk state is written with cluster uuid ClusterState.UNKNOWN_UUID but with remote restored Metadata
     * 4. Restart the cluster manager node with correct seeding.
     * 5. After restart the cluster manager picks up the local disk state with has same Metadata as remote but cluster uuid is still ClusterState.UNKNOWN_UUID
     * 6. The cluster manager will try to restore from remote again.
     * 7. Metadata loaded from local disk state will be overridden with remote Metadata and no conflict should arise.
     * 8. Add data nodes to recover index data
     * 9. Verify Metadata and index data is restored.
     */
    public void testFullClusterRestoreDoesntFailWithConflictingLocalState() throws Exception {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        // index some data to generate files in remote directory
        Map<String, Long> indexStats = initialTestSetup(shardCount, replicaCount, dataNodeCount, 1);
        String prevClusterUUID = clusterService().state().metadata().clusterUUID();
        long prevClusterStateVersion = clusterService().state().version();

        // stop all nodes
        internalCluster().stopAllNodes();

        // start a cluster manager node with no cluster manager seeding.
        // This should fail with IllegalStateException as cluster manager fails to form without any initial seed
        assertThrows(
            IllegalStateException.class,
            () -> internalCluster().startClusterManagerOnlyNodes(
                clusterManagerNodeCount,
                Settings.builder()
                    .putList(INITIAL_CLUSTER_MANAGER_NODES_SETTING.getKey()) // disable seeding during bootstrapping
                    .build()
            )
        );

        // verify cluster manager not elected
        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert Objects.equals(newClusterUUID, ClusterState.UNKNOWN_UUID)
            : "Disabling Cluster manager seeding failed. cluster uuid is not unknown";

        // restart cluster manager with correct seed
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder()
                    .putList(INITIAL_CLUSTER_MANAGER_NODES_SETTING.getKey(), nodeName)  // Seed with correct Cluster Manager node
                    .build();
            }
        });

        // validate new cluster state formed
        newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, ClusterState.UNKNOWN_UUID) : "cluster restart not successful. cluster uuid is still unknown";
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        long newClusterStateVersion = clusterService().state().version();
        assert prevClusterStateVersion < newClusterStateVersion : String.format(
            Locale.ROOT,
            "ClusterState version is not restored. previousClusterVersion: [%s] is greater than current [%s]",
            prevClusterStateVersion,
            newClusterStateVersion
        );
        validateMetadata(List.of(INDEX_NAME));

        // start data nodes to trigger index data recovery
        internalCluster().startDataOnlyNodes(dataNodeCount);
        verifyRedIndicesAndTriggerRestore(indexStats, INDEX_NAME, true);
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
        long prevClusterStateVersion = clusterService().state().version();

        // Step - 2 Replace all nodes in the cluster with new nodes. This ensures new cluster state doesn't have previous index metadata
        resetCluster(dataNodeCount, clusterManagerNodeCount);

        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        // Step - 3 validate cluster state restored
        long newClusterStateVersion = clusterService().state().version();
        assert prevClusterStateVersion < newClusterStateVersion : String.format(
            Locale.ROOT,
            "ClusterState version is not restored. previousClusterVersion: [%s] is greater than current [%s]",
            prevClusterStateVersion,
            newClusterStateVersion
        );
        validateMetadata(List.of(INDEX_NAME, secondIndexName));
        verifyRedIndicesAndTriggerRestore(indexStats, INDEX_NAME, false);
        verifyRedIndicesAndTriggerRestore(indexStats2, secondIndexName, false);
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
                segmentRepoPath.resolve(encodeString(clusterName) + "/cluster-state/" + prevClusterUUID + "/index"),
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
        long prevClusterStateVersion = clusterService().state().version();
        // Delete index metadata file in remote
        try {
            Files.move(
                segmentRepoPath.resolve(
                    encodeString(clusterService().state().getClusterName().value()) + "/cluster-state/" + prevClusterUUID + "/manifest"
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

        long newClusterStateVersion = clusterService().state().version();
        assert prevClusterStateVersion < newClusterStateVersion : String.format(
            Locale.ROOT,
            "ClusterState version is not restored. previousClusterVersion: [%s] is greater than current [%s]",
            prevClusterStateVersion,
            newClusterStateVersion
        );
        validateCurrentMetadata();
        verifyRedIndicesAndTriggerRestore(indexStats, INDEX_NAME, true);
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
            ClusterMetadataManifest manifest;
            try {
                manifest = remoteClusterStateService.getLatestClusterMetadataManifest(
                    getClusterState().getClusterName().value(),
                    getClusterState().metadata().clusterUUID()
                ).get();
            } catch (IllegalStateException e) {
                // AssertionError helps us use assertBusy and retry validation if failed due to a race condition.
                throw new AssertionError("Error while validating latest cluster metadata", e);
            }
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

    public void testDataStreamPostRemoteStateRestore() throws Exception {
        new DataStreamRolloverIT() {
            protected boolean triggerRemoteStateRestore() {
                return true;
            }
        }.testDataStreamRollover();
    }

    public void testFullClusterRestoreGlobalMetadata() throws Exception {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        // Step - 1 index some data to generate files in remote directory
        Map<String, Long> indexStats = initialTestSetup(shardCount, replicaCount, dataNodeCount, 1);
        String prevClusterUUID = clusterService().state().metadata().clusterUUID();
        long prevClusterStateVersion = clusterService().state().version();

        // Create global metadata - register a custom repo
        Path repoPath = registerCustomRepository();

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

        // Step - 3 validate cluster state restored
        long newClusterStateVersion = clusterService().state().version();
        assert prevClusterStateVersion < newClusterStateVersion : String.format(
            Locale.ROOT,
            "ClusterState version is not restored. previousClusterVersion: [%s] is greater than current [%s]",
            prevClusterStateVersion,
            newClusterStateVersion
        );

        validateCurrentMetadata();
        assertEquals(Integer.valueOf(34), SETTING_CLUSTER_MAX_SHARDS_PER_NODE.get(clusterService().state().metadata().settings()));
        assertEquals(true, SETTING_READ_ONLY_SETTING.get(clusterService().state().metadata().settings()));
        assertTrue(clusterService().state().blocks().hasGlobalBlock(CLUSTER_READ_ONLY_BLOCK));
        // Remote the cluster read only block to ensure proper cleanup
        updatePersistentSettings(Settings.builder().put(SETTING_READ_ONLY_SETTING.getKey(), false).build());
        assertFalse(clusterService().state().blocks().hasGlobalBlock(CLUSTER_READ_ONLY_BLOCK));

        verifyRedIndicesAndTriggerRestore(indexStats, INDEX_NAME, false);

        // validate global metadata restored
        verifyRestoredRepositories(repoPath);
        verifyRestoredIndexTemplate();
    }

    private Path registerCustomRepository() {
        Path path = randomRepoPath();
        createRepository("custom-repo", "fs", Settings.builder().put("location", path).put("compress", false));
        return path;
    }

    private void verifyRestoredRepositories(Path repoPath) {
        RepositoriesMetadata repositoriesMetadata = clusterService().state().metadata().custom(RepositoriesMetadata.TYPE);
        assertEquals(3, repositoriesMetadata.repositories().size()); // includes remote store repo as well
        assertTrue(SYSTEM_REPOSITORY_SETTING.get(repositoriesMetadata.repository(REPOSITORY_NAME).settings()));
        assertTrue(SYSTEM_REPOSITORY_SETTING.get(repositoriesMetadata.repository(REPOSITORY_2_NAME).settings()));
        assertEquals("fs", repositoriesMetadata.repository("custom-repo").type());
        assertEquals(
            Settings.builder().put("location", repoPath).put("compress", false).build(),
            repositoriesMetadata.repository("custom-repo").settings()
        );

        // repo cleanup post verification
        clusterAdmin().prepareDeleteRepository("custom-repo").get();
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
