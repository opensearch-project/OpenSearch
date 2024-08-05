/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.model.RemoteRoutingTableBlobStore;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.common.util.FeatureFlags.REMOTE_PUBLICATION_EXPERIMENTAL;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable.INDEX_ROUTING_TABLE;
import static org.opensearch.indices.IndicesService.CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteRoutingTableServiceIT extends RemoteStoreBaseIntegTestCase {
    private static final String INDEX_NAME = "test-index";
    private static final String INDEX_NAME_1 = "test-index-1";
    BlobPath indexRoutingPath;
    AtomicInteger indexRoutingFiles = new AtomicInteger();
    private final RemoteStoreEnums.PathType pathType = RemoteStoreEnums.PathType.HASHED_PREFIX;

    @Before
    public void setup() {
        asyncUploadMockFsRepo = false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(
                RemoteRoutingTableBlobStore.REMOTE_ROUTING_TABLE_PATH_TYPE_SETTING.getKey(),
                RemoteStoreEnums.PathType.HASHED_PREFIX.toString()
            )
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, REMOTE_ROUTING_TABLE_REPO)
            .put(REMOTE_PUBLICATION_EXPERIMENTAL, true)
            .build();
    }

    public void testRemoteRoutingTableIndexLifecycle() throws Exception {
        BlobStoreRepository repository = prepareClusterAndVerifyRepository();

        RemoteClusterStateService remoteClusterStateService = internalCluster().getClusterManagerNodeInstance(
            RemoteClusterStateService.class
        );
        RemoteManifestManager remoteManifestManager = remoteClusterStateService.getRemoteManifestManager();
        Optional<ClusterMetadataManifest> latestManifest = remoteManifestManager.getLatestClusterMetadataManifest(
            getClusterState().getClusterName().value(),
            getClusterState().getMetadata().clusterUUID()
        );
        List<String> expectedIndexNames = new ArrayList<>();
        List<String> deletedIndexNames = new ArrayList<>();
        verifyUpdatesInManifestFile(latestManifest, expectedIndexNames, 1, deletedIndexNames, true);

        List<RoutingTable> routingTableVersions = getRoutingTableFromAllNodes();
        assertTrue(areRoutingTablesSame(routingTableVersions));

        // Update index settings
        updateIndexSettings(INDEX_NAME, IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2);
        ensureGreen(INDEX_NAME);
        assertBusy(() -> {
            int indexRoutingFilesAfterUpdate = repository.blobStore().blobContainer(indexRoutingPath).listBlobs().size();
            // At-least 3 new index routing files will be created as shards will transition from INIT -> UNASSIGNED -> STARTED state
            assertTrue(indexRoutingFilesAfterUpdate >= indexRoutingFiles.get() + 3);
        });

        latestManifest = remoteManifestManager.getLatestClusterMetadataManifest(
            getClusterState().getClusterName().value(),
            getClusterState().getMetadata().clusterUUID()
        );
        verifyUpdatesInManifestFile(latestManifest, expectedIndexNames, 1, deletedIndexNames, true);

        routingTableVersions = getRoutingTableFromAllNodes();
        assertTrue(areRoutingTablesSame(routingTableVersions));

        // Delete the index and assert its deletion
        deleteIndexAndVerify(remoteManifestManager);

        routingTableVersions = getRoutingTableFromAllNodes();
        assertTrue(areRoutingTablesSame(routingTableVersions));
    }

    public void testRemoteRoutingTableEmptyRoutingTableDiff() throws Exception {
        prepareClusterAndVerifyRepository();

        RemoteClusterStateService remoteClusterStateService = internalCluster().getClusterManagerNodeInstance(
            RemoteClusterStateService.class
        );
        RemoteManifestManager remoteManifestManager = remoteClusterStateService.getRemoteManifestManager();
        Optional<ClusterMetadataManifest> latestManifest = remoteManifestManager.getLatestClusterMetadataManifest(
            getClusterState().getClusterName().value(),
            getClusterState().getMetadata().clusterUUID()
        );
        List<String> expectedIndexNames = new ArrayList<>();
        List<String> deletedIndexNames = new ArrayList<>();
        verifyUpdatesInManifestFile(latestManifest, expectedIndexNames, 1, deletedIndexNames, true);

        List<RoutingTable> routingTableVersions = getRoutingTableFromAllNodes();
        assertTrue(areRoutingTablesSame(routingTableVersions));

        // Update cluster settings
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING.getKey(), 0, TimeUnit.SECONDS))
            .get();
        assertTrue(response.isAcknowledged());

        latestManifest = remoteManifestManager.getLatestClusterMetadataManifest(
            getClusterState().getClusterName().value(),
            getClusterState().getMetadata().clusterUUID()
        );
        verifyUpdatesInManifestFile(latestManifest, expectedIndexNames, 1, deletedIndexNames, false);

        routingTableVersions = getRoutingTableFromAllNodes();
        assertTrue(areRoutingTablesSame(routingTableVersions));
    }

    public void testRemoteRoutingTableIndexNodeRestart() throws Exception {
        BlobStoreRepository repository = prepareClusterAndVerifyRepository();

        List<RoutingTable> routingTableVersions = getRoutingTableFromAllNodes();
        assertTrue(areRoutingTablesSame(routingTableVersions));

        // Ensure node comes healthy after restart
        Set<String> dataNodes = internalCluster().getDataNodeNames();
        internalCluster().restartNode(randomFrom(dataNodes));
        ensureGreen();
        ensureGreen(INDEX_NAME);

        // ensure restarted node joins and the cluster is stable
        assertEquals(3, internalCluster().clusterService().state().nodes().getDataNodes().size());
        ensureStableCluster(4);
        assertRemoteStoreRepositoryOnAllNodes(REMOTE_ROUTING_TABLE_REPO);

        assertBusy(() -> {
            int indexRoutingFilesAfterNodeDrop = repository.blobStore().blobContainer(indexRoutingPath).listBlobs().size();
            assertTrue(indexRoutingFilesAfterNodeDrop > indexRoutingFiles.get());
        });

        RemoteClusterStateService remoteClusterStateService = internalCluster().getClusterManagerNodeInstance(
            RemoteClusterStateService.class
        );
        RemoteManifestManager remoteManifestManager = remoteClusterStateService.getRemoteManifestManager();
        Optional<ClusterMetadataManifest> latestManifest = remoteManifestManager.getLatestClusterMetadataManifest(
            getClusterState().getClusterName().value(),
            getClusterState().getMetadata().clusterUUID()
        );
        List<String> expectedIndexNames = new ArrayList<>();
        List<String> deletedIndexNames = new ArrayList<>();
        verifyUpdatesInManifestFile(latestManifest, expectedIndexNames, 1, deletedIndexNames, true);
    }

    public void testRemoteRoutingTableIndexMasterRestart() throws Exception {
        BlobStoreRepository repository = prepareClusterAndVerifyRepository();

        List<RoutingTable> routingTableVersions = getRoutingTableFromAllNodes();
        assertTrue(areRoutingTablesSame(routingTableVersions));

        // Ensure node comes healthy after restart
        String clusterManagerName = internalCluster().getClusterManagerName();
        internalCluster().restartNode(clusterManagerName);
        ensureGreen();
        ensureGreen(INDEX_NAME);

        // ensure master is elected and the cluster is stable
        assertNotNull(internalCluster().clusterService().state().nodes().getClusterManagerNode());
        ensureStableCluster(4);
        assertRemoteStoreRepositoryOnAllNodes(REMOTE_ROUTING_TABLE_REPO);

        assertBusy(() -> {
            int indexRoutingFilesAfterNodeDrop = repository.blobStore().blobContainer(indexRoutingPath).listBlobs().size();
            assertTrue(indexRoutingFilesAfterNodeDrop > indexRoutingFiles.get());
        });

        RemoteClusterStateService remoteClusterStateService = internalCluster().getClusterManagerNodeInstance(
            RemoteClusterStateService.class
        );
        RemoteManifestManager remoteManifestManager = remoteClusterStateService.getRemoteManifestManager();
        Optional<ClusterMetadataManifest> latestManifest = remoteManifestManager.getLatestClusterMetadataManifest(
            getClusterState().getClusterName().value(),
            getClusterState().getMetadata().clusterUUID()
        );
        List<String> expectedIndexNames = new ArrayList<>();
        List<String> deletedIndexNames = new ArrayList<>();
        verifyUpdatesInManifestFile(latestManifest, expectedIndexNames, 1, deletedIndexNames, true);
    }

    private BlobStoreRepository prepareClusterAndVerifyRepository() throws Exception {
        clusterSettingsSuppliedByTest = true;
        Path segmentRepoPath = randomRepoPath();
        Path translogRepoPath = randomRepoPath();
        Path remoteRoutingTableRepoPath = randomRepoPath();
        Settings settings = buildRemoteStoreNodeAttributes(
            REPOSITORY_NAME,
            segmentRepoPath,
            REPOSITORY_2_NAME,
            translogRepoPath,
            REMOTE_ROUTING_TABLE_REPO,
            remoteRoutingTableRepoPath,
            false
        );
        prepareCluster(1, 3, INDEX_NAME, 1, 5, settings);
        ensureGreen(INDEX_NAME);

        RepositoriesService repositoriesService = internalCluster().getClusterManagerNodeInstance(RepositoriesService.class);
        BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(REMOTE_ROUTING_TABLE_REPO);

        BlobPath baseMetadataPath = getBaseMetadataPath(repository);
        List<IndexRoutingTable> indexRoutingTables = new ArrayList<>(getClusterState().routingTable().indicesRouting().values());
        indexRoutingPath = getIndexRoutingPath(baseMetadataPath.add(INDEX_ROUTING_TABLE), indexRoutingTables.get(0).getIndex().getUUID());

        assertBusy(() -> {
            indexRoutingFiles.set(repository.blobStore().blobContainer(indexRoutingPath).listBlobs().size());
            // There would be >=3 files as shards will transition from UNASSIGNED -> INIT -> STARTED state
            assertTrue(indexRoutingFiles.get() >= 3);
        });
        assertRemoteStoreRepositoryOnAllNodes(REMOTE_ROUTING_TABLE_REPO);
        return repository;
    }

    private BlobPath getBaseMetadataPath(BlobStoreRepository repository) {
        return repository.basePath()
            .add(
                Base64.getUrlEncoder()
                    .withoutPadding()
                    .encodeToString(getClusterState().getClusterName().value().getBytes(StandardCharsets.UTF_8))
            )
            .add("cluster-state")
            .add(getClusterState().metadata().clusterUUID());
    }

    private BlobPath getIndexRoutingPath(BlobPath indexRoutingPath, String indexUUID) {
        RemoteStoreEnums.PathHashAlgorithm pathHashAlgo = RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64;
        return pathType.path(
            RemoteStorePathStrategy.PathInput.builder().basePath(indexRoutingPath).indexUUID(indexUUID).build(),
            pathHashAlgo
        );
    }

    private void verifyUpdatesInManifestFile(
        Optional<ClusterMetadataManifest> latestManifest,
        List<String> expectedIndexNames,
        int expectedIndicesRoutingFilesInManifest,
        List<String> expectedDeletedIndex,
        boolean isRoutingTableDiffFileExpected
    ) {
        assertTrue(latestManifest.isPresent());
        ClusterMetadataManifest manifest = latestManifest.get();

        assertEquals(expectedIndexNames, manifest.getDiffManifest().getIndicesRoutingUpdated());
        assertEquals(expectedDeletedIndex, manifest.getDiffManifest().getIndicesDeleted());
        assertEquals(expectedIndicesRoutingFilesInManifest, manifest.getIndicesRouting().size());
        for (ClusterMetadataManifest.UploadedIndexMetadata uploadedFilename : manifest.getIndicesRouting()) {
            assertTrue(uploadedFilename.getUploadedFilename().contains(indexRoutingPath.buildAsString()));
        }
        assertEquals(isRoutingTableDiffFileExpected, manifest.getDiffManifest().getIndicesRoutingDiffPath() != null);
    }

    private List<RoutingTable> getRoutingTableFromAllNodes() throws ExecutionException, InterruptedException {
        String[] allNodes = internalCluster().getNodeNames();
        List<RoutingTable> routingTables = new ArrayList<>();
        for (String node : allNodes) {
            RoutingTable routingTable = internalCluster().client(node)
                .admin()
                .cluster()
                .state(new ClusterStateRequest().local(true))
                .get()
                .getState()
                .routingTable();
            routingTables.add(routingTable);
        }
        return routingTables;
    }

    private boolean areRoutingTablesSame(List<RoutingTable> routingTables) {
        if (routingTables == null || routingTables.isEmpty()) {
            return false;
        }

        RoutingTable firstRoutingTable = routingTables.get(0);
        for (RoutingTable routingTable : routingTables) {
            if (!compareRoutingTables(firstRoutingTable, routingTable)) {
                logger.info("Responses are not the same: {} {}", firstRoutingTable, routingTable);
                return false;
            }
        }
        return true;
    }

    private boolean compareRoutingTables(RoutingTable a, RoutingTable b) {
        if (a == b) return true;
        if (b == null || a.getClass() != b.getClass()) return false;
        if (a.version() != b.version()) return false;
        if (a.indicesRouting().size() != b.indicesRouting().size()) return false;

        for (Map.Entry<String, IndexRoutingTable> entry : a.indicesRouting().entrySet()) {
            IndexRoutingTable thisIndexRoutingTable = entry.getValue();
            IndexRoutingTable thatIndexRoutingTable = b.indicesRouting().get(entry.getKey());
            if (!thatIndexRoutingTable.equals(thatIndexRoutingTable)) {
                return false;
            }
        }
        return true;
    }

    private void updateIndexSettings(String indexName, String settingKey, int settingValue) {
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(settingKey, settingValue))
            .execute()
            .actionGet();
    }

    private void deleteIndexAndVerify(RemoteManifestManager remoteManifestManager) {
        client().admin().indices().prepareDelete(INDEX_NAME).execute().actionGet();
        assertFalse(client().admin().indices().prepareExists(INDEX_NAME).get().isExists());

        // Verify index is marked deleted in manifest
        Optional<ClusterMetadataManifest> latestManifest = remoteManifestManager.getLatestClusterMetadataManifest(
            getClusterState().getClusterName().value(),
            getClusterState().getMetadata().clusterUUID()
        );
        assertTrue(latestManifest.isPresent());
        ClusterMetadataManifest manifest = latestManifest.get();
        assertTrue(manifest.getDiffManifest().getIndicesRoutingUpdated().isEmpty());
        assertTrue(manifest.getDiffManifest().getIndicesDeleted().contains(INDEX_NAME));
        assertTrue(manifest.getIndicesRouting().isEmpty());
    }

}
