/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.Client;
import org.opensearch.client.Requests;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.snapshots.AbstractSnapshotIntegTestCase;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotRestoreException;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.SYSTEM_REPOSITORY_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RestoreShallowSnapshotV2IT extends AbstractSnapshotIntegTestCase {

    private static final String BASE_REMOTE_REPO = "test-rs-repo" + TEST_REMOTE_STORE_REPO_SUFFIX;
    private Path remoteRepoPath;

    @Before
    public void setup() {
        remoteRepoPath = randomRepoPath().toAbsolutePath();
    }

    @After
    public void teardown() {
        clusterAdmin().prepareCleanupRepository(BASE_REMOTE_REPO).get();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(BASE_REMOTE_REPO, remoteRepoPath))
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.getKey(), true)
            .build();
    }

    @Override
    protected Settings.Builder getRepositorySettings(Path location, boolean shallowCopyEnabled) {
        Settings.Builder settingsBuilder = randomRepositorySettings();
        settingsBuilder.put("location", location);
        if (shallowCopyEnabled) {
            settingsBuilder.put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
                .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        }
        return settingsBuilder;
    }

    protected Settings.Builder getRepositorySettings(String sourceRepository, boolean readOnly) throws ExecutionException,
        InterruptedException {
        GetRepositoriesRequest gr = new GetRepositoriesRequest(new String[] { sourceRepository });
        GetRepositoriesResponse res = client().admin().cluster().getRepositories(gr).get();
        RepositoryMetadata rmd = res.repositories().get(0);
        return Settings.builder()
            .put(rmd.settings())
            .put(BlobStoreRepository.READONLY_SETTING.getKey(), readOnly)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), false)
            .put(SYSTEM_REPOSITORY_SETTING.getKey(), false);
    }

    private Settings.Builder getIndexSettings(int numOfShards, int numOfReplicas) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numOfShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numOfReplicas)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s");
        return settingsBuilder;
    }

    private void indexDocuments(Client client, String indexName, int numOfDocs) {
        indexDocuments(client, indexName, 0, numOfDocs);
    }

    protected void indexDocuments(Client client, String indexName, int fromId, int toId) {
        for (int i = fromId; i < toId; i++) {
            String id = Integer.toString(i);
            client.prepareIndex(indexName).setId(id).setSource("text", "sometext").get();
        }
    }

    private void assertDocsPresentInIndex(Client client, String indexName, int numOfDocs) {
        for (int i = 0; i < numOfDocs; i++) {
            String id = Integer.toString(i);
            logger.info("checking for index " + indexName + " with docId" + id);
            assertTrue("doc with id" + id + " is not present for index " + indexName, client.prepareGet(indexName, id).get().isExists());
        }
    }

    public void testRestoreOperationsShallowCopyEnabled() throws Exception {
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        String primary = internalCluster().startDataOnlyNode();
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String snapshotRepoName = "test-restore-snapshot-repo";
        String snapshotName1 = "test-restore-snapshot1";
        String snapshotName2 = "test-restore-snapshot2";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);
        String restoredIndexName1 = indexName1 + "-restored";
        String restoredIndexName2 = indexName2 + "-restored";

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName1, indexSettings);

        Settings indexSettings2 = getIndexSettings(1, 0).build();
        createIndex(indexName2, indexSettings2);

        final int numDocsInIndex1 = 5;
        final int numDocsInIndex2 = 6;
        indexDocuments(client, indexName1, numDocsInIndex1);
        indexDocuments(client, indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        internalCluster().startDataOnlyNode();
        logger.info("--> snapshot");

        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName1, new ArrayList<>());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));

        updateRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, false));
        SnapshotInfo snapshotInfo2 = createSnapshot(
            snapshotRepoName,
            snapshotName2,
            new ArrayList<>(Arrays.asList(indexName1, indexName2))
        );
        assertThat(snapshotInfo2.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo2.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo2.successfulShards(), equalTo(snapshotInfo2.totalShards()));

        DeleteResponse deleteResponse = client().prepareDelete(indexName1, "0").execute().actionGet();
        assertEquals(deleteResponse.getResult(), DocWriteResponse.Result.DELETED);
        indexDocuments(client, indexName1, numDocsInIndex1, numDocsInIndex1 + randomIntBetween(2, 5));
        ensureGreen(indexName1);

        RestoreSnapshotResponse restoreSnapshotResponse1 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(false)
            .setIndices(indexName1)
            .setRenamePattern(indexName1)
            .setRenameReplacement(restoredIndexName1)
            .get();
        RestoreSnapshotResponse restoreSnapshotResponse2 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName2)
            .setWaitForCompletion(false)
            .setIndices(indexName2)
            .setRenamePattern(indexName2)
            .setRenameReplacement(restoredIndexName2)
            .get();
        assertEquals(restoreSnapshotResponse1.status(), RestStatus.ACCEPTED);
        assertEquals(restoreSnapshotResponse2.status(), RestStatus.ACCEPTED);
        ensureGreen(restoredIndexName1, restoredIndexName2);
        assertDocsPresentInIndex(client, restoredIndexName1, numDocsInIndex1);
        assertDocsPresentInIndex(client, restoredIndexName2, numDocsInIndex2);

        // deleting data for restoredIndexName1 and restoring from remote store.
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));
        ensureRed(restoredIndexName1);
        // Re-initialize client to make sure we are not using client from stopped node.
        client = client(clusterManagerNode);
        assertAcked(client.admin().indices().prepareClose(restoredIndexName1));
        client.admin()
            .cluster()
            .restoreRemoteStore(
                new RestoreRemoteStoreRequest().indices(restoredIndexName1).restoreAllShards(true),
                PlainActionFuture.newFuture()
            );
        ensureYellowAndNoInitializingShards(restoredIndexName1);
        ensureGreen(restoredIndexName1);
        assertDocsPresentInIndex(client(), restoredIndexName1, numDocsInIndex1);
        // indexing some new docs and validating
        indexDocuments(client, restoredIndexName1, numDocsInIndex1, numDocsInIndex1 + 2);
        ensureGreen(restoredIndexName1);
        assertDocsPresentInIndex(client, restoredIndexName1, numDocsInIndex1 + 2);
    }

    public void testRemoteStoreCustomDataOnIndexCreationAndRestore() {
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String snapshotRepoName = "test-restore-snapshot-repo";
        String snapshotName1 = "test-restore-snapshot1";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);
        String restoredIndexName1version1 = indexName1 + "-restored-1";
        String restoredIndexName1version2 = indexName1 + "-restored-2";

        client(clusterManagerNode).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), RemoteStoreEnums.PathType.FIXED))
            .get();
        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));
        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName1, indexSettings);

        indexDocuments(client, indexName1, randomIntBetween(5, 10));
        ensureGreen(indexName1);
        validatePathType(indexName1, RemoteStoreEnums.PathType.FIXED);

        logger.info("--> snapshot");
        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName1, new ArrayList<>(Arrays.asList(indexName1)));
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertTrue(snapshotInfo.successfulShards() > 0);
        assertEquals(snapshotInfo.totalShards(), snapshotInfo.successfulShards());

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(false)
            .setRenamePattern(indexName1)
            .setRenameReplacement(restoredIndexName1version1)
            .get();
        assertEquals(RestStatus.ACCEPTED, restoreSnapshotResponse.status());
        ensureGreen(restoredIndexName1version1);
        validatePathType(restoredIndexName1version1, RemoteStoreEnums.PathType.FIXED);

        client(clusterManagerNode).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), RemoteStoreEnums.PathType.HASHED_PREFIX)
            )
            .get();

        restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(false)
            .setRenamePattern(indexName1)
            .setRenameReplacement(restoredIndexName1version2)
            .get();
        assertEquals(RestStatus.ACCEPTED, restoreSnapshotResponse.status());
        ensureGreen(restoredIndexName1version2);
        validatePathType(
            restoredIndexName1version2,
            RemoteStoreEnums.PathType.HASHED_PREFIX,
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1
        );

        // Create index with cluster setting cluster.remote_store.index.path.type as hashed_prefix.
        indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName2, indexSettings);
        ensureGreen(indexName2);
        validatePathType(indexName2, RemoteStoreEnums.PathType.HASHED_PREFIX, RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);

        // Validating that custom data has not changed for indexes which were created before the cluster setting got updated
        validatePathType(indexName1, RemoteStoreEnums.PathType.FIXED);

        // Create Snapshot of index 2
        String snapshotName2 = "test-restore-snapshot2";
        snapshotInfo = createSnapshot(snapshotRepoName, snapshotName2, new ArrayList<>(List.of(indexName2)));
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertTrue(snapshotInfo.successfulShards() > 0);
        assertEquals(snapshotInfo.totalShards(), snapshotInfo.successfulShards());

        // Update cluster settings to FIXED
        client(clusterManagerNode).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), RemoteStoreEnums.PathType.FIXED))
            .get();

        // Close index 2
        assertAcked(client().admin().indices().prepareClose(indexName2));
        restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName2)
            .setWaitForCompletion(false)
            .setIndices(indexName2)
            .get();
        assertEquals(RestStatus.ACCEPTED, restoreSnapshotResponse.status());
        ensureGreen(indexName2);

        // Validating that custom data has not changed for testindex2 which was created before the cluster setting got updated
        validatePathType(indexName2, RemoteStoreEnums.PathType.HASHED_PREFIX, RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
    }

    private void validatePathType(String index, RemoteStoreEnums.PathType pathType) {
        validatePathType(index, pathType, null);
    }

    private void validatePathType(
        String index,
        RemoteStoreEnums.PathType pathType,
        @Nullable RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm
    ) {
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        // Validate that the remote_store custom data is present in index metadata for the created index.
        Map<String, String> remoteCustomData = state.metadata().index(index).getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY);
        assertNotNull(remoteCustomData);
        assertEquals(pathType.name(), remoteCustomData.get(RemoteStoreEnums.PathType.NAME));
        if (Objects.nonNull(pathHashAlgorithm)) {
            assertEquals(pathHashAlgorithm.name(), remoteCustomData.get(RemoteStoreEnums.PathHashAlgorithm.NAME));
        }
    }

    public void testRestoreInSameRemoteStoreEnabledIndex() throws IOException {
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        String primary = internalCluster().startDataOnlyNode();
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String snapshotRepoName = "test-restore-snapshot-repo";
        String snapshotName1 = "test-restore-snapshot1";
        String snapshotName2 = "test-restore-snapshot2";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);
        String restoredIndexName2 = indexName2 + "-restored";

        boolean enableShallowCopy = randomBoolean();
        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, enableShallowCopy));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName1, indexSettings);

        Settings indexSettings2 = getIndexSettings(1, 0).build();
        createIndex(indexName2, indexSettings2);

        final int numDocsInIndex1 = 5;
        final int numDocsInIndex2 = 6;
        indexDocuments(client, indexName1, numDocsInIndex1);
        indexDocuments(client, indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        internalCluster().startDataOnlyNode();
        logger.info("--> snapshot");
        SnapshotInfo snapshotInfo1 = createSnapshot(
            snapshotRepoName,
            snapshotName1,
            new ArrayList<>(Arrays.asList(indexName1, indexName2))
        );
        assertThat(snapshotInfo1.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo1.successfulShards(), equalTo(snapshotInfo1.totalShards()));
        assertThat(snapshotInfo1.state(), equalTo(SnapshotState.SUCCESS));

        updateRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, false));
        SnapshotInfo snapshotInfo2 = createSnapshot(
            snapshotRepoName,
            snapshotName2,
            new ArrayList<>(Arrays.asList(indexName1, indexName2))
        );
        assertThat(snapshotInfo2.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo2.successfulShards(), equalTo(snapshotInfo2.totalShards()));
        assertThat(snapshotInfo2.state(), equalTo(SnapshotState.SUCCESS));

        DeleteResponse deleteResponse = client().prepareDelete(indexName1, "0").execute().actionGet();
        assertEquals(deleteResponse.getResult(), DocWriteResponse.Result.DELETED);
        indexDocuments(client, indexName1, numDocsInIndex1, numDocsInIndex1 + randomIntBetween(2, 5));
        ensureGreen(indexName1);

        assertAcked(client().admin().indices().prepareClose(indexName1));

        RestoreSnapshotResponse restoreSnapshotResponse1 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(false)
            .setIndices(indexName1)
            .get();
        RestoreSnapshotResponse restoreSnapshotResponse2 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName2)
            .setWaitForCompletion(false)
            .setIndices(indexName2)
            .setRenamePattern(indexName2)
            .setRenameReplacement(restoredIndexName2)
            .get();
        assertEquals(restoreSnapshotResponse1.status(), RestStatus.ACCEPTED);
        assertEquals(restoreSnapshotResponse2.status(), RestStatus.ACCEPTED);
        ensureGreen(indexName1, restoredIndexName2);

        assertRemoteSegmentsAndTranslogUploaded(restoredIndexName2);
        assertDocsPresentInIndex(client, indexName1, numDocsInIndex1);
        assertDocsPresentInIndex(client, restoredIndexName2, numDocsInIndex2);
        // indexing some new docs and validating
        indexDocuments(client, indexName1, numDocsInIndex1, numDocsInIndex1 + 2);
        ensureGreen(indexName1);
        assertDocsPresentInIndex(client, indexName1, numDocsInIndex1 + 2);

        // deleting data for restoredIndexName1 and restoring from remote store.
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));
        ensureRed(indexName1);
        // Re-initialize client to make sure we are not using client from stopped node.
        client = client(clusterManagerNode);
        assertAcked(client.admin().indices().prepareClose(indexName1));
        client.admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(indexName1).restoreAllShards(true), PlainActionFuture.newFuture());
        ensureYellowAndNoInitializingShards(indexName1);
        ensureGreen(indexName1);
        assertDocsPresentInIndex(client(), indexName1, numDocsInIndex1);
        // indexing some new docs and validating
        indexDocuments(client, indexName1, numDocsInIndex1 + 2, numDocsInIndex1 + 4);
        ensureGreen(indexName1);
        assertDocsPresentInIndex(client, indexName1, numDocsInIndex1 + 4);
    }

    void assertRemoteSegmentsAndTranslogUploaded(String idx) throws IOException {
        Client client = client();
        String translogPathFixedPrefix = RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX.get(getNodeSettings());
        String segmentsPathFixedPrefix = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.get(getNodeSettings());
        String path = getShardLevelBlobPath(client, idx, new BlobPath(), "0", TRANSLOG, METADATA, translogPathFixedPrefix).buildAsString();
        Path remoteTranslogMetadataPath = Path.of(remoteRepoPath + "/" + path);
        path = getShardLevelBlobPath(client, idx, new BlobPath(), "0", TRANSLOG, DATA, translogPathFixedPrefix).buildAsString();
        Path remoteTranslogDataPath = Path.of(remoteRepoPath + "/" + path);
        path = getShardLevelBlobPath(client, idx, new BlobPath(), "0", SEGMENTS, METADATA, segmentsPathFixedPrefix).buildAsString();
        Path segmentMetadataPath = Path.of(remoteRepoPath + "/" + path);
        path = getShardLevelBlobPath(client, idx, new BlobPath(), "0", SEGMENTS, DATA, segmentsPathFixedPrefix).buildAsString();
        Path segmentDataPath = Path.of(remoteRepoPath + "/" + path);

        try (
            Stream<Path> translogMetadata = Files.list(remoteTranslogMetadataPath);
            Stream<Path> translogData = Files.list(remoteTranslogDataPath);
            Stream<Path> segmentMetadata = Files.list(segmentMetadataPath);
            Stream<Path> segmentData = Files.list(segmentDataPath);

        ) {
            assertTrue(translogData.count() > 0);
            assertTrue(translogMetadata.count() > 0);
            assertTrue(segmentMetadata.count() > 0);
            assertTrue(segmentData.count() > 0);
        }

    }

    public void testRemoteRestoreIndexRestoredFromSnapshot() throws IOException, ExecutionException, InterruptedException {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        String indexName1 = "testindex1";
        String snapshotRepoName = "test-restore-snapshot-repo";
        String snapshotName1 = "test-restore-snapshot1";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName1, indexSettings);

        final int numDocsInIndex1 = randomIntBetween(20, 30);
        indexDocuments(client(), indexName1, numDocsInIndex1);
        flushAndRefresh(indexName1);
        ensureGreen(indexName1);

        logger.info("--> snapshot");
        SnapshotInfo snapshotInfo1 = createSnapshot(snapshotRepoName, snapshotName1, new ArrayList<>(Arrays.asList(indexName1)));
        assertThat(snapshotInfo1.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo1.successfulShards(), equalTo(snapshotInfo1.totalShards()));
        assertThat(snapshotInfo1.state(), equalTo(SnapshotState.SUCCESS));

        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName1)).get());
        assertFalse(indexExists(indexName1));

        RestoreSnapshotResponse restoreSnapshotResponse1 = client().admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(false)
            .setIndices(indexName1)
            .get();

        assertEquals(restoreSnapshotResponse1.status(), RestStatus.ACCEPTED);
        ensureGreen(indexName1);
        assertDocsPresentInIndex(client(), indexName1, numDocsInIndex1);

        assertRemoteSegmentsAndTranslogUploaded(indexName1);

        // Clear the local data before stopping the node. This will make sure that remote translog is empty.
        IndexShard indexShard = getIndexShard(primaryNodeName(indexName1), indexName1);
        try (Stream<Path> files = Files.list(indexShard.shardPath().resolveTranslog())) {
            IOUtils.deleteFilesIgnoringExceptions(files.collect(Collectors.toList()));
        }
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(indexName1)));

        ensureRed(indexName1);

        client().admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(indexName1).restoreAllShards(false), PlainActionFuture.newFuture());

        ensureGreen(indexName1);
        assertDocsPresentInIndex(client(), indexName1, numDocsInIndex1);
    }

    private IndexShard getIndexShard(String node, String indexName) {
        final Index index = resolveIndex(indexName);
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        IndexService indexService = indicesService.indexService(index);
        assertNotNull(indexService);
        final Optional<Integer> shardId = indexService.shardIds().stream().findFirst();
        return shardId.map(indexService::getShard).orElse(null);
    }

    public void testRestoreShallowSnapshotRepository() throws ExecutionException, InterruptedException {
        String indexName1 = "testindex1";
        String snapshotRepoName = "test-restore-snapshot-repo";
        String remoteStoreRepoNameUpdated = "test-rs-repo-updated" + TEST_REMOTE_STORE_REPO_SUFFIX;
        String snapshotName1 = "test-restore-snapshot1";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        Path absolutePath2 = randomRepoPath().toAbsolutePath();
        String[] pathTokens = absolutePath1.toString().split("/");
        String basePath = pathTokens[pathTokens.length - 1];
        Arrays.copyOf(pathTokens, pathTokens.length - 1);
        Path location = PathUtils.get(String.join("/", pathTokens));
        pathTokens = absolutePath2.toString().split("/");
        String basePath2 = pathTokens[pathTokens.length - 1];
        Arrays.copyOf(pathTokens, pathTokens.length - 1);
        Path location2 = PathUtils.get(String.join("/", pathTokens));
        logger.info("Path 1 [{}]", absolutePath1);
        logger.info("Path 2 [{}]", absolutePath2);
        String restoredIndexName1 = indexName1 + "-restored";

        createRepository(snapshotRepoName, "fs", getRepositorySettings(location, basePath, true));

        Client client = client();
        Settings indexSettings = Settings.builder()
            .put(super.indexSettings())
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s")
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        createIndex(indexName1, indexSettings);

        int numDocsInIndex1 = randomIntBetween(2, 5);
        indexDocuments(client, indexName1, numDocsInIndex1);

        ensureGreen(indexName1);

        logger.info("--> snapshot");
        SnapshotInfo snapshotInfo1 = createSnapshot(snapshotRepoName, snapshotName1, new ArrayList<>(List.of(indexName1)));
        assertThat(snapshotInfo1.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo1.successfulShards(), equalTo(snapshotInfo1.totalShards()));
        assertThat(snapshotInfo1.state(), equalTo(SnapshotState.SUCCESS));

        client().admin().indices().close(Requests.closeIndexRequest(indexName1)).get();
        createRepository(remoteStoreRepoNameUpdated, "fs", remoteRepoPath);
        RestoreSnapshotResponse restoreSnapshotResponse2 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(true)
            .setIndices(indexName1)
            .setRenamePattern(indexName1)
            .setRenameReplacement(restoredIndexName1)
            .setSourceRemoteStoreRepository(remoteStoreRepoNameUpdated)
            .get();

        assertTrue(restoreSnapshotResponse2.getRestoreInfo().failedShards() == 0);
        ensureGreen(restoredIndexName1);
        assertDocsPresentInIndex(client, restoredIndexName1, numDocsInIndex1);

        // ensure recovery details are non-zero
        RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(restoredIndexName1).execute().actionGet();
        assertEquals(1, recoveryResponse.getTotalShards());
        assertEquals(1, recoveryResponse.getSuccessfulShards());
        assertEquals(0, recoveryResponse.getFailedShards());
        assertEquals(1, recoveryResponse.shardRecoveryStates().size());
        assertTrue(recoveryResponse.shardRecoveryStates().containsKey(restoredIndexName1));
        assertEquals(1, recoveryResponse.shardRecoveryStates().get(restoredIndexName1).size());

        RecoveryState recoveryState = recoveryResponse.shardRecoveryStates().get(restoredIndexName1).get(0);
        assertEquals(RecoveryState.Stage.DONE, recoveryState.getStage());
        assertEquals(0, recoveryState.getShardId().getId());
        assertTrue(recoveryState.getPrimary());
        assertEquals(RecoverySource.Type.SNAPSHOT, recoveryState.getRecoverySource().getType());
        assertThat(recoveryState.getIndex().time(), greaterThanOrEqualTo(0L));

        // ensure populated file details
        assertTrue(recoveryState.getIndex().totalFileCount() > 0);
        assertTrue(recoveryState.getIndex().totalRecoverFiles() > 0);
        assertTrue(recoveryState.getIndex().recoveredFileCount() > 0);
        assertThat(recoveryState.getIndex().recoveredFilesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat(recoveryState.getIndex().recoveredFilesPercent(), lessThanOrEqualTo(100.0f));
        assertFalse(recoveryState.getIndex().fileDetails().isEmpty());

        // ensure populated bytes details
        assertTrue(recoveryState.getIndex().recoveredBytes() > 0L);
        assertTrue(recoveryState.getIndex().totalBytes() > 0L);
        assertTrue(recoveryState.getIndex().totalRecoverBytes() > 0L);
        assertThat(recoveryState.getIndex().recoveredBytesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat(recoveryState.getIndex().recoveredBytesPercent(), lessThanOrEqualTo(100.0f));

        // indexing some new docs and validating
        indexDocuments(client, restoredIndexName1, numDocsInIndex1, numDocsInIndex1 + 2);
        ensureGreen(restoredIndexName1);
        assertDocsPresentInIndex(client, restoredIndexName1, numDocsInIndex1 + 2);
    }

    public void testRestoreShallowSnapshotIndexAfterSnapshot() throws ExecutionException, InterruptedException {
        String indexName1 = "testindex1";
        String snapshotRepoName = "test-restore-snapshot-repo";
        String remoteStoreRepoNameUpdated = "test-rs-repo-updated" + TEST_REMOTE_STORE_REPO_SUFFIX;
        String snapshotName1 = "test-restore-snapshot1";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        Path absolutePath2 = randomRepoPath().toAbsolutePath();
        String[] pathTokens = absolutePath1.toString().split("/");
        String basePath = pathTokens[pathTokens.length - 1];
        Arrays.copyOf(pathTokens, pathTokens.length - 1);
        Path location = PathUtils.get(String.join("/", pathTokens));
        pathTokens = absolutePath2.toString().split("/");
        String basePath2 = pathTokens[pathTokens.length - 1];
        Arrays.copyOf(pathTokens, pathTokens.length - 1);
        Path location2 = PathUtils.get(String.join("/", pathTokens));
        logger.info("Path 1 [{}]", absolutePath1);
        logger.info("Path 2 [{}]", absolutePath2);
        String restoredIndexName1 = indexName1 + "-restored";

        createRepository(snapshotRepoName, "fs", getRepositorySettings(location, basePath, true));

        Client client = client();
        Settings indexSettings = Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        createIndex(indexName1, indexSettings);

        int numDocsInIndex1 = randomIntBetween(2, 5);
        indexDocuments(client, indexName1, numDocsInIndex1);

        ensureGreen(indexName1);

        logger.info("--> snapshot");
        SnapshotInfo snapshotInfo1 = createSnapshot(snapshotRepoName, snapshotName1, new ArrayList<>(List.of(indexName1)));
        assertThat(snapshotInfo1.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo1.successfulShards(), equalTo(snapshotInfo1.totalShards()));
        assertThat(snapshotInfo1.state(), equalTo(SnapshotState.SUCCESS));

        int extraNumDocsInIndex1 = randomIntBetween(20, 50);
        indexDocuments(client, indexName1, extraNumDocsInIndex1);
        refresh(indexName1);

        client().admin().indices().close(Requests.closeIndexRequest(indexName1)).get();
        createRepository(remoteStoreRepoNameUpdated, "fs", remoteRepoPath);
        RestoreSnapshotResponse restoreSnapshotResponse2 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(true)
            .setIndices(indexName1)
            .setRenamePattern(indexName1)
            .setRenameReplacement(restoredIndexName1)
            .setSourceRemoteStoreRepository(remoteStoreRepoNameUpdated)
            .get();

        assertTrue(restoreSnapshotResponse2.getRestoreInfo().failedShards() == 0);
        ensureGreen(restoredIndexName1);
        assertDocsPresentInIndex(client, restoredIndexName1, numDocsInIndex1);

        // indexing some new docs and validating
        indexDocuments(client, restoredIndexName1, numDocsInIndex1, numDocsInIndex1 + 2);
        ensureGreen(restoredIndexName1);
        assertDocsPresentInIndex(client, restoredIndexName1, numDocsInIndex1 + 2);
    }

    public void testInvalidRestoreRequestScenarios() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        String index = "test-index";
        String snapshotRepo = "test-restore-snapshot-repo";
        String newRemoteStoreRepo = "test-new-rs-repo";
        String snapshotName1 = "test-restore-snapshot1";
        String snapshotName2 = "test-restore-snapshot2";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);
        String restoredIndex = index + "-restored";

        createRepository(snapshotRepo, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(index, indexSettings);

        final int numDocsInIndex = 5;
        indexDocuments(client, index, numDocsInIndex);
        ensureGreen(index);

        internalCluster().startDataOnlyNode();
        logger.info("--> snapshot");

        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepo, snapshotName1, new ArrayList<>(List.of(index)));
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));

        updateRepository(snapshotRepo, "fs", getRepositorySettings(absolutePath1, false));
        SnapshotInfo snapshotInfo2 = createSnapshot(snapshotRepo, snapshotName2, new ArrayList<>(List.of(index)));
        assertThat(snapshotInfo2.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo2.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo2.successfulShards(), equalTo(snapshotInfo2.totalShards()));

        DeleteResponse deleteResponse = client().prepareDelete(index, "0").execute().actionGet();
        assertEquals(deleteResponse.getResult(), DocWriteResponse.Result.DELETED);
        indexDocuments(client, index, numDocsInIndex, numDocsInIndex + randomIntBetween(2, 5));
        ensureGreen(index);

        // try index restore with remote store disabled
        SnapshotRestoreException exception = expectThrows(
            SnapshotRestoreException.class,
            () -> client().admin()
                .cluster()
                .prepareRestoreSnapshot(snapshotRepo, snapshotName1)
                .setWaitForCompletion(false)
                .setIgnoreIndexSettings(SETTING_REMOTE_STORE_ENABLED)
                .setIndices(index)
                .setRenamePattern(index)
                .setRenameReplacement(restoredIndex)
                .get()
        );
        assertTrue(exception.getMessage().contains("cannot remove setting [index.remote_store.enabled] on restore"));

        // try index restore with remote store repository modified
        Settings remoteStoreIndexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, newRemoteStoreRepo)
            .build();

        exception = expectThrows(
            SnapshotRestoreException.class,
            () -> client().admin()
                .cluster()
                .prepareRestoreSnapshot(snapshotRepo, snapshotName1)
                .setWaitForCompletion(false)
                .setIndexSettings(remoteStoreIndexSettings)
                .setIndices(index)
                .setRenamePattern(index)
                .setRenameReplacement(restoredIndex)
                .get()
        );
        assertTrue(exception.getMessage().contains("cannot modify setting [index.remote_store.segment.repository]" + " on restore"));

        // try index restore with remote store repository and translog store repository disabled
        exception = expectThrows(
            SnapshotRestoreException.class,
            () -> client().admin()
                .cluster()
                .prepareRestoreSnapshot(snapshotRepo, snapshotName1)
                .setWaitForCompletion(false)
                .setIgnoreIndexSettings(
                    IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY,
                    IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY
                )
                .setIndices(index)
                .setRenamePattern(index)
                .setRenameReplacement(restoredIndex)
                .get()
        );
        assertTrue(exception.getMessage().contains("cannot remove setting [index.remote_store.segment.repository]" + " on restore"));
    }

    public void testRestoreOperationsUsingDifferentRepos() throws Exception {
        disableRepoConsistencyCheck("Remote store repo");
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        String primary = internalCluster().startDataOnlyNode();
        String indexName1 = "testindex1";
        String snapshotRepoName = "test-snapshot-repo";
        String snapshotName1 = "test-snapshot1";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        // Create repo
        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        // Create index
        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName1, indexSettings);
        ensureGreen(indexName1);

        // Index 5 documents, refresh, index 5 documents
        final int numDocsInIndex1 = 5;
        indexDocuments(client, indexName1, 0, numDocsInIndex1);
        refresh(indexName1);
        indexDocuments(client, indexName1, numDocsInIndex1, 2 * numDocsInIndex1);

        // Take V2 snapshot
        logger.info("--> snapshot");
        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName1, new ArrayList<>());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));

        // Create new snapshot, segment and translog repositories
        String newSnapshotRepo = "backup-snapshot";
        String newSegmentRepo = "backup-segment";
        String newTranslogRepo = "backup-translog";
        createRepository(newSnapshotRepo, "fs", getRepositorySettings(snapshotRepoName, true));
        createRepository(newSegmentRepo, "fs", getRepositorySettings(BASE_REMOTE_REPO, true));
        createRepository(newTranslogRepo, "fs", getRepositorySettings(BASE_REMOTE_REPO, true));

        // Delete index
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName1)).get());
        assertFalse(indexExists(indexName1));

        // Restore using new repos
        RestoreSnapshotResponse restoreSnapshotResponse1 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(newSnapshotRepo, snapshotName1)
            .setWaitForCompletion(false)
            .setIndices(indexName1)
            .setSourceRemoteStoreRepository(newSegmentRepo)
            .setSourceRemoteTranslogRepository(newTranslogRepo)
            .get();

        assertEquals(restoreSnapshotResponse1.status(), RestStatus.ACCEPTED);

        // Verify restored index's stats
        ensureYellowAndNoInitializingShards(indexName1);
        ensureGreen(indexName1);
        assertDocsPresentInIndex(client(), indexName1, 2 * numDocsInIndex1);

        // indexing some new docs and validating
        indexDocuments(client, indexName1, 2 * numDocsInIndex1, 3 * numDocsInIndex1);
        ensureGreen(indexName1);
        assertDocsPresentInIndex(client, indexName1, 3 * numDocsInIndex1);
    }

    public void testContinuousIndexing() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        String index = "test-index";
        String snapshotRepo = "test-restore-snapshot-repo";
        String baseSnapshotName = "snapshot_";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        createRepository(snapshotRepo, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        Settings indexSettings = Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        createIndex(index, indexSettings);
        ensureGreen(index);

        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
            RemoteStorePinnedTimestampService.class,
            primaryNodeName(index)
        );

        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueSeconds(randomIntBetween(1, 5)));
        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.timeValueSeconds(randomIntBetween(1, 5)));

        long totalDocs = 0;
        Map<String, Long> snapshots = new HashMap<>();
        int numDocs = randomIntBetween(200, 300);
        totalDocs += numDocs;
        try (BackgroundIndexer indexer = new BackgroundIndexer(index, MapperService.SINGLE_MAPPING_NAME, client(), numDocs)) {
            int numberOfSnapshots = 2;
            for (int i = 0; i < numberOfSnapshots; i++) {
                logger.info("--> waiting for {} docs to be indexed ...", numDocs);
                long finalTotalDocs1 = totalDocs;
                assertBusy(() -> assertEquals(finalTotalDocs1, indexer.totalIndexedDocs()), 120, TimeUnit.SECONDS);
                logger.info("--> {} total docs indexed", totalDocs);
                String snapshotName = baseSnapshotName + i;
                createSnapshot(snapshotRepo, snapshotName, new ArrayList<>());
                snapshots.put(snapshotName, totalDocs);
                if (i < numberOfSnapshots - 1) {
                    numDocs = randomIntBetween(200, 300);
                    indexer.continueIndexing(numDocs);
                    totalDocs += numDocs;
                }
            }
        }

        logger.info("Snapshots Status: " + snapshots);

        for (String snapshot : snapshots.keySet()) {
            logger.info("Restoring snapshot: {}", snapshot);
            assertAcked(client().admin().indices().delete(new DeleteIndexRequest(index)).get());

            RestoreSnapshotResponse restoreSnapshotResponse1 = client.admin()
                .cluster()
                .prepareRestoreSnapshot(snapshotRepo, snapshot)
                .setWaitForCompletion(true)
                .setIndices()
                .get();

            assertEquals(RestStatus.OK, restoreSnapshotResponse1.status());

            // Verify restored index's stats
            ensureGreen(TimeValue.timeValueSeconds(60), index);
            long finalTotalDocs = totalDocs;
            assertBusy(() -> {
                Long hits = client().prepareSearch(index)
                    .setQuery(matchAllQuery())
                    .setSize((int) finalTotalDocs)
                    .storedFields()
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits().value;

                assertEquals(snapshots.get(snapshot), hits);
            });
        }
    }

    public void testHashedPrefixTranslogMetadataCombination() throws Exception {
        Settings settings = Settings.builder()
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), randomFrom(RemoteStoreEnums.PathType.values()))
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_METADATA.getKey(), randomBoolean())
            .build();

        internalCluster().startClusterManagerOnlyNode(settings);
        internalCluster().startDataOnlyNode(settings);
        String index = "test-index";
        String snapshotRepo = "test-restore-snapshot-repo";
        String baseSnapshotName = "snapshot_";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        createRepository(snapshotRepo, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        Settings indexSettings = Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        createIndex(index, indexSettings);
        ensureGreen(index);

        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
            RemoteStorePinnedTimestampService.class,
            primaryNodeName(index)
        );

        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueSeconds(randomIntBetween(1, 5)));
        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.timeValueSeconds(randomIntBetween(1, 5)));

        long totalDocs = 0;
        Map<String, Long> snapshots = new HashMap<>();
        int numDocs = randomIntBetween(200, 300);
        totalDocs += numDocs;
        try (BackgroundIndexer indexer = new BackgroundIndexer(index, MapperService.SINGLE_MAPPING_NAME, client(), numDocs)) {
            int numberOfSnapshots = 2;
            for (int i = 0; i < numberOfSnapshots; i++) {
                logger.info("--> waiting for {} docs to be indexed ...", numDocs);
                long finalTotalDocs1 = totalDocs;
                assertBusy(() -> assertEquals(finalTotalDocs1, indexer.totalIndexedDocs()), 120, TimeUnit.SECONDS);
                logger.info("--> {} total docs indexed", totalDocs);
                String snapshotName = baseSnapshotName + i;
                createSnapshot(snapshotRepo, snapshotName, new ArrayList<>());
                snapshots.put(snapshotName, totalDocs);
                if (i < numberOfSnapshots - 1) {
                    numDocs = randomIntBetween(200, 300);
                    indexer.continueIndexing(numDocs);
                    totalDocs += numDocs;
                }
            }
        }

        logger.info("Snapshots Status: " + snapshots);

        for (String snapshot : snapshots.keySet()) {
            logger.info("Restoring snapshot: {}", snapshot);

            if (randomBoolean()) {
                assertAcked(client().admin().indices().delete(new DeleteIndexRequest(index)).get());
            } else {
                assertAcked(client().admin().indices().prepareClose(index));
            }

            assertTrue(
                internalCluster().client()
                    .admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(
                        Settings.builder()
                            .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), randomFrom(RemoteStoreEnums.PathType.values()))
                            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_METADATA.getKey(), randomBoolean())
                    )
                    .get()
                    .isAcknowledged()
            );

            RestoreSnapshotResponse restoreSnapshotResponse1 = client.admin()
                .cluster()
                .prepareRestoreSnapshot(snapshotRepo, snapshot)
                .setWaitForCompletion(true)
                .setIndices()
                .get();

            assertEquals(RestStatus.OK, restoreSnapshotResponse1.status());

            // Verify restored index's stats
            ensureGreen(TimeValue.timeValueSeconds(60), index);
            long finalTotalDocs = totalDocs;
            assertBusy(() -> {
                Long hits = client().prepareSearch(index)
                    .setQuery(matchAllQuery())
                    .setSize((int) finalTotalDocs)
                    .storedFields()
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits().value;

                assertEquals(snapshots.get(snapshot), hits);
            });
        }
    }
}
