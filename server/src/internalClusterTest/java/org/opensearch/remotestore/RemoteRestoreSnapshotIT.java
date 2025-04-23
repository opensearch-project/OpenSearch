/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotRestoreException;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.Requests;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY;
import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING;
import static org.opensearch.snapshots.SnapshotsService.getPinningEntity;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteRestoreSnapshotIT extends RemoteSnapshotIT {

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

        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName1, new ArrayList<>(Arrays.asList(indexName1, indexName2)));
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

    /**
     * In this test, we validate presence of remote_store custom data in index metadata for standard index creation and
     * on snapshot restore.
     */
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
            .setTransientSettings(Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.FIXED))
            .get();
        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));
        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName1, indexSettings);

        indexDocuments(client, indexName1, randomIntBetween(5, 10));
        ensureGreen(indexName1);
        validatePathType(indexName1, PathType.FIXED);

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
        validatePathType(restoredIndexName1version1, PathType.FIXED);

        client(clusterManagerNode).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX))
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
        validatePathType(restoredIndexName1version2, PathType.HASHED_PREFIX, PathHashAlgorithm.FNV_1A_COMPOSITE_1);

        // Create index with cluster setting cluster.remote_store.index.path.type as hashed_prefix.
        indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName2, indexSettings);
        ensureGreen(indexName2);
        validatePathType(indexName2, PathType.HASHED_PREFIX, PathHashAlgorithm.FNV_1A_COMPOSITE_1);

        // Validating that custom data has not changed for indexes which were created before the cluster setting got updated
        validatePathType(indexName1, PathType.FIXED);

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
            .setTransientSettings(Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), PathType.FIXED))
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
        validatePathType(indexName2, PathType.HASHED_PREFIX, PathHashAlgorithm.FNV_1A_COMPOSITE_1);
    }

    private void validatePathType(String index, PathType pathType) {
        validatePathType(index, pathType, null);
    }

    private void validatePathType(String index, PathType pathType, @Nullable PathHashAlgorithm pathHashAlgorithm) {
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        // Validate that the remote_store custom data is present in index metadata for the created index.
        Map<String, String> remoteCustomData = state.metadata().index(index).getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY);
        assertNotNull(remoteCustomData);
        assertEquals(pathType.name(), remoteCustomData.get(PathType.NAME));
        if (Objects.nonNull(pathHashAlgorithm)) {
            assertEquals(pathHashAlgorithm.name(), remoteCustomData.get(PathHashAlgorithm.NAME));
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

    public void testSuccessfulIndexRestoredFromSnapshotWithUpdatedSetting() throws IOException, ExecutionException, InterruptedException {
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

        // try index restore with index.number_of_replicas setting modified. index.number_of_replicas can be modified on restore
        Settings numberOfReplicasSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build();

        RestoreSnapshotResponse restoreSnapshotResponse1 = client().admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(false)
            .setIndexSettings(numberOfReplicasSettings)
            .setIndices(indexName1)
            .get();

        assertEquals(restoreSnapshotResponse1.status(), RestStatus.ACCEPTED);
        ensureGreen(indexName1);
        assertDocsPresentInIndex(client(), indexName1, numDocsInIndex1);
    }

    protected IndexShard getIndexShard(String node, String indexName) {
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

        // try index restore with index.remote_store.enabled ignored
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

        // try index restore with index.remote_store.segment.repository ignored
        exception = expectThrows(
            SnapshotRestoreException.class,
            () -> client().admin()
                .cluster()
                .prepareRestoreSnapshot(snapshotRepo, snapshotName1)
                .setWaitForCompletion(false)
                .setIgnoreIndexSettings(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY)
                .setIndices(index)
                .setRenamePattern(index)
                .setRenameReplacement(restoredIndex)
                .get()
        );
        assertTrue(exception.getMessage().contains("cannot remove setting [index.remote_store.segment.repository] on restore"));

        // try index restore with index.remote_store.translog.repository ignored
        exception = expectThrows(
            SnapshotRestoreException.class,
            () -> client().admin()
                .cluster()
                .prepareRestoreSnapshot(snapshotRepo, snapshotName1)
                .setWaitForCompletion(false)
                .setIgnoreIndexSettings(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY)
                .setIndices(index)
                .setRenamePattern(index)
                .setRenameReplacement(restoredIndex)
                .get()
        );
        assertTrue(exception.getMessage().contains("cannot remove setting [index.remote_store.translog.repository] on restore"));

        // try index restore with index.remote_store.segment.repository and index.remote_store.translog.repository ignored
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

        // try index restore with index.remote_store.enabled modified
        Settings remoteStoreIndexSettings = Settings.builder().put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, false).build();

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
        assertTrue(exception.getMessage().contains("cannot modify setting [index.remote_store.enabled]" + " on restore"));

        // try index restore with index.remote_store.segment.repository modified
        Settings remoteStoreSegmentIndexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, newRemoteStoreRepo)
            .build();

        exception = expectThrows(
            SnapshotRestoreException.class,
            () -> client().admin()
                .cluster()
                .prepareRestoreSnapshot(snapshotRepo, snapshotName1)
                .setWaitForCompletion(false)
                .setIndexSettings(remoteStoreSegmentIndexSettings)
                .setIndices(index)
                .setRenamePattern(index)
                .setRenameReplacement(restoredIndex)
                .get()
        );
        assertTrue(exception.getMessage().contains("cannot modify setting [index.remote_store.segment.repository]" + " on restore"));

        // try index restore with index.remote_store.translog.repository modified
        Settings remoteStoreTranslogIndexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, newRemoteStoreRepo)
            .build();

        exception = expectThrows(
            SnapshotRestoreException.class,
            () -> client().admin()
                .cluster()
                .prepareRestoreSnapshot(snapshotRepo, snapshotName1)
                .setWaitForCompletion(false)
                .setIndexSettings(remoteStoreTranslogIndexSettings)
                .setIndices(index)
                .setRenamePattern(index)
                .setRenameReplacement(restoredIndex)
                .get()
        );
        assertTrue(exception.getMessage().contains("cannot modify setting [index.remote_store.translog.repository]" + " on restore"));

        // try index restore with index.remote_store.translog.repository and index.remote_store.segment.repository modified
        Settings multipleRemoteStoreIndexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, newRemoteStoreRepo)
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, newRemoteStoreRepo)
            .build();

        exception = expectThrows(
            SnapshotRestoreException.class,
            () -> client().admin()
                .cluster()
                .prepareRestoreSnapshot(snapshotRepo, snapshotName1)
                .setWaitForCompletion(false)
                .setIndexSettings(multipleRemoteStoreIndexSettings)
                .setIndices(index)
                .setRenamePattern(index)
                .setRenameReplacement(restoredIndex)
                .get()
        );
        assertTrue(exception.getMessage().contains("cannot modify setting [index.remote_store.segment.repository]" + " on restore"));
    }

    public void testCreateSnapshotV2_Orphan_Timestamp_Cleanup() throws Exception {
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String snapshotRepoName = "test-create-snapshot-repo";
        String snapshotName1 = "test-create-snapshot1";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);

        createRepository(snapshotRepoName, FsRepository.TYPE, settings);

        Client client = client();
        Settings indexSettings = getIndexSettings(20, 0).build();
        createIndex(indexName1, indexSettings);

        Settings indexSettings2 = getIndexSettings(15, 0).build();
        createIndex(indexName2, indexSettings2);

        final int numDocsInIndex1 = 10;
        final int numDocsInIndex2 = 20;
        indexDocuments(client, indexName1, numDocsInIndex1);
        indexDocuments(client, indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        // create an orphan timestamp related to this repo
        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
            RemoteStorePinnedTimestampService.class,
            internalCluster().getClusterManagerName()
        );
        forceSyncPinnedTimestamps();

        long pinnedTimestamp = System.currentTimeMillis();
        final CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<Void> latchedActionListener = new LatchedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {}
        }, latch);

        remoteStorePinnedTimestampService.pinTimestamp(
            pinnedTimestamp,
            getPinningEntity(snapshotRepoName, "some_uuid"),
            latchedActionListener
        );
        latch.await();

        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName1, Collections.emptyList());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.getPinnedTimestamp(), greaterThan(0L));

        waitUntil(() -> 1 == RemoteStorePinnedTimestampService.getPinnedEntities().size());
    }

    public void testMixedSnapshotCreationWithV2RepositorySetting() throws Exception {

        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String indexName3 = "testindex3";
        String snapshotRepoName = "test-create-snapshot-repo";
        String snapshotName1 = "test-create-snapshot-v1";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), false);
        createRepository(snapshotRepoName, FsRepository.TYPE, settings);

        Client client = client();
        Settings indexSettings = getIndexSettings(20, 0).build();
        createIndex(indexName1, indexSettings);

        Settings indexSettings2 = getIndexSettings(15, 0).build();
        createIndex(indexName2, indexSettings2);

        final int numDocsInIndex1 = 10;
        final int numDocsInIndex2 = 20;
        indexDocuments(client, indexName1, numDocsInIndex1);
        indexDocuments(client, indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName1, Collections.emptyList());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.getPinnedTimestamp(), equalTo(0L));

        // enable shallow_snapshot_v2
        settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        createRepository(snapshotRepoName, FsRepository.TYPE, settings);

        indexDocuments(client, indexName1, 10);
        indexDocuments(client, indexName2, 20);

        createIndex(indexName3, indexSettings);
        indexDocuments(client, indexName3, 10);

        String snapshotName2 = "test-create-snapshot-v2";

        // verify even if waitForCompletion is not true, the request executes in a sync manner
        CreateSnapshotResponse createSnapshotResponse2 = client().admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, snapshotName2)
            .setWaitForCompletion(true)
            .get();
        snapshotInfo = createSnapshotResponse2.getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.snapshotId().getName(), equalTo(snapshotName2));
        assertThat(snapshotInfo.getPinnedTimestamp(), greaterThan(0L));
        forceSyncPinnedTimestamps();
        assertEquals(RemoteStorePinnedTimestampService.getPinnedEntities().size(), 1);
    }

    public void testConcurrentSnapshotV2CreateOperation() throws InterruptedException, ExecutionException {
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String snapshotRepoName = "test-create-snapshot-repo";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        createRepository(snapshotRepoName, FsRepository.TYPE, settings);

        Client client = client();
        Settings indexSettings = getIndexSettings(20, 0).build();
        createIndex(indexName1, indexSettings);

        Settings indexSettings2 = getIndexSettings(15, 0).build();
        createIndex(indexName2, indexSettings2);

        final int numDocsInIndex1 = 10;
        final int numDocsInIndex2 = 20;
        indexDocuments(client, indexName1, numDocsInIndex1);
        indexDocuments(client, indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        int concurrentSnapshots = 5;

        // Prepare threads for concurrent snapshot creation
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < concurrentSnapshots; i++) {
            int snapshotIndex = i;
            Thread thread = new Thread(() -> {
                try {
                    String snapshotName = "snapshot-concurrent-" + snapshotIndex;
                    client().admin().cluster().prepareCreateSnapshot(snapshotRepoName, snapshotName).setWaitForCompletion(true).get();
                    logger.info("Snapshot completed {}", snapshotName);
                } catch (Exception e) {}
            });
            threads.add(thread);
        }
        // start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Sleeping 10 sec for earlier created snapshot to complete runNextQueuedOperation and be ready for next snapshot
        // We can't put `waitFor` since we don't have visibility on its completion
        Thread.sleep(TimeValue.timeValueSeconds(10).seconds());
        client().admin().cluster().prepareCreateSnapshot(snapshotRepoName, "snapshot-cleanup-timestamp").setWaitForCompletion(true).get();
        Repository repository = internalCluster().getInstance(RepositoriesService.class).repository(snapshotRepoName);
        PlainActionFuture<RepositoryData> repositoryDataPlainActionFuture = new PlainActionFuture<>();
        repository.getRepositoryData(repositoryDataPlainActionFuture);
        RepositoryData repositoryData = repositoryDataPlainActionFuture.get();
        assertThat(repositoryData.getSnapshotIds().size(), greaterThanOrEqualTo(2));
        waitUntil(() -> {
            forceSyncPinnedTimestamps();
            return RemoteStorePinnedTimestampService.getPinnedEntities().size() == repositoryData.getSnapshotIds().size();
        });
    }

    public void testConcurrentSnapshotV2CreateOperation_MasterChange() throws Exception {
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String snapshotRepoName = "test-create-snapshot-repo";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        createRepository(snapshotRepoName, FsRepository.TYPE, settings);

        Client client = client();
        Settings indexSettings = getIndexSettings(20, 0).build();
        createIndex(indexName1, indexSettings);

        Settings indexSettings2 = getIndexSettings(15, 0).build();
        createIndex(indexName2, indexSettings2);

        final int numDocsInIndex1 = 10;
        final int numDocsInIndex2 = 20;
        indexDocuments(client, indexName1, numDocsInIndex1);
        indexDocuments(client, indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        Thread thread = new Thread(() -> {
            try {
                String snapshotName = "snapshot-earlier-master";
                internalCluster().nonClusterManagerClient()
                    .admin()
                    .cluster()
                    .prepareCreateSnapshot(snapshotRepoName, snapshotName)
                    .setWaitForCompletion(true)
                    .setClusterManagerNodeTimeout(TimeValue.timeValueSeconds(60))
                    .get();

            } catch (Exception ignored) {}
        });
        thread.start();

        // stop existing master
        final String clusterManagerNode = internalCluster().getClusterManagerName();
        stopNode(clusterManagerNode);

        // Validate that we have greater one snapshot has been created
        String snapshotName = "new-snapshot";
        try {
            client().admin().cluster().prepareCreateSnapshot(snapshotRepoName, snapshotName).setWaitForCompletion(true).get();
        } catch (Exception e) {
            logger.info("Exception while creating new-snapshot", e);
        }

        AtomicLong totalSnaps = new AtomicLong();

        // Validate that snapshot is present in repository data
        assertBusy(() -> {
            GetSnapshotsRequest request = new GetSnapshotsRequest(snapshotRepoName);
            GetSnapshotsResponse response2 = client().admin().cluster().getSnapshots(request).actionGet();
            assertThat(response2.getSnapshots().size(), greaterThanOrEqualTo(1));
            totalSnaps.set(response2.getSnapshots().size());

        }, 30, TimeUnit.SECONDS);
        thread.join();
        forceSyncPinnedTimestamps();
        waitUntil(() -> {
            this.forceSyncPinnedTimestamps();
            return RemoteStorePinnedTimestampService.getPinnedEntities().size() == totalSnaps.intValue();
        });
    }

    public void testCreateSnapshotV2() throws Exception {
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String indexName3 = "testindex3";
        String snapshotRepoName = "test-create-snapshot-repo";
        String snapshotName1 = "test-create-snapshot1";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);

        createRepository(snapshotRepoName, FsRepository.TYPE, settings);

        Client client = client();
        Settings indexSettings = getIndexSettings(20, 0).build();
        createIndex(indexName1, indexSettings);

        Settings indexSettings2 = getIndexSettings(15, 0).build();
        createIndex(indexName2, indexSettings2);

        final int numDocsInIndex1 = 10;
        final int numDocsInIndex2 = 20;
        indexDocuments(client, indexName1, numDocsInIndex1);
        indexDocuments(client, indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName1, Collections.emptyList());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.getPinnedTimestamp(), greaterThan(0L));

        indexDocuments(client, indexName1, 10);
        indexDocuments(client, indexName2, 20);

        createIndex(indexName3, indexSettings);
        indexDocuments(client, indexName3, 10);

        String snapshotName2 = "test-create-snapshot2";

        // verify response status if waitForCompletion is not true
        RestStatus createSnapshotResponseStatus = client().admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, snapshotName2)
            .get()
            .status();
        assertEquals(RestStatus.ACCEPTED, createSnapshotResponseStatus);
        forceSyncPinnedTimestamps();
        assertEquals(2, RemoteStorePinnedTimestampService.getPinnedEntities().size());
    }

    public void forceSyncPinnedTimestamps() {
        // for all nodes , run forceSyncPinnedTimestamps()
        for (String node : internalCluster().getNodeNames()) {
            RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
                RemoteStorePinnedTimestampService.class,
                node
            );
            remoteStorePinnedTimestampService.forceSyncPinnedTimestamps();
        }
    }

    public void testCreateSnapshotV2WithRedIndex() throws Exception {
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String snapshotRepoName = "test-create-snapshot-repo";
        String snapshotName1 = "test-create-snapshot1";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        createRepository(snapshotRepoName, FsRepository.TYPE, settings);

        Client client = client();
        Settings indexSettings = getIndexSettings(20, 0).build();
        createIndex(indexName1, indexSettings);

        Settings indexSettings2 = getIndexSettings(15, 0).build();
        createIndex(indexName2, indexSettings2);

        final int numDocsInIndex1 = 10;
        final int numDocsInIndex2 = 20;
        indexDocuments(client, indexName1, numDocsInIndex1);
        indexDocuments(client, indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        internalCluster().ensureAtLeastNumDataNodes(0);
        ensureRed(indexName1);
        ensureRed(indexName2);
        CreateSnapshotResponse createSnapshotResponse2 = client().admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(true)
            .get();
        SnapshotInfo snapshotInfo = createSnapshotResponse2.getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.snapshotId().getName(), equalTo(snapshotName1));
        assertThat(snapshotInfo.getPinnedTimestamp(), greaterThan(0L));
    }

    public void testCreateSnapshotV2WithIndexingLoad() throws Exception {
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String snapshotRepoName = "test-create-snapshot-repo";
        String snapshotName1 = "test-create-snapshot1";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        createRepository(snapshotRepoName, FsRepository.TYPE, settings);

        Client client = client();
        Settings indexSettings = getIndexSettings(20, 0).build();
        createIndex(indexName1, indexSettings);

        Settings indexSettings2 = getIndexSettings(15, 0).build();
        createIndex(indexName2, indexSettings2);

        final int numDocsInIndex1 = 10;
        final int numDocsInIndex2 = 20;
        indexDocuments(client, indexName1, numDocsInIndex1);
        indexDocuments(client, indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        Thread indexingThread = new Thread(() -> {
            try {
                for (int i = 0; i < 50; i++) {
                    internalCluster().client().prepareIndex("test-index-load").setSource("field", "value" + i).execute().actionGet();
                }
            } catch (Exception e) {
                fail("indexing failed due to exception: " + e.getMessage());
            }
        });

        // Start indexing
        indexingThread.start();

        // Wait for a bit to let some documents be indexed
        Thread.sleep(1000);

        // Create a snapshot while indexing is ongoing
        CreateSnapshotResponse createSnapshotResponse2 = client().admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(true)
            .get();

        SnapshotInfo snapshotInfo = createSnapshotResponse2.getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.snapshotId().getName(), equalTo(snapshotName1));
        assertThat(snapshotInfo.getPinnedTimestamp(), greaterThan(0L));
        assertTrue(snapshotInfo.indices().contains("test-index-load"));
        assertTrue(snapshotInfo.indices().contains(indexName1));
        assertTrue(snapshotInfo.indices().contains(indexName2));
        indexingThread.join();

    }

    public void testCreateSnapshotV2WithShallowCopySettingDisabled() throws Exception {
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String snapshotRepoName = "test-create-snapshot-repo";
        String snapshotName1 = "test-create-snapshot1";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), false)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        createRepository(snapshotRepoName, FsRepository.TYPE, settings);

        Client client = client();
        Settings indexSettings = getIndexSettings(20, 0).build();
        createIndex(indexName1, indexSettings);

        Settings indexSettings2 = getIndexSettings(15, 0).build();
        createIndex(indexName2, indexSettings2);

        final int numDocsInIndex1 = 10;
        final int numDocsInIndex2 = 20;
        indexDocuments(client, indexName1, numDocsInIndex1);
        indexDocuments(client, indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        // Will create full copy snapshot if `REMOTE_STORE_INDEX_SHALLOW_COPY` is false but `SHALLOW_SNAPSHOT_V2` is true
        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName1, Collections.emptyList());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.getPinnedTimestamp(), equalTo(0L));

        // Validate that snapshot is present in repository data
        Repository repository = internalCluster().getInstance(RepositoriesService.class).repository(snapshotRepoName);
        PlainActionFuture<RepositoryData> repositoryDataPlainActionFuture = new PlainActionFuture<>();
        repository.getRepositoryData(repositoryDataPlainActionFuture);

        RepositoryData repositoryData = repositoryDataPlainActionFuture.get();
        assertTrue(repositoryData.getSnapshotIds().contains(snapshotInfo.snapshotId()));
    }

    public void testClusterManagerFailoverDuringSnapshotCreation() throws Exception {

        internalCluster().startClusterManagerOnlyNodes(3, pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String snapshotRepoName = "test-create-snapshot-repo";
        String snapshotName1 = "test-create-snapshot1";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        createRepository(snapshotRepoName, FsRepository.TYPE, settings);

        Client client = client();
        Settings indexSettings = getIndexSettings(20, 0).build();
        createIndex(indexName1, indexSettings);

        Settings indexSettings2 = getIndexSettings(15, 0).build();
        createIndex(indexName2, indexSettings2);

        final int numDocsInIndex1 = 10;
        final int numDocsInIndex2 = 20;
        indexDocuments(client, indexName1, numDocsInIndex1);
        indexDocuments(client, indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        ensureStableCluster(4, internalCluster().getClusterManagerName());

        final AtomicReference<SnapshotInfo> snapshotInfoRef = new AtomicReference<>();
        final AtomicBoolean snapshotFailed = new AtomicBoolean(false);
        Thread snapshotThread = new Thread(() -> {
            try {
                // Start snapshot creation
                CreateSnapshotResponse createSnapshotResponse = client().admin()
                    .cluster()
                    .prepareCreateSnapshot(snapshotRepoName, snapshotName1)
                    .setWaitForCompletion(true)
                    .get();
                snapshotInfoRef.set(createSnapshotResponse.getSnapshotInfo());

            } catch (Exception e) {
                snapshotFailed.set(true);
            }
        });
        snapshotThread.start();
        Thread.sleep(100);

        internalCluster().stopCurrentClusterManagerNode();

        // Wait for the cluster to elect a new Cluster Manager and stabilize
        ensureStableCluster(3, internalCluster().getClusterManagerName());

        // Wait for the snapshot thread to complete
        snapshotThread.join();

        // Validate that the snapshot was created or handled gracefully
        Repository repository = internalCluster().getInstance(RepositoriesService.class).repository(snapshotRepoName);
        PlainActionFuture<RepositoryData> repositoryDataPlainActionFuture = new PlainActionFuture<>();
        repository.getRepositoryData(repositoryDataPlainActionFuture);

        RepositoryData repositoryData = repositoryDataPlainActionFuture.get();
        SnapshotInfo snapshotInfo = snapshotInfoRef.get();
        if (snapshotFailed.get()) {
            assertTrue(repositoryData.getSnapshotIds().isEmpty());
        } else {
            assertTrue(repositoryData.getSnapshotIds().contains(snapshotInfo.snapshotId()));
        }
    }

    public void testConcurrentV1SnapshotAndV2RepoSettingUpdate() throws Exception {
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String snapshotRepoName = "test-create-snapshot-repo";
        String snapshotName1 = "test-create-snapshot-v1";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);
        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), false);
        createRepository(snapshotRepoName, FsRepository.TYPE, settings);

        Client client = client();
        Settings indexSettings = getIndexSettings(20, 0).build();

        for (int i = 0; i < 10; i++) {
            createIndex("index" + i, indexSettings);
        }
        ensureStableCluster(3);
        for (int i = 0; i < 10; i++) {
            indexDocuments(client, "index" + i, 15);
        }

        ensureStableCluster(3);
        for (int i = 0; i < 10; i++) {
            ensureGreen("index" + i);
        }
        final CreateSnapshotResponse[] snapshotV1Response = new CreateSnapshotResponse[1];
        // Create a separate thread to create the first snapshot
        Thread createV1SnapshotThread = new Thread(() -> {
            try {
                snapshotV1Response[0] = client().admin()
                    .cluster()
                    .prepareCreateSnapshot(snapshotRepoName, snapshotName1)
                    .setWaitForCompletion(true)
                    .get();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Create a separate thread to enable shallow_snapshot_v2
        Thread enableV2Thread = new Thread(() -> {
            try {

                assertThrows(
                    IllegalStateException.class,
                    () -> createRepository(
                        snapshotRepoName,
                        FsRepository.TYPE,
                        Settings.builder()
                            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
                            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
                            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
                            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true)
                    )
                );

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        createV1SnapshotThread.start();

        Thread.sleep(100);

        enableV2Thread.start();

        enableV2Thread.join();
        createV1SnapshotThread.join();
    }

}
