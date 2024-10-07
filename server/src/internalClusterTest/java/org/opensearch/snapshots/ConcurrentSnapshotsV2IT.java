/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.remotestore.RemoteSnapshotIT;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ConcurrentSnapshotsV2IT extends RemoteSnapshotIT {

    public void testLongRunningSnapshotDontAllowConcurrentSnapshot() throws Exception {
        final String clusterManagerName = internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String repoName = "test-create-snapshot-repo";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        createRepository(repoName, "mock", settings);

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

        blockClusterManagerOnWriteIndexFile(repoName);

        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, "snapshot-queued");
        awaitNumberOfSnapshotsInProgress(1);

        try {
            String snapshotName = "snapshot-concurrent";
            client().admin().cluster().prepareCreateSnapshot(repoName, snapshotName).setWaitForCompletion(true).get();
            fail();
        } catch (Exception e) {}

        unblockNode(repoName, clusterManagerName);
        CreateSnapshotResponse csr = snapshotFuture.actionGet();
        List<SnapshotInfo> snapInfo = client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots();
        assertEquals(1, snapInfo.size());
        assertThat(snapInfo, contains(csr.getSnapshotInfo()));
    }

    public void testCreateSnapshotFailInFinalize() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String repoName = "test-create-snapshot-repo";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        createRepository(repoName, "mock", settings);

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

        blockClusterManagerFromFinalizingSnapshotOnIndexFile(repoName);
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, "snapshot-queued");
        awaitNumberOfSnapshotsInProgress(1);
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));
        unblockNode(repoName, clusterManagerNode);
        expectThrows(SnapshotException.class, snapshotFuture::actionGet);

        final ActionFuture<CreateSnapshotResponse> snapshotFuture2 = startFullSnapshot(repoName, "snapshot-success");
        // Second create works out cleanly since the repo
        CreateSnapshotResponse csr = snapshotFuture2.actionGet();

        List<SnapshotInfo> snapInfo = client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots();
        assertEquals(1, snapInfo.size());
        assertThat(snapInfo, contains(csr.getSnapshotInfo()));
    }

    public void testCreateSnapshotV2MasterSwitch() throws Exception {
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());

        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String repoName = "test-create-snapshot-repo";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        createRepository(repoName, "mock", settings);

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

        String clusterManagerNode = internalCluster().getClusterManagerName();

        blockClusterManagerFromFinalizingSnapshotOnIndexFile(repoName);
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, "snapshot-queued");
        awaitNumberOfSnapshotsInProgress(1);
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));

        // Fail the cluster manager
        stopNode(clusterManagerNode);

        ensureGreen();

        final ActionFuture<CreateSnapshotResponse> snapshotFuture2 = startFullSnapshot(repoName, "snapshot-success");
        // Second create works out cleanly since the repo
        CreateSnapshotResponse csr = snapshotFuture2.actionGet();

        List<SnapshotInfo> snapInfo = client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots();
        assertEquals(1, snapInfo.size());
        assertThat(snapInfo, contains(csr.getSnapshotInfo()));

    }

    public void testPinnedTimestampFailSnapshot() throws Exception {
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String repoName = "test-create-snapshot-repo";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        createRepository(repoName, "mock", settings);

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

        // fail segment repo - this is to fail the timestamp pinning
        setFailRate(BASE_REMOTE_REPO, 100);

        try {
            String snapshotName = "snapshot-fail";
            CreateSnapshotResponse createSnapshotResponse = client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true)
                .get();
            fail();
        } catch (Exception e) {}

        setFailRate(BASE_REMOTE_REPO, 0);
        String snapshotName = "snapshot-success";
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .get();

        List<SnapshotInfo> snapInfo = client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots();
        assertEquals(1, snapInfo.size());
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
                    CreateSnapshotResponse createSnapshotResponse2 = client().admin()
                        .cluster()
                        .prepareCreateSnapshot(snapshotRepoName, snapshotName)
                        .setWaitForCompletion(true)
                        .get();
                    SnapshotInfo snapshotInfo = createSnapshotResponse2.getSnapshotInfo();
                    assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
                    assertThat(snapshotInfo.successfulShards(), greaterThan(0));
                    assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));
                    assertThat(snapshotInfo.snapshotId().getName(), equalTo(snapshotName));
                    assertThat(snapshotInfo.getPinnedTimestamp(), greaterThan(0L));
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

        // Validate that only one snapshot has been created
        Repository repository = internalCluster().getInstance(RepositoriesService.class).repository(snapshotRepoName);
        PlainActionFuture<RepositoryData> repositoryDataPlainActionFuture = new PlainActionFuture<>();
        repository.getRepositoryData(repositoryDataPlainActionFuture);

        RepositoryData repositoryData = repositoryDataPlainActionFuture.get();
        assertThat(repositoryData.getSnapshotIds().size(), greaterThanOrEqualTo(1));
    }

    public void testLongRunningSnapshotDontAllowConcurrentClone() throws Exception {
        final String clusterManagerName = internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String repoName = "test-create-snapshot-repo";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        createRepository(repoName, "mock", settings);

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

        String sourceSnap = "snapshot-source";

        final CreateSnapshotResponse csr = startFullSnapshot(repoName, sourceSnap).actionGet();
        blockClusterManagerOnWriteIndexFile(repoName);

        final ActionFuture<AcknowledgedResponse> snapshotFuture = startCloneSnapshot(repoName, sourceSnap, "snapshot-clone");
        awaitNumberOfSnapshotsInProgress(1);

        final ActionFuture<AcknowledgedResponse> snapshotFuture2 = startCloneSnapshot(repoName, sourceSnap, "snapshot-clone-2");
        assertThrows(ConcurrentSnapshotExecutionException.class, snapshotFuture2::actionGet);

        unblockNode(repoName, clusterManagerName);
        assertThrows(SnapshotException.class, snapshotFuture2::actionGet);

        snapshotFuture.get();

        List<SnapshotInfo> snapInfo = client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots();
        assertEquals(2, snapInfo.size());
    }

    public void testCloneSnapshotFailInFinalize() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String repoName = "test-create-snapshot-repo";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        createRepository(repoName, "mock", settings);

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

        String sourceSnap = "snapshot-source";
        CreateSnapshotResponse sourceResp = startFullSnapshot(repoName, sourceSnap).actionGet();

        blockClusterManagerFromFinalizingSnapshotOnIndexFile(repoName);
        final ActionFuture<AcknowledgedResponse> snapshotFuture = startCloneSnapshot(repoName, sourceSnap, "snapshot-queued");
        awaitNumberOfSnapshotsInProgress(1);
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));
        unblockNode(repoName, clusterManagerNode);
        assertThrows(SnapshotException.class, snapshotFuture::actionGet);

        final ActionFuture<CreateSnapshotResponse> snapshotFuture2 = startFullSnapshot(repoName, "snapshot-success");
        // Second create works out cleanly since the repo is cleaned up
        CreateSnapshotResponse csr = snapshotFuture2.actionGet();

        List<SnapshotInfo> snapInfo = client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots();
        assertEquals(2, snapInfo.size());
        assertThat(snapInfo, containsInAnyOrder(csr.getSnapshotInfo(), sourceResp.getSnapshotInfo()));
    }

    public void testCloneSnapshotV2MasterSwitch() throws Exception {
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());

        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String repoName = "test-create-snapshot-repo";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        createRepository(repoName, "mock", settings);

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

        String sourceSnap = "snapshot-source";
        CreateSnapshotResponse csr = startFullSnapshot(repoName, sourceSnap).actionGet();

        String clusterManagerNode = internalCluster().getClusterManagerName();

        blockClusterManagerFromFinalizingSnapshotOnIndexFile(repoName);
        final ActionFuture<AcknowledgedResponse> snapshotFuture = startCloneSnapshot(repoName, sourceSnap, "snapshot-queued");
        awaitNumberOfSnapshotsInProgress(1);
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));

        // Fail the cluster manager
        stopNode(clusterManagerNode);

        ensureGreen();

        final ActionFuture<CreateSnapshotResponse> snapshotFuture2 = startFullSnapshot(repoName, "snapshot-success");
        // Second create works out cleanly since the repo
        CreateSnapshotResponse csr2 = snapshotFuture2.actionGet();
        List<SnapshotInfo> snapInfo = client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots();
        assertEquals(2, snapInfo.size());
        assertThat(snapInfo, containsInAnyOrder(csr.getSnapshotInfo(), csr2.getSnapshotInfo()));
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/pull/16191")
    public void testDeleteWhileV2CreateOngoing() throws Exception {
        final String clusterManagerName = internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String repoName = "test-create-snapshot-repo";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), false);
        createRepository(repoName, "mock", settings);

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

        startFullSnapshot(repoName, "snapshot-v1").actionGet();

        // Creating a v2 repo
        settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        createRepository(repoName, "mock", settings);

        blockClusterManagerOnWriteIndexFile(repoName);

        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, "snapshot-v2");
        awaitNumberOfSnapshotsInProgress(1);

        ActionFuture<AcknowledgedResponse> a = startDeleteSnapshot(repoName, "snapshot-v1");

        unblockNode(repoName, clusterManagerName);
        CreateSnapshotResponse csr = snapshotFuture.actionGet();
        assertTrue(csr.getSnapshotInfo().getPinnedTimestamp() != 0);
        assertTrue(a.actionGet().isAcknowledged());
        List<SnapshotInfo> snapInfo = client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots();
        assertEquals(1, snapInfo.size());
        assertThat(snapInfo, contains(csr.getSnapshotInfo()));
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/pull/16191")
    public void testDeleteAndCloneV1WhileV2CreateOngoing() throws Exception {
        final String clusterManagerName = internalCluster().startClusterManagerOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        internalCluster().startDataOnlyNode(pinnedTimestampSettings());
        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String repoName = "test-create-snapshot-repo";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Settings.Builder settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), false);
        createRepository(repoName, "mock", settings);

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

        startFullSnapshot(repoName, "snapshot-v1").actionGet();
        startFullSnapshot(repoName, "snapshot-v1-2").actionGet();

        // Creating a v2 repo
        settings = Settings.builder()
            .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
            .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
            .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        createRepository(repoName, "mock", settings);

        blockClusterManagerOnWriteIndexFile(repoName);

        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, "snapshot-v2");
        awaitNumberOfSnapshotsInProgress(1);

        ActionFuture<AcknowledgedResponse> startDeleteSnapshot = startDeleteSnapshot(repoName, "snapshot-v1");
        ActionFuture<AcknowledgedResponse> startCloneSnapshot = startCloneSnapshot(repoName, "snapshot-v1-2", "snapshot-v1-2-clone");

        unblockNode(repoName, clusterManagerName);
        CreateSnapshotResponse csr = snapshotFuture.actionGet();
        assertTrue(csr.getSnapshotInfo().getPinnedTimestamp() != 0);
        assertTrue(startDeleteSnapshot.actionGet().isAcknowledged());
        assertTrue(startCloneSnapshot.actionGet().isAcknowledged());
        List<SnapshotInfo> snapInfo = client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots();
        assertEquals(3, snapInfo.size());
    }

    protected ActionFuture<AcknowledgedResponse> startCloneSnapshot(String repoName, String sourceSnapshotName, String snapshotName) {
        logger.info("--> creating full snapshot [{}] to repo [{}]", snapshotName, repoName);
        return clusterAdmin().prepareCloneSnapshot(repoName, sourceSnapshotName, snapshotName).setIndices("*").execute();
    }
}
