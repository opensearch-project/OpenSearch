/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DeleteSnapshotV2IT extends AbstractSnapshotIntegTestCase {

    private static final String REMOTE_REPO_NAME = "remote-store-repo-name";

    public void testDeleteShallowCopyV2() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");

        final Path remoteStoreRepoPath = randomRepoPath();
        internalCluster().startClusterManagerOnlyNode(snapshotV2Settings(remoteStoreRepoPath));

        internalCluster().startDataOnlyNode(snapshotV2Settings(remoteStoreRepoPath));
        internalCluster().startDataOnlyNode(snapshotV2Settings(remoteStoreRepoPath));

        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String indexName3 = "testindex3";
        String snapshotRepoName = "test-create-snapshot-repo";
        String snapshotName1 = "test-create-snapshot1";
        String snapshotName2 = "test-create-snapshot2";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Client client = client();

        assertAcked(
            client.admin()
                .cluster()
                .preparePutRepository(snapshotRepoName)
                .setType(FsRepository.TYPE)
                .setSettings(
                    Settings.builder()
                        .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
                        .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
                        .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                        .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
                        .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true)
                )
        );

        createIndex(indexName1, getRemoteStoreBackedIndexSettings());
        createIndex(indexName2, getRemoteStoreBackedIndexSettings());

        final int numDocsInIndex1 = 10;
        final int numDocsInIndex2 = 20;
        indexRandomDocs(indexName1, numDocsInIndex1);
        indexRandomDocs(indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(true)
            .get();
        SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.snapshotId().getName(), equalTo(snapshotName1));

        createIndex(indexName3, getRemoteStoreBackedIndexSettings());
        indexRandomDocs(indexName3, 10);
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

        assertAcked(client().admin().indices().prepareDelete(indexName1));
        Thread.sleep(100);

        AcknowledgedResponse deleteResponse = client().admin()
            .cluster()
            .prepareDeleteSnapshot(snapshotRepoName, snapshotName2)
            .setSnapshots(snapshotName2)
            .get();
        assertTrue(deleteResponse.isAcknowledged());

        // test delete non-existent snapshot
        assertThrows(
            SnapshotMissingException.class,
            () -> client().admin().cluster().prepareDeleteSnapshot(snapshotRepoName, "random-snapshot").setSnapshots(snapshotName2).get()
        );

    }

    public void testDeleteShallowCopyV2MultipleSnapshots() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");
        final Path remoteStoreRepoPath = randomRepoPath();

        internalCluster().startClusterManagerOnlyNode(snapshotV2Settings(remoteStoreRepoPath));
        internalCluster().startDataOnlyNode(snapshotV2Settings(remoteStoreRepoPath));
        internalCluster().startDataOnlyNode(snapshotV2Settings(remoteStoreRepoPath));

        String indexName1 = "testindex1";
        String indexName2 = "testindex2";
        String indexName3 = "testindex3";
        String snapshotRepoName = "test-create-snapshot-repo";
        String snapshotName1 = "test-create-snapshot1";
        String snapshotName2 = "test-create-snapshot2";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        Client client = client();

        assertAcked(
            client.admin()
                .cluster()
                .preparePutRepository(snapshotRepoName)
                .setType(FsRepository.TYPE)
                .setSettings(
                    Settings.builder()
                        .put(FsRepository.LOCATION_SETTING.getKey(), absolutePath1)
                        .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
                        .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                        .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
                        .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true)
                )
        );

        createIndex(indexName1, getRemoteStoreBackedIndexSettings());

        createIndex(indexName2, getRemoteStoreBackedIndexSettings());

        final int numDocsInIndex1 = 10;
        final int numDocsInIndex2 = 20;
        indexRandomDocs(indexName1, numDocsInIndex1);
        indexRandomDocs(indexName2, numDocsInIndex2);
        ensureGreen(indexName1, indexName2);

        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, snapshotName1)
            .setWaitForCompletion(true)
            .get();
        SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.snapshotId().getName(), equalTo(snapshotName1));

        createIndex(indexName3, getRemoteStoreBackedIndexSettings());
        indexRandomDocs(indexName3, 10);

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

        AcknowledgedResponse deleteResponse = client().admin()
            .cluster()
            .prepareDeleteSnapshot(snapshotRepoName, snapshotName1, snapshotName2)
            .setSnapshots(snapshotName2)
            .get();
        assertTrue(deleteResponse.isAcknowledged());

        // test delete non-existent snapshot
        assertThrows(
            SnapshotMissingException.class,
            () -> client().admin().cluster().prepareDeleteSnapshot(snapshotRepoName, "random-snapshot").setSnapshots(snapshotName2).get()
        );

    }

    public void testRemoteStoreCleanupForDeletedIndexForSnapshotV2() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");
        final Path remoteStoreRepoPath = randomRepoPath();
        Settings settings = remoteStoreClusterSettings(REMOTE_REPO_NAME, remoteStoreRepoPath);
        settings = Settings.builder()
            .put(settings)
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.getKey(), true)
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), RemoteStoreEnums.PathType.FIXED.toString())
            .build();
        String clusterManagerName = internalCluster().startClusterManagerOnlyNode(settings);
        internalCluster().startDataOnlyNode(settings);
        final Client clusterManagerClient = internalCluster().clusterManagerClient();
        ensureStableCluster(2);

        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
            RemoteStorePinnedTimestampService.class,
            clusterManagerName
        );
        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueSeconds(1));

        final String snapshotRepoName = "snapshot-repo-name";
        final Path snapshotRepoPath = randomRepoPath();
        createRepository(snapshotRepoName, "mock", snapshotRepoSettingsForShallowV2(snapshotRepoPath));

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        indexRandomDocs(remoteStoreEnabledIndexName, 5);

        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(remoteStoreEnabledIndexName)
            .get()
            .getSetting(remoteStoreEnabledIndexName, IndexMetadata.SETTING_INDEX_UUID);

        String numShards = client().admin()
            .indices()
            .prepareGetSettings(remoteStoreEnabledIndexName)
            .get()
            .getSetting(remoteStoreEnabledIndexName, IndexMetadata.SETTING_NUMBER_OF_SHARDS);

        logger.info("--> create two remote index shallow snapshots");
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, "snap1")
            .setWaitForCompletion(true)
            .get();
        SnapshotInfo snapshotInfo1 = createSnapshotResponse.getSnapshotInfo();

        indexRandomDocs(remoteStoreEnabledIndexName, 25);

        CreateSnapshotResponse createSnapshotResponse2 = client().admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, "snap2")
            .setWaitForCompletion(true)
            .get();
        SnapshotInfo snapshotInfo2 = createSnapshotResponse2.getSnapshotInfo();
        assertThat(snapshotInfo2.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo2.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo2.successfulShards(), equalTo(snapshotInfo2.totalShards()));
        assertThat(snapshotInfo2.snapshotId().getName(), equalTo("snap2"));

        // delete remote store index
        assertAcked(client().admin().indices().prepareDelete(remoteStoreEnabledIndexName));

        logger.info("--> delete snapshot 2");

        Path indexPath = Path.of(String.valueOf(remoteStoreRepoPath), indexUUID);
        Path shardPath = Path.of(String.valueOf(indexPath), "0");
        Path segmentsPath = Path.of(String.valueOf(shardPath), "segments");
        Path translogPath = Path.of(String.valueOf(shardPath), "translog");

        // Get total segments remote store directory file count for deleted index and shard 0
        int segmentFilesCountBeforeDeletingSnapshot1 = RemoteStoreBaseIntegTestCase.getFileCount(segmentsPath);
        int translogFilesCountBeforeDeletingSnapshot1 = RemoteStoreBaseIntegTestCase.getFileCount(translogPath);

        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);

        AcknowledgedResponse deleteSnapshotResponse = clusterManagerClient.admin()
            .cluster()
            .prepareDeleteSnapshot(snapshotRepoName, snapshotInfo2.snapshotId().getName())
            .get();
        assertAcked(deleteSnapshotResponse);

        Thread.sleep(5000);

        assertBusy(() -> {
            try {
                assertThat(RemoteStoreBaseIntegTestCase.getFileCount(segmentsPath), lessThan(segmentFilesCountBeforeDeletingSnapshot1));
            } catch (Exception e) {}
        }, 30, TimeUnit.SECONDS);
        int segmentFilesCountAfterDeletingSnapshot1 = RemoteStoreBaseIntegTestCase.getFileCount(segmentsPath);

        logger.info("--> delete snapshot 1");
        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);
        // on snapshot deletion, remote store segment files should get cleaned up for deleted index - `remote-index-1`
        deleteSnapshotResponse = clusterManagerClient.admin()
            .cluster()
            .prepareDeleteSnapshot(snapshotRepoName, snapshotInfo1.snapshotId().getName())
            .get();
        assertAcked(deleteSnapshotResponse);

        // Delete is async. Give time for it
        assertBusy(() -> {
            try {
                assertThat(RemoteStoreBaseIntegTestCase.getFileCount(segmentsPath), lessThan(segmentFilesCountAfterDeletingSnapshot1));
            } catch (Exception e) {}
        }, 60, TimeUnit.SECONDS);

        assertBusy(() -> {
            try {
                assertThat(RemoteStoreBaseIntegTestCase.getFileCount(translogPath), lessThan(translogFilesCountBeforeDeletingSnapshot1));
            } catch (Exception e) {}
        }, 60, TimeUnit.SECONDS);

    }

    private Settings snapshotV2Settings(Path remoteStoreRepoPath) {
        Settings settings = Settings.builder()
            .put(remoteStoreClusterSettings(REMOTE_REPO_NAME, remoteStoreRepoPath))
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.getKey(), true)
            .build();
        return settings;
    }

    protected Settings.Builder snapshotRepoSettingsForShallowV2(Path path) {
        final Settings.Builder settings = Settings.builder();
        settings.put("location", path);
        settings.put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), Boolean.TRUE);
        settings.put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        return settings;
    }
}
