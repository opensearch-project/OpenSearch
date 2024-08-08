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
import org.opensearch.common.UUIDs;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.store.RemoteBufferedOutputDirectory;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.LOCK_FILES;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.comparesEqualTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DeleteSnapshotIT extends AbstractSnapshotIntegTestCase {

    private static final String REMOTE_REPO_NAME = "remote-store-repo-name";

    public void testDeleteSnapshot() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");
        final Path remoteStoreRepoPath = randomRepoPath();
        internalCluster().startClusterManagerOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME, remoteStoreRepoPath));
        internalCluster().startDataOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME, remoteStoreRepoPath));

        final String snapshotRepoName = "snapshot-repo-name";
        final Path snapshotRepoPath = randomRepoPath();
        createRepository(snapshotRepoName, "fs", snapshotRepoPath);

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        indexRandomDocs(remoteStoreEnabledIndexName, randomIntBetween(5, 10));

        final String snapshot = "snapshot";
        createFullSnapshot(snapshotRepoName, snapshot);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length == 0);
        assert (getRepositoryData(snapshotRepoName).getSnapshotIds().size() == 1);

        assertAcked(startDeleteSnapshot(snapshotRepoName, snapshot).get());
        assert (getRepositoryData(snapshotRepoName).getSnapshotIds().size() == 0);
    }

    public void testDeleteShallowCopySnapshot() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");
        final Path remoteStoreRepoPath = randomRepoPath();
        internalCluster().startClusterManagerOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME, remoteStoreRepoPath));
        internalCluster().startDataOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME, remoteStoreRepoPath));

        final String snapshotRepoName = "snapshot-repo-name";
        createRepository(snapshotRepoName, "fs", snapshotRepoSettingsForShallowCopy());

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        indexRandomDocs(remoteStoreEnabledIndexName, randomIntBetween(5, 10));

        final String shallowSnapshot = "shallow-snapshot";
        SnapshotInfo snapshotInfo = createFullSnapshot(snapshotRepoName, shallowSnapshot);
        assertEquals(1, getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length);
        RepositoryData repositoryData = getRepositoryData(snapshotRepoName);
        assertEquals(1, repositoryData.getSnapshotIds().size());
        assertSame(repositoryData.getSnapshotType(snapshotInfo.snapshotId()), SnapshotType.SHALLOW_COPY);

        // Delete snapshot
        assertAcked(startDeleteSnapshot(snapshotRepoName, shallowSnapshot).get());

        // Get updated repository data.
        repositoryData = getRepositoryData(snapshotRepoName);
        assertEquals(0, repositoryData.getSnapshotIds().size());
        assertEquals(0, getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length);
        assertNull(repositoryData.getSnapshotType(snapshotInfo.snapshotId()));
    }

    // Deleting multiple shallow copy snapshots as part of single delete call with repo having only shallow copy snapshots.
    public void testDeleteMultipleShallowCopySnapshotsCase1() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");
        final Path remoteStoreRepoPath = randomRepoPath();
        internalCluster().startClusterManagerOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME, remoteStoreRepoPath));
        internalCluster().startDataOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME, remoteStoreRepoPath));
        final Client clusterManagerClient = internalCluster().clusterManagerClient();
        ensureStableCluster(2);

        final String snapshotRepoName = "snapshot-repo-name";
        final Path snapshotRepoPath = randomRepoPath();
        createRepository(snapshotRepoName, "mock", snapshotRepoSettingsForShallowCopy(snapshotRepoPath));
        final String testIndex = "index-test";
        createIndexWithContent(testIndex);

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        indexRandomDocs(remoteStoreEnabledIndexName, randomIntBetween(5, 10));

        // Creating some shallow copy snapshots
        int totalShallowCopySnapshotsCount = randomIntBetween(4, 10);
        List<SnapshotId> shallowCopySnapshots = createNSnapshots(snapshotRepoName, totalShallowCopySnapshotsCount);
        List<SnapshotId> snapshotsToBeDeleted = shallowCopySnapshots.subList(0, randomIntBetween(2, totalShallowCopySnapshotsCount));
        int tobeDeletedSnapshotsCount = snapshotsToBeDeleted.size();
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length == totalShallowCopySnapshotsCount);

        RepositoryData repositoryData = getRepositoryData(snapshotRepoName);
        assert (repositoryData.getSnapshotIds().size() == totalShallowCopySnapshotsCount);

        validateSnapshotTypes(repositoryData, shallowCopySnapshots, null, null);
        // Deleting subset of shallow copy snapshots
        assertAcked(
            clusterManagerClient.admin()
                .cluster()
                .prepareDeleteSnapshot(snapshotRepoName, snapshotsToBeDeleted.stream().map(SnapshotId::getName).toArray(String[]::new))
                .get()
        );

        // getting updated repository data
        repositoryData = getRepositoryData(snapshotRepoName);
        for (SnapshotId snapshotId : shallowCopySnapshots) {
            if (snapshotsToBeDeleted.contains(snapshotId)) {
                assertNull(repositoryData.getSnapshotType(snapshotId));
            } else {
                assertEquals(repositoryData.getSnapshotType(snapshotId), SnapshotType.SHALLOW_COPY);
            }
        }
        validateSnapshotTypes(
            repositoryData,
            shallowCopySnapshots.stream().filter(snapId -> !snapshotsToBeDeleted.contains(snapId)).collect(Collectors.toList()),
            null,
            snapshotsToBeDeleted
        );
        assert (repositoryData.getSnapshotIds().size() == totalShallowCopySnapshotsCount - tobeDeletedSnapshotsCount);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length == totalShallowCopySnapshotsCount
            - tobeDeletedSnapshotsCount);
    }

    // Deleting multiple shallow copy snapshots as part of single delete call with both partial and full copy snapshot present in the repo
    // And then deleting multiple full copy snapshots as part of single delete call with both partial and shallow copy snapshots present in
    // the repo
    public void testDeleteMultipleShallowCopySnapshotsCase2() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");
        final Path remoteStoreRepoPath = randomRepoPath();
        internalCluster().startClusterManagerOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME, remoteStoreRepoPath));
        final String dataNode = internalCluster().startDataOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME, remoteStoreRepoPath));
        ensureStableCluster(2);
        final String clusterManagerNode = internalCluster().getClusterManagerName();

        final String snapshotRepoName = "snapshot-repo-name";
        final Path snapshotRepoPath = randomRepoPath();
        createRepository(snapshotRepoName, "mock", snapshotRepoSettingsForShallowCopy(snapshotRepoPath));
        final String testIndex = "remote-index-1";
        createIndexWithContent(testIndex);
        ensureGreen(testIndex);

        final String testIndex2 = "remote-index-2";
        createIndexWithContent(testIndex2);
        ensureGreen(testIndex, testIndex2);

        // Creating a partial shallow copy snapshot
        final String snapshot = "snapshot";

        final Client clusterManagerClient = internalCluster().clusterManagerClient();

        int partialShallowCopySnapshotCount = 1;
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = clusterManagerClient.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, snapshot)
            .setWaitForCompletion(true)
            .setPartial(true)
            .execute();

        blockNodeOnAnyFiles(snapshotRepoName, dataNode);
        awaitNumberOfSnapshotsInProgress(1);
        waitForBlock(dataNode, snapshotRepoName, TimeValue.timeValueSeconds(30L));
        internalCluster().restartNode(dataNode);
        SnapshotInfo partialSnapshotInfo = snapshotFuture.get().getSnapshotInfo();
        assertEquals(partialSnapshotInfo.state(), SnapshotState.PARTIAL);

        unblockAllDataNodes(snapshotRepoName);

        ensureStableCluster(2, clusterManagerNode);
        String[] initialLockFiles = getLockFilesInRemoteStore(testIndex2, REMOTE_REPO_NAME);

        // Creating some shallow copy snapshots
        int fullShallowCopySnapshotsCount = randomIntBetween(4, 10);
        List<SnapshotId> shallowCopySnapshots = createNSnapshots(snapshotRepoName, fullShallowCopySnapshotsCount);

        List<SnapshotId> shallowCopySnapshotsToBeDeleted = shallowCopySnapshots.subList(
            0,
            randomIntBetween(2, fullShallowCopySnapshotsCount)
        );
        int tobeDeletedShallowCopySnapshotsCount = shallowCopySnapshotsToBeDeleted.size();
        // TODO: remove this line: successfulShallowCopySnapshotsCount += 1; // Adding partial shallow snapshot here
        // Updating the snapshot repository flag to disable shallow snapshots
        createRepository(snapshotRepoName, "mock", snapshotRepoPath);
        // Creating some full copy snapshots
        int totalFullCopySnapshotsCount = randomIntBetween(4, 10);
        List<SnapshotId> fullCopySnapshots = createNSnapshots(snapshotRepoName, totalFullCopySnapshotsCount);
        List<SnapshotId> fullCopySnapshotsToBeDeleted = fullCopySnapshots.stream()
            .filter(snapshotId -> snapshotId != partialSnapshotInfo.snapshotId())
            .collect(Collectors.toList())
            .subList(0, randomIntBetween(2, totalFullCopySnapshotsCount));
        int tobeDeletedFullCopySnapshotsCount = fullCopySnapshotsToBeDeleted.size();

        int totalSnapshotsCount = totalFullCopySnapshotsCount + fullShallowCopySnapshotsCount + partialShallowCopySnapshotCount;

        String[] lockFiles = getLockFilesInRemoteStore(testIndex2, REMOTE_REPO_NAME);

        assertEquals(lockFiles.length - initialLockFiles.length, fullShallowCopySnapshotsCount);
        RepositoryData repositoryData = getRepositoryData(snapshotRepoName);
        assert (repositoryData.getSnapshotIds().size() == totalSnapshotsCount);

        // Validate snapshot types list in repository data.
        validateSnapshotTypes(repositoryData, shallowCopySnapshots, fullCopySnapshots, null);

        // Deleting subset of shallow copy snapshots
        assertAcked(
            clusterManagerClient.admin()
                .cluster()
                .prepareDeleteSnapshot(
                    snapshotRepoName,
                    shallowCopySnapshotsToBeDeleted.stream().map(SnapshotId::getName).toArray(String[]::new)
                )
                .get()
        );
        totalSnapshotsCount -= tobeDeletedShallowCopySnapshotsCount;
        fullShallowCopySnapshotsCount -= tobeDeletedShallowCopySnapshotsCount;
        // Get updated repository data.
        repositoryData = getRepositoryData(snapshotRepoName);
        assert (repositoryData.getSnapshotIds().size() == totalSnapshotsCount);
        assert (getLockFilesInRemoteStore(testIndex2, REMOTE_REPO_NAME).length - initialLockFiles.length == fullShallowCopySnapshotsCount);
        // Validate snapshot types list in repository data.
        validateSnapshotTypes(
            repositoryData,
            shallowCopySnapshots.stream().filter(snapId -> !shallowCopySnapshotsToBeDeleted.contains(snapId)).collect(Collectors.toList()),
            fullCopySnapshots,
            shallowCopySnapshotsToBeDeleted
        );

        // Deleting subset of full copy snapshots
        assertAcked(
            clusterManagerClient.admin()
                .cluster()
                .prepareDeleteSnapshot(
                    snapshotRepoName,
                    fullCopySnapshotsToBeDeleted.stream().map(SnapshotId::getName).toArray(String[]::new)
                )
                .get()
        );
        totalSnapshotsCount -= tobeDeletedFullCopySnapshotsCount;
        // Get updated repository data.
        repositoryData = getRepositoryData(snapshotRepoName);
        assert (repositoryData.getSnapshotIds().size() == totalSnapshotsCount);
        assert (getLockFilesInRemoteStore(testIndex2, REMOTE_REPO_NAME).length - initialLockFiles.length == fullShallowCopySnapshotsCount);

        // Validate snapshot types list in repository data.
        validateSnapshotTypes(
            repositoryData,
            shallowCopySnapshots.stream().filter(snapId -> !shallowCopySnapshotsToBeDeleted.contains(snapId)).collect(Collectors.toList()),
            fullCopySnapshots.stream().filter(snapId -> !fullCopySnapshotsToBeDeleted.contains(snapId)).collect(Collectors.toList()),
            Stream.concat(shallowCopySnapshotsToBeDeleted.stream(), fullCopySnapshotsToBeDeleted.stream()).collect(Collectors.toList())
        );
    }

    // Deleting subset of shallow and full copy snapshots as part of single delete call and then deleting all snapshots in the repo.
    public void testDeleteMultipleShallowCopySnapshotsCase3() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");
        final Path remoteStoreRepoPath = randomRepoPath();
        internalCluster().startClusterManagerOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME, remoteStoreRepoPath));
        internalCluster().startDataOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME, remoteStoreRepoPath));
        final Client clusterManagerClient = internalCluster().clusterManagerClient();
        ensureStableCluster(2);

        final String snapshotRepoName = "snapshot-repo-name";
        final Path snapshotRepoPath = randomRepoPath();
        createRepository(snapshotRepoName, "mock", snapshotRepoSettingsForShallowCopy(snapshotRepoPath));

        final String testIndex = "index-test";
        createIndexWithContent(testIndex);

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        indexRandomDocs(remoteStoreEnabledIndexName, randomIntBetween(5, 10));

        // Creating some shallow copy snapshots
        int totalShallowCopySnapshotsCount = randomIntBetween(4, 10);
        List<SnapshotId> shallowCopySnapshots = createNSnapshots(snapshotRepoName, totalShallowCopySnapshotsCount);
        List<SnapshotId> shallowCopySnapshotsToBeDeleted = shallowCopySnapshots.subList(
            0,
            randomIntBetween(2, totalShallowCopySnapshotsCount)
        );
        int tobeDeletedShallowCopySnapshotsCount = shallowCopySnapshotsToBeDeleted.size();
        // Updating the snapshot repository flag to disable shallow snapshots
        createRepository(snapshotRepoName, "mock", snapshotRepoPath);
        // Creating some full copy snapshots
        int totalFullCopySnapshotsCount = randomIntBetween(4, 10);
        List<SnapshotId> fullCopySnapshots = createNSnapshots(snapshotRepoName, totalFullCopySnapshotsCount);
        List<SnapshotId> fullCopySnapshotsToBeDeleted = fullCopySnapshots.subList(0, randomIntBetween(2, totalFullCopySnapshotsCount));
        int tobeDeletedFullCopySnapshotsCount = fullCopySnapshotsToBeDeleted.size();

        int totalSnapshotsCount = totalFullCopySnapshotsCount + totalShallowCopySnapshotsCount;

        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length == totalShallowCopySnapshotsCount);

        RepositoryData repositoryData = getRepositoryData(snapshotRepoName);
        assert (repositoryData.getSnapshotIds().size() == totalSnapshotsCount);
        validateSnapshotTypes(repositoryData, shallowCopySnapshots, fullCopySnapshots, null);
        // Deleting subset of shallow copy snapshots and full copy snapshots
        assertAcked(
            clusterManagerClient.admin()
                .cluster()
                .prepareDeleteSnapshot(
                    snapshotRepoName,
                    Stream.concat(
                        shallowCopySnapshotsToBeDeleted.stream().map(SnapshotId::getName),
                        fullCopySnapshotsToBeDeleted.stream().map(SnapshotId::getName)
                    ).toArray(String[]::new)
                )
                .get()
        );
        totalSnapshotsCount -= (tobeDeletedShallowCopySnapshotsCount + tobeDeletedFullCopySnapshotsCount);
        totalShallowCopySnapshotsCount -= tobeDeletedShallowCopySnapshotsCount;
        // Get updated repository data.
        repositoryData = getRepositoryData(snapshotRepoName);
        assert (repositoryData.getSnapshotIds().size() == totalSnapshotsCount);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length == totalShallowCopySnapshotsCount);

        validateSnapshotTypes(
            repositoryData,
            shallowCopySnapshots.stream().filter(snapId -> !shallowCopySnapshotsToBeDeleted.contains(snapId)).collect(Collectors.toList()),
            fullCopySnapshots.stream().filter(snapId -> !fullCopySnapshotsToBeDeleted.contains(snapId)).collect(Collectors.toList()),
            Stream.concat(shallowCopySnapshotsToBeDeleted.stream(), fullCopySnapshotsToBeDeleted.stream()).collect(Collectors.toList())
        );

        // Deleting all the remaining snapshots
        assertAcked(clusterManagerClient.admin().cluster().prepareDeleteSnapshot(snapshotRepoName, "*").get());
        // Get updated repository data.
        repositoryData = getRepositoryData(snapshotRepoName);
        assert (repositoryData.getSnapshotIds().size() == 0);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length == 0);

        validateSnapshotTypes(
            repositoryData,
            null,
            null,
            Stream.concat(shallowCopySnapshots.stream(), fullCopySnapshots.stream()).collect(Collectors.toList())
        );
    }

    public void testRemoteStoreCleanupForDeletedIndex() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");
        final Path remoteStoreRepoPath = randomRepoPath();
        internalCluster().startClusterManagerOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME, remoteStoreRepoPath));
        internalCluster().startDataOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME, remoteStoreRepoPath));
        final Client clusterManagerClient = internalCluster().clusterManagerClient();
        ensureStableCluster(2);

        final String snapshotRepoName = "snapshot-repo-name";
        final Path snapshotRepoPath = randomRepoPath();
        createRepository(snapshotRepoName, "mock", snapshotRepoSettingsForShallowCopy(snapshotRepoPath));

        final String testIndex = "index-test";
        createIndexWithContent(testIndex);

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        indexRandomDocs(remoteStoreEnabledIndexName, randomIntBetween(5, 10));

        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(remoteStoreEnabledIndexName)
            .get()
            .getSetting(remoteStoreEnabledIndexName, IndexMetadata.SETTING_INDEX_UUID);

        logger.info("--> create two remote index shallow snapshots");
        SnapshotInfo snapshotInfo1 = createFullSnapshot(snapshotRepoName, "snap1");
        SnapshotInfo snapshotInfo2 = createFullSnapshot(snapshotRepoName, "snap2");

        final RepositoriesService repositoriesService = internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class);
        final BlobStoreRepository remoteStoreRepository = (BlobStoreRepository) repositoriesService.repository(REMOTE_REPO_NAME);
        BlobPath shardLevelBlobPath = getShardLevelBlobPath(
            client(),
            remoteStoreEnabledIndexName,
            remoteStoreRepository.basePath(),
            "0",
            SEGMENTS,
            LOCK_FILES
        );
        BlobContainer blobContainer = remoteStoreRepository.blobStore().blobContainer(shardLevelBlobPath);
        String[] lockFiles;
        try (RemoteBufferedOutputDirectory lockDirectory = new RemoteBufferedOutputDirectory(blobContainer)) {
            lockFiles = lockDirectory.listAll();
        }
        assert (lockFiles.length == 2) : "lock files are " + Arrays.toString(lockFiles);

        // delete remote store index
        assertAcked(client().admin().indices().prepareDelete(remoteStoreEnabledIndexName));

        logger.info("--> delete snapshot 1");
        AcknowledgedResponse deleteSnapshotResponse = clusterManagerClient.admin()
            .cluster()
            .prepareDeleteSnapshot(snapshotRepoName, snapshotInfo1.snapshotId().getName())
            .get();
        assertAcked(deleteSnapshotResponse);

        try (RemoteBufferedOutputDirectory lockDirectory = new RemoteBufferedOutputDirectory(blobContainer)) {
            lockFiles = lockDirectory.listAll();
        }
        assert (lockFiles.length == 1) : "lock files are " + Arrays.toString(lockFiles);
        assertTrue(lockFiles[0].contains(snapshotInfo2.snapshotId().getUUID()));

        logger.info("--> delete snapshot 2");
        deleteSnapshotResponse = clusterManagerClient.admin()
            .cluster()
            .prepareDeleteSnapshot(snapshotRepoName, snapshotInfo2.snapshotId().getName())
            .get();
        assertAcked(deleteSnapshotResponse);

        Path indexPath = Path.of(String.valueOf(remoteStoreRepoPath), indexUUID);
        // Delete is async. Give time for it
        assertBusy(() -> {
            try {
                assertThat(RemoteStoreBaseIntegTestCase.getFileCount(indexPath), comparesEqualTo(0));
            } catch (Exception e) {}
        }, 30, TimeUnit.SECONDS);
    }

    private List<SnapshotId> createNSnapshots(String repoName, int count) {
        final List<SnapshotId> snapshotIds = new ArrayList<>(count);
        final String prefix = "snap-" + UUIDs.randomBase64UUID(random()).toLowerCase(Locale.ROOT) + "-";
        for (int i = 0; i < count; i++) {
            final String name = prefix + i;
            SnapshotInfo snapshotInfo = createFullSnapshot(repoName, name);
            snapshotIds.add(snapshotInfo.snapshotId());
        }
        logger.info("--> created {} in [{}]", snapshotIds, repoName);
        return snapshotIds;
    }

    private void validateSnapshotTypes(
        RepositoryData repositoryData,
        List<SnapshotId> shallowSnaps,
        List<SnapshotId> fullSnaps,
        List<SnapshotId> shouldNotPresentSnaps
    ) {
        if (shallowSnaps != null) {
            for (SnapshotId snapId : shallowSnaps) {
                assertEquals(repositoryData.getSnapshotType(snapId), SnapshotType.SHALLOW_COPY);
            }
        }
        if (fullSnaps != null) {
            for (SnapshotId snapId : fullSnaps) {
                assertEquals(repositoryData.getSnapshotType(snapId), SnapshotType.FULL_COPY);
            }
        }
        if (shouldNotPresentSnaps != null) {
            for (SnapshotId snapId : shouldNotPresentSnaps) {
                assertNull(repositoryData.getSnapshotType(snapId));
            }
        }
    }
}
