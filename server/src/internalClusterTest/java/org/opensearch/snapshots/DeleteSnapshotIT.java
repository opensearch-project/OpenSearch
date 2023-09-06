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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.is;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DeleteSnapshotIT extends AbstractSnapshotIntegTestCase {

    private static final String REMOTE_REPO_NAME = "remote-store-repo-name";

    public void testDeleteSnapshot() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        internalCluster().startClusterManagerOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME));
        internalCluster().startDataOnlyNode();

        final String snapshotRepoName = "snapshot-repo-name";
        final Path snapshotRepoPath = randomRepoPath();
        createRepository(snapshotRepoName, "fs", snapshotRepoPath);

        final Path remoteStoreRepoPath = randomRepoPath();
        createRepository(REMOTE_REPO_NAME, "fs", remoteStoreRepoPath);

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
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        internalCluster().startClusterManagerOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME));
        internalCluster().startDataOnlyNode();

        final String snapshotRepoName = "snapshot-repo-name";
        createRepository(snapshotRepoName, "fs", snapshotRepoSettingsForShallowCopy());

        final Path remoteStoreRepoPath = randomRepoPath();
        createRepository(REMOTE_REPO_NAME, "fs", remoteStoreRepoPath);

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        indexRandomDocs(remoteStoreEnabledIndexName, randomIntBetween(5, 10));

        final String shallowSnapshot = "shallow-snapshot";
        createFullSnapshot(snapshotRepoName, shallowSnapshot);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length == 1);
        assert (getRepositoryData(snapshotRepoName).getSnapshotIds().size() == 1);

        assertAcked(startDeleteSnapshot(snapshotRepoName, shallowSnapshot).get());
        assert (getRepositoryData(snapshotRepoName).getSnapshotIds().size() == 0);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length == 0);
    }

    // Deleting multiple shallow copy snapshots as part of single delete call with repo having only shallow copy snapshots.
    public void testDeleteMultipleShallowCopySnapshotsCase1() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);

        internalCluster().startClusterManagerOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME));
        internalCluster().startDataOnlyNode();
        final Client clusterManagerClient = internalCluster().clusterManagerClient();
        ensureStableCluster(2);

        final Path remoteStoreRepoPath = randomRepoPath();
        createRepository(REMOTE_REPO_NAME, "fs", remoteStoreRepoPath);

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
        List<String> shallowCopySnapshots = createNSnapshots(snapshotRepoName, totalShallowCopySnapshotsCount);
        List<String> snapshotsToBeDeleted = shallowCopySnapshots.subList(0, randomIntBetween(2, totalShallowCopySnapshotsCount));
        int tobeDeletedSnapshotsCount = snapshotsToBeDeleted.size();
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length == totalShallowCopySnapshotsCount);
        assert (getRepositoryData(snapshotRepoName).getSnapshotIds().size() == totalShallowCopySnapshotsCount);
        // Deleting subset of shallow copy snapshots
        assertAcked(
            clusterManagerClient.admin()
                .cluster()
                .prepareDeleteSnapshot(snapshotRepoName, snapshotsToBeDeleted.toArray(new String[0]))
                .get()
        );
        awaitNoMoreRunningOperations();
        assert (getRepositoryData(snapshotRepoName).getSnapshotIds().size() == totalShallowCopySnapshotsCount - tobeDeletedSnapshotsCount);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length == totalShallowCopySnapshotsCount
            - tobeDeletedSnapshotsCount);
    }

    // Deleting multiple shallow copy snapshots as part of single delete call with both partial and full copy snapshot present in the repo
    // And then deleting multiple full copy snapshots as part of single delete call with both partial and shallow copy snapshots present in
    // the repo
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8610")
    public void testDeleteMultipleShallowCopySnapshotsCase2() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);

        internalCluster().startClusterManagerOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME));
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        final String clusterManagerNode = internalCluster().getClusterManagerName();

        final String snapshotRepoName = "snapshot-repo-name";
        final Path snapshotRepoPath = randomRepoPath();
        createRepository(snapshotRepoName, "mock", snapshotRepoSettingsForShallowCopy(snapshotRepoPath));
        final String testIndex = "index-test";
        createIndexWithContent(testIndex);

        final Path remoteStoreRepoPath = randomRepoPath();
        createRepository(REMOTE_REPO_NAME, "fs", remoteStoreRepoPath);

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        indexRandomDocs(remoteStoreEnabledIndexName, randomIntBetween(5, 10));

        // Creating a partial shallow copy snapshot
        final String snapshot = "snapshot";
        blockNodeWithIndex(snapshotRepoName, testIndex);
        blockDataNode(snapshotRepoName, dataNode);

        final Client clusterManagerClient = internalCluster().clusterManagerClient();
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = clusterManagerClient.admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, snapshot)
            .setWaitForCompletion(true)
            .execute();

        awaitNumberOfSnapshotsInProgress(1);
        waitForBlock(dataNode, snapshotRepoName, TimeValue.timeValueSeconds(30L));
        internalCluster().restartNode(dataNode);
        assertThat(snapshotFuture.get().getSnapshotInfo().state(), is(SnapshotState.PARTIAL));

        unblockAllDataNodes(snapshotRepoName);

        ensureStableCluster(2, clusterManagerNode);

        // Creating some shallow copy snapshots
        int totalShallowCopySnapshotsCount = randomIntBetween(4, 10);
        List<String> shallowCopySnapshots = createNSnapshots(snapshotRepoName, totalShallowCopySnapshotsCount);
        List<String> shallowCopySnapshotsToBeDeleted = shallowCopySnapshots.subList(0, randomIntBetween(2, totalShallowCopySnapshotsCount));
        int tobeDeletedShallowCopySnapshotsCount = shallowCopySnapshotsToBeDeleted.size();
        totalShallowCopySnapshotsCount += 1; // Adding partial shallow snapshot here
        // Updating the snapshot repository flag to disable shallow snapshots
        createRepository(snapshotRepoName, "mock", snapshotRepoPath);
        // Creating some full copy snapshots
        int totalFullCopySnapshotsCount = randomIntBetween(4, 10);
        List<String> fullCopySnapshots = createNSnapshots(snapshotRepoName, totalFullCopySnapshotsCount);
        List<String> fullCopySnapshotsToBeDeleted = fullCopySnapshots.subList(0, randomIntBetween(2, totalFullCopySnapshotsCount));
        int tobeDeletedFullCopySnapshotsCount = fullCopySnapshotsToBeDeleted.size();

        int totalSnapshotsCount = totalFullCopySnapshotsCount + totalShallowCopySnapshotsCount;

        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length == totalShallowCopySnapshotsCount);
        assert (getRepositoryData(snapshotRepoName).getSnapshotIds().size() == totalSnapshotsCount);
        // Deleting subset of shallow copy snapshots
        assertAcked(
            clusterManagerClient.admin()
                .cluster()
                .prepareDeleteSnapshot(snapshotRepoName, shallowCopySnapshotsToBeDeleted.toArray(new String[0]))
                .get()
        );
        totalSnapshotsCount -= tobeDeletedShallowCopySnapshotsCount;
        totalShallowCopySnapshotsCount -= tobeDeletedShallowCopySnapshotsCount;
        assert (getRepositoryData(snapshotRepoName).getSnapshotIds().size() == totalSnapshotsCount);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length == totalShallowCopySnapshotsCount);

        // Deleting subset of full copy snapshots
        assertAcked(
            clusterManagerClient.admin()
                .cluster()
                .prepareDeleteSnapshot(snapshotRepoName, fullCopySnapshotsToBeDeleted.toArray(new String[0]))
                .get()
        );
        totalSnapshotsCount -= tobeDeletedFullCopySnapshotsCount;
        assert (getRepositoryData(snapshotRepoName).getSnapshotIds().size() == totalSnapshotsCount);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length == totalShallowCopySnapshotsCount);
    }

    // Deleting subset of shallow and full copy snapshots as part of single delete call and then deleting all snapshots in the repo.
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8610")
    public void testDeleteMultipleShallowCopySnapshotsCase3() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);

        internalCluster().startClusterManagerOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME));
        internalCluster().startDataOnlyNode();
        final Client clusterManagerClient = internalCluster().clusterManagerClient();
        ensureStableCluster(2);

        final String snapshotRepoName = "snapshot-repo-name";
        final Path snapshotRepoPath = randomRepoPath();
        createRepository(snapshotRepoName, "mock", snapshotRepoSettingsForShallowCopy(snapshotRepoPath));

        final Path remoteStoreRepoPath = randomRepoPath();
        createRepository(REMOTE_REPO_NAME, "fs", remoteStoreRepoPath);

        final String testIndex = "index-test";
        createIndexWithContent(testIndex);

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        indexRandomDocs(remoteStoreEnabledIndexName, randomIntBetween(5, 10));

        // Creating some shallow copy snapshots
        int totalShallowCopySnapshotsCount = randomIntBetween(4, 10);
        List<String> shallowCopySnapshots = createNSnapshots(snapshotRepoName, totalShallowCopySnapshotsCount);
        List<String> shallowCopySnapshotsToBeDeleted = shallowCopySnapshots.subList(0, randomIntBetween(2, totalShallowCopySnapshotsCount));
        int tobeDeletedShallowCopySnapshotsCount = shallowCopySnapshotsToBeDeleted.size();
        // Updating the snapshot repository flag to disable shallow snapshots
        createRepository(snapshotRepoName, "mock", snapshotRepoPath);
        // Creating some full copy snapshots
        int totalFullCopySnapshotsCount = randomIntBetween(4, 10);
        List<String> fullCopySnapshots = createNSnapshots(snapshotRepoName, totalFullCopySnapshotsCount);
        List<String> fullCopySnapshotsToBeDeleted = fullCopySnapshots.subList(0, randomIntBetween(2, totalFullCopySnapshotsCount));
        int tobeDeletedFullCopySnapshotsCount = fullCopySnapshotsToBeDeleted.size();

        int totalSnapshotsCount = totalFullCopySnapshotsCount + totalShallowCopySnapshotsCount;

        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length == totalShallowCopySnapshotsCount);
        assert (getRepositoryData(snapshotRepoName).getSnapshotIds().size() == totalSnapshotsCount);
        // Deleting subset of shallow copy snapshots and full copy snapshots
        assertAcked(
            clusterManagerClient.admin()
                .cluster()
                .prepareDeleteSnapshot(
                    snapshotRepoName,
                    Stream.concat(shallowCopySnapshotsToBeDeleted.stream(), fullCopySnapshotsToBeDeleted.stream()).toArray(String[]::new)
                )
                .get()
        );
        totalSnapshotsCount -= (tobeDeletedShallowCopySnapshotsCount + tobeDeletedFullCopySnapshotsCount);
        totalShallowCopySnapshotsCount -= tobeDeletedShallowCopySnapshotsCount;
        assert (getRepositoryData(snapshotRepoName).getSnapshotIds().size() == totalSnapshotsCount);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length == totalShallowCopySnapshotsCount);

        // Deleting all the remaining snapshots
        assertAcked(clusterManagerClient.admin().cluster().prepareDeleteSnapshot(snapshotRepoName, "*").get());
        assert (getRepositoryData(snapshotRepoName).getSnapshotIds().size() == 0);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME).length == 0);
    }

    // Checking snapshot deletion after inducing lock release failure for shallow copy snapshot.
    public void testReleaseLockFailure() throws Exception {
        disableRepoConsistencyCheck("This test uses remote store repository");
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        internalCluster().startClusterManagerOnlyNode(remoteStoreClusterSettings("remote-store-repo-name"));
        internalCluster().startDataOnlyNode();

        logger.info("-->  creating snapshot repository");
        final String shallowSnapshotRepoName = "shallow-snapshot-repo-name";
        final Path shallowSnapshotRepoPath = randomRepoPath();
        createRepository(shallowSnapshotRepoName, "mock", snapshotRepoSettingsForShallowCopy(shallowSnapshotRepoPath));

        logger.info("-->  creating remote store repository");
        final String remoteStoreRepoName = "remote-store-repo-name";
        final Path remoteStoreRepoPath = randomRepoPath();
        Settings.Builder remoteStoreRepoSettingsBuilder = Settings.builder().put("location", remoteStoreRepoPath);
        createRepository(remoteStoreRepoName, "mock", remoteStoreRepoSettingsBuilder);

        createIndexWithContent("index-test-1");

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        indexRandomDocs(remoteStoreEnabledIndexName, randomIntBetween(5, 10));

        createFullSnapshot(shallowSnapshotRepoName, "snapshot_one");

        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, remoteStoreRepoName).length == 1);
        assert (clusterAdmin().prepareGetSnapshots(shallowSnapshotRepoName).get().getSnapshots().size() == 1);

        logger.info("Updating repo settings");
        remoteStoreRepoSettingsBuilder.putList("regexes_to_fail_io", "lock$");
        createRepository(remoteStoreRepoName, "mock", remoteStoreRepoSettingsBuilder);

        // Deleting snapshot_one after updating repo settings to fail on lock file release operation.
        // Expecting snapshot to be deleted but the lock file as well as snapshot files to be still present.
        // index.N and index.latest are the only two files expected to be present after snapshot deletion.
        assertAcked(startDeleteSnapshot(shallowSnapshotRepoName, "snapshot_one").get());
        assert (clusterAdmin().prepareGetSnapshots(shallowSnapshotRepoName).get().getSnapshots().size() == 0);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, remoteStoreRepoName).length == 1);
        assert (numberOfFiles(shallowSnapshotRepoPath) != 2);

        logger.info("Updating repo settings");
        remoteStoreRepoSettingsBuilder.remove("regexes_to_fail_io");
        createRepository(remoteStoreRepoName, "mock", remoteStoreRepoSettingsBuilder);

        createIndexWithContent("test-index-2");
        createFullSnapshot(shallowSnapshotRepoName, "snapshot_two");

        // Deleting snapshot_two after reverting the repo settings. Expecting snapshot count as well as lock
        // file count to be zero after the snapshot deletion. Also the files present in the snapshot repository
        // should equal to 2 (index.latest and index.N)
        assertAcked(startDeleteSnapshot(shallowSnapshotRepoName, "snapshot_two").get());
        assert (clusterAdmin().prepareGetSnapshots(shallowSnapshotRepoName).get().getSnapshots().size() == 0);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, remoteStoreRepoName).length == 0);
        assert (numberOfFiles(shallowSnapshotRepoPath) == 2);
    }

    public void testRemoteStoreCleanupForDeletedIndex() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);

        internalCluster().startClusterManagerOnlyNode(remoteStoreClusterSettings(REMOTE_REPO_NAME));
        internalCluster().startDataOnlyNode();
        final Client clusterManagerClient = internalCluster().clusterManagerClient();
        ensureStableCluster(2);

        final String snapshotRepoName = "snapshot-repo-name";
        final Path snapshotRepoPath = randomRepoPath();
        createRepository(snapshotRepoName, "mock", snapshotRepoSettingsForShallowCopy(snapshotRepoPath));

        final Path remoteStoreRepoPath = randomRepoPath();
        createRepository(REMOTE_REPO_NAME, "fs", remoteStoreRepoPath);

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
        List<String> shallowCopySnapshots = createNSnapshots(snapshotRepoName, 2);

        String[] lockFiles = getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME);
        assert (lockFiles.length == 2) : "lock files are " + Arrays.toString(lockFiles);

        // delete remote store index
        assertAcked(client().admin().indices().prepareDelete(remoteStoreEnabledIndexName));

        logger.info("--> delete snapshot 1");
        AcknowledgedResponse deleteSnapshotResponse = clusterManagerClient.admin()
            .cluster()
            .prepareDeleteSnapshot(snapshotRepoName, shallowCopySnapshots.get(0))
            .get();
        assertAcked(deleteSnapshotResponse);

        lockFiles = getLockFilesInRemoteStore(remoteStoreEnabledIndexName, REMOTE_REPO_NAME, indexUUID);
        assert (lockFiles.length == 1) : "lock files are " + Arrays.toString(lockFiles);

        logger.info("--> delete snapshot 2");
        deleteSnapshotResponse = clusterManagerClient.admin()
            .cluster()
            .prepareDeleteSnapshot(snapshotRepoName, shallowCopySnapshots.get(1))
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

    private List<String> createNSnapshots(String repoName, int count) {
        final List<String> snapshotNames = new ArrayList<>(count);
        final String prefix = "snap-" + UUIDs.randomBase64UUID(random()).toLowerCase(Locale.ROOT) + "-";
        for (int i = 0; i < count; i++) {
            final String name = prefix + i;
            createFullSnapshot(repoName, name);
            snapshotNames.add(name);
        }
        logger.info("--> created {} in [{}]", snapshotNames, repoName);
        return snapshotNames;
    }
}
