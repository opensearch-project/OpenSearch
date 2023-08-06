/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.action.ActionFuture;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.client.Client;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;
import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

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
        final String testIndex = "index-test";
        createIndexWithContent(testIndex);

        final Path remoteStoreRepoPath = randomRepoPath();
        createRepository(REMOTE_REPO_NAME, "fs", remoteStoreRepoPath);

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
