/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DeleteSnapshotIT extends AbstractSnapshotIntegTestCase {

    public void testDeleteSnapshot() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        final String snapshotRepoName = "snapshot-repo-name";
        final Path snapshotRepoPath = randomRepoPath();
        createRepository(snapshotRepoName, "fs", snapshotRepoPath);

        final Path remoteStoreRepoPath = randomRepoPath();
        final String remoteStoreRepoName = "remote-store-repo-name";
        createRepository(remoteStoreRepoName, "fs", remoteStoreRepoPath);

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings(remoteStoreRepoName);
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        indexRandomDocs(remoteStoreEnabledIndexName, randomIntBetween(5, 10));

        final String snapshot = "snapshot";
        createFullSnapshot(snapshotRepoName, snapshot);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, remoteStoreRepoName).length == 0);
        assert (getRepositoryData(snapshotRepoName).getSnapshotIds().size() == 1);

        assertAcked(startDeleteSnapshot(snapshotRepoName, snapshot).get());
        assert (getRepositoryData(snapshotRepoName).getSnapshotIds().size() == 0);
    }

    public void testDeleteShallowCopySnapshot() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used in the test");
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        final String snapshotRepoName = "snapshot-repo-name";
        createRepository(snapshotRepoName, "fs", snapshotRepoSettingsForShallowCopy());

        final Path remoteStoreRepoPath = randomRepoPath();
        final String remoteStoreRepoName = "remote-store-repo-name";
        createRepository(remoteStoreRepoName, "fs", remoteStoreRepoPath);

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings(remoteStoreRepoName);
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        indexRandomDocs(remoteStoreEnabledIndexName, randomIntBetween(5, 10));

        final String shallowSnapshot = "shallow-snapshot";
        createFullSnapshot(snapshotRepoName, shallowSnapshot);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, remoteStoreRepoName).length == 1);
        assert (getRepositoryData(snapshotRepoName).getSnapshotIds().size() == 1);

        assertAcked(startDeleteSnapshot(snapshotRepoName, shallowSnapshot).get());
        assert (getRepositoryData(snapshotRepoName).getSnapshotIds().size() == 0);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, remoteStoreRepoName).length == 0);
    }
}
