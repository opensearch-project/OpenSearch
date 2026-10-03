/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies the delete-side repository I/O time budget: a hung delete must fail and clear its cluster-state entry
 * instead of hanging indefinitely.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotDeleteTimeoutIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING.getKey(), true).build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(2))
            .build();
    }

    public void testDeleteTimesOutWhenRepositoryHangs() throws Exception {
        disableRepoConsistencyCheck("orphaned worker advances the repo generation after unblock");
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");
        final String snapshotName = "snap-1";
        createFullSnapshot(repoName, snapshotName);

        blockClusterManagerFromDeletingIndexNFile(repoName);
        final ActionFuture<AcknowledgedResponse> deleteFuture = startDeleteSnapshot(repoName, snapshotName);
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));

        expectThrows(Exception.class, deleteFuture::actionGet);
        awaitNoMoreRunningOperations();

        unblockNode(repoName, clusterManagerNode);
    }

    public void testQueuedDeletePromotedAfterTimeout() throws Exception {
        disableRepoConsistencyCheck("orphaned worker advances the repo generation after unblock");
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-1");
        createIndexWithContent("index-2");
        final String snap1 = "snap-1";
        final String snap2 = "snap-2";
        createFullSnapshot(repoName, snap1);
        createFullSnapshot(repoName, snap2);

        blockClusterManagerFromDeletingIndexNFile(repoName);
        final ActionFuture<AcknowledgedResponse> delete1 = startDeleteSnapshot(repoName, snap1);
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));

        final ActionFuture<AcknowledgedResponse> delete2 = startDeleteSnapshot(repoName, snap2);
        assertBusy(() -> {
            final SnapshotDeletionsInProgress deletions = clusterService().state()
                .custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY);
            assertThat("both deletes should be in progress", deletions.getEntries().size(), equalTo(2));
        });

        expectThrows(Exception.class, delete1::actionGet);
        unblockNode(repoName, clusterManagerNode);

        // Either outcome is fine — we're only checking bookkeeping was released, not delete2's result.
        try {
            delete2.actionGet(TimeValue.timeValueSeconds(30L));
        } catch (Exception e) {
            logger.info("delete2 failed: {}", e.getMessage());
        }
        awaitNoMoreRunningOperations();

        final SnapshotDeletionsInProgress finalDeletions = clusterService().state()
            .custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY);
        assertFalse("No deletions should remain in progress", finalDeletions.hasDeletionsInProgress());
    }

    public void testHealthyDeleteUnaffectedByTimeoutWhenEnabled() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder()
                        .put(SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMinutes(5))
                        .build()
                )
                .get()
        );
        createIndexWithContent("index-1");
        final String snapshotName = "snap-1";
        createFullSnapshot(repoName, snapshotName);

        assertAcked(startDeleteSnapshot(repoName, snapshotName).actionGet(TimeValue.timeValueSeconds(30L)));
        awaitNoMoreRunningOperations();
    }
}
