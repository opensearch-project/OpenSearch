/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Verifies that a cluster-manager-side repository call that never returns is terminated by
 * {@link SnapshotsService#SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING} instead of stranding the snapshot in progress forever.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotRepositoryIoTimeoutIT extends AbstractSnapshotIntegTestCase {

    private static final TimeValue IO_TIMEOUT = TimeValue.timeValueSeconds(5);

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING.getKey(), true).build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.getKey(), IO_TIMEOUT)
            .build();
    }

    public void testHungFinalizationTimesOutAndClearsMarker() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createRepository("test-repo", "mock");
        assertAcked(prepareCreate("test-idx", 0, indexSettingsNoReplicas(1)));
        ensureGreen();
        indexRandomDocs("test-idx", randomIntBetween(10, 50));

        logger.info("--> block the cluster-manager writing index-N, without failing it");
        final String blockedNode = blockClusterManagerOnWriteIndexFile("test-repo");

        client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(30));

        logger.info("--> the io_timeout must terminate finalization and remove the in-progress marker");
        assertBusy(
            () -> assertTrue("in-progress snapshot marker should be gone", snapshotsInProgress().entries().isEmpty()),
            60,
            TimeUnit.SECONDS
        );

        logger.info("--> let the orphaned repository worker finish; the cluster state must stay authoritative");
        unblockNode("test-repo", blockedNode);

        assertBusy(() -> assertTrue("marker must not reappear", snapshotsInProgress().entries().isEmpty()), 30, TimeUnit.SECONDS);
        assertSnapshotNotRecordedAsSuccessful("test-repo", "test-snap");

        logger.info("--> a released repository loop lets the index be deleted");
        assertAcked(client().admin().indices().prepareDelete("test-idx").get());
    }

    public void testHungDeleteTimesOutAndReleasesRepositoryLoop() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createRepository("test-repo", "mock");
        assertAcked(prepareCreate("test-idx", 0, indexSettingsNoReplicas(1)));
        ensureGreen();
        indexRandomDocs("test-idx", randomIntBetween(10, 50));

        createFullSnapshot("test-repo", "snap-1");
        createFullSnapshot("test-repo", "snap-2");

        logger.info("--> block the cluster-manager deleting index-N, then start a delete");
        final String clusterManagerName = internalCluster().getClusterManagerName();
        final ActionFuture<AcknowledgedResponse> blockedDelete = deleteSnapshotBlockedOnClusterManager("test-repo", "snap-1");
        waitForBlock(clusterManagerName, "test-repo", TimeValue.timeValueSeconds(30));

        logger.info("--> the io_timeout must terminate the delete and remove its cluster state entry");
        assertBusy(() -> assertTrue("delete marker should be gone", deletionsInProgress().getEntries().isEmpty()), 60, TimeUnit.SECONDS);

        logger.info("--> the caller is told the delete failed rather than hanging forever");
        expectThrows(Exception.class, blockedDelete::actionGet);

        logger.info("--> let the orphaned repository worker finish");
        unblockNode("test-repo", clusterManagerName);
        assertBusy(() -> assertTrue("delete marker must not reappear", deletionsInProgress().getEntries().isEmpty()), 30, TimeUnit.SECONDS);

        logger.info("--> the repository loop was released, so a further delete can run");
        assertAcked(client().admin().cluster().prepareDeleteSnapshot("test-repo", "snap-2").get());
    }

    private SnapshotsInProgress snapshotsInProgress() {
        return clusterService().state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
    }

    private SnapshotDeletionsInProgress deletionsInProgress() {
        return clusterService().state().custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY);
    }

    private void assertSnapshotNotRecordedAsSuccessful(String repository, String snapshot) {
        final GetSnapshotsResponse response = client().admin()
            .cluster()
            .prepareGetSnapshots(repository)
            .setSnapshots(snapshot)
            .setIgnoreUnavailable(true)
            .get();
        if (response.getSnapshots().isEmpty()) {
            // The timeout removed the entry before the repository recorded it at all, which is the expected common case.
            return;
        }
        final SnapshotState state = response.getSnapshots().get(0).state();
        assertTrue("snapshot should be completed, was " + state, state.completed());
        assertNotEquals("a timed-out snapshot must not be reported as successful", SnapshotState.SUCCESS, state);
    }
}
