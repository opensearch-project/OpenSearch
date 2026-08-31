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

    public void testHungDeleteTimesOutBeforeRepositoryIsMutated() throws Exception {
        final String clusterManagerName = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createRepository("test-repo", "mock");
        createIndexWithContent("test-idx");
        createFullSnapshot("test-repo", "snap-1");

        // Blocking on the first repository file touched means the delete times out with the snapshot ids still present in the
        // repository data. The removal must therefore take the failure path, which does not assert their absence.
        blockNodeOnAnyFiles("test-repo", clusterManagerName);
        final ActionFuture<AcknowledgedResponse> blockedDelete = deleteSnapshot("test-repo", "snap-1");
        waitForBlock(clusterManagerName, "test-repo", TimeValue.timeValueSeconds(30));

        assertBusy(() -> assertTrue("delete marker should be gone", deletionsInProgress().getEntries().isEmpty()), 60, TimeUnit.SECONDS);
        expectThrows(Exception.class, blockedDelete::actionGet);

        unblockNode("test-repo", clusterManagerName);
        assertBusy(() -> assertTrue("delete marker must not reappear", deletionsInProgress().getEntries().isEmpty()), 30, TimeUnit.SECONDS);
    }

    public void testHealthyDuplicateDeleteAttachesInsteadOfRedriving() throws Exception {
        final String clusterManagerName = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createRepository("test-repo", "mock");
        createIndexWithContent("test-idx");
        createFullSnapshot("test-repo", "snap-1");

        logger.info("--> raise the io_timeout so no timeout fires: this test is about a HEALTHY in-flight delete");
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder()
                        .put(SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMinutes(30))
                        .build()
                )
                .get()
        );

        // Block on the first repository file touched, before the delete mutates the repository data, so a duplicate delete can
        // still resolve the snapshot name.
        blockNodeOnAnyFiles("test-repo", clusterManagerName);
        final ActionFuture<AcknowledgedResponse> firstDelete = deleteSnapshot("test-repo", "snap-1");
        waitForBlock(clusterManagerName, "test-repo", TimeValue.timeValueSeconds(30));

        logger.info("--> a duplicate delete must attach to the running one, not re-drive it");
        final ActionFuture<AcknowledgedResponse> duplicateDelete = deleteSnapshot("test-repo", "snap-1");
        assertBusy(
            () -> assertEquals("exactly one delete entry expected", 1, deletionsInProgress().getEntries().size()),
            30,
            TimeUnit.SECONDS
        );
        assertFalse("the duplicate must wait, not complete on its own", duplicateDelete.isDone());

        unblockNode("test-repo", clusterManagerName);

        assertAcked(firstDelete.actionGet());
        assertAcked(duplicateDelete.actionGet());
        assertBusy(() -> assertTrue(deletionsInProgress().getEntries().isEmpty()), 30, TimeUnit.SECONDS);
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
