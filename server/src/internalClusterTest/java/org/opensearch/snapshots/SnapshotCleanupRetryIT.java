/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotCleanupRetryIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING.getKey(), true).build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SnapshotsService.SNAPSHOT_CLEANUP_RETRIES_SETTING.getKey(), 3)
            .put(SnapshotsService.SNAPSHOT_CLEANUP_RETRY_BACKOFF_SETTING.getKey(), "1s")
            .build();
    }

    public void testSnapshotMarkerCleanedUpAfterClusterManagerFailover() throws Exception {
        logger.info("--> starting cluster-manager and data nodes");
        internalCluster().startClusterManagerOnlyNodes(3);
        internalCluster().startDataOnlyNodes(2);

        createRepository("test-repo", "mock");
        assertAcked(prepareCreate("test-idx", 0, indexSettingsNoReplicas(1)));
        ensureGreen();
        indexRandomDocs("test-idx", randomIntBetween(10, 50));

        logger.info("--> block cluster-manager from finalizing snapshot on index file");
        String blockedNode = blockClusterManagerFromFinalizingSnapshotOnIndexFile("test-repo");

        logger.info("--> start snapshot (non-blocking)");
        client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> wait for snapshot to hit the block");
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(30));

        logger.info("--> stopping cluster-manager node to induce failover");
        internalCluster().stopCurrentClusterManagerNode();

        logger.info("--> wait for snapshot marker to be cleaned up after new cluster-manager takes over");
        assertBusy(() -> {
            SnapshotsInProgress snapshotsInProgress = clusterService().state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            assertTrue("Snapshot marker should be cleaned up", snapshotsInProgress.entries().isEmpty());
        }, 60, TimeUnit.SECONDS);

        logger.info("--> verify snapshot state after failover");
        GetSnapshotsResponse snapshotResponse = client().admin()
            .cluster()
            .prepareGetSnapshots("test-repo")
            .setSnapshots("test-snap")
            .setIgnoreUnavailable(true)
            .get();
        if (snapshotResponse.getSnapshots().isEmpty() == false) {
            SnapshotState state = snapshotResponse.getSnapshots().get(0).state();
            assertTrue("Snapshot should be completed but was " + state, state.completed());
        }

        logger.info("--> verify index deletion is unblocked");
        assertAcked(client().admin().indices().prepareDelete("test-idx").get());
    }
}
