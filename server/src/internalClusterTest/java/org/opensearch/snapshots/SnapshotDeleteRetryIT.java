/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
import org.opensearch.cluster.coordination.ClusterStatePublisher;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.discovery.Discovery;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotDeleteRetryIT extends AbstractSnapshotIntegTestCase {

    private final AtomicInteger removalPublishesToFail = new AtomicInteger();
    private final AtomicInteger removalPublishes = new AtomicInteger();

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING.getKey(), true).build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SnapshotsService.SNAPSHOT_CLEANUP_RETRY_BACKOFF_SETTING.getKey(), "100ms")
            .build();
    }

    public void testDeleteRemovalIsRetriedOnSameClusterManager() throws Exception {
        final String clusterManager = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        createRepository("test-repo", "mock");
        createIndexWithContent("test-idx");
        createFullSnapshot("test-repo", "snap-1");
        final long term = internalCluster().clusterService(clusterManager).state().term();

        failDeleteRemovalPublishes(1);
        assertAcked(startDeleteSnapshot("test-repo", "snap-1").get(60, TimeUnit.SECONDS));

        assertEquals("the failed removal publish and its retry", 2, removalPublishes.get());
        assertEquals(clusterManager, internalCluster().getClusterManagerName());
        assertEquals(term, internalCluster().clusterService(clusterManager).state().term());
        awaitNoMoreRunningOperations();
        assertThat(getRepositoryData("test-repo").getSnapshotIds(), empty());
    }

    public void testFailedRepositoryDeleteStaysFailedAcrossRetry() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        createRepository("test-repo", "mock");
        createIndexWithContent("test-idx");
        createFullSnapshot("test-repo", "snap-1");

        final String clusterManager = blockClusterManagerFromFinalizingSnapshotOnIndexFile("test-repo");
        failDeleteRemovalPublishes(1);
        final ActionFuture<AcknowledgedResponse> delete = startDeleteSnapshot("test-repo", "snap-1");
        waitForBlock(clusterManager, "test-repo", TimeValue.timeValueSeconds(30));
        unblockNode("test-repo", clusterManager);

        final Exception e = expectThrows(Exception.class, () -> delete.actionGet(TimeValue.timeValueSeconds(60)));
        final String trace = ExceptionsHelper.stackTrace(e);
        assertThat("the caller must get the repository failure", trace, containsString("exception after block"));
        assertThat(trace, not(containsString("Failed to update cluster state during repository operation")));
        assertEquals("the failed removal publish and its retry", 2, removalPublishes.get());
        awaitNoMoreRunningOperations();
        assertThat(getRepositoryData("test-repo").getSnapshotIds(), hasSize(1));
    }

    public void testNewClusterManagerFinishesDeleteWhoseRemovalIsRetrying() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        createRepository("test-repo", "mock");
        createIndexWithContent("test-idx");
        createFullSnapshot("test-repo", "snap-1");
        assertAcked(
            clusterAdmin().prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(SnapshotsService.SNAPSHOT_CLEANUP_RETRY_BACKOFF_SETTING.getKey(), "1m"))
        );
        final String oldClusterManager = internalCluster().getClusterManagerName();
        failDeleteRemovalPublishes(1);
        final ActionFuture<AcknowledgedResponse> delete = internalCluster().client(dataNode)
            .admin()
            .cluster()
            .prepareDeleteSnapshot("test-repo", "snap-1")
            .execute();
        assertBusy(() -> assertEquals("the removal publish failed and its retry is waiting", 1, removalPublishes.get()));

        internalCluster().stopCurrentClusterManagerNode();
        ensureStableCluster(3, dataNode);
        assertNotEquals(oldClusterManager, internalCluster().getClusterManagerName());
        awaitNoMoreRunningOperations();
        assertThat("the new cluster manager finishes the delete", getRepositoryData("test-repo").getSnapshotIds(), empty());
        try {
            assertAcked(delete.get(60, TimeUnit.SECONDS));
        } catch (ExecutionException e) {
            assertThat(ExceptionsHelper.unwrapCause(e.getCause()), instanceOf(SnapshotMissingException.class));
        }
        createFullSnapshot("test-repo", "snap-2");
    }

    private void failDeleteRemovalPublishes(int count) {
        final ClusterService clusterService = internalCluster().getCurrentClusterManagerNodeInstance(ClusterService.class);
        final ClusterStatePublisher publisher = internalCluster().getCurrentClusterManagerNodeInstance(Discovery.class);
        removalPublishesToFail.set(count);
        clusterService.getClusterManagerService().setClusterStatePublisher((event, publishListener, ackListener) -> {
            if (removesADelete(event)) {
                removalPublishes.incrementAndGet();
                if (removalPublishesToFail.getAndDecrement() > 0) {
                    publishListener.onFailure(new FailedToCommitClusterStateException("injected publish failure"));
                    return;
                }
            }
            publisher.publish(event, publishListener, ackListener);
        });
    }

    private static boolean removesADelete(ClusterChangedEvent event) {
        final SnapshotDeletionsInProgress before = event.previousState()
            .custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY);
        final SnapshotDeletionsInProgress after = event.state().custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY);
        return before.getEntries()
            .stream()
            .anyMatch(entry -> after.getEntries().stream().noneMatch(remaining -> remaining.uuid().equals(entry.uuid())));
    }
}
