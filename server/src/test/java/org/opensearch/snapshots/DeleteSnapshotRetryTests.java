/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Covers the delete-side publish-retry conversions: a retryable publish failure must resubmit a fresh task without releasing the
 * delete bookkeeping, and only the final failure may release it. Releasing per attempt would let a later release trip
 * {@code leaveRepoLoop}'s {@code assert removed}.
 */
public class DeleteSnapshotRetryTests extends OpenSearchTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private SnapshotsService snapshotsService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = mock(ClusterService.class);
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        final TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);

        final Settings settings = Settings.builder()
            .put("node.name", "test")
            .putList("node.roles", "cluster_manager", "data")
            .put(SnapshotsService.SNAPSHOT_CLEANUP_RETRY_BACKOFF_SETTING.getKey(), "100ms")
            .build();

        snapshotsService = new SnapshotsService(
            settings,
            clusterService,
            mock(org.opensearch.cluster.metadata.IndexNameExpressionResolver.class),
            mock(org.opensearch.repositories.RepositoriesService.class),
            transportService,
            mock(org.opensearch.action.support.ActionFilters.class),
            null,
            new org.opensearch.indices.RemoteStoreSettings(Settings.EMPTY, clusterSettings),
            null
        );
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    private SnapshotDeletionsInProgress.Entry deleteEntry() {
        return new SnapshotDeletionsInProgress.Entry(
            Collections.singletonList(new SnapshotId("snap-1", UUIDs.randomBase64UUID())),
            "test-repo",
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            SnapshotDeletionsInProgress.State.STARTED
        );
    }

    private ClusterStateUpdateTask removalTask(int attempt, SnapshotDeletionsInProgress.Entry entry) {
        return snapshotsService.createRemoveSnapshotDeletionTask(
            attempt,
            entry,
            new RuntimeException("delete failed"),
            RepositoryData.EMPTY
        );
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRetryableFailureDoesNotReleaseDeleteBookkeeping() {
        final SnapshotDeletionsInProgress.Entry entry = deleteEntry();
        assertTrue(snapshotsService.repositoryOperations.startDeletion(entry.uuid()));

        removalTask(0, entry).onFailure("remove snapshot deletion metadata", new FailedToCommitClusterStateException("publish failed"));

        assertFalse(
            "a retryable publish failure must not release the delete bookkeeping, or a later release trips leaveRepoLoop's assert",
            snapshotsService.repositoryOperations.isNotRunning(entry.uuid())
        );
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testExhaustedRetriesReleaseDeleteBookkeeping() {
        final SnapshotDeletionsInProgress.Entry entry = deleteEntry();
        assertTrue(snapshotsService.repositoryOperations.startDeletion(entry.uuid()));

        // Default snapshot.cleanup.retries is 3, so attempt 3 is the final failure.
        removalTask(3, entry).onFailure("remove snapshot deletion metadata", new FailedToCommitClusterStateException("publish failed"));

        assertTrue(
            "the final failure must release the delete bookkeeping exactly once",
            snapshotsService.repositoryOperations.isNotRunning(entry.uuid())
        );
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testClusterManagerFailOverReleasesDeleteBookkeepingImmediately() {
        final SnapshotDeletionsInProgress.Entry entry = deleteEntry();
        assertTrue(snapshotsService.repositoryOperations.startDeletion(entry.uuid()));

        // Losing the cluster-manager role is not retryable: the next cluster-manager picks the work up.
        removalTask(0, entry).onFailure("remove snapshot deletion metadata", new NotClusterManagerException("no longer cluster-manager"));

        assertTrue(snapshotsService.repositoryOperations.isNotRunning(entry.uuid()));
    }

    public void testFeatureFlagDisabledReleasesDeleteBookkeepingImmediately() {
        final SnapshotDeletionsInProgress.Entry entry = deleteEntry();
        assertTrue(snapshotsService.repositoryOperations.startDeletion(entry.uuid()));

        removalTask(0, entry).onFailure("remove snapshot deletion metadata", new FailedToCommitClusterStateException("publish failed"));

        assertTrue(
            "with the flag off the pre-existing behaviour applies: release immediately, no retry",
            snapshotsService.repositoryOperations.isNotRunning(entry.uuid())
        );
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRemovalTaskFactoryReturnsFreshInstancePerAttempt() {
        final SnapshotDeletionsInProgress.Entry entry = deleteEntry();
        // TaskBatcher rejects resubmitting the same task identity, so every retry needs a distinct instance.
        assertNotSame(removalTask(0, entry), removalTask(1, entry));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRunReadyDeletionsTaskFactoryReturnsFreshInstancePerAttempt() {
        assertNotSame(
            snapshotsService.createRunReadyDeletionsTask(0, RepositoryData.EMPTY, "test-repo"),
            snapshotsService.createRunReadyDeletionsTask(1, RepositoryData.EMPTY, "test-repo")
        );
    }

    /**
     * isNotRunning is one of the two guards on the duplicate-delete re-drive. It must report "not running" only when no thread owns
     * the delete, which is what distinguishes an abandoned delete from a healthy in-flight one.
     */
    public void testIsNotRunningTracksDeleteOwnership() {
        final SnapshotDeletionsInProgress.Entry entry = deleteEntry();

        assertTrue("an unknown delete is not running", snapshotsService.repositoryOperations.isNotRunning(entry.uuid()));

        assertTrue(snapshotsService.repositoryOperations.startDeletion(entry.uuid()));
        assertFalse("a delete being executed is running", snapshotsService.repositoryOperations.isNotRunning(entry.uuid()));

        assertFalse(
            "a second start must not claim an already-running delete",
            snapshotsService.repositoryOperations.startDeletion(entry.uuid())
        );
        assertFalse(snapshotsService.repositoryOperations.isNotRunning(entry.uuid()));

        snapshotsService.repositoryOperations.finishDeletion(entry.uuid());
        assertTrue("a released delete is available to be re-driven", snapshotsService.repositoryOperations.isNotRunning(entry.uuid()));
    }
}
