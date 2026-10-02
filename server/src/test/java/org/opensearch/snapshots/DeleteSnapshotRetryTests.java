/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.mockito.ArgumentMatchers;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeleteSnapshotRetryTests extends OpenSearchTestCase {

    private static final String SOURCE = "remove snapshot deletion metadata";
    private static final String REPO = "repo";
    private static final SnapshotId SNAPSHOT = new SnapshotId("snap-1", UUIDs.randomBase64UUID());

    private final List<Runnable> scheduled = new ArrayList<>();
    private final List<ClusterStateUpdateTask> submitted = new ArrayList<>();
    private final List<ActionListener<RepositoryData>> repositoryReads = new ArrayList<>();
    private final List<ActionListener<RepositoryData>> repositoryDeletes = new ArrayList<>();
    private RepositoriesService repositoriesService;

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testDeleteRemovalStopsRetryingAtRetryLimit() throws Exception {
        final SnapshotsService service = service(false);
        final int retries = SnapshotsService.SNAPSHOT_CLEANUP_RETRIES_SETTING.getDefault(Settings.EMPTY);
        final SnapshotDeletionsInProgress.Entry delete = startedDelete();
        claim(service, delete);
        ClusterStateUpdateTask removal = service.createRemoveSnapshotDeletionTask(0, delete, null, RepositoryData.EMPTY);
        for (int attempt = 0; attempt < retries; attempt++) {
            removal.onFailure(SOURCE, new FailedToCommitClusterStateException("publish failed"));
            assertThat("attempt " + attempt + " must schedule one retry", scheduled, hasSize(1));
            scheduled.remove(0).run();
            assertThat(submitted, hasSize(1));
            final ClusterStateUpdateTask retry = submitted.remove(0);
            assertNotSame("a retry must be a new task", removal, retry);
            removal = retry;
        }
        removal.onFailure(SOURCE, new FailedToCommitClusterStateException("publish failed"));
        assertThat("the removal must give up after " + retries + " retries", scheduled, empty());
        assertTrue("a delete whose removal gave up must be marked", service.unpublishedDeletes.contains(delete.uuid()));
    }

    public void testDeleteRemovalGivesUpImmediatelyWhenFlagOff() throws Exception {
        final SnapshotsService service = service(false);
        final List<Exception> answers = new ArrayList<>();
        final ClusterState running = startDeleteThroughTheService(service, ActionListener.wrap(ignored -> {}, answers::add));
        final SnapshotDeletionsInProgress.Entry delete = onlyDelete(running);
        repositoryDeletes.remove(0).onResponse(RepositoryData.EMPTY);
        submitted.remove(0).onFailure(SOURCE, new FailedToCommitClusterStateException("publish failed"));

        assertThat("with the flag off a failed removal must not be retried", scheduled, empty());
        assertThat("with the flag off the caller must be failed at once", answers, hasSize(1));
        assertThat(answers.get(0).getMessage(), containsString("Failed to update cluster state during repository operation"));
        assertTrue("with the flag off the delete must be released at once", service.repositoryOperations.isNotRunning(delete.uuid()));
        assertThat("with the flag off nothing may be marked", service.unpublishedDeletes, empty());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testDeleteRetryIsSkippedWhenFailoverHandlingReleasedDelete() throws Exception {
        final SnapshotsService service = service(false);
        final ClusterState running = startDeleteThroughTheService(service);
        final SnapshotDeletionsInProgress.Entry delete = onlyDelete(running);
        repositoryDeletes.remove(0).onResponse(RepositoryData.EMPTY);
        submitted.remove(0).onFailure(SOURCE, new FailedToCommitClusterStateException("publish failed"));
        assertThat(scheduled, hasSize(1));

        runFailoverHandling(service);
        scheduled.remove(0).run();
        assertThat("a retry of a delete that failover handling released must not be submitted", submitted, empty());
        assertTrue("the delete must stay marked for a re-drive", service.unpublishedDeletes.contains(delete.uuid()));

        retryTheDelete(service, running);
        assertThat("retrying the same delete must re-drive it", repositoryReads, hasSize(1));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testDeleteRetryArmedAfterFailoverHandlingIsSkipped() throws Exception {
        final SnapshotsService service = service(false);
        final SnapshotDeletionsInProgress.Entry delete = onlyDelete(startDeleteThroughTheService(service));
        repositoryDeletes.remove(0).onResponse(RepositoryData.EMPTY);
        final ClusterStateUpdateTask removal = submitted.remove(0);
        runFailoverHandling(service);
        assertTrue("failover handling released the delete", service.repositoryOperations.isNotRunning(delete.uuid()));

        removal.onFailure(SOURCE, new FailedToCommitClusterStateException("publish failed"));
        assertThat(scheduled, hasSize(1));
        scheduled.remove(0).run();
        assertThat("a retry armed after failover handling released the delete must not be submitted", submitted, empty());
        assertTrue("the delete must stay marked for a re-drive", service.unpublishedDeletes.contains(delete.uuid()));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testHealthyDeleteIsNotRedrivenAfterFailoverHandling() throws Exception {
        final SnapshotsService service = service(false);
        final ClusterState running = startDeleteThroughTheService(service);
        assertThat("the delete's repository work is running", repositoryDeletes, hasSize(1));
        runFailoverHandling(service);

        retryTheDelete(service, running);
        assertThat("a healthy delete must not be re-driven", repositoryReads, empty());
        assertFalse("a declined retry must not hold the repository", service.currentlyFinalizing.contains(REPO));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testMarkedDeleteIsNotRedrivenWhileRepositoryIsHeld() throws Exception {
        final SnapshotsService service = service(false);
        final SnapshotDeletionsInProgress.Entry delete = startedDelete();
        giveUpOnTheRemoval(service, delete);
        service.tryEnterRepoLoop(REPO);

        retryTheDelete(service, stateWith(delete));
        assertThat("a marked delete must not be re-driven while the repository is held", repositoryReads, empty());

        service.leaveRepoLoop(REPO);
        retryTheDelete(service, stateWith(delete));
        assertThat("once the repository is free a later retry re-drives the delete", repositoryReads, hasSize(1));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testMarkedDeleteIsNotRedrivenAfterNodeReclaimsIt() throws Exception {
        final SnapshotsService service = service(false);
        final SnapshotDeletionsInProgress.Entry delete = startedDelete();
        giveUpOnTheRemoval(service, delete);
        service.tryEnterRepoLoop(REPO);
        service.deleteSnapshotsFromRepository(delete, RepositoryData.EMPTY, Version.CURRENT);
        assertThat("the node is running the delete again", repositoryDeletes, hasSize(1));
        runFailoverHandling(service);

        retryTheDelete(service, stateWith(delete));
        assertThat("a delete whose worker started after it was marked must not be re-driven", repositoryReads, empty());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testMarkedDeleteIsNotRedrivenWhileRetryHoldsClaim() throws Exception {
        final SnapshotsService service = service(false);
        final SnapshotDeletionsInProgress.Entry delete = startedDelete();
        claim(service, delete);
        service.createRemoveSnapshotDeletionTask(0, delete, null, RepositoryData.EMPTY)
            .onFailure(SOURCE, new FailedToCommitClusterStateException("publish failed"));
        assertThat("the delete's retry is scheduled", scheduled, hasSize(1));

        retryTheDelete(service, stateWith(delete));
        assertThat("a delete its own retry still holds must not be re-driven", repositoryReads, empty());
        assertFalse("a declined retry must not hold the repository", service.currentlyFinalizing.contains(REPO));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRedriveKeepsMarkWhenRepositoryReadFails() throws Exception {
        final SnapshotsService service = service(false);
        final SnapshotDeletionsInProgress.Entry delete = startedDelete();
        giveUpOnTheRemoval(service, delete);

        retryTheDelete(service, stateWith(delete));
        assertThat(repositoryReads, hasSize(1));
        repositoryReads.remove(0).onFailure(new IOException("repository unreadable"));
        assertThat("the re-drive must fail the repository's pending work", submitted, hasSize(1));
        assertTrue("the delete must stay marked in case that update cannot publish", service.unpublishedDeletes.contains(delete.uuid()));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRedriveReleasesRepositoryWhenRetryTakesMark() throws Exception {
        final SnapshotsService service = service(false);
        final SnapshotDeletionsInProgress.Entry delete = startedDelete();
        giveUpOnTheRemoval(service, delete);
        final ClusterState state = stateWith(delete);
        final ClusterStateUpdateTask retry = deleteRequest(service);
        final ClusterState after = retry.execute(state);
        service.unpublishedDeletes.remove(delete.uuid());

        retry.clusterStateProcessed("delete snapshot", state, after);
        assertThat("a re-drive that lost the mark must not read the repository", repositoryReads, empty());
        assertFalse("a re-drive that lost the mark must not hold the repository", service.currentlyFinalizing.contains(REPO));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testShallowCopyDeleteIsNeverMarked() throws Exception {
        final SnapshotsService service = service(true);
        final SnapshotDeletionsInProgress.Entry delete = startedDelete();
        claim(service, delete);
        service.createRemoveSnapshotDeletionTask(0, delete, null, RepositoryData.EMPTY)
            .onFailure(SOURCE, new FailedToCommitClusterStateException("publish failed"));
        assertThat("a shallow-copy delete must never be marked for a re-drive", service.unpublishedDeletes, empty());
        scheduled.remove(0).run();
        assertThat("the retry of a shallow-copy delete must still be submitted", submitted, hasSize(1));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testSameNamedNewSnapshotIsNotResolvedToMarkedDelete() throws Exception {
        for (boolean inProgress : new boolean[] { true, false }) {
            scheduled.clear();
            submitted.clear();
            final SnapshotsService service = service(false);
            final SnapshotDeletionsInProgress.Entry delete = startedDelete();
            giveUpOnTheRemoval(service, delete);
            final SnapshotId sameName = new SnapshotId(SNAPSHOT.getName(), UUIDs.randomBase64UUID());
            ClusterState state = stateWith(delete);
            RepositoryData repositoryData = RepositoryData.EMPTY;
            if (inProgress) {
                state = ClusterState.builder(state)
                    .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(List.of(finalizingSnapshot(sameName))))
                    .build();
            } else {
                repositoryData = repositoryData.addSnapshot(
                    sameName,
                    SnapshotState.SUCCESS,
                    Version.CURRENT,
                    ShardGenerations.EMPTY,
                    null,
                    null
                );
            }

            final List<SnapshotDeletionsInProgress.Entry> deletes = resolveAndExecute(service, state, repositoryData, SNAPSHOT.getName())
                .custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY)
                .getEntries();
            final String arrangement = inProgress ? "in progress" : "in the repository";
            assertTrue(arrangement + ": the marked delete must be left as it was", deletes.contains(delete));
            assertThat(arrangement + ": the request must add one delete of its own", deletes, hasSize(2));
            for (SnapshotDeletionsInProgress.Entry entry : deletes) {
                if (entry.equals(delete) == false) {
                    assertEquals(
                        arrangement + ": the request must resolve to the new snapshot only",
                        List.of(sameName),
                        entry.getSnapshots()
                    );
                }
            }
        }
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRequestNamingExtraSnapshotFailsAsMissing() throws Exception {
        final SnapshotsService service = service(false);
        final SnapshotDeletionsInProgress.Entry delete = startedDelete();
        giveUpOnTheRemoval(service, delete);
        final SnapshotId other = new SnapshotId("snap-2", UUIDs.randomBase64UUID());
        final RepositoryData repositoryData = RepositoryData.EMPTY.addSnapshot(
            other,
            SnapshotState.SUCCESS,
            Version.CURRENT,
            ShardGenerations.EMPTY,
            null,
            null
        );

        final SnapshotMissingException e = expectThrows(
            SnapshotMissingException.class,
            () -> resolveAndExecute(service, stateWith(delete), repositoryData, SNAPSHOT.getName(), other.getName())
        );
        assertThat(e.getMessage(), containsString(SNAPSHOT.getName()));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testScheduledRetryYieldsToRedrive() throws Exception {
        final SnapshotsService service = service(false);
        final SnapshotDeletionsInProgress.Entry delete = startedDelete();
        service.createRemoveSnapshotDeletionTask(0, delete, null, RepositoryData.EMPTY)
            .onFailure(SOURCE, new FailedToCommitClusterStateException("publish failed"));
        assertThat(scheduled, hasSize(1));

        retryTheDelete(service, stateWith(delete));
        assertThat("the retried delete is re-driven", repositoryReads, hasSize(1));
        repositoryReads.remove(0).onResponse(RepositoryData.EMPTY);
        assertThat("the re-drive removes the delete it found already applied", submitted, hasSize(1));
        assertFalse(
            "the removed delete must stay claimed until its removal publishes",
            service.repositoryOperations.isNotRunning(delete.uuid())
        );

        scheduled.remove(0).run();
        assertThat("the retry scheduled before the re-drive must not also remove the delete", submitted, hasSize(1));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testMarkedDeleteOfOtherRepositoryIsNotResolved() throws Exception {
        final SnapshotsService service = service(false);
        final SnapshotDeletionsInProgress.Entry otherRepositoryDelete = new SnapshotDeletionsInProgress.Entry(
            List.of(SNAPSHOT),
            "other-repo",
            0L,
            1L,
            SnapshotDeletionsInProgress.State.STARTED
        );
        giveUpOnTheRemoval(service, otherRepositoryDelete);
        final ClusterState state = stateWith(otherRepositoryDelete);

        expectThrows(SnapshotMissingException.class, () -> resolveAndExecute(service, state, RepositoryData.EMPTY, SNAPSHOT.getName()));

        final RepositoryData repositoryData = RepositoryData.EMPTY.addSnapshot(
            SNAPSHOT,
            SnapshotState.SUCCESS,
            Version.CURRENT,
            ShardGenerations.EMPTY,
            null,
            null
        );
        final List<SnapshotDeletionsInProgress.Entry> deletes = resolveAndExecute(service, state, repositoryData, SNAPSHOT.getName())
            .custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY)
            .getEntries();
        assertTrue("the other repository's marked delete must be left as it was", deletes.contains(otherRepositoryDelete));
        assertThat("the request must add a delete in its own repository", deletes, hasSize(2));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testMarkIsClearedWhenFailingPendingTasksRemovesDelete() throws Exception {
        final SnapshotsService service = service(false);
        final SnapshotDeletionsInProgress.Entry delete = startedDelete();
        giveUpOnTheRemoval(service, delete);
        final ClusterState state = stateWith(delete);

        retryTheDelete(service, state);
        repositoryReads.remove(0).onFailure(new IOException("repository unreadable"));
        final ClusterStateUpdateTask failPending = submitted.remove(0);
        failPending.clusterStateProcessed("fail repo tasks", state, failPending.execute(state));
        assertFalse("a delete removed from the cluster state must not stay marked", service.unpublishedDeletes.contains(delete.uuid()));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testStaleRetryDoesNotTakeMarkOfReclaimedDelete() throws Exception {
        final SnapshotsService service = service(false);
        final SnapshotDeletionsInProgress.Entry delete = startedDelete();
        claim(service, delete);
        service.createRemoveSnapshotDeletionTask(0, delete, null, RepositoryData.EMPTY)
            .onFailure(SOURCE, new FailedToCommitClusterStateException("publish failed"));
        assertThat(scheduled, hasSize(1));

        runFailoverHandling(service);
        service.tryEnterRepoLoop(REPO);
        service.deleteSnapshotsFromRepository(delete, RepositoryData.EMPTY, Version.CURRENT);
        assertFalse("a worker has claimed the delete again", service.repositoryOperations.isNotRunning(delete.uuid()));
        service.createRemoveSnapshotDeletionTask(0, delete, null, RepositoryData.EMPTY)
            .onFailure(SOURCE, new FailedToCommitClusterStateException("publish failed"));
        assertThat("the stale retry and the new one are both scheduled", scheduled, hasSize(2));

        scheduled.remove(0).run();
        assertThat("the retry scheduled before failover handling must be dropped", submitted, empty());
        assertTrue("the dropped retry must leave the new mark in place", service.unpublishedDeletes.contains(delete.uuid()));
        scheduled.remove(0).run();
        assertThat("the new retry must take the mark and be submitted", submitted, hasSize(1));
    }

    private void giveUpOnTheRemoval(SnapshotsService service, SnapshotDeletionsInProgress.Entry delete) {
        final int retries = SnapshotsService.SNAPSHOT_CLEANUP_RETRIES_SETTING.getDefault(Settings.EMPTY);
        service.createRemoveSnapshotDeletionTask(retries, delete, null, RepositoryData.EMPTY)
            .onFailure(SOURCE, new FailedToCommitClusterStateException("publish failed"));
        assertThat(scheduled, empty());
    }

    private static void runFailoverHandling(SnapshotsService service) {
        final Snapshot other = new Snapshot(REPO, new SnapshotId("snap-2", UUIDs.randomBase64UUID()));
        service.createRemoveFailedSnapshotTask("remove snapshot metadata", 0, other, new RuntimeException("snapshot failed"), null, null)
            .onNoLongerClusterManager("remove snapshot metadata");
    }

    private static ClusterState startDeleteThroughTheService(SnapshotsService service) throws Exception {
        return startDeleteThroughTheService(service, ActionListener.wrap(() -> {}));
    }

    private static ClusterState startDeleteThroughTheService(SnapshotsService service, ActionListener<Void> listener) throws Exception {
        final ClusterState before = stateWith();
        final ClusterStateUpdateTask start = deleteRequest(service, listener);
        final ClusterState after = start.execute(before);
        start.clusterStateProcessed("delete snapshot", before, after);
        return after;
    }

    private static SnapshotDeletionsInProgress.Entry onlyDelete(ClusterState state) {
        final List<SnapshotDeletionsInProgress.Entry> deletes = state.custom(
            SnapshotDeletionsInProgress.TYPE,
            SnapshotDeletionsInProgress.EMPTY
        ).getEntries();
        assertThat(deletes, hasSize(1));
        return deletes.get(0);
    }

    private ClusterState resolveAndExecute(SnapshotsService service, ClusterState state, RepositoryData repositoryData, String... names)
        throws Exception {
        final List<ClusterStateUpdateTask> resolved = new ArrayList<>();
        final Repository repository = repositoriesService.repository(REPO);
        doAnswer(invocation -> {
            final Function<RepositoryData, ClusterStateUpdateTask> createUpdateTask = invocation.getArgument(0);
            resolved.add(createUpdateTask.apply(repositoryData));
            return null;
        }).when(repository).executeConsistentStateUpdate(any(), anyString(), any());
        service.deleteSnapshots(new DeleteSnapshotRequest(REPO, names), ActionListener.wrap(() -> {}));
        return resolved.get(0).execute(state);
    }

    private static SnapshotsInProgress.Entry finalizingSnapshot(SnapshotId snapshotId) {
        return SnapshotsInProgress.startedEntry(
            new Snapshot(REPO, snapshotId),
            true,
            false,
            Collections.emptyList(),
            Collections.emptyList(),
            0L,
            1L,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Version.CURRENT,
            false
        );
    }

    private static void retryTheDelete(SnapshotsService service, ClusterState state) throws Exception {
        final ClusterStateUpdateTask retry = deleteRequest(service);
        retry.clusterStateProcessed("delete snapshot", state, retry.execute(state));
    }

    private static ClusterStateUpdateTask deleteRequest(SnapshotsService service) {
        return deleteRequest(service, ActionListener.wrap(() -> {}));
    }

    private static ClusterStateUpdateTask deleteRequest(SnapshotsService service, ActionListener<Void> listener) {
        return service.createDeleteStateUpdate(List.of(SNAPSHOT), REPO, RepositoryData.EMPTY, Priority.NORMAL, listener);
    }

    private static SnapshotDeletionsInProgress.Entry startedDelete() {
        return new SnapshotDeletionsInProgress.Entry(List.of(SNAPSHOT), REPO, 0L, 1L, SnapshotDeletionsInProgress.State.STARTED);
    }

    private static ClusterState stateWith(SnapshotDeletionsInProgress.Entry... deletes) {
        final DiscoveryNode localNode = new DiscoveryNode(
            "local",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId("local").clusterManagerNodeId("local"))
            .putCustom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.of(List.of(deletes)))
            .build();
    }

    private SnapshotsService service(boolean shallowCopy) {
        final ThreadPool capturing = mock(ThreadPool.class);
        when(capturing.schedule(any(Runnable.class), any(TimeValue.class), anyString())).thenAnswer(invocation -> {
            scheduled.add(invocation.getArgument(0));
            return mock(Scheduler.ScheduledCancellable.class);
        });
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        doAnswer(invocation -> {
            submitted.add(invocation.getArgument(1));
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), ArgumentMatchers.<ClusterStateUpdateTask>any());
        final TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(capturing);

        final Repository repository = mock(Repository.class);
        when(repository.getMetadata()).thenReturn(
            new RepositoryMetadata(
                REPO,
                "fs",
                Settings.builder().put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), shallowCopy).build()
            )
        );
        doAnswer(invocation -> {
            repositoryDeletes.add(invocation.getArgument(3));
            return null;
        }).when(repository).deleteSnapshots(any(), anyLong(), any(), any());
        repositoriesService = mock(RepositoriesService.class);
        when(repositoriesService.repository(anyString())).thenReturn(repository);
        doAnswer(invocation -> {
            repositoryReads.add(invocation.getArgument(1));
            return null;
        }).when(repositoriesService).getRepositoryData(anyString(), any());

        return new SnapshotsService(
            Settings.builder().put("node.name", "test").putList("node.roles", "cluster_manager", "data").build(),
            clusterService,
            mock(org.opensearch.cluster.metadata.IndexNameExpressionResolver.class),
            repositoriesService,
            transportService,
            mock(org.opensearch.action.support.ActionFilters.class),
            null,
            new org.opensearch.indices.RemoteStoreSettings(Settings.EMPTY, clusterSettings),
            null
        );
    }

    private static void claim(SnapshotsService service, SnapshotDeletionsInProgress.Entry delete) {
        assertTrue(service.repositoryOperations.startDeletion(delete.uuid()));
    }
}
