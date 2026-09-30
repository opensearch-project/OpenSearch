/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
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
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

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
