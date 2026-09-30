/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.repositories.SnapshotDeletionAttempt;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PromotedSnapshotDeleteTests extends OpenSearchTestCase {

    private static final String REPO = "test-repo";

    private static final String SOURCE = "remove snapshot deletion metadata";

    private static final long CAPTURED_GEN = 5L;

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private RepositoriesService repositoriesService;
    private Repository repository;
    private SnapshotsService snapshotsService;

    private final AtomicInteger promotedDeleteReads = new AtomicInteger();

    private final AtomicInteger promotedFinalizationReads = new AtomicInteger();

    private final Deque<ActionListener<RepositoryData>> finalizationReadListeners = new ArrayDeque<>();

    private final AtomicLong finalizedGeneration = new AtomicLong(Long.MIN_VALUE);

    private final AtomicReference<Exception> failNextFreshRead = new AtomicReference<>();

    private final AtomicInteger deleteDispatches = new AtomicInteger();

    private final AtomicInteger wideDispatches = new AtomicInteger();
    private final AtomicInteger narrowDispatches = new AtomicInteger();

    private final AtomicReference<ActionListener<RepositoryData>> dispatchedNarrowListener = new AtomicReference<>();

    private Repository.AbandonableSnapshotDelete abandonableEntrypoint;

    private final AtomicLong dispatchedGeneration = new AtomicLong(Long.MIN_VALUE);

    private final List<String> submittedSources = new ArrayList<>();
    private final List<ClusterStateUpdateTask> submittedTasks = new ArrayList<>();

    private final AtomicReference<RepositoryData> freshRepositoryData = new AtomicReference<>();

    private final AtomicReference<SnapshotDeletionAttempt> dispatchedAttempt = new AtomicReference<>();

    private final AtomicReference<ActionListener<RepositoryData>> dispatchedListener = new AtomicReference<>();

    private final AtomicReference<List<SnapshotId>> dispatchedSnapshotIds = new AtomicReference<>();

    private final Deque<Runnable> pendingTimers = new ArrayDeque<>();

    private final AtomicReference<TimeValue> lastTimerDelay = new AtomicReference<>();

    private final List<Boolean> abandonedAtSubmit = new ArrayList<>();

    private static final Scheduler.ScheduledCancellable NOOP_CANCELLABLE = new Scheduler.ScheduledCancellable() {
        @Override
        public long getDelay(TimeUnit unit) {
            return 0L;
        }

        @Override
        public int compareTo(Delayed other) {
            return 0;
        }

        @Override
        public boolean cancel() {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }
    };

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        newFixture();
    }

    private void newFixture() {
        promotedDeleteReads.set(0);
        promotedFinalizationReads.set(0);
        finalizationReadListeners.clear();
        finalizedGeneration.set(Long.MIN_VALUE);
        failNextFreshRead.set(null);
        deleteDispatches.set(0);
        wideDispatches.set(0);
        narrowDispatches.set(0);
        dispatchedNarrowListener.set(null);
        dispatchedGeneration.set(Long.MIN_VALUE);
        submittedSources.clear();
        submittedTasks.clear();
        freshRepositoryData.set(null);
        dispatchedAttempt.set(null);
        dispatchedListener.set(null);
        dispatchedSnapshotIds.set(null);
        pendingTimers.clear();
        lastTimerDelay.set(null);
        abandonedAtSubmit.clear();

        repository = mock(Repository.class);
        when(repository.getMetadata()).thenReturn(new RepositoryMetadata(REPO, "mock", Settings.EMPTY));
        abandonableEntrypoint = (snapshotIds, repositoryStateId, repositoryMetaVersion, deletion, listener) -> {
            deleteDispatches.incrementAndGet();
            wideDispatches.incrementAndGet();
            dispatchedGeneration.set(repositoryStateId);
            dispatchedSnapshotIds.set(List.copyOf(snapshotIds));
            dispatchedAttempt.set(deletion);
            dispatchedListener.set(listener);
        };
        doAnswer(invocation -> {
            deleteDispatches.incrementAndGet();
            narrowDispatches.incrementAndGet();
            dispatchedGeneration.set(invocation.<Long>getArgument(1));
            dispatchedSnapshotIds.set(List.copyOf(invocation.<List<SnapshotId>>getArgument(0)));
            dispatchedNarrowListener.set(invocation.getArgument(3));
            return null;
        }).when(repository).deleteSnapshots(any(), anyLong(), any(), any());
        doAnswer(invocation -> {
            promotedFinalizationReads.incrementAndGet();
            finalizationReadListeners.add(invocation.getArgument(0));
            return null;
        }).when(repository).getRepositoryData(any());
        doAnswer(invocation -> {
            finalizedGeneration.set(invocation.<Long>getArgument(1));
            return null;
        }).when(repository).finalizeSnapshot(any(), anyLong(), any(), any(), any(), any(), any(), any());

        repositoriesService = mock(RepositoriesService.class);
        when(repositoriesService.repository(REPO)).thenReturn(repository);
        doAnswer(invocation -> {
            promotedDeleteReads.incrementAndGet();
            final ActionListener<RepositoryData> listener = invocation.getArgument(1);
            final Exception failure = failNextFreshRead.getAndSet(null);
            if (failure != null) {
                listener.onFailure(failure);
                return null;
            }
            listener.onResponse(freshRepositoryData.get());
            return null;
        }).when(repositoriesService).getRepositoryData(anyString(), any());

        clusterService = mock(ClusterService.class);
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        doAnswer(invocation -> {
            submittedSources.add(invocation.getArgument(0));
            submittedTasks.add(invocation.getArgument(1));
            final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
            abandonedAtSubmit.add(attempt != null && attempt.isAbandoned());
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));

        final ThreadPool interceptedThreadPool = spy(threadPool);
        doAnswer(invocation -> {
            if (ThreadPool.Names.GENERIC.equals(invocation.getArgument(2)) == false) {
                return invocation.callRealMethod();
            }
            lastTimerDelay.set(invocation.getArgument(1));
            pendingTimers.add(invocation.getArgument(0));
            return NOOP_CANCELLABLE;
        }).when(interceptedThreadPool).schedule(any(), any(), anyString());

        final TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(interceptedThreadPool);

        snapshotsService = new SnapshotsService(
            Settings.builder().put("node.name", "test").putList("node.roles", "cluster_manager", "data").build(),
            clusterService,
            mock(IndexNameExpressionResolver.class),
            repositoriesService,
            transportService,
            mock(ActionFilters.class),
            null,
            new RemoteStoreSettings(Settings.EMPTY, clusterSettings),
            null
        );
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testSuccessfulDeleteHandsItsOwnFreshDataToThePromotedDelete() throws Exception {
        for (boolean featureOn : new boolean[] { false, true }) {
            try (
                FeatureFlags.TestUtils.FlagWriteLock ignored = new FeatureFlags.TestUtils.FlagWriteLock(
                    FeatureFlags.SNAPSHOT_RESILIENCE,
                    featureOn
                )
            ) {
                newFixture();
                supportAbandonment();
                final String row = featureOn ? "feature on" : "feature off";
                final SnapshotId promotedSnapshot = newSnapshotId("promoted");
                final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
                final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(promotedSnapshot, CAPTURED_GEN + 2L);
                freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, promotedSnapshot));

                runRemoval(
                    removed,
                    SnapshotDeletionsInProgress.of(List.of(removed, queued)),
                    SnapshotsInProgress.EMPTY,
                    null,
                    repositoryDataWith(CAPTURED_GEN, promotedSnapshot)
                );

                assertEquals(row + ": a successful delete must not add a repository read", 0, promotedDeleteReads.get());
                assertEquals(row + ": the promoted delete must still be dispatched", 1, deleteDispatches.get());
                assertEquals(
                    row + ": the promoted delete must run against the data the completed delete returned",
                    CAPTURED_GEN,
                    dispatchedGeneration.get()
                );
                assertEquals(
                    row + ": the promoted delete is budgeted exactly when the feature is on",
                    featureOn ? 1 : 0,
                    wideDispatches.get()
                );
                assertEquals(row, featureOn ? 1 : 0, pendingTimers.size());
                assertEquals(row + ": and is handed an attempt exactly then", featureOn, dispatchedAttempt.get() != null);
            }
        }
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAPromotedDeleteIsDispatchedOnlyTheSnapshotsTheFreshReadStillHolds() throws Exception {
        final SnapshotId surviving = newSnapshotId("surviving");
        final SnapshotId committed = newSnapshotId("committed");
        final List<Tuple<List<SnapshotId>, List<SnapshotId>>> rows = List.of(
            Tuple.tuple(List.of(surviving), List.of(surviving)),
            Tuple.tuple(List.of(surviving, committed), List.of(surviving)),
            Tuple.tuple(List.of(committed), List.of())
        );
        for (Tuple<List<SnapshotId>, List<SnapshotId>> row : rows) {
            newFixture();
            final List<SnapshotId> stillHeld = row.v2();
            final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
            final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(row.v1(), CAPTURED_GEN + 2L);
            freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, stillHeld.toArray(new SnapshotId[0])));

            final ClusterState afterRemoval = runRemoval(
                removed,
                SnapshotDeletionsInProgress.of(List.of(removed, queued)),
                SnapshotsInProgress.EMPTY,
                new RepositoryException(REPO, "the delete did not complete"),
                repositoryDataWith(CAPTURED_GEN, row.v1().toArray(new SnapshotId[0]))
            );

            assertEquals(row + ": a failed delete must hand the promoted delete a fresh repository read", 1, promotedDeleteReads.get());
            assertTrue(row + ": no budget may be armed against a repository that will not stop for it", pendingTimers.isEmpty());
            assertNull(row + ": nor an attempt created for it", dispatchedAttempt.get());
            if (stillHeld.isEmpty()) {
                assertEquals(row + ": work the abandoned call already applied must not be issued a second time", 0, deleteDispatches.get());
                assertEquals(row + ": and the entry's own removal is the only update", List.of(SOURCE), submittedSources);
                final ClusterState afterWithdrawal = submittedTasks.get(0).execute(afterRemoval);
                assertTrue(
                    row + ": the promoted entry must leave the cluster state, which is what releases its waiting listeners",
                    deletionsOf(afterWithdrawal).getEntries().stream().noneMatch(entry -> entry.uuid().equals(queued.uuid()))
                );
            } else {
                assertEquals(row + ": the promoted delete must be dispatched, through the narrow overload", 1, narrowDispatches.get());
                assertEquals(row + ": against the generation the read returned", CAPTURED_GEN + 4L, dispatchedGeneration.get());
                assertEquals(row + ": with only the snapshots that read still holds", stillHeld, dispatchedSnapshotIds.get());
                assertTrue(row + ": work that is still outstanding must be dispatched, not withdrawn", submittedSources.isEmpty());
            }
        }
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testFailedDeleteMakesAPromotedFinalizationReReadToo() throws Exception {
        final SnapshotId promotedSnapshot = newSnapshotId("promoted");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(newSnapshotId("queued"), CAPTURED_GEN);
        final SnapshotsInProgress.Entry completedSnapshot = completedSnapshotEntry(promotedSnapshot);

        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, promotedSnapshot));

        runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, queued)),
            SnapshotsInProgress.of(List.of(completedSnapshot)),
            new RepositoryException(REPO, "the delete did not complete"),
            repositoryDataWith(CAPTURED_GEN, promotedSnapshot)
        );

        assertEquals("a promoted finalization must be told to re-read by being handed null", 1, promotedFinalizationReads.get());
        finalizationReadListeners.poll().onResponse(repositoryDataWith(CAPTURED_GEN + 4L));
        assertEquals("and is finalized from that read", CAPTURED_GEN + 4L, finalizedGeneration.get());
        assertEquals("the queued delete must stay waiting behind the finalization", 0, deleteDispatches.get());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testSuccessfulDeleteMakesThePromotedFinalizationIssueNoRepositoryRead() throws Exception {
        final SnapshotId promotedSnapshot = newSnapshotId("promoted");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(newSnapshotId("queued"), CAPTURED_GEN);
        final SnapshotsInProgress.Entry completedSnapshot = completedSnapshotEntry(promotedSnapshot);

        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, promotedSnapshot));

        runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, queued)),
            SnapshotsInProgress.of(List.of(completedSnapshot)),
            null,
            repositoryDataWith(CAPTURED_GEN)
        );

        assertEquals("a successful delete must not make its promoted finalization re-read", 0, promotedFinalizationReads.get());
        assertEquals(
            "the promoted finalization must be written against the generation of the data the successful delete returned",
            CAPTURED_GEN,
            finalizedGeneration.get()
        );
        assertEquals("the queued delete must stay waiting behind the finalization", 0, deleteDispatches.get());
    }

    private ClusterState runRemoval(
        SnapshotDeletionsInProgress.Entry removed,
        SnapshotDeletionsInProgress deletions,
        SnapshotsInProgress snapshotsInProgress,
        Exception failure,
        RepositoryData capturedData
    ) throws Exception {
        primeRunningDelete(removed.uuid());

        final String localNodeId = UUIDs.randomBase64UUID();
        final ClusterState currentState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(DiscoveryNodes.builder().localNodeId(localNodeId).clusterManagerNodeId(localNodeId).build())
            .putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress)
            .putCustom(SnapshotDeletionsInProgress.TYPE, deletions)
            .build();

        final ClusterStateUpdateTask task = snapshotsService.createRemoveSnapshotDeletionTask(
            SOURCE,
            0,
            removed,
            failure,
            capturedData,
            null,
            FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING)
        );
        final ClusterState newState = task.execute(currentState);
        task.clusterStateProcessed(SOURCE, currentState, newState);
        return newState;
    }

    @SuppressForbidden(reason = "the service's running-delete and repository-loop bookkeeping has no test seam")
    @SuppressWarnings("unchecked")
    private void primeRunningDelete(String deleteUuid) throws Exception {
        final Field repositoryLoop = SnapshotsService.class.getDeclaredField("currentlyFinalizing");
        repositoryLoop.setAccessible(true);
        ((Set<String>) repositoryLoop.get(snapshotsService)).add(REPO);

        final Field operations = SnapshotsService.class.getDeclaredField("repositoryOperations");
        operations.setAccessible(true);
        final Object repositoryOperations = operations.get(snapshotsService);
        final Field running = repositoryOperations.getClass().getDeclaredField("runningDeletions");
        running.setAccessible(true);
        ((Set<String>) running.get(repositoryOperations)).add(deleteUuid);
    }

    private static SnapshotId newSnapshotId(String name) {
        return new SnapshotId(name, UUIDs.randomBase64UUID());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testARerunOfAnAppliedDeleteLeavesTheCreatesBehindItForReconciliation() throws Exception {
        final IndexMetadata index = indexMetadata("idx");
        final ShardId createShard = new ShardId(index.getIndex(), 0);
        final ShardId otherShard = new ShardId(new Index("other-idx", UUIDs.randomBase64UUID()), 0);
        final ShardId otherLiveShard = new ShardId(otherShard.getIndex(), 1);
        final DiscoveryNode clusterManager = newNode("cluster-manager");
        final DiscoveryNode leaving = newNode("leaving");
        final SnapshotsInProgress.Entry create = snapshotEntry(
            new Snapshot(REPO, newSnapshotId("queued-create")),
            List.of(new IndexId("idx", UUIDs.randomBase64UUID())),
            Map.of(createShard, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotsInProgress.Entry other = snapshotEntry(
            new Snapshot("other-repo", newSnapshotId("other")),
            List.of(new IndexId("other-idx", UUIDs.randomBase64UUID())),
            Map.of(
                otherShard,
                new SnapshotsInProgress.ShardSnapshotStatus(leaving.getId(), UUIDs.randomBase64UUID()),
                otherLiveShard,
                new SnapshotsInProgress.ShardSnapshotStatus(clusterManager.getId(), UUIDs.randomBase64UUID())
            )
        );
        final SnapshotDeletionsInProgress.Entry delete = startedDeletion(newSnapshotId("gone"));
        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN));
        final ClusterState before = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(
                DiscoveryNodes.builder()
                    .add(clusterManager)
                    .add(leaving)
                    .localNodeId(clusterManager.getId())
                    .clusterManagerNodeId(clusterManager.getId())
            )
            .metadata(Metadata.builder().put(index, false))
            .routingTable(RoutingTable.builder().add(startedPrimary(createShard, clusterManager.getId())).build())
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(List.of(other, create)))
            .putCustom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.of(List.of(delete)))
            .build();
        final ClusterState after = ClusterState.builder(before)
            .nodes(DiscoveryNodes.builder(before.nodes()).remove(leaving.getId()))
            .build();

        snapshotsService.applyClusterState(new ClusterChangedEvent("test", after, before));
        final ClusterState processed = runSubmittedExternalChanges(after);
        assertEquals("nothing may be dispatched for snapshots the repository no longer holds", 0, deleteDispatches.get());
        final AtomicReference<Object> outcome = listenForDelete(delete.uuid());

        final ClusterState removed = runSubmittedRemoval(processed);
        assertEquals(
            "the create queued behind the delete must stay queued",
            SnapshotsInProgress.ShardState.QUEUED,
            createOf(removed).shards().get(createShard).state()
        );
        assertEquals("and the delete is answered as done", SUCCESS, outcome.get());
    }

    public void testAnElectedClusterManagerRerunsAStartedDeleteAsTheBaseDoesWithTheFeatureOff() throws Exception {
        final DiscoveryNode clusterManager = newNode("cluster-manager");
        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN));
        elect(
            ClusterState.builder(ClusterState.EMPTY_STATE)
                .nodes(
                    DiscoveryNodes.builder()
                        .add(clusterManager)
                        .localNodeId(clusterManager.getId())
                        .clusterManagerNodeId(clusterManager.getId())
                )
                .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .putCustom(
                    SnapshotDeletionsInProgress.TYPE,
                    SnapshotDeletionsInProgress.of(List.of(startedDeletion(newSnapshotId("gone"))))
                )
                .build()
        );
        assertEquals("the re-run must dispatch the delete", 1, deleteDispatches.get());
    }

    private ClusterState runSubmittedExternalChanges(ClusterState state) throws Exception {
        assertEquals("exactly one update must have been submitted", 1, submittedTasks.size());
        assertTrue(submittedSources.get(0).startsWith("update snapshot after shards started"));
        final ClusterStateUpdateTask task = submittedTasks.remove(0);
        submittedSources.remove(0);
        final ClusterState processed = task.execute(state);
        task.clusterStateProcessed("update", state, processed);
        return processed;
    }

    private static SnapshotsInProgress.Entry createOf(ClusterState state) {
        return snapshotsOf(state).entries().stream().filter(entry -> entry.repository().equals(REPO)).findFirst().orElseThrow();
    }

    private static DiscoveryNode newNode(String name) {
        return new DiscoveryNode(name, name, buildNewFakeTransportAddress(), Map.of(), DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
    }

    private static IndexMetadata indexMetadata(String indexName) {
        return IndexMetadata.builder(indexName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    private static IndexRoutingTable startedPrimary(ShardId shardId, String nodeId) {
        return IndexRoutingTable.builder(shardId.getIndex())
            .addIndexShard(
                new IndexShardRoutingTable.Builder(shardId).addShard(
                    TestShardRouting.newShardRouting(shardId, nodeId, true, ShardRoutingState.STARTED)
                ).build()
            )
            .build();
    }

    private static SnapshotsInProgress.Entry snapshotEntry(
        Snapshot snapshot,
        List<IndexId> indices,
        Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards
    ) {
        return SnapshotsInProgress.startedEntry(
            snapshot,
            false,
            false,
            indices,
            List.of(),
            0L,
            CAPTURED_GEN,
            shards,
            Map.of(),
            Version.CURRENT,
            false
        );
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAPromotedDeleteWhoseReReadFailsIsFailedAlone() throws Exception {
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry promoted = waitingDeletion(newSnapshotId("promoted"), CAPTURED_GEN);
        final SnapshotsInProgress.Entry create = queuedCreate("queued-create");
        final SnapshotsInProgress.Entry clone = queuedClone("queued-clone");
        final Exception injected = new RepositoryException(REPO, "injected read failure");
        failNextFreshRead.set(injected);
        final AtomicReference<Object> promotedOutcome = listenForDelete(promoted.uuid());

        final ClusterState state = runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, promoted)),
            SnapshotsInProgress.of(List.of(create, clone)),
            new RepositoryException(REPO, "the delete did not complete"),
            repositoryDataWith(CAPTURED_GEN)
        );

        assertEquals("a promoted delete's own re-read failing must fail only that delete", List.of(SOURCE), submittedSources);
        final ClusterState after = runSubmittedRemoval(state);
        assertSame("the promoted delete fails with the read's own failure", injected, promotedOutcome.get());
        assertEquals(
            "and the create and the clone queued behind it stay",
            List.of(create.snapshot(), clone.snapshot()),
            snapshotsOf(after).entries().stream().map(SnapshotsInProgress.Entry::snapshot).collect(Collectors.toList())
        );
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAPromotionWithNothingToFinalizeReleasesTheRepository() throws Exception {
        final SnapshotsInProgress.Entry aborted = SnapshotsInProgress.startedEntry(
            new Snapshot(REPO, newSnapshotId("aborted")),
            false,
            false,
            Collections.emptyList(),
            Collections.emptyList(),
            0L,
            RepositoryData.UNKNOWN_REPO_GEN,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Version.CURRENT,
            false
        );
        final ClusterState state = runRemovalPromoting(List.of(aborted));
        assertEquals("nothing is finalized, so nothing is read", 0, promotedFinalizationReads.get());
        runSubmitted("Run ready deletions", runSubmitted(REMOVE_SNAPSHOT, state));

        final ClusterState later = ClusterState.builder(state)
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(List.of(completedSnapshotEntry(newSnapshotId("later")))))
            .putCustom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY)
            .build();
        elect(later);
        assertEquals("a promotion with nothing to finalize releases the repository", 1, promotedFinalizationReads.get());
    }

    private ClusterState elect(ClusterState state) throws Exception {
        snapshotsService.applyClusterState(
            new ClusterChangedEvent(
                "test",
                state,
                ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).clusterManagerNodeId(null)).build()
            )
        );
        return runSubmittedExternalChanges(state);
    }

    private ClusterState runRemovalPromoting(List<SnapshotsInProgress.Entry> completed, SnapshotDeletionsInProgress.Entry... waiting)
        throws Exception {
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final List<SnapshotDeletionsInProgress.Entry> deletions = new ArrayList<>();
        deletions.add(removed);
        deletions.addAll(List.of(waiting));
        return runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(deletions),
            SnapshotsInProgress.of(completed),
            new RepositoryException(REPO, "the delete did not complete"),
            repositoryDataWith(CAPTURED_GEN)
        );
    }

    private ClusterState runSubmitted(String source, ClusterState state) throws Exception {
        final int index = submittedSources.indexOf(source);
        assertTrue("an update [" + source + "] must have been submitted, but saw " + submittedSources, index >= 0);
        submittedSources.remove(index);
        final ClusterStateUpdateTask task = submittedTasks.remove(index);
        final ClusterState after = task.execute(state);
        task.clusterStateProcessed(source, state, after);
        return after;
    }

    private SnapshotsInProgress.Entry queuedCreate(String name) {
        return snapshotEntry(
            new Snapshot(REPO, newSnapshotId(name)),
            List.of(new IndexId("idx", UUIDs.randomBase64UUID())),
            Map.of(new ShardId(new Index("idx", UUIDs.randomBase64UUID()), 0), SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
    }

    private SnapshotsInProgress.Entry queuedClone(String name) {
        return SnapshotsInProgress.startClone(
            new Snapshot(REPO, newSnapshotId(name)),
            newSnapshotId("source"),
            List.of(new IndexId("idx", UUIDs.randomBase64UUID())),
            0L,
            CAPTURED_GEN,
            Version.CURRENT
        );
    }

    private static SnapshotsInProgress snapshotsOf(ClusterState state) {
        return state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
    }

    private enum Fate {
        LIVE,
        EXPIRED,
        ANSWERED_WITH_A_TIMEOUT,
        UNBUDGETED_ANSWERED_WITH_A_TIMEOUT
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testARequestJoinsARunningDeleteUnlessThisNodeGaveUpOnIt() throws Exception {
        for (Fate fate : Fate.values()) {
            newFixture();
            if (fate != Fate.UNBUDGETED_ANSWERED_WITH_A_TIMEOUT) {
                supportAbandonment();
            }
            final SnapshotId snapshot = newSnapshotId("repeated");
            final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
            final SnapshotDeletionsInProgress.Entry promoted = waitingDeletion(snapshot, CAPTURED_GEN);
            freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 1L, snapshot));
            final ClusterState running = runRemoval(
                removed,
                SnapshotDeletionsInProgress.of(List.of(removed, promoted)),
                SnapshotsInProgress.EMPTY,
                new RepositoryException(REPO, "the delete did not complete"),
                repositoryDataWith(CAPTURED_GEN, snapshot)
            );
            final OpenSearchTimeoutException ownDeadline = new OpenSearchTimeoutException("the repository gave up on its own request");
            switch (fate) {
                case LIVE:
                    break;
                case EXPIRED:
                    assertEquals(
                        fate + ": budgeted at the configured budget",
                        snapshotsService.repositoryIoTimeout(),
                        lastTimerDelay.get()
                    );
                    pendingTimers.poll().run();
                    assertTrue(
                        fate + ": the attempt must already be abandoned when the update that releases queued work is submitted",
                        abandonedAtSubmit.get(0)
                    );
                    assertEquals(fate + ": and that removal is the only update the expiry submits", List.of(SOURCE), submittedSources);
                    break;
                case ANSWERED_WITH_A_TIMEOUT:
                    dispatchedListener.get().onFailure(ownDeadline);
                    pendingTimers.poll().run();
                    assertFalse(
                        fate + ": a timer that lost the race must not abandon an answered attempt",
                        dispatchedAttempt.get().isAbandoned()
                    );
                    assertEquals(
                        fate + ": the entry's removal is submitted once, by the repository's own failure",
                        List.of(SOURCE),
                        submittedSources
                    );
                    break;
                case UNBUDGETED_ANSWERED_WITH_A_TIMEOUT:
                    assertTrue(fate + ": the premise: nothing is budgeted against this repository", pendingTimers.isEmpty());
                    dispatchedNarrowListener.get().onFailure(ownDeadline);
                    assertEquals(fate + ": its failure submits its own removal", List.of(SOURCE), submittedSources);
                    break;
            }

            final AtomicReference<Object> outcome = new AtomicReference<>();
            final ClusterState requested = requestDeleteIn(running, snapshot, outcome);
            if (fate != Fate.EXPIRED) {
                assertEquals(
                    fate + ": a request for a delete this node has not given up on joins it",
                    List.of(promoted.uuid()),
                    uuids(requested)
                );
                continue;
            }
            final List<SnapshotDeletionsInProgress.Entry> entries = deletionsOf(requested).getEntries();
            assertEquals(fate + ": a request after the delete it repeats timed out gets an entry of its own", 2, entries.size());
            final SnapshotDeletionsInProgress.Entry own = entries.get(1);
            assertNotEquals(promoted.uuid(), own.uuid());
            assertEquals(SnapshotDeletionsInProgress.State.WAITING, own.state());

            final int readsBefore = promotedDeleteReads.get();
            final ClusterState afterRemoval = runSubmittedRemoval(requested);
            assertNull(fate + ": the request is not answered with the given-up delete's outcome", outcome.get());
            assertEquals(List.of(own.uuid()), uuids(afterRemoval));
            assertEquals(SnapshotDeletionsInProgress.State.STARTED, deletionsOf(afterRemoval).getEntries().get(0).state());
            assertEquals(fate + ": and its own delete starts from a fresh read", readsBefore + 1, promotedDeleteReads.get());
        }
    }

    private static List<String> uuids(ClusterState state) {
        return deletionsOf(state).getEntries().stream().map(SnapshotDeletionsInProgress.Entry::uuid).collect(Collectors.toList());
    }

    private ClusterState requestDeleteIn(ClusterState state, SnapshotId snapshot, AtomicReference<Object> outcome) throws Exception {
        final ClusterStateUpdateTask request = deleteRequest(List.of(snapshot), repositoryDataWith(CAPTURED_GEN, snapshot), outcome);
        final ClusterState requested = request.execute(state);
        request.clusterStateProcessed("delete snapshots", state, requested);
        return requested;
    }

    @SuppressForbidden(reason = "the service's delete request update is private")
    private ClusterStateUpdateTask deleteRequest(
        List<SnapshotId> snapshotIds,
        RepositoryData repositoryData,
        AtomicReference<Object> outcome
    ) throws Exception {
        final Method method = SnapshotsService.class.getDeclaredMethod(
            "createDeleteStateUpdate",
            List.class,
            String.class,
            RepositoryData.class,
            Priority.class,
            ActionListener.class
        );
        method.setAccessible(true);
        return (ClusterStateUpdateTask) method.invoke(
            snapshotsService,
            snapshotIds,
            REPO,
            repositoryData,
            Priority.NORMAL,
            ActionListener.<Void>wrap(ignored -> outcome.set(SUCCESS), outcome::set)
        );
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testTheRepositoryIsHandedOnOnlyOnceTheRemovalHasPublished() throws Exception {
        final ClusterState state = runRemovalPromoting(
            List.of(completedSnapshotEntry(newSnapshotId("first")), completedSnapshotEntry(newSnapshotId("second")))
        );
        assertEquals("the premise: the first finalization reads the repository", 1, promotedFinalizationReads.get());
        pendingTimers.poll().run();
        assertEquals("the expired read submits the finalization's own removal", List.of(REMOVE_SNAPSHOT), submittedSources);
        final int index = submittedSources.indexOf(REMOVE_SNAPSHOT);
        submittedSources.remove(index);
        final ClusterStateUpdateTask removal = submittedTasks.remove(index);

        removal.onFailure(REMOVE_SNAPSHOT, new FailedToCommitClusterStateException("simulated publish failure"));
        assertEquals("the repository is handed on only once the removal has published", 1, promotedFinalizationReads.get());
        assertFalse(submittedSources.contains("Run ready deletions"));

        pendingTimers.poll().run();
        runSubmitted(REMOVE_SNAPSHOT, state);
        assertEquals("once it has, the next finalization reads for itself", 2, promotedFinalizationReads.get());
    }

    private enum StaleCallback {
        READ_ANSWERED,
        READ_FAILED,
        BUDGET_EXPIRED
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testACallbackIssuedBeforeAFailoverDoesNotActAfterTheNextElection() throws Exception {
        for (StaleCallback stale : StaleCallback.values()) {
            newFixture();
            supportAbandonment();
            final Deque<ActionListener<RepositoryData>> reads = new ArrayDeque<>();
            doAnswer(invocation -> {
                reads.add(invocation.getArgument(1));
                return null;
            }).when(repositoriesService).getRepositoryData(anyString(), any());
            final SnapshotId snapshot = newSnapshotId("rerun");
            final SnapshotDeletionsInProgress.Entry delete = startedDeletion(snapshot);
            final DiscoveryNode clusterManager = newNode("cluster-manager");
            final ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
                .nodes(
                    DiscoveryNodes.builder()
                        .add(clusterManager)
                        .localNodeId(clusterManager.getId())
                        .clusterManagerNodeId(clusterManager.getId())
                )
                .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .putCustom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.of(List.of(delete)))
                .build();

            elect(state);
            if (stale == StaleCallback.BUDGET_EXPIRED) {
                reads.removeFirst().onResponse(repositoryDataWith(CAPTURED_GEN, snapshot));
                assertEquals(stale + ": the premise: the first re-run was dispatched with a budget", 1, pendingTimers.size());
            }
            failOver();
            elect(state);
            final int dispatchesBefore = deleteDispatches.get();
            final AtomicReference<Object> joiner = listenForDelete(delete.uuid());

            switch (stale) {
                case READ_ANSWERED:
                    reads.removeFirst().onResponse(repositoryDataWith(CAPTURED_GEN, snapshot));
                    break;
                case READ_FAILED:
                    reads.removeFirst().onFailure(new RepositoryException(REPO, "the read failed"));
                    break;
                case BUDGET_EXPIRED:
                    pendingTimers.poll().run();
                    verify(repositoriesService, times(1).description(stale + ": the expired budget must still record its running call"))
                        .callPastBudget(any(AtomicBoolean.class), eq(REPO));
                    break;
            }
            assertTrue(stale + ": a callback issued before a failover must not act after it", submittedTasks.isEmpty());
            assertEquals(stale + ": nor dispatch anything", dispatchesBefore, deleteDispatches.get());

            assertEquals(stale + ": the premise: the latest re-run's read is the one outstanding", 1, reads.size());
            reads.removeFirst().onResponse(repositoryDataWith(CAPTURED_GEN, snapshot));
            assertEquals(stale + ": the latest re-run is dispatched", dispatchesBefore + 1, deleteDispatches.get());
            assertFalse(stale + ": with a live attempt", dispatchedAttempt.get().isAbandoned());
            assertNull(stale + ": and the callers that joined it are not answered", joiner.get());
            final ClusterState requested = requestDeleteIn(state, snapshot, new AtomicReference<>());
            assertEquals(stale + ": a request for its snapshots joins it", List.of(delete.uuid()), uuids(requested));
        }
    }

    private enum AfterTheClaim {
        BUDGET_EXPIRES_AFTER_THE_COMMIT,
        CALL_FAILS_AFTER_THE_COMMIT,
        BUDGET_EXPIRES_WHILE_THE_COMMIT_IS_IN_FLIGHT
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testABudgetedDeleteWhoseCommitTookEffectIsAnsweredWithSuccessByItsRemoval() throws Exception {
        for (AfterTheClaim row : AfterTheClaim.values()) {
            newFixture();
            final BudgetedDelete budgeted = budgetedDelete();
            final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
            final RepositoryData committed = repositoryDataWith(CAPTURED_GEN + 5L, budgeted.remaining);
            assertTrue(row.toString(), attempt.claimCommit());
            switch (row) {
                case BUDGET_EXPIRES_AFTER_THE_COMMIT:
                    attempt.committed(committed);
                    pendingTimers.poll().run();
                    break;
                case CALL_FAILS_AFTER_THE_COMMIT:
                    attempt.committed(committed);
                    dispatchedListener.get().onFailure(new RepositoryException(REPO, "a step after the commit failed"));
                    break;
                case BUDGET_EXPIRES_WHILE_THE_COMMIT_IS_IN_FLIGHT:
                    pendingTimers.poll().run();
                    assertTrue(row + ": nothing may be submitted while the commit is in flight", submittedTasks.isEmpty());
                    assertNull(row + ": and the delete must not be answered yet", budgeted.outcome.get());
                    attempt.committed(committed);
                    break;
            }
            final ClusterState after = runSubmittedRemoval(budgeted.state);
            assertEquals(
                row + ": the success removal must take the deleted snapshot out of the waiting delete",
                List.of(budgeted.remaining),
                deletionsOf(after).getEntries().get(0).getSnapshots()
            );
            assertEquals(
                row + ": and must distrust the data it carries, so the delete it promotes reads the repository",
                budgeted.readsBefore + 1,
                promotedDeleteReads.get()
            );
            assertWarnings(budgeted.warning());
            assertEquals(row + ": a delete whose generation committed must be answered with success", SUCCESS, budgeted.outcome.get());
        }
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testABudgetedDeleteIsFailedWithTheFailureOfItsInFlightCommit() throws Exception {
        final BudgetedDelete budgeted = budgetedDelete();
        final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
        assertTrue(attempt.claimCommit());
        pendingTimers.poll().run();
        assertTrue("nothing may be submitted while the commit is in flight", submittedTasks.isEmpty());
        final Exception expected = new RepositoryException(REPO, "the commit was not published");
        attempt.commitUnconfirmed(expected);
        runSubmittedRemoval(budgeted.state);
        assertSame("the delete must fail with the commit's own failure, not a timeout", expected, budgeted.outcome.get());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testACommittedDeleteWhoseRemovalLosesTheClusterManagerIsAnsweredWithSuccess() throws Exception {
        final BudgetedDelete budgeted = budgetedDelete();
        final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
        assertTrue(attempt.claimCommit());
        attempt.committed(repositoryDataWith(CAPTURED_GEN + 5L, budgeted.remaining));
        pendingTimers.poll().run();
        assertEquals(1, submittedTasks.size());
        final ClusterStateUpdateTask removal = submittedTasks.remove(0);
        removal.execute(budgeted.state);
        removal.onFailure(SOURCE, new NotClusterManagerException("no longer cluster manager"));
        assertEquals("a delete whose generation committed must be answered with success", SUCCESS, budgeted.outcome.get());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testACommittedDeleteIsAnsweredWithSuccessWhenAnotherUpdateLosesTheClusterManager() throws Exception {
        final BudgetedDelete budgeted = budgetedDelete();
        final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
        assertTrue(attempt.claimCommit());
        attempt.committed(repositoryDataWith(CAPTURED_GEN + 5L, budgeted.remaining));
        snapshotsService.createRemoveSnapshotDeletionTask(
            SOURCE,
            0,
            startedDeletion(newSnapshotId("unrelated")),
            new RepositoryException(REPO, "an unrelated delete failed"),
            RepositoryData.EMPTY,
            null,
            true
        ).onFailure(SOURCE, new NotClusterManagerException("no longer cluster manager"));
        assertEquals("a delete whose generation committed must be answered with success", SUCCESS, budgeted.outcome.get());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testACommittedDeleteIsAnsweredWithSuccessWhenItsRepositorysPendingOperationsAreFailed() throws Exception {
        final BudgetedDelete budgeted = budgetedDelete();
        final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
        assertTrue(attempt.claimCommit());
        attempt.committed(repositoryDataWith(CAPTURED_GEN + 5L, budgeted.remaining));
        final ClusterStateUpdateTask failPending = failPendingRepoTasksTask();
        final ClusterState after = failPending.execute(budgeted.state);
        snapshotsService.applyClusterState(new ClusterChangedEvent(SOURCE, after, budgeted.state));
        failPending.clusterStateProcessed(SOURCE, budgeted.state, after);
        assertEquals("a delete whose generation committed must be answered with success", SUCCESS, budgeted.outcome.get());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testADeleteThatCommitsBetweenTwoFailoversIsAnsweredWithSuccess() throws Exception {
        final BudgetedDelete budgeted = budgetedDelete();
        final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
        failOver();
        final AtomicReference<Object> answer = listenForDelete(startedDeletionIn(budgeted.state).uuid());
        assertTrue(attempt.claimCommit());
        attempt.committed(repositoryDataWith(CAPTURED_GEN + 5L, budgeted.remaining));
        failOver();
        assertEquals("a delete whose generation committed must be answered with success", SUCCESS, answer.get());
    }

    private static SnapshotDeletionsInProgress.Entry startedDeletionIn(ClusterState state) {
        return deletionsOf(state).getEntries()
            .stream()
            .filter(entry -> entry.state() == SnapshotDeletionsInProgress.State.STARTED)
            .findFirst()
            .orElseThrow();
    }

    private void failOver() {
        snapshotsService.createRemoveFailedSnapshotTask(
            REMOVE_SNAPSHOT,
            0,
            new Snapshot("other-repo", newSnapshotId("other")),
            new RepositoryException("other-repo", "failed"),
            null,
            null
        ).onFailure(REMOVE_SNAPSHOT, new NotClusterManagerException("no longer cluster manager"));
    }

    private static final String REMOVE_SNAPSHOT = "remove snapshot metadata";

    private static final String SUCCESS = "success";

    private static final class BudgetedDelete {
        final ClusterState state;
        final SnapshotId deleted;
        final SnapshotId remaining;
        final AtomicReference<Object> outcome;
        final int readsBefore;

        BudgetedDelete(ClusterState state, SnapshotId deleted, SnapshotId remaining, AtomicReference<Object> outcome, int readsBefore) {
            this.state = state;
            this.deleted = deleted;
            this.remaining = remaining;
            this.outcome = outcome;
            this.readsBefore = readsBefore;
        }

        String warning() {
            return "snapshots ["
                + deleted.getName()
                + "] were deleted from repository ["
                + REPO
                + "], but removal of the files they no longer use did not finish; some of those files may remain in the repository";
        }
    }

    private BudgetedDelete budgetedDelete() throws Exception {
        supportAbandonment();
        final SnapshotId deleted = newSnapshotId("deleted");
        final SnapshotId remaining = newSnapshotId("remaining");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(deleted, CAPTURED_GEN);
        final SnapshotDeletionsInProgress.Entry behind = waitingDeletion(List.of(deleted, remaining), CAPTURED_GEN);
        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, deleted, remaining));
        final ClusterState state = runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, queued, behind)),
            SnapshotsInProgress.EMPTY,
            new RepositoryException(REPO, "the delete did not complete"),
            repositoryDataWith(CAPTURED_GEN, deleted, remaining)
        );
        assertEquals("the premise: the promoted delete was budgeted", 1, pendingTimers.size());
        assertEquals("and dispatched alone", List.of(deleted), dispatchedSnapshotIds.get());
        final AtomicReference<Object> outcome = listenForDelete(queued.uuid());
        return new BudgetedDelete(state, deleted, remaining, outcome, promotedDeleteReads.get());
    }

    private ClusterState runSubmittedRemoval(ClusterState state) throws Exception {
        assertEquals("exactly one update must have been submitted", 1, submittedTasks.size());
        final ClusterStateUpdateTask removal = submittedTasks.remove(0);
        submittedSources.remove(0);
        final ClusterState after = removal.execute(state);
        removal.clusterStateProcessed(SOURCE, state, after);
        return after;
    }

    private static SnapshotDeletionsInProgress deletionsOf(ClusterState state) {
        return state.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY);
    }

    @SuppressForbidden(reason = "the service's delete listeners have no test seam")
    @SuppressWarnings("unchecked")
    private AtomicReference<Object> listenForDelete(String deleteUuid) throws Exception {
        final AtomicReference<Object> outcome = new AtomicReference<>();
        final Field field = SnapshotsService.class.getDeclaredField("snapshotDeletionListeners");
        field.setAccessible(true);
        ((Map<String, List<ActionListener<Void>>>) field.get(snapshotsService)).computeIfAbsent(deleteUuid, uuid -> new ArrayList<>())
            .add(ActionListener.wrap(ignored -> outcome.set(SUCCESS), outcome::set));
        return outcome;
    }

    @SuppressForbidden(reason = "the service's fail-pending-repository-tasks update is a private inner class")
    private ClusterStateUpdateTask failPendingRepoTasksTask() throws Exception {
        for (final Class<?> nested : SnapshotsService.class.getDeclaredClasses()) {
            if (nested.getSimpleName().equals("FailPendingRepoTasksTask") == false) {
                continue;
            }
            final Constructor<?> ctor = nested.getDeclaredConstructor(SnapshotsService.class, String.class, Exception.class);
            ctor.setAccessible(true);
            return (ClusterStateUpdateTask) ctor.newInstance(snapshotsService, REPO, new RepositoryException(REPO, "a read failed"));
        }
        throw new AssertionError("SnapshotsService no longer declares FailPendingRepoTasksTask");
    }

    private void supportAbandonment() {
        when(repository.abandonableSnapshotDelete()).thenReturn(Optional.of(abandonableEntrypoint));
    }

    private static SnapshotDeletionsInProgress.Entry startedDeletion(SnapshotId snapshotId) {
        return new SnapshotDeletionsInProgress.Entry(
            List.of(snapshotId),
            REPO,
            0L,
            CAPTURED_GEN,
            SnapshotDeletionsInProgress.State.STARTED
        );
    }

    private static SnapshotDeletionsInProgress.Entry waitingDeletion(SnapshotId snapshotId, long recordedGeneration) {
        return waitingDeletion(List.of(snapshotId), recordedGeneration);
    }

    private static SnapshotDeletionsInProgress.Entry waitingDeletion(List<SnapshotId> snapshotIds, long recordedGeneration) {
        return new SnapshotDeletionsInProgress.Entry(snapshotIds, REPO, 0L, recordedGeneration, SnapshotDeletionsInProgress.State.WAITING);
    }

    private static SnapshotsInProgress.Entry completedSnapshotEntry(SnapshotId snapshotId) {
        final SnapshotsInProgress.Entry entry = snapshotEntry(
            new Snapshot(REPO, snapshotId),
            Collections.emptyList(),
            Collections.emptyMap()
        );
        assert entry.state().completed() : "an entry with no shards must be completed for this test to mean anything";
        assert entry.repositoryStateId() != RepositoryData.UNKNOWN_REPO_GEN;
        return entry;
    }

    private static RepositoryData repositoryDataWith(long generation, SnapshotId... snapshotIds) {
        RepositoryData data = RepositoryData.EMPTY;
        for (SnapshotId snapshotId : snapshotIds) {
            data = data.addSnapshot(snapshotId, SnapshotState.SUCCESS, Version.CURRENT, ShardGenerations.EMPTY, null, null);
        }
        return data.withGenId(generation);
    }
}
