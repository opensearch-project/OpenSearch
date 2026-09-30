/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.opensearch.Version;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.RepositoryShardId;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class QueuedSnapshotReconciliationTests extends OpenSearchTestCase {

    private static final String REPO = "test-repo";

    private static final String SOURCE = "remove snapshot deletion metadata";

    private static final long CAPTURED_GEN = 5L;

    private TestThreadPool threadPool;
    private Repository repository;
    private RepositoriesService repositoriesService;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private SnapshotsService snapshotsService;

    private final AtomicReference<RepositoryData> reconciliationRepositoryData = new AtomicReference<>(RepositoryData.EMPTY);

    private final AtomicReference<ClusterState> reconciliationInput = new AtomicReference<>();
    private final AtomicReference<ClusterState> reconciliationOutput = new AtomicReference<>();

    private final Deque<Runnable> pendingRepositoryReads = new ArrayDeque<>();

    private final AtomicInteger readFailuresBeforeAnAnswer = new AtomicInteger();

    private final AtomicInteger metadataMovesBeforeExecute = new AtomicInteger();

    private record Schedule(Runnable task, TimeValue delay, String executor) {
    }

    private final Deque<Schedule> pendingSchedules = new ArrayDeque<>();

    private final Deque<Schedule> pendingBudgetTimers = new ArrayDeque<>();

    private final Deque<Runnable> pendingDispatchedDrives = new ArrayDeque<>();

    private final AtomicInteger schedulesToRejectBeforeOneIsArmed = new AtomicInteger();

    private final List<String> submittedSources = new ArrayList<>();
    private final List<ClusterStateUpdateTask> submittedTasks = new ArrayList<>();

    private final AtomicInteger reconciliationPasses = new AtomicInteger();

    private final AtomicInteger finalizationReads = new AtomicInteger();

    private final AtomicLong finalizedGeneration = new AtomicLong(Long.MIN_VALUE);
    private final AtomicReference<ActionListener<RepositoryData>> finalizationListener = new AtomicReference<>();

    private boolean captureSnapshotExecutor;

    private final Deque<Runnable> pendingSnapshotTasks = new ArrayDeque<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());

        repository = mock(Repository.class);
        when(repository.getMetadata()).thenReturn(new RepositoryMetadata(REPO, "mock", Settings.EMPTY));
        doAnswer(invocation -> {
            finalizationReads.incrementAndGet();
            return null;
        }).when(repository).getRepositoryData(any());
        doAnswer(invocation -> {
            finalizedGeneration.set(invocation.<Long>getArgument(1));
            finalizationListener.set(invocation.getArgument(7));
            return null;
        }).when(repository).finalizeSnapshot(any(), anyLong(), any(), any(), any(), any(), any(), any());
        doAnswer(invocation -> {
            pendingRepositoryReads.add(repositoryRead(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2)));
            return null;
        }).when(repository).executeConsistentStateUpdate(any(), anyString(), any());

        repositoriesService = mock(RepositoriesService.class);
        when(repositoriesService.repository(REPO)).thenReturn(repository);

        clusterService = mock(ClusterService.class);
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        doAnswer(invocation -> {
            submittedSources.add(invocation.getArgument(0));
            submittedTasks.add(invocation.getArgument(1));
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        when(clusterService.state()).thenReturn(clusterState(List.of(), List.of(), List.of(), null));
        indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);

        final ThreadPool interceptedThreadPool = spy(threadPool);
        doAnswer(invocation -> {
            final String executor = invocation.getArgument(2);
            if (ThreadPool.Names.GENERIC.equals(executor) && invocation.getArgument(0) instanceof ActionListener) {
                final Schedule budget = new Schedule(invocation.getArgument(0), invocation.getArgument(1), executor);
                pendingBudgetTimers.add(budget);
                final Scheduler.ScheduledCancellable handle = mock(Scheduler.ScheduledCancellable.class);
                doAnswer(ignored -> pendingBudgetTimers.remove(budget)).when(handle).cancel();
                return handle;
            }
            if (ThreadPool.Names.GENERIC.equals(executor) == false && ThreadPool.Names.SNAPSHOT.equals(executor) == false) {
                return invocation.callRealMethod();
            }
            if (schedulesToRejectBeforeOneIsArmed.get() > 0) {
                schedulesToRejectBeforeOneIsArmed.decrementAndGet();
                throw new OpenSearchRejectedExecutionException("the scheduler is shut down", true);
            }
            pendingSchedules.add(new Schedule(invocation.getArgument(0), invocation.getArgument(1), executor));
            return mock(Scheduler.ScheduledCancellable.class);
        }).when(interceptedThreadPool).schedule(any(), any(), anyString());
        final ExecutorService dispatchedDrives = mock(ExecutorService.class);
        doAnswer(invocation -> {
            pendingDispatchedDrives.add(invocation.getArgument(0));
            return null;
        }).when(dispatchedDrives).execute(any());
        doAnswer(invocation -> dispatchedDrives).when(interceptedThreadPool).generic();

        final ExecutorService capturedSnapshotPool = mock(ExecutorService.class);
        doAnswer(invocation -> {
            pendingSnapshotTasks.add(invocation.getArgument(0));
            return null;
        }).when(capturedSnapshotPool).execute(any());
        doAnswer(invocation -> captureSnapshotExecutor ? capturedSnapshotPool : invocation.callRealMethod()).when(interceptedThreadPool)
            .executor(ThreadPool.Names.SNAPSHOT);

        final TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(interceptedThreadPool);

        snapshotsService = new SnapshotsService(
            Settings.builder().put("node.name", "test").putList("node.roles", "cluster_manager", "data").build(),
            clusterService,
            indexNameExpressionResolver,
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

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testQueuedSnapshotOfAnIndexTheRepositoryDoesNotKnowIsReboundAndStarted() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final String dataNodeId = uuid();
        final IndexId preAbandonment = new IndexId("idx", uuid());

        final SnapshotsInProgress.Entry queuedEntry = queuedSnapshot(newSnapshotId("queued"), preAbandonment, shardId);
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();

        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));

        final ClusterState reconciled = runRemovalAndReconciliation(
            clusterState(indexMetadata, startedPrimary(shardId, dataNodeId), List.of(queuedEntry), abandoned),
            abandoned
        );

        assertEquals("the removal owes and drives exactly one reconciliation", 1, reconciliationPasses.get());
        final SnapshotsInProgress.Entry updated = entryOf(reconciled, 0);
        assertEquals("the rebind is by name, so the name may not change", "idx", updated.indices().get(0).getName());
        assertNotEquals(
            "the entry must not keep the identifier it resolved before the delete was given up on",
            preAbandonment.getId(),
            updated.indices().get(0).getId()
        );
        assertEquals(SnapshotsInProgress.ShardState.INIT, updated.shards().get(shardId).state());
        assertEquals(dataNodeId, updated.shards().get(shardId).nodeId());
        assertTrue("an entry that was started owes no further rebind", reconciliationOwed().isEmpty());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testSuccessfulDeleteRemovalDoesNotStartShardsQueuedBehindAnAbandonedDelete() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId preAbandonment = new IndexId("idx", uuid());

        final SnapshotsInProgress.Entry queuedEntry = queuedSnapshot(newSnapshotId("queued"), preAbandonment, shardId);
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion();
        reconciliationOwed().add(REPO);

        final ClusterState currentState = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queuedEntry), removed);
        primeRunningDelete(removed.uuid());
        final ClusterState newState = snapshotsService.createRemoveSnapshotDeletionTask(
            SOURCE,
            0,
            removed,
            null,
            RepositoryData.EMPTY.withGenId(CAPTURED_GEN),
            null,
            true
        ).execute(currentState);

        final SnapshotsInProgress.Entry unchanged = entryOf(newState, 0);
        assertEquals(
            "a queued shard must not be started while its entry's identity is unbound",
            SnapshotsInProgress.ShardState.QUEUED,
            unchanged.shards().get(shardId).state()
        );
        assertEquals("and its identity must not have been touched either", List.of(preAbandonment), unchanged.indices());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testCreateWaitsOnlyForAnIdentifierItInheritedFromAnotherEntry() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId preAbandonment = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queued = queuedSnapshot(newSnapshotId("queued"), preAbandonment, shardId);
        reconciliationOwed().add(REPO);

        final SnapshotsInProgress.Entry inherited = createdEntry(
            clusterState(List.of(indexMetadata), List.of(startedPrimary(shardId, uuid())), List.of(queued), null),
            RepositoryData.EMPTY.withGenId(CAPTURED_GEN)
        );
        assertEquals("the create inherits the queued entry's identifier", List.of(preAbandonment), inherited.indices());
        assertEquals(
            "so it must not start before a reconciliation has replaced it",
            SnapshotsInProgress.ShardState.QUEUED,
            inherited.shards().get(shardId).state()
        );

        final IndexId authoritative = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry known = createdEntry(
            clusterState(List.of(indexMetadata), List.of(startedPrimary(shardId, uuid())), List.of(), null),
            repositoryDataHolding(CAPTURED_GEN, authoritative)
        );
        assertEquals("the create is bound to the repository's own identifier", List.of(authoritative), known.indices());
        assertEquals(
            "which a reconciliation would not change, so there is nothing to wait for",
            SnapshotsInProgress.ShardState.INIT,
            known.shards().get(shardId).state()
        );
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnEntryThePassCompletesIsFinalizedWithTheDataThePassRead() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final ShardId deletedIndexShardId = new ShardId(new Index("gone", uuid()), 0);
        final SnapshotsInProgress.Entry completedByThePass = partialSnapshotEntry(
            newSnapshotId("completed-by-the-pass"),
            List.of(new IndexId("gone", uuid())),
            Map.of(deletedIndexShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final IndexId idx = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry startedByThePass = queuedSnapshot(newSnapshotId("started-by-the-pass"), idx, shardId);
        final SnapshotsInProgress.Entry queuedBehindIt = queuedSnapshot(newSnapshotId("queued-behind-it"), idx, shardId);
        final AtomicReference<Object> completedOutcome = listenFor(completedByThePass.snapshot());
        final AtomicReference<Object> startedOutcome = listenFor(startedByThePass.snapshot());
        final AtomicReference<Object> queuedOutcome = listenFor(queuedBehindIt.snapshot());
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        final long passGeneration = CAPTURED_GEN + 3L;
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(passGeneration));

        final ClusterState reconciled = runRemovalAndReconciliation(
            clusterState(
                indexMetadata,
                startedPrimary(shardId, uuid()),
                List.of(completedByThePass, startedByThePass, queuedBehindIt),
                abandoned
            ),
            abandoned
        );
        assertEquals(
            "every shard of the entry of the deleted index resolves missing",
            SnapshotsInProgress.ShardState.MISSING,
            entryOf(reconciled, 0).shards().get(deletedIndexShardId).state()
        );
        assertEquals(
            "so it has nobody left to report to it and must be published as complete",
            SnapshotsInProgress.State.SUCCESS,
            entryOf(reconciled, 0).state()
        );
        assertEquals(SnapshotsInProgress.ShardState.INIT, entryOf(reconciled, 1).shards().get(shardId).state());
        assertEquals(SnapshotsInProgress.ShardState.QUEUED, entryOf(reconciled, 2).shards().get(shardId).state());

        assertEquals("the entry the pass completed must be finalized without reading the repository again", 0, finalizationReads.get());
        assertEquals("with the generation the pass read", passGeneration, finalizedGeneration.get());

        final int submittedBefore = submittedSources.size();
        finalizationListener.get().onFailure(new RepositoryException(REPO, "the finalization did not complete"));
        assertEquals(
            "a failed finalization of the completed entry must submit its own removal and nothing that fails other entries",
            List.of("remove snapshot metadata"),
            submittedSources.subList(submittedBefore, submittedSources.size())
        );
        final ClusterStateUpdateTask removal = submittedTasks.get(submittedBefore);
        final ClusterState afterRemoval = removal.execute(reconciled);
        removal.clusterStateProcessed("remove snapshot metadata", reconciled, afterRemoval);
        assertEquals(
            "only the entry whose finalization failed may leave the cluster state",
            List.of(startedByThePass.snapshot(), queuedBehindIt.snapshot()),
            snapshotsOf(afterRemoval).entries().stream().map(SnapshotsInProgress.Entry::snapshot).collect(Collectors.toList())
        );
        assertNotNull("its caller is told", completedOutcome.get());
        assertNull("the snapshot the pass started is not failed", startedOutcome.get());
        assertNull("nor the one queued behind it", queuedOutcome.get());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testARetainedDebtIsReDrivenOnlyByAChangeToASnapshotOrDeletionCustom() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId shared = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry running = snapshotEntry(
            newSnapshotId("running"),
            List.of(shared),
            Map.of(shardId, new SnapshotsInProgress.ShardSnapshotStatus(uuid(), uuid()))
        );
        final SnapshotsInProgress.Entry queued = queuedSnapshot(newSnapshotId("queued"), shared, shardId);
        reconciliationOwed().add(REPO);
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));
        final ClusterState state = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(running, queued), null);
        reconciliationInput.set(state);

        applyAsClusterManager(state, ClusterState.builder(state).routingTable(RoutingTable.builder().build()).build());
        assertEquals("an event that changes neither custom must drive no repository read", 0, reconciliationPasses.get());

        applyAsClusterManager(state, ClusterState.builder(state).putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).build());
        assertEquals("a change to the snapshots in progress must re-drive the debt", 1, reconciliationPasses.get());
        assertTrue("which this pass retains again, because the name is still held", reconciliationOwed().contains(REPO));

        applyAsClusterManager(
            state,
            ClusterState.builder(state)
                .putCustom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.of(List.of(startedDeletion())))
                .build()
        );
        assertEquals("and so must a change to the deletions in progress on its own", 2, reconciliationPasses.get());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAReconciliationReadThatOutlivesItsBudgetIsRetriedWithoutASecondRead() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final String dataNodeId = uuid();
        final IndexId preAbandonment = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queued = queuedSnapshot(newSnapshotId("queued"), preAbandonment, shardId);
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));
        final AtomicReference<Object> createOutcome = listenFor(queued.snapshot());
        setBudget(TimeValue.timeValueSeconds(7));

        runRemovalLeavingTheReadOut(
            clusterState(indexMetadata, startedPrimary(shardId, dataNodeId), List.of(queued), abandoned),
            abandoned
        );
        assertEquals(
            "the attempt's read must be given the repository I/O budget",
            List.of(TimeValue.timeValueSeconds(7)),
            pendingBudgetTimers.stream().map(Schedule::delay).collect(Collectors.toList())
        );
        assertEquals("the attempt's read must be given the repository I/O budget", 1, pendingRepositoryReads.size());

        runBudgetTimer();
        assertEquals("its expiry must arm the next attempt one rung up", 1, pendingSchedules.size());
        assertEquals("its expiry must arm the next attempt one rung up", ThreadPool.Names.GENERIC, pendingSchedules.peek().executor());
        assertEquals("its expiry must arm the next attempt one rung up", TimeValue.timeValueSeconds(2), pendingSchedules.peek().delay());
        assertTrue("and keep the guard and the debt", reconcilingRepositories().contains(REPO) && reconciliationOwed().contains(REPO));
        assertNull("without failing or starting anything", createOutcome.get());
        assertNull("without failing or starting anything", reconciliationOutput.get());
        verify(clusterService, never()).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));

        runOnlyPendingSchedule();
        assertEquals("no further read of a repository may be issued while one given up on is still out", 1, pendingRepositoryReads.size());
        assertTrue("no further read of a repository may be issued while one given up on is still out", pendingBudgetTimers.isEmpty());
        verify(repository, times(1)).executeConsistentStateUpdate(any(), anyString(), any());
        assertEquals("the attempt that found the read out arms the next one", 1, pendingSchedules.size());
        assertEquals(
            "the attempt that found the read out arms the next one",
            TimeValue.timeValueSeconds(4),
            pendingSchedules.peek().delay()
        );

        pendingRepositoryReads.poll().run();
        assertSame(
            "an answer after the budget ran out must leave the state as it found it",
            reconciliationInput.get(),
            reconciliationOutput.get()
        );
        assertTrue(
            "and must not release the guard or the debt",
            reconcilingRepositories().contains(REPO) && reconciliationOwed().contains(REPO)
        );
        assertTrue("and must clear the way for the next attempt to read", readsOut().isEmpty());
        assertEquals("and must clear the way for the next attempt to read", 1, pendingSchedules.size());

        runOnlyPendingSchedule();
        assertEquals("the next attempt reads under its own budget", 1, pendingBudgetTimers.size());
        runPendingRepositoryReads();
        final SnapshotsInProgress.Entry started = entryOf(reconciliationOutput.get(), 0);
        assertEquals(
            "the update that claims its attempt must start the queued shard",
            SnapshotsInProgress.ShardState.INIT,
            started.shards().get(shardId).state()
        );
        assertEquals("the update that claims its attempt must start the queued shard", dataNodeId, started.shards().get(shardId).nodeId());
        assertNotEquals(
            "the update that claims its attempt must start the queued shard",
            preAbandonment.getId(),
            started.indices().get(0).getId()
        );
        assertTrue("the update that claims its attempt must start the queued shard", reconciliationOwed().isEmpty());
        assertFalse("the update that claims its attempt must start the queued shard", reconcilingRepositories().contains(REPO));
        assertTrue("and cancel its budget", pendingBudgetTimers.isEmpty());
        assertTrue("and cancel its budget", pendingSchedules.isEmpty());
        assertNull("and cancel its budget", createOutcome.get());
        assertTrue("and cancel its budget", readsOut().isEmpty());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testTheBudgetOfAReconciliationReadCoversARereadOfIt() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final String dataNodeId = uuid();
        final SnapshotsInProgress.Entry queued = queuedSnapshot(newSnapshotId("queued"), new IndexId("idx", uuid()), shardId);
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));
        final AtomicReference<Object> createOutcome = listenFor(queued.snapshot());
        setBudget(TimeValue.timeValueSeconds(7));
        metadataMovesBeforeExecute.set(1);

        runRemovalLeavingTheReadOut(
            clusterState(indexMetadata, startedPrimary(shardId, dataNodeId), List.of(queued), abandoned),
            abandoned
        );
        pendingRepositoryReads.poll().run();
        assertEquals("the premise: the read's update was built, not run, and the read asked for again", 1, pendingRepositoryReads.size());
        assertNull("the premise: the read's update was built, not run, and the read asked for again", reconciliationOutput.get());
        assertEquals("the budget must still govern a re-read", 1, pendingBudgetTimers.size());
        assertTrue("and the re-read must hold the repository as the read out", readsOut().contains(repository));

        runBudgetTimer();
        runOnlyPendingSchedule();
        assertEquals("no further read while the re-read is out", 1, pendingRepositoryReads.size());
        verify(repository, times(1)).executeConsistentStateUpdate(any(), anyString(), any());
        assertEquals("no further read while the re-read is out", TimeValue.timeValueSeconds(4), pendingSchedules.peek().delay());

        pendingRepositoryReads.poll().run();
        assertSame(
            "the re-read answered after the budget must leave the state as it found it",
            reconciliationInput.get(),
            reconciliationOutput.get()
        );
        assertTrue("and give the repository back", readsOut().isEmpty());

        runOnlyPendingSchedule();
        runPendingRepositoryReads();
        assertEquals(
            "the next attempt reads and starts the queued shard",
            SnapshotsInProgress.ShardState.INIT,
            entryOf(reconciliationOutput.get(), 0).shards().get(shardId).state()
        );
        assertTrue("and cancels its budget", pendingBudgetTimers.isEmpty());
        assertNull("and fails nothing", createOutcome.get());
        assertTrue("and gives the repository back", readsOut().isEmpty());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testADemotedNodeKeepsItsReconciliationDebt() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final SnapshotsInProgress.Entry queued = queuedSnapshot(newSnapshotId("queued"), new IndexId("idx", uuid()), shardId);
        reconciliationOwed().add(REPO);
        final ClusterState asClusterManager = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queued), null);
        final ClusterState demoted = withoutLocalClusterManager(asClusterManager);

        snapshotsService.applyClusterState(new ClusterChangedEvent("test", demoted, asClusterManager));

        assertTrue("a demoted node keeps the debt, because nothing else records it", reconciliationOwed().contains(REPO));
        assertTrue("and hands no attempt for it to the generic pool", pendingDispatchedDrives.isEmpty());
        assertTrue("nor arms any", pendingSchedules.isEmpty());
        assertFalse("nor admits a loop for it", reconcilingRepositories().contains(REPO));

        reconciliationOwed().remove(REPO);
        snapshotsService.applyClusterState(new ClusterChangedEvent("test", demoted, demoted));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnArmedRetryHoldsItsPlaceWhileThisNodeIsNotClusterManager() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final String dataNodeId = uuid();
        final IndexId preAbandonment = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queuedEntry = queuedSnapshot(newSnapshotId("queued"), preAbandonment, shardId);
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));
        readFailuresBeforeAnAnswer.set(1);

        runRemoval(clusterState(indexMetadata, startedPrimary(shardId, dataNodeId), List.of(queuedEntry), abandoned), abandoned);
        assertEquals("the failed read must have armed a retry", 1, pendingSchedules.size());
        final TimeValue armed = pendingSchedules.peekLast().delay();

        final ClusterState reElected = reconciliationInput.get();
        snapshotsService.applyClusterState(new ClusterChangedEvent("test", reElected, withoutLocalClusterManager(reElected)));
        runPendingRepositoryReads();
        assertEquals("the resume path must decline while a loop is in flight", 0, reconciliationPasses.get());
        assertEquals("and must not arm a second attempt alongside the one that is armed", 1, pendingSchedules.size());
        assertTrue("nor hand one to the generic pool", pendingDispatchedDrives.isEmpty());

        when(clusterService.state()).thenReturn(withoutLocalClusterManager(reElected));
        runOnlyPendingSchedule();
        runPendingRepositoryReads();

        assertEquals("a demoted node must not read a repository it no longer publishes for", 0, reconciliationPasses.get());
        assertEquals(
            "but the chain has to stay armed, because it is the only thing that comes back to this debt",
            1,
            pendingSchedules.size()
        );
        assertEquals(
            "at the delay it already had, because a role check is not a failed attempt",
            armed,
            pendingSchedules.peekLast().delay()
        );
        assertTrue("and the debt stays, because only the attempt was held", reconciliationOwed().contains(REPO));
        assertTrue("with the repository still inside the in-flight guard it never released", reconcilingRepositories().contains(REPO));

        when(clusterService.state()).thenReturn(reconciliationInput.get());
        runArmedRetry();
        assertEquals("the suspended chain is what reconciles once the role returns", 1, reconciliationPasses.get());
        final SnapshotsInProgress.Entry updated = entryOf(reconciliationOutput.get(), 0);
        assertNotEquals(
            "which replaces the identifier the entry resolved before the delete was given up on",
            preAbandonment.getId(),
            updated.indices().get(0).getId()
        );
        assertEquals(SnapshotsInProgress.ShardState.INIT, updated.shards().get(shardId).state());
        assertTrue("and discharges the debt", reconciliationOwed().isEmpty());
        assertFalse("releasing the guard with it", reconcilingRepositories().contains(REPO));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testTheRetryLaddersAreTimedAndNeverImmediate() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final SnapshotsInProgress.Entry queuedEntry = queuedSnapshot(newSnapshotId("queued"), new IndexId("idx", uuid()), shardId);
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));
        final int failures = 12;
        readFailuresBeforeAnAnswer.set(failures);
        final AtomicInteger warnings = new AtomicInteger();
        final List<TimeValue> readRetries = new ArrayList<>();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(SnapshotsService.class))) {
            appender.addExpectation(new MockLogAppender.LoggingExpectation() {
                @Override
                public void match(LogEvent event) {
                    if (event.getLevel() == Level.WARN
                        && event.getMessage().getFormattedMessage().contains("still cannot reconcile queued snapshots")) {
                        warnings.incrementAndGet();
                    }
                }

                @Override
                public void assertMatched() {}
            });
            runRemoval(clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queuedEntry), abandoned), abandoned);
            assertTrue("a failed read must give its repository back for the next attempt", readsOut().isEmpty());
            assertTrue("and cancel its budget", pendingBudgetTimers.isEmpty());
            for (int attempt = 1; attempt < failures; attempt++) {
                assertEquals(
                    "the attempt past the budget must still be armed, because nothing else comes back to a debt a read failure holds up",
                    1,
                    pendingSchedules.size()
                );
                assertTrue("with the debt still recorded", reconciliationOwed().contains(REPO));
                readRetries.add(pendingSchedules.peekLast().delay());
                assertEquals(ThreadPool.Names.GENERIC, pendingSchedules.peekLast().executor());
                runArmedRetry();
            }
            readRetries.add(pendingSchedules.peekLast().delay());
            assertEquals(ThreadPool.Names.GENERIC, pendingSchedules.peekLast().executor());
        }
        final List<TimeValue> ladder = new ArrayList<>(
            List.of(
                TimeValue.timeValueSeconds(2),
                TimeValue.timeValueSeconds(4),
                TimeValue.timeValueSeconds(8),
                TimeValue.timeValueSeconds(16)
            )
        );
        while (ladder.size() < failures) {
            ladder.add(TimeValue.timeValueSeconds(30));
        }
        assertEquals("a failed reconciliation read is retried one rung up each time, up to the cap", ladder, readRetries);
        assertTrue("and never at once", readRetries.stream().allMatch(delay -> delay.millis() > 0));
        assertEquals("the operator is warned once in the run", 1, warnings.get());
        assertEquals("a read that failed is not a reconciliation", 0, reconciliationPasses.get());
        applyAsClusterManager(
            reconciliationInput.get(),
            ClusterState.builder(reconciliationInput.get()).putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).build()
        );
        assertEquals("a re-drive must not arm a second retry", 1, pendingSchedules.size());
        assertEquals("nor drive a read alongside the armed one", 0, reconciliationPasses.get());
        runArmedRetry();
        assertEquals("the attempt past the budget reconciles once the read answers", 1, reconciliationPasses.get());
        assertTrue("the read that answers discharges the debt", reconciliationOwed().isEmpty());
        assertTrue("leaving no retry armed", pendingSchedules.isEmpty());

        final SnapshotsInProgress.Entry queuedCreate = queuedSnapshot(newSnapshotId("queued-create"), new IndexId("idx", uuid()), shardId);
        final AtomicReference<Object> createOutcome = listenFor(queuedCreate.snapshot());
        reconciliationOwed().add(REPO);
        final ClusterState state = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queuedCreate), null);
        reconciliationInput.set(state);
        ClusterStateUpdateTask failTask = failPendingRepoTasksTask();
        final List<TimeValue> publicationRetries = new ArrayList<>();
        for (int attempt = 0; attempt < 6; attempt++) {
            failTask.execute(state);
            final int submitted = submittedTasks.size();
            failTask.onFailure(FAIL_TASKS_SOURCE, new FailedToCommitClusterStateException("simulated publish failure"));
            assertNull("a publication that did not commit must not fail the create it retained", createOutcome.get());
            assertTrue("the publication must be retried instead", pendingSchedules.isEmpty() == false);
            assertEquals("and nothing else may be submitted in its place", submitted, submittedTasks.size());
            publicationRetries.add(runOnlyPendingSchedule().delay());
            assertEquals("the retry resubmits the update", submitted + 1, submittedTasks.size());
            failTask = submittedTasks.get(submitted);
        }
        assertEquals(
            "a publication that keeps failing to commit is retried from one second, doubling to the cap",
            List.of(
                TimeValue.timeValueSeconds(1),
                TimeValue.timeValueSeconds(2),
                TimeValue.timeValueSeconds(4),
                TimeValue.timeValueSeconds(8),
                TimeValue.timeValueSeconds(16),
                TimeValue.timeValueSeconds(30)
            ),
            publicationRetries
        );
        reconciliationOwed().remove(REPO);
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnArmedRetryWithNothingOwedReadsNothingAndReleasesTheRepository() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final String dataNodeId = uuid();
        final IndexId preAbandonment = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queuedEntry = queuedSnapshot(newSnapshotId("queued"), preAbandonment, shardId);
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));
        readFailuresBeforeAnAnswer.set(1);

        runRemoval(clusterState(indexMetadata, startedPrimary(shardId, dataNodeId), List.of(queuedEntry), abandoned), abandoned);
        assertEquals("the failed read must have armed a retry", 1, pendingSchedules.size());

        reconciliationOwed().remove(REPO);
        runArmedRetry();

        assertEquals("an attempt with nothing owed must not read the repository", 0, reconciliationPasses.get());
        assertTrue("nor arm another attempt", pendingSchedules.isEmpty());
        assertFalse("and must release the repository", reconcilingRepositories().contains(REPO));

        reconciliationOwed().add(REPO);
        final ClusterState published = reDriveFromClusterState(reconciliationInput.get());
        assertEquals("a later debt must get a loop of its own", 1, reconciliationPasses.get());
        assertEquals(SnapshotsInProgress.ShardState.INIT, entryOf(published, 0).shards().get(shardId).state());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAFailoverRecordsWhatThePassOwesAndDrivesItOnlyWithTheRepositoryFree() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata first = indexMetadata("first");
        final IndexMetadata second = indexMetadata("second");
        final ShardId firstShardId = new ShardId(first.getIndex(), 0);
        final ShardId secondShardId = new ShardId(second.getIndex(), 0);
        final IndexId firstId = new IndexId("first", uuid());
        final IndexId secondId = new IndexId("second", uuid());
        final RepositoryShardId firstRepoShardId = new RepositoryShardId(firstId, 0);
        final SnapshotId source = newSnapshotId("clone-source");
        final String dataNodeId = uuid();

        for (boolean ofAClone : new boolean[] { false, true }) {
            for (boolean begun : new boolean[] { false, true }) {
                for (boolean deletionOwnsTheRepository : new boolean[] { false, true }) {
                    final String row = (ofAClone ? "a clone" : "a create") + (begun ? " that has begun" : " that has begun nothing")
                        + (deletionOwnsTheRepository ? " while a deletion owns the repository" : " with the repository free");
                    final SnapshotsInProgress.Entry entry = ofAClone
                        ? cloneEntry(
                            "clone",
                            source,
                            List.of(firstId, secondId),
                            begun
                                ? Map.of(
                                    firstRepoShardId,
                                    SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED,
                                    new RepositoryShardId(secondId, 0),
                                    new SnapshotsInProgress.ShardSnapshotStatus(uuid(), "gen-begun")
                                )
                                : Map.of(firstRepoShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
                        )
                        : snapshotEntry(
                            newSnapshotId("create"),
                            begun ? List.of(firstId, secondId) : List.of(firstId),
                            begun
                                ? Map.of(
                                    firstShardId,
                                    SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED,
                                    secondShardId,
                                    new SnapshotsInProgress.ShardSnapshotStatus(dataNodeId, "gen-begun")
                                )
                                : Map.of(firstShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
                        );
                    final ClusterState elected = clusterState(
                        List.of(first, second),
                        List.of(startedPrimary(firstShardId, dataNodeId), startedPrimary(secondShardId, dataNodeId)),
                        List.of(entry),
                        deletionOwnsTheRepository ? startedDeletion() : null
                    );
                    stubLocalNode(elected);
                    reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, firstId, secondId));
                    reconciliationOwed().clear();
                    reconcilingRepositories().clear();
                    pendingDispatchedDrives.clear();
                    pendingSnapshotTasks.clear();
                    reconciliationInput.set(elected);
                    reconciliationOutput.set(null);

                    snapshotsService.applyClusterState(new ClusterChangedEvent("test", elected, withoutLocalClusterManager(elected)));

                    final boolean owed = ofAClone == false || begun == false;
                    assertEquals("the debt inferred for " + row, owed, reconciliationOwed().contains(REPO));
                    assertEquals(
                        "the pass driven for " + row,
                        owed && deletionOwnsTheRepository == false ? 1 : 0,
                        pendingDispatchedDrives.size()
                    );
                    assertTrue("nothing may be read on the applier thread for " + row, pendingRepositoryReads.isEmpty());
                    assertTrue("nor any retry armed for " + row, pendingSchedules.isEmpty());
                    if (owed == false || deletionOwnsTheRepository) {
                        continue;
                    }
                    when(clusterService.state()).thenReturn(withoutLocalClusterManager(elected));
                    runDispatchedReconciliation();
                    assertEquals(
                        "what the dispatched attempt makes of " + row + ", whatever the role reads",
                        begun ? SnapshotsInProgress.ShardState.QUEUED : SnapshotsInProgress.ShardState.INIT,
                        ofAClone
                            ? cloneStatus(reconciliationOutput.get(), entry, firstRepoShardId).state()
                            : entryFor(reconciliationOutput.get(), entry.snapshot()).shards().get(firstShardId).state()
                    );
                    assertTrue("and discharges the debt for " + row, reconciliationOwed().isEmpty());
                    pendingSnapshotTasks.clear();
                }
            }
        }
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnAttemptThatThrowsArmsItsSuccessor() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final SnapshotsInProgress.Entry queuedEntry = queuedSnapshot(newSnapshotId("queued"), new IndexId("idx", uuid()), shardId);
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        doThrow(new IllegalStateException("the repository threw before it read")).when(repository)
            .executeConsistentStateUpdate(any(), anyString(), any());

        runRemoval(clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queuedEntry), abandoned), abandoned);

        assertEquals("a throw out of an attempt must arm its successor", 1, pendingSchedules.size());
        assertTrue("and the budget armed before the throw must be failed with it", pendingBudgetTimers.isEmpty());
        assertTrue("and the repository given back", readsOut().isEmpty());
        assertEquals("having driven no pass", 0, reconciliationPasses.get());
        assertTrue("with the debt still recorded", reconciliationOwed().contains(REPO));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnAttemptForAMissingRepositoryClearsBothTheGuardAndTheDebt() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final SnapshotsInProgress.Entry queuedEntry = queuedSnapshot(newSnapshotId("queued"), new IndexId("idx", uuid()), shardId);
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        final AtomicReference<Object> queuedCreateOutcome = listenFor(queuedEntry.snapshot());
        readFailuresBeforeAnAnswer.set(1);

        runRemoval(clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queuedEntry), abandoned), abandoned);
        assertEquals("the failed read must have armed a retry", 1, pendingSchedules.size());
        assertTrue("which holds the in-flight guard while it is armed", reconcilingRepositories().contains(REPO));
        assertTrue("and the debt it was armed for", reconciliationOwed().contains(REPO));

        doThrow(new RepositoryMissingException(REPO)).when(repositoriesService).repository(REPO);
        runArmedRetry();

        assertFalse(
            "a gone repository must release the in-flight guard, or one registered again under the same name is refused a loop",
            reconcilingRepositories().contains(REPO)
        );
        assertFalse(
            "and the debt with it, because no pass can discharge a debt whose repository no longer exists",
            reconciliationOwed().contains(REPO)
        );
        assertEquals("having read no repository", 0, reconciliationPasses.get());
        assertTrue("and armed nothing that would read one later", pendingSchedules.isEmpty());
        assertNull("and without failing the queued snapshot, which this loop must never do", queuedCreateOutcome.get());
        verify(clusterService, never()).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testARejectedRetryArmReleasesTheGuardAndKeepsTheDebt() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId preAbandonment = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queuedEntry = queuedSnapshot(newSnapshotId("queued"), preAbandonment, shardId);
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        final AtomicReference<Object> queuedCreateOutcome = listenFor(queuedEntry.snapshot());
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));
        readFailuresBeforeAnAnswer.set(2);

        runRemoval(clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queuedEntry), abandoned), abandoned);
        assertEquals("the failed read must have armed a retry", 1, pendingSchedules.size());

        schedulesToRejectBeforeOneIsArmed.set(1);
        runArmedRetry();

        assertFalse(
            "a rejected arm must release the in-flight guard, or the successor that could start a loop is refused as already in flight",
            reconcilingRepositories().contains(REPO)
        );
        assertTrue("while the debt stays recorded, because one attempt was lost and not the work", reconciliationOwed().contains(REPO));
        assertTrue(
            "and nothing is armed, because the scheduler that refused this attempt would refuse its successor too",
            pendingSchedules.isEmpty()
        );
        assertNull("and no queued snapshot is failed, which is the one outcome this path exists to avoid", queuedCreateOutcome.get());
        verify(clusterService, never()).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        assertEquals("and nothing has reconciled yet", 0, reconciliationPasses.get());

        reconciliationOwed().remove(REPO);
        final ClusterState published = reconciliationInput.get();
        reconciliationOutput.set(null);
        snapshotsService.applyClusterState(new ClusterChangedEvent("test", published, withoutLocalClusterManager(published)));
        assertTrue("its resume infers the debt from the cluster state", reconciliationOwed().contains(REPO));
        assertEquals("and hands a first attempt to the generic pool", 1, pendingDispatchedDrives.size());

        runDispatchedReconciliation();
        assertEquals("which reconciles the work the refused arm left owed", 1, reconciliationPasses.get());
        final SnapshotsInProgress.Entry updated = entryOf(reconciliationOutput.get(), 0);
        assertNotEquals("under a rebound identifier", preAbandonment.getId(), updated.indices().get(0).getId());
        assertEquals(SnapshotsInProgress.ShardState.INIT, updated.shards().get(shardId).state());
        assertTrue("and discharges the debt", reconciliationOwed().isEmpty());
        assertNull("and at no point is the queued snapshot failed", queuedCreateOutcome.get());
        assertFalse("nor is a task submitted that would fail it", submittedSources.contains(FAIL_TASKS_SOURCE));
    }

    private static ClusterState withoutLocalClusterManager(ClusterState state) {
        return ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).clusterManagerNodeId(null).build()).build();
    }

    private ClusterState runRemovalAndReconciliation(ClusterState currentState, SnapshotDeletionsInProgress.Entry removed)
        throws Exception {
        runRemoval(currentState, removed);
        assertNotNull("the removal must have owed and driven a reconciliation", reconciliationOutput.get());
        return reconciliationOutput.get();
    }

    private void runRemoval(ClusterState currentState, SnapshotDeletionsInProgress.Entry removed) throws Exception {
        runRemovalTask(currentState, removed);
        runDispatchedReconciliation();
    }

    private void runRemovalLeavingTheReadOut(ClusterState currentState, SnapshotDeletionsInProgress.Entry removed) throws Exception {
        runRemovalTask(currentState, removed);
        assertEquals("the removal must have dispatched an attempt", 1, pendingDispatchedDrives.size());
        pendingDispatchedDrives.poll().run();
    }

    private void runRemovalTask(ClusterState currentState, SnapshotDeletionsInProgress.Entry removed) throws Exception {
        primeRunningDelete(removed.uuid());
        final ClusterStateUpdateTask task = snapshotsService.createRemoveSnapshotDeletionTask(
            SOURCE,
            0,
            removed,
            new RepositoryException(REPO, "the delete did not complete"),
            RepositoryData.EMPTY.withGenId(CAPTURED_GEN),
            null,
            true
        );
        final ClusterState newState = task.execute(currentState);
        assertTrue(
            "the removal step must record the debt in the same step that leaves the shards queued",
            reconciliationOwed().contains(REPO)
        );
        reconciliationInput.set(newState);
        task.clusterStateProcessed(SOURCE, currentState, newState);
    }

    private Runnable repositoryRead(
        Function<RepositoryData, ClusterStateUpdateTask> createUpdateTask,
        String source,
        Consumer<Exception> onReadFailure
    ) {
        return () -> {
            if (readFailuresBeforeAnAnswer.get() > 0) {
                readFailuresBeforeAnAnswer.decrementAndGet();
                onReadFailure.accept(new RepositoryException(REPO, "the repository read did not answer"));
                return;
            }
            final ClusterStateUpdateTask task = createUpdateTask.apply(reconciliationRepositoryData.get());
            reconciliationPasses.incrementAndGet();
            if (metadataMovesBeforeExecute.get() > 0) {
                metadataMovesBeforeExecute.decrementAndGet();
                pendingRepositoryReads.add(repositoryRead(createUpdateTask, source, onReadFailure));
                return;
            }
            final ClusterState before = reconciliationInput.get();
            final ClusterState after;
            try {
                after = task.execute(before);
            } catch (Exception e) {
                throw new AssertionError("a consistent state update of this suite must not throw", e);
            }
            reconciliationOutput.set(after);
            task.clusterStateProcessed(source, before, after);
        };
    }

    private void runBudgetTimer() {
        assertEquals("exactly one budget may be armed", 1, pendingBudgetTimers.size());
        pendingBudgetTimers.poll().task().run();
    }

    private void setBudget(TimeValue budget) {
        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put("snapshot.repository.io_timeout", budget.getStringRep()).build());
    }

    @SuppressForbidden(reason = "the service's record of reconciliation reads out has no test seam")
    @SuppressWarnings("unchecked")
    private Set<Repository> readsOut() throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("reconciliationReadsOut");
        field.setAccessible(true);
        return (Set<Repository>) field.get(snapshotsService);
    }

    private void runPendingRepositoryReads() {
        while (pendingRepositoryReads.isEmpty() == false) {
            pendingRepositoryReads.poll().run();
        }
    }

    private void runArmedRetry() {
        assertEquals("exactly one retry may be armed for a repository", 1, pendingSchedules.size());
        runOnlyPendingSchedule();
        runPendingRepositoryReads();
    }

    private Schedule runOnlyPendingSchedule() {
        assertEquals("exactly one schedule may be pending", 1, pendingSchedules.size());
        final Schedule schedule = pendingSchedules.poll();
        schedule.task().run();
        return schedule;
    }

    private void runDispatchedReconciliation() {
        if (pendingDispatchedDrives.isEmpty() == false) {
            pendingDispatchedDrives.poll().run();
        }
        runPendingRepositoryReads();
    }

    private ClusterState reDriveFromClusterState(ClusterState state) {
        reconciliationInput.set(state);
        reconciliationOutput.set(null);
        applyAsClusterManager(state, ClusterState.builder(state).putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).build());
        return reconciliationOutput.get();
    }

    private void applyAsClusterManager(ClusterState state, ClusterState previous) {
        snapshotsService.applyClusterState(new ClusterChangedEvent("test", state, previous));
        runDispatchedReconciliation();
    }

    private SnapshotsInProgress.Entry createdEntry(ClusterState currentState, RepositoryData repositoryData) {
        when(indexNameExpressionResolver.resolveDateMathExpression(anyString())).thenAnswer(invocation -> invocation.getArgument(0));
        when(indexNameExpressionResolver.concreteIndexNames(any(ClusterState.class), any(IndicesRequest.class))).thenReturn(
            new String[] { "idx" }
        );
        when(clusterService.state()).thenReturn(currentState);
        when(repository.adaptUserMetadata(any())).thenReturn(Map.of());
        reconciliationRepositoryData.set(repositoryData);
        reconciliationInput.set(currentState);
        reconciliationOutput.set(null);
        snapshotsService.createSnapshot(new CreateSnapshotRequest(REPO, "created"), ActionListener.wrap(created -> {}, e -> {
            throw new AssertionError("the create must not fail", e);
        }));
        runPendingRepositoryReads();
        final ClusterState published = reconciliationOutput.get();
        assertNotNull("the create must have published an entry", published);
        final List<SnapshotsInProgress.Entry> entries = snapshotsOf(published).entries();
        return entries.get(entries.size() - 1);
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

    @SuppressForbidden(reason = "the service's reconciliation debt has no test seam")
    @SuppressWarnings("unchecked")
    private Set<String> reconciliationOwed() throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("reconciliationOwed");
        field.setAccessible(true);
        return (Set<String>) field.get(snapshotsService);
    }

    @SuppressForbidden(reason = "the service's in-flight reconciliation guard has no test seam")
    @SuppressWarnings("unchecked")
    private Set<String> reconcilingRepositories() throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("reconcilingRepositories");
        field.setAccessible(true);
        return (Set<String>) field.get(snapshotsService);
    }

    @SuppressForbidden(reason = "the service's snapshot completion listeners have no test seam")
    @SuppressWarnings("unchecked")
    private void listenFor(Snapshot snapshot, AtomicReference<Object> outcome) throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("snapshotCompletionListeners");
        field.setAccessible(true);
        final Map<Snapshot, List<ActionListener<Tuple<RepositoryData, SnapshotInfo>>>> listeners = (Map<
            Snapshot,
            List<ActionListener<Tuple<RepositoryData, SnapshotInfo>>>>) field.get(snapshotsService);
        final ActionListener<Tuple<RepositoryData, SnapshotInfo>> listener = ActionListener.wrap(outcome::set, outcome::set);
        listeners.computeIfAbsent(snapshot, ignored -> new CopyOnWriteArrayList<>()).add(listener);
    }

    private AtomicReference<Object> listenFor(Snapshot snapshot) throws Exception {
        final AtomicReference<Object> outcome = new AtomicReference<>();
        listenFor(snapshot, outcome);
        return outcome;
    }

    @SuppressForbidden(reason = "the service's delete listeners have no test seam")
    @SuppressWarnings("unchecked")
    private void listenForDelete(String deleteUuid, AtomicInteger answers) throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("snapshotDeletionListeners");
        field.setAccessible(true);
        ((Map<String, List<ActionListener<Void>>>) field.get(snapshotsService)).computeIfAbsent(deleteUuid, uuid -> new ArrayList<>())
            .add(ActionListener.wrap(ignored -> answers.incrementAndGet(), e -> answers.incrementAndGet()));
    }

    @SuppressForbidden(reason = "the service's failover count has no test seam")
    private long failovers() throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("failovers");
        field.setAccessible(true);
        return ((AtomicLong) field.get(snapshotsService)).get();
    }

    private static ClusterState clusterState(
        IndexMetadata indexMetadata,
        IndexRoutingTable indexRoutingTable,
        List<SnapshotsInProgress.Entry> snapshots,
        SnapshotDeletionsInProgress.Entry deletion
    ) {
        return clusterState(List.of(indexMetadata), List.of(indexRoutingTable), snapshots, deletion);
    }

    private static ClusterState clusterState(
        List<IndexMetadata> indexMetadata,
        List<IndexRoutingTable> indexRoutingTables,
        List<SnapshotsInProgress.Entry> snapshots,
        SnapshotDeletionsInProgress.Entry deletion
    ) {
        final String localNodeId = UUIDs.randomBase64UUID();
        final Metadata.Builder metadata = Metadata.builder(Metadata.EMPTY_METADATA)
            .putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(List.of(new RepositoryMetadata(REPO, "mock", Settings.EMPTY))));
        for (IndexMetadata index : indexMetadata) {
            metadata.put(index, false);
        }
        final RoutingTable.Builder routingTable = RoutingTable.builder();
        for (IndexRoutingTable index : indexRoutingTables) {
            routingTable.add(index);
        }
        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(DiscoveryNodes.builder().localNodeId(localNodeId).clusterManagerNodeId(localNodeId).build())
            .metadata(metadata)
            .routingTable(routingTable.build())
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(snapshots))
            .putCustom(
                SnapshotDeletionsInProgress.TYPE,
                deletion == null ? SnapshotDeletionsInProgress.EMPTY : SnapshotDeletionsInProgress.of(List.of(deletion))
            )
            .build();
    }

    private static RepositoryData repositoryDataHolding(long generation, IndexId... indices) {
        final ShardGenerations.Builder shardGenerations = ShardGenerations.builder();
        for (IndexId indexId : indices) {
            shardGenerations.put(indexId, 0, "gen-" + indexId.getName());
        }
        return RepositoryData.EMPTY.withGenId(generation)
            .addSnapshot(newSnapshotId("finalized"), SnapshotState.SUCCESS, Version.CURRENT, shardGenerations.build(), null, null);
    }

    private static IndexMetadata indexMetadata(String indexName) {
        return IndexMetadata.builder(indexName)
            .settings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT.id))
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
        SnapshotId snapshotId,
        List<IndexId> indices,
        Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards
    ) {
        return SnapshotsInProgress.startedEntry(
            new Snapshot(REPO, snapshotId),
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

    private static SnapshotsInProgress.Entry queuedSnapshot(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
        return snapshotEntry(snapshotId, List.of(indexId), Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED));
    }

    private static SnapshotsInProgress.Entry partialSnapshotEntry(
        SnapshotId snapshotId,
        List<IndexId> indices,
        Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards
    ) {
        return SnapshotsInProgress.startedEntry(
            new Snapshot(REPO, snapshotId),
            false,
            true,
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

    private static SnapshotDeletionsInProgress.Entry startedDeletion() {
        return new SnapshotDeletionsInProgress.Entry(
            List.of(newSnapshotId("deleted")),
            REPO,
            0L,
            CAPTURED_GEN,
            SnapshotDeletionsInProgress.State.STARTED
        );
    }

    private static SnapshotsInProgress snapshotsOf(ClusterState state) {
        return state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
    }

    private static SnapshotsInProgress.Entry entryOf(ClusterState state, int index) {
        return snapshotsOf(state).entries().get(index);
    }

    private static SnapshotId newSnapshotId(String name) {
        return new SnapshotId(name, UUIDs.randomBase64UUID());
    }

    private static String uuid() {
        return UUIDs.randomBase64UUID();
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testARemovalThatRecordedTheOnlyDebtKeepsRetryingItsPublication() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        for (boolean ofAClone : new boolean[] { false, true }) {
            final String row = ofAClone ? "a clone queued behind the delete" : "a create queued behind the delete";
            final IndexId indexId = new IndexId("idx", uuid());
            final SnapshotsInProgress.Entry queued = ofAClone
                ? queuedClone("queued-clone", newSnapshotId("clone-source"), indexId, new RepositoryShardId(indexId, 0))
                : queuedSnapshot(newSnapshotId("queued-create"), indexId, shardId);
            final AtomicReference<Object> outcome = listenFor(queued.snapshot());
            final SnapshotDeletionsInProgress.Entry removed = startedDeletion();
            reconciliationOwed().clear();
            pendingSchedules.clear();
            stubLocalNode(clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queued), removed));

            final ClusterStateUpdateTask task = snapshotsService.createRemoveSnapshotDeletionTask(
                SOURCE,
                SnapshotsService.SNAPSHOT_CLEANUP_RETRIES_SETTING.get(Settings.EMPTY),
                removed,
                new RepositoryException(REPO, "the delete did not complete"),
                RepositoryData.EMPTY.withGenId(CAPTURED_GEN),
                null,
                true
            );
            task.execute(clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queued), removed));
            assertTrue("the premise: the removal deferred " + row + " and recorded the only debt", reconciliationOwed().contains(REPO));

            task.onFailure(SOURCE, new FailedToCommitClusterStateException("simulated publish failure"));

            assertNull("a publication that did not commit must not fail " + row, outcome.get());
            assertEquals("the publication must be retried instead, for " + row, 1, pendingSchedules.size());
            assertTrue("and no shard clone started for " + row, pendingSnapshotTasks.isEmpty());
        }
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testARemovalRetryThatFindsItsDeleteGoneReDrivesTheDeleteItPromoted() throws Exception {
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion();
        final AtomicInteger answers = new AtomicInteger();
        listenForDelete(removed.uuid(), answers);
        final SnapshotDeletionsInProgress.Entry waiting = new SnapshotDeletionsInProgress.Entry(
            List.of(newSnapshotId("waiting")),
            REPO,
            0L,
            CAPTURED_GEN,
            SnapshotDeletionsInProgress.State.WAITING
        );
        final ClusterState s0 = ClusterState.builder(clusterState(List.of(), List.of(), List.of(), removed))
            .putCustom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.of(List.of(removed, waiting)))
            .build();
        final ClusterState s1 = removeThenRetry(removed, s0);
        final long failoversBefore = failovers();
        final ClusterStateUpdateTask retry = submittedTasks.get(submittedTasks.size() - 1);

        assertSame("a retry that finds its delete gone must change nothing", s1, retry.execute(s1));
        retry.clusterStateProcessed(SOURCE, s1, s1);
        assertEquals("and must answer the delete once", 1, answers.get());
        assertEquals("without failing anything over", failoversBefore, failovers());
        final int handOn = submittedSources.indexOf("Run ready deletions");
        assertTrue("a retry that finds its delete gone must hand the repository on", handOn >= 0);
        final ClusterStateUpdateTask runReady = submittedTasks.get(handOn);
        runReady.clusterStateProcessed("Run ready deletions", s1, runReady.execute(s1));
        verify(repositoriesService, times(1).description("so the delete its removal promoted is re-driven")).getRepositoryData(
            anyString(),
            any()
        );
    }

    private ClusterState removeThenRetry(SnapshotDeletionsInProgress.Entry removed, ClusterState s0) throws Exception {
        primeRunningDelete(removed.uuid());
        final ClusterStateUpdateTask task = snapshotsService.createRemoveSnapshotDeletionTask(
            SOURCE,
            0,
            removed,
            null,
            RepositoryData.EMPTY.withGenId(CAPTURED_GEN),
            null,
            true
        );
        final ClusterState s1 = task.execute(s0);
        task.onFailure(SOURCE, new FailedToCommitClusterStateException("simulated publish failure"));
        runOnlyPendingSchedule();
        assertEquals("the premise: the retry submitted the removal again", SOURCE, submittedSources.get(submittedSources.size() - 1));
        return s1;
    }

    private void stubLocalNode(ClusterState state) {
        when(clusterService.localNode()).thenReturn(
            new DiscoveryNode(state.nodes().getLocalNodeId(), buildNewFakeTransportAddress(), Version.CURRENT)
        );
    }

    private static SnapshotsInProgress.Entry cloneEntry(
        String name,
        SnapshotId source,
        List<IndexId> indices,
        Map<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> clones
    ) {
        return SnapshotsInProgress.startClone(new Snapshot(REPO, newSnapshotId(name)), source, indices, 0L, CAPTURED_GEN, Version.CURRENT)
            .withClones(clones);
    }

    private static SnapshotsInProgress.Entry queuedClone(String name, SnapshotId source, IndexId indexId, RepositoryShardId shardId) {
        return cloneEntry(name, source, List.of(indexId), Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED));
    }

    private static SnapshotsInProgress.Entry entryFor(ClusterState state, Snapshot snapshot) {
        return state == null ? null : snapshotsOf(state).snapshot(snapshot);
    }

    private static SnapshotsInProgress.ShardSnapshotStatus cloneStatus(
        ClusterState state,
        SnapshotsInProgress.Entry clone,
        RepositoryShardId shardId
    ) {
        final SnapshotsInProgress.Entry entry = entryFor(state, clone.snapshot());
        return entry == null ? null : entry.clones().get(shardId);
    }

    private static final String FAIL_TASKS_SOURCE = "fail repo tasks for [" + REPO + "]";

    @SuppressForbidden(reason = "the service's fail-pending-repository-tasks update is a private inner class")
    private ClusterStateUpdateTask failPendingRepoTasksTask() throws Exception {
        for (final Class<?> nested : SnapshotsService.class.getDeclaredClasses()) {
            if (nested.getSimpleName().equals("FailPendingRepoTasksTask") == false) {
                continue;
            }
            final Constructor<?> ctor = nested.getDeclaredConstructor(SnapshotsService.class, String.class, Exception.class);
            ctor.setAccessible(true);
            return (ClusterStateUpdateTask) ctor.newInstance(
                snapshotsService,
                REPO,
                new RepositoryException(REPO, "the repository read did not answer")
            );
        }
        throw new AssertionError("SnapshotsService no longer declares FailPendingRepoTasksTask");
    }

}
