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
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.NotClusterManagerException;
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
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
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
import java.util.stream.IntStream;

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

/**
 * Unit tests for what happens to a snapshot that is queued behind a delete the service gave up on: the identity its blobs are
 * written under, and when that identity may be re-derived.
 * <p>
 * Three propositions are under test. The first is that a queued snapshot is never started against an identity resolved before the
 * abandonment, whichever route the start would have come by -- the removal of a later, successful delete, or a shard of another
 * snapshot completing. The second is the invariant that bounds the first: one {@link IndexId} per index name per repository.
 * {@code RepositoryData} and the service's in-flight identifier lookup both build their name-keyed maps with no merge function, so
 * a second live identifier for one name is not a degraded outcome but a hard failure at the next finalization. When an identity
 * cannot be re-derived without breaking that, the entry stays queued, which is the permitted answer and the one these tests pin.
 * The third is that a queued snapshot left waiting by either of those is returned to: a debt a pass retained is re-driven by a change
 * to the snapshots or deletions in progress, and a debt held up by a failing repository read is retried on a capped timer that does
 * not give up.
 * <p>
 * Every test but four locks the snapshot resilience feature flag on, because with it off no delete is ever given up on and none of
 * this machinery has a writer. The four exceptions are deliberate: they pin, with the flag off, what keeps it that way -- the
 * resume path's own flag gate, the arms through which a failed delete and a create would otherwise record or act on a debt, and
 * the arms that would otherwise keep a queued clone, infer a debt for it or hold its preparation's shard clones.
 * <p>
 * The delete removal and the reconciliation it owes are driven through the package-private task factory and a stubbed
 * {@code Repository#executeConsistentStateUpdate}, rather than through a live cluster, so that the repository data each pass sees
 * is chosen by the test and the number of passes is a counter rather than a race. As in {@link PromotedSnapshotDeleteTests}, two
 * pieces of the service's bookkeeping have to be primed by reflection first, because the removal task asserts on both and neither
 * has a test seam.
 */
public class QueuedSnapshotReconciliationTests extends OpenSearchTestCase {

    private static final String REPO = "test-repo";

    /** The source string the production dispatcher submits the removal task under. */
    private static final String SOURCE = "remove snapshot deletion metadata";

    /** Generation of the repository data the removal task carries -- what the pass that just ended saw. */
    private static final long CAPTURED_GEN = 5L;

    private TestThreadPool threadPool;
    private Repository repository;
    private RepositoriesService repositoriesService;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private SnapshotsService snapshotsService;

    /** What the reconciliation's fresh repository read answers with. Set per scenario. */
    private final AtomicReference<RepositoryData> reconciliationRepositoryData = new AtomicReference<>(RepositoryData.EMPTY);

    /** The state a reconciliation pass runs against, and the state it produced. */
    private final AtomicReference<ClusterState> reconciliationInput = new AtomicReference<>();
    private final AtomicReference<ClusterState> reconciliationOutput = new AtomicReference<>();

    /**
     * Repository reads the service has asked for and that have not been answered yet. Production reads the repository on another
     * thread and submits the resulting cluster state update only afterwards, so answering one inside the call that asked for it
     * would run a whole reconciliation inside {@code applyClusterState}, ahead of the assertions that publication runs.
     */
    private final Deque<Runnable> pendingRepositoryReads = new ArrayDeque<>();

    /**
     * Repository reads that must fail before one is allowed to answer, which is how a store slow enough to make a delete give up is
     * modelled. A failing read is answered through the failure consumer the service passes to
     * {@code Repository#executeConsistentStateUpdate}, which is the arm a repository read failure really takes.
     */
    private final AtomicInteger readFailuresBeforeAnAnswer = new AtomicInteger();

    /**
     * Repository reads whose update must be built and then dropped unexecuted, with the same read asked for again, which is what the
     * repository does when its metadata moved between the read and the update. Counted down like {@link #readFailuresBeforeAnAnswer}.
     */
    private final AtomicInteger metadataMovesBeforeExecute = new AtomicInteger();

    /** A task the service handed {@code ThreadPool#schedule}: what it runs, after how long, and on which pool. */
    private record Schedule(Runnable task, TimeValue delay, String executor) {
    }

    /**
     * Generic-pool and snapshot-pool schedules the service has asked for and that have not been run yet, in the order it asked. On
     * this path they are the reconciliation retries, the cluster-state-update retries, and the budget of a finalization's repository
     * read. Captured instead of scheduled, because both retry ladders top out at delays no test may wait for, and because "a retry is
     * armed" is the whole assertion of the test that drives the attempt count past the point where the operator is warned. The pool is
     * kept with each one, so that a test can say which pool a retry was put on.
     */
    private final Deque<Schedule> pendingSchedules = new ArrayDeque<>();

    /**
     * Time budgets armed on the generic pool that have neither run nor been cancelled: what {@code ListenerTimeouts} schedules for a
     * listener it bounds. Kept apart from {@link #pendingSchedules}, so that a budget is never counted as a retry, run in place of
     * one, or refused by the rejection counter, and removed when the wrapper cancels it.
     */
    private final Deque<Schedule> pendingBudgetTimers = new ArrayDeque<>();

    /**
     * A reconciliation's first attempt, handed to the generic pool and captured here rather than run. Production hands a loop's
     * first attempt to {@code ThreadPool#generic()}, which nothing else in this service calls, and schedules every later attempt
     * through {@code ThreadPool#schedule}, so a capture here is a first attempt and never a retry -- those are
     * {@link #pendingSchedules}, and a helper that ran one of them in place of a drive's own attempt would be running the wrong task.
     */
    private final Deque<Runnable> pendingDispatchedDrives = new ArrayDeque<>();

    /**
     * Captured schedules that must be rejected before one is allowed to be armed, which is how a node on its way down is modelled:
     * {@code ThreadPool#schedule} hands the task to a scheduler whose rejected-execution policy throws once that scheduler has been shut
     * down. Mirrors {@link #readFailuresBeforeAnAnswer}, and is a count rather than a flag on purpose -- a successor the rejection arm
     * must not have armed would itself be rejected under a flag, and so would be silently refused instead of captured and asserted on.
     */
    private final AtomicInteger schedulesToRejectBeforeOneIsArmed = new AtomicInteger();

    /** Sources and task instances handed to {@code submitStateUpdateTask}, in order. */
    private final List<String> submittedSources = new ArrayList<>();
    private final List<ClusterStateUpdateTask> submittedTasks = new ArrayList<>();

    /**
     * Reconciliation passes driven so far, so that "a retained debt is re-driven" and "a discharged one is not" are assertions on a
     * counter rather than on a scheduler. A read that fails is not a pass and is not counted here.
     */
    private final AtomicInteger reconciliationPasses = new AtomicInteger();

    /** Repository reads a finalization asked for. Counted and left unanswered, for the reason given in {@code setUp}. */
    private final AtomicInteger finalizationReads = new AtomicInteger();

    /** The listener the last finalization read was handed, so a test can fail it the way the repository would. */
    private final AtomicReference<ActionListener<RepositoryData>> finalizationReadListener = new AtomicReference<>();

    /** The generation and the listener the last finalization handed the repository, so a test can check the one and fail the other. */
    private final AtomicLong finalizedGeneration = new AtomicLong(Long.MIN_VALUE);
    private final AtomicReference<ActionListener<RepositoryData>> finalizationListener = new AtomicReference<>();

    /**
     * Whether the snapshot pool is captured rather than run, which every test about clones sets: the service hands a clone's
     * preparation and each shard clone to that pool, and a real thread running one would reach a repository this test does not own.
     */
    private boolean captureSnapshotExecutor;

    /** Tasks the service handed the snapshot pool while {@link #captureSnapshotExecutor} was set, in the order it handed them over. */
    private final Deque<Runnable> pendingSnapshotTasks = new ArrayDeque<>();

    /** One shard clone the service asked the repository for, with the listener it is to be answered through. */
    private record CloneCall(SnapshotId source, SnapshotId target, RepositoryShardId shardId, String generation, ActionListener<
        String> listener) {
    }

    /** The shard clones the service asked the repository for, in order. Left unanswered unless a test answers one. */
    private final List<CloneCall> cloneCalls = new ArrayList<>();

    /** A shard-state update the service submitted, with the listener its publication is to be reported to. */
    private record ShardStateSubmission(SnapshotsService.ShardSnapshotUpdate update, ClusterStateTaskListener listener) {
    }

    /** Shard-state updates submitted through the overload that takes an executor, in order. Recorded rather than run. */
    private final List<ShardStateSubmission> submittedShardUpdates = new ArrayList<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());

        repository = mock(Repository.class);
        when(repository.getMetadata()).thenReturn(new RepositoryMetadata(REPO, "mock", Settings.EMPTY));
        // Left unanswered on purpose: a finalization that proceeded past its read would need a repository this test does not own.
        doAnswer(invocation -> {
            finalizationReads.incrementAndGet();
            finalizationReadListener.set(invocation.getArgument(0));
            return null;
        }).when(repository).getRepositoryData(any());
        // Recorded and left unanswered, for the same reason: a test that needs the outcome answers the captured listener itself.
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
        // Recorded rather than run. The tests about failing a repository's pending tasks need the task instance the service
        // submits, because that class is private and a test cannot build one.
        doAnswer(invocation -> {
            submittedSources.add(invocation.getArgument(0));
            submittedTasks.add(invocation.getArgument(1));
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        // A reconciliation retry reads the role before it runs an attempt, so every test needs a state that says this node is the
        // elected cluster manager. Tests that drive the create path replace this with the state they publish against, which says the
        // same, and the test about a demotion replaces it with one that says the opposite.
        when(clusterService.state()).thenReturn(clusterState(List.of(), List.of(), List.of(), null));
        indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);

        // Generic-pool and snapshot-pool schedules are captured rather than run, with their delay and their pool; see
        // pendingSchedules. A handle is returned rather than null, because the wrapper that budgets a finalization's repository read
        // cancels what this returns once the read is answered. Every other schedule is delegated.
        final ThreadPool interceptedThreadPool = spy(threadPool);
        doAnswer(invocation -> {
            final String executor = invocation.getArgument(2);
            if (ThreadPool.Names.GENERIC.equals(executor) && invocation.getArgument(0) instanceof ActionListener) {
                // A time budget: the wrapper schedules the listener it bounds, where every retry schedules a lambda.
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
        // A reconciliation's first attempt is handed to the generic pool rather than scheduled. Captured, for the reason the
        // repository reads above are: running it here would reconcile inside the call that drove it.
        final ExecutorService dispatchedDrives = mock(ExecutorService.class);
        doAnswer(invocation -> {
            pendingDispatchedDrives.add(invocation.getArgument(0));
            return null;
        }).when(dispatchedDrives).execute(any());
        doAnswer(invocation -> dispatchedDrives).when(interceptedThreadPool).generic();

        // The snapshot pool is captured only by the tests that set captureSnapshotExecutor; every other test gets the real pool.
        final ExecutorService capturedSnapshotPool = mock(ExecutorService.class);
        doAnswer(invocation -> {
            pendingSnapshotTasks.add(invocation.getArgument(0));
            return null;
        }).when(capturedSnapshotPool).execute(any());
        doAnswer(invocation -> captureSnapshotExecutor ? capturedSnapshotPool : invocation.callRealMethod()).when(interceptedThreadPool)
            .executor(ThreadPool.Names.SNAPSHOT);
        doAnswer(invocation -> {
            cloneCalls.add(
                new CloneCall(
                    invocation.getArgument(0),
                    invocation.getArgument(1),
                    invocation.getArgument(2),
                    invocation.getArgument(3),
                    invocation.getArgument(4)
                )
            );
            return null;
        }).when(repository).cloneShardSnapshot(any(), any(), any(), any(), any());
        doAnswer(invocation -> {
            submittedShardUpdates.add(new ShardStateSubmission(invocation.getArgument(1), invocation.getArgument(4)));
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(), any(), any(), any());

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

    /**
     * The assertion is on both halves of the entry at once, because they are only meaningful together: a new identifier without a
     * started shard is a rebind that achieved nothing, and a started shard without a new identifier can lose the blobs it writes.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testQueuedSnapshotOfAnIndexTheRepositoryDoesNotKnowIsReboundAndStarted() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final String dataNodeId = uuid();
        final IndexId preAbandonment = new IndexId("idx", uuid());

        final SnapshotsInProgress.Entry queuedEntry = snapshotEntry(
            newSnapshotId("queued"),
            List.of(preAbandonment),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();

        // The repository has never held this index, which is what makes resolveIndexId throw and what the mint arm is for.
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

    /**
     * The debt, not the outcome of the delete being removed, is what decides whether promotion is safe.
     * <p>
     * Only {@code execute} is driven here. The proposition is about the state that step computes, and running the completion
     * callback as well would start a reconciliation whose own result is the subject of the other tests.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testSuccessfulDeleteRemovalDoesNotStartShardsQueuedBehindAnAbandonedDelete() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId preAbandonment = new IndexId("idx", uuid());

        final SnapshotsInProgress.Entry queuedEntry = snapshotEntry(
            newSnapshotId("queued"),
            List.of(preAbandonment),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion();
        // An earlier delete of this repository was given up on and its debt is still outstanding. Everything else about this
        // scenario is a healthy delete completing.
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

    /**
     * A second identifier for an index name another entry still holds would throw {@code IllegalStateException: Duplicate key} at
     * the next finalization and at every later create for the repository, so the entry is left exactly as it is and the debt stays
     * recorded. That is deferral, not failure: the snapshot is still queued, and it is started once the entry holding the name leaves
     * the cluster state.
     * <p>
     * A retained debt is only a deferral if something comes back to it, so the last test asserts the re-drive. The event that
     * re-drives it carries a change to the snapshots in progress, which is what production re-drives on and what every event that
     * could release this particular deferral is: the holding entry finishing, or its entry leaving.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testQueuedEntryIsLeftAloneWhenAnotherEntryHoldsItsIndexName() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        // One identifier for the name, shared by both entries, which is what the create path hands a second snapshot of an index
        // another snapshot is already writing -- and what must still be true of the state this reconciliation publishes.
        final IndexId shared = new IndexId("idx", uuid());

        final SnapshotsInProgress.Entry running = snapshotEntry(
            newSnapshotId("running"),
            List.of(shared),
            Map.of(shardId, new SnapshotsInProgress.ShardSnapshotStatus(uuid(), uuid()))
        );
        final SnapshotsInProgress.Entry queued = snapshotEntry(
            newSnapshotId("queued"),
            List.of(shared),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();

        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));

        final ClusterState reconciled = runRemovalAndReconciliation(
            clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(running, queued), abandoned),
            abandoned
        );

        assertEquals("the removal owes and drives exactly one reconciliation", 1, reconciliationPasses.get());
        final SnapshotsInProgress.Entry unchanged = entryOf(reconciled, 1);
        assertEquals("the entry must be left exactly as it was", List.of(shared), unchanged.indices());
        assertEquals(
            "which means still queued, not started and not failed",
            SnapshotsInProgress.ShardState.QUEUED,
            unchanged.shards().get(shardId).state()
        );

        assertIdentifiersAreUniqueByName(reconciled);
        assertTrue("an identity that could not be re-derived is still owed one", reconciliationOwed().contains(REPO));

        reDriveFromClusterState(reconciled);
        assertEquals(
            "and a retained debt must be re-driven by the next change to the snapshots in progress",
            2,
            reconciliationPasses.get()
        );
        assertTrue("the name is still held, so the debt is retained again -- and re-driven again", reconciliationOwed().contains(REPO));
    }

    /**
     * Two never-snapshotted indices are the minimum shape. The first queued entry holds both, in that order, so it mints for the
     * first and is only then ruled out by the second, which a started entry holds. The second queued entry holds only the first
     * index, and is the entry an identifier the first one did not keep would otherwise reach.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testNoIdentifierIsMintedForANameAnEntryLeftBehindStillHolds() throws Exception {
        final IndexMetadata mintable = indexMetadata("mintable");
        final IndexMetadata held = indexMetadata("held");
        final ShardId mintableShardId = new ShardId(mintable.getIndex(), 0);
        final ShardId heldShardId = new ShardId(held.getIndex(), 0);
        final IndexId mintableId = new IndexId("mintable", uuid());
        final IndexId heldId = new IndexId("held", uuid());

        // Started, so the pass can never rebind it, which is what pins the identifier it holds for "held".
        final SnapshotsInProgress.Entry running = snapshotEntry(
            newSnapshotId("running"),
            List.of(heldId),
            Map.of(heldShardId, new SnapshotsInProgress.ShardSnapshotStatus(uuid(), uuid()))
        );
        // Mints for "mintable" and is ruled out only afterwards, by "held". The order of its indices is the whole point of the test.
        final SnapshotsInProgress.Entry mintedThenLeftBehind = snapshotEntry(
            newSnapshotId("minted-then-left-behind"),
            List.of(mintableId, heldId),
            Map.of(
                mintableShardId,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED,
                heldShardId,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED
            )
        );
        // Resolved after the entry above, so it is the entry an identifier that entry minted and did not keep would otherwise reach.
        final SnapshotsInProgress.Entry wantsTheSameName = snapshotEntry(
            newSnapshotId("wants-the-same-name"),
            List.of(mintableId),
            Map.of(mintableShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();

        // The repository has never held either index, so neither name can be resolved authoritatively.
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));

        final ClusterState reconciled = runRemovalAndReconciliation(
            clusterState(
                List.of(mintable, held),
                List.of(startedPrimary(mintableShardId, uuid()), startedPrimary(heldShardId, uuid())),
                List.of(running, mintedThenLeftBehind, wantsTheSameName),
                abandoned
            ),
            abandoned
        );

        assertIdentifiersAreUniqueByName(reconciled);
        assertEquals(
            "the entry that was left behind keeps what it came in with",
            List.of(mintableId, heldId),
            entryOf(reconciled, 1).indices()
        );
        assertEquals(
            "so no later entry of this repository may be published with a different identifier for a name it holds",
            List.of(mintableId),
            entryOf(reconciled, 2).indices()
        );
        assertTrue("and the debt stays recorded, because a name could not be re-derived", reconciliationOwed().contains(REPO));
    }

    /**
     * A promotion guard scoped to the repository rather than to the entry, or a deferral with no re-drive, would leave the mixed entry
     * below -- one shard started, one queued behind another snapshot -- unable to start that shard, complete or finalize, and every
     * later pass would leave the third entry queued for the same reason: a fixed point that neither a delete removal nor an election
     * breaks.
     * <p>
     * Three concurrent snapshots over two never-snapshotted indices, driven the whole way out: the pass leaves the third entry
     * alone, the mixed entry's queued shard is promoted through the service's own shard-update executor and therefore under the
     * service's own debt, the entries ahead of it finalize, and the change to the snapshots in progress that follows re-drives the
     * pass that discharges the debt and starts the entry that was waiting.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnEntryHoldingAnUnrebindableNameIsNotWedgedByTheDebtItKeeps() throws Exception {
        final IndexMetadata first = indexMetadata("first");
        final IndexMetadata second = indexMetadata("second");
        final ShardId firstShardId = new ShardId(first.getIndex(), 0);
        final ShardId secondShardId = new ShardId(second.getIndex(), 0);
        final IndexId firstId = new IndexId("first", uuid());
        final IndexId secondId = new IndexId("second", uuid());
        final String dataNodeId = uuid();

        final SnapshotsInProgress.Entry running = snapshotEntry(
            newSnapshotId("running"),
            List.of(firstId),
            Map.of(firstShardId, new SnapshotsInProgress.ShardSnapshotStatus(dataNodeId, uuid()))
        );
        // The mixed holder. Its second shard has begun, so its identity can never be rebound and the name it holds for that index
        // cannot be minted for; its first shard is queued behind the entry above.
        final SnapshotsInProgress.Entry mixed = snapshotEntry(
            newSnapshotId("mixed"),
            List.of(firstId, secondId),
            Map.of(
                firstShardId,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED,
                secondShardId,
                new SnapshotsInProgress.ShardSnapshotStatus(dataNodeId, uuid())
            )
        );
        final SnapshotsInProgress.Entry waiting = snapshotEntry(
            newSnapshotId("waiting"),
            List.of(secondId),
            Map.of(secondShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();

        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));

        final ClusterState deferred = runRemovalAndReconciliation(
            clusterState(
                List.of(first, second),
                List.of(startedPrimary(firstShardId, dataNodeId), startedPrimary(secondShardId, dataNodeId)),
                List.of(running, mixed, waiting),
                abandoned
            ),
            abandoned
        );
        assertIdentifiersAreUniqueByName(deferred);
        assertTrue("the name the mixed entry holds cannot be re-derived, so the debt is retained", reconciliationOwed().contains(REPO));
        assertEquals(
            "and the entry that wanted that name stays queued",
            SnapshotsInProgress.ShardState.QUEUED,
            entryOf(deferred, 2).shards().get(secondShardId).state()
        );

        // The mixed entry has to be able to make progress under the very debt its own held name keeps alive.
        final ClusterState promoted = snapshotsService.shardStateExecutor.execute(
            deferred,
            List.of(
                new SnapshotsService.ShardSnapshotUpdate(
                    running.snapshot(),
                    firstShardId,
                    new SnapshotsInProgress.ShardSnapshotStatus(dataNodeId, SnapshotsInProgress.ShardState.SUCCESS, uuid())
                )
            )
        ).resultingState;
        assertEquals(
            "an entry that has already started a shard is past rebinding and must be allowed to finish",
            SnapshotsInProgress.ShardState.INIT,
            entryOf(promoted, 1).shards().get(firstShardId).state()
        );

        // What finishing leads to: the two entries ahead finalize and leave the cluster state, and the repository now holds both
        // names, so the name that could not be re-derived can be.
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 4L, firstId, secondId));
        final ClusterState discharged = reDriveFromClusterState(
            clusterState(
                List.of(first, second),
                List.of(startedPrimary(firstShardId, dataNodeId), startedPrimary(secondShardId, dataNodeId)),
                List.of(waiting),
                null
            )
        );

        assertNotNull("a retained debt must be re-driven by the next change to the snapshots in progress", discharged);
        assertEquals(
            "the entry that was waiting is bound to the identifier the repository now holds",
            List.of(secondId),
            entryOf(discharged, 0).indices()
        );
        assertEquals("and started", SnapshotsInProgress.ShardState.INIT, entryOf(discharged, 0).shards().get(secondShardId).state());
        assertTrue("with no name left waiting for an identifier, the debt is discharged", reconciliationOwed().isEmpty());
    }

    /**
     * The create path is the only production producer of the identity-rebind argument, and both of its answers matter. A create that
     * inherits an identifier from an entry of the repository must not start: {@code getInFlightIndexIds} donates from queued entries
     * with no filter on their state, so that identifier can be one the abandoned cleanup is about to walk. A create bound to an
     * identifier the repository itself holds must start: the reconciliation would resolve the same name to the same identifier, so
     * waiting achieves nothing and queueing every create of the repository until an unrelated debt clears would eventually fail them
     * outright at the concurrency limit.
     * <p>
     * Driven through {@code createSnapshot} rather than through the shard assignment alone, because it is the wiring of that argument
     * that is under test.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testCreateWaitsOnlyForAnIdentifierItInheritedFromAnotherEntry() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId preAbandonment = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queued = snapshotEntry(
            newSnapshotId("queued"),
            List.of(preAbandonment),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        // An earlier delete of this repository was given up on and its debt is still outstanding. Everything else about both tests
        // below is an ordinary create.
        reconciliationOwed().add(REPO);

        final SnapshotsInProgress.Entry inherited = createdEntry(
            clusterState(List.of(indexMetadata), List.of(startedPrimary(shardId, uuid())), List.of(queued), null),
            // The repository does not hold the index, so the resolution falls through to what the queued entry donates.
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

    /**
     * An entry whose every queued shard resolves to {@code MISSING} is complete the moment it is published, so the entry the
     * reconciliation publishes must be promoted to {@code SUCCESS}: the private constructor's own consistency assertion refuses the
     * other shape, and that {@code AssertionError} is not caught by the cluster state service's task handling, so nothing would retry
     * and the reconciliation guard for the repository would never be released.
     * <p>
     * The index is absent from the cluster state entirely, which is the ordinary way every shard of an entry resolves to missing:
     * the index was deleted while the snapshot sat behind the delete. Only a partial snapshot lets its index be deleted, so the
     * entry is partial, which is also what lets the finalization that follows accept an index the cluster no longer has.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAllQueuedEntryWhoseShardsAllResolveMissingIsPublishedAsSuccess() throws Exception {
        final ShardId deletedIndexShardId = new ShardId(new Index("gone", uuid()), 0);
        final SnapshotsInProgress.Entry queuedEntry = partialSnapshotEntry(
            newSnapshotId("queued"),
            List.of(new IndexId("gone", uuid())),
            Map.of(deletedIndexShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();

        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));

        // No metadata and no routing table for the index, so every shard of this entry resolves to missing.
        final ClusterState reconciled = runRemovalAndReconciliation(
            clusterState((IndexMetadata) null, (IndexRoutingTable) null, List.of(queuedEntry), abandoned),
            abandoned
        );

        assertEquals("the removal owes and drives exactly one reconciliation", 1, reconciliationPasses.get());
        final SnapshotsInProgress.Entry updated = entryOf(reconciled, 0);
        assertEquals(SnapshotsInProgress.ShardState.MISSING, updated.shards().get(deletedIndexShardId).state());
        assertEquals(
            "an entry with nothing left to report has nobody left to finalize it and must be published as complete",
            SnapshotsInProgress.State.SUCCESS,
            updated.state()
        );
    }

    /**
     * An entry a reconciliation pass completes -- every shard of it resolved to {@code MISSING} -- is finalized with the repository
     * data the pass read and published against, and the repository is not read again for it. A second read would be a finalization
     * read, and that read's failure fails every entry of the repository, including the ones the same pass has just started.
     * <p>
     * The completed entry is partial, because only a partial snapshot lets its index be deleted while it waits. The other two are
     * snapshots of one shard, so the pass starts the first and leaves the second queued behind it. The completed entry's own
     * finalization is then failed, as a repository write can fail: that removes it and nothing else.
     */
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
        final SnapshotsInProgress.Entry startedByThePass = snapshotEntry(
            newSnapshotId("started-by-the-pass"),
            List.of(idx),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotsInProgress.Entry queuedBehindIt = snapshotEntry(
            newSnapshotId("queued-behind-it"),
            List.of(idx),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final AtomicReference<Object> completedOutcome = new AtomicReference<>();
        final AtomicReference<Object> startedOutcome = new AtomicReference<>();
        final AtomicReference<Object> queuedOutcome = new AtomicReference<>();
        listenFor(completedByThePass.snapshot(), completedOutcome);
        listenFor(startedByThePass.snapshot(), startedOutcome);
        listenFor(queuedBehindIt.snapshot(), queuedOutcome);
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
        // Preconditions, not regression guards: the pass completed the first entry, started the second, queued the third.
        assertTrue("the pass must have completed the entry of the deleted index", entryOf(reconciled, 0).state().completed());
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

    /**
     * A reconciliation pass does not start a queued snapshot while another snapshot of the repository is complete and waiting to be
     * finalized. That finalization reads the repository, and the read's failure fails every entry of the repository the debt does not
     * cover; a snapshot the pass had started would no longer be covered. The pass leaves the entries queued and keeps the debt, and the
     * finalizing entry's exit, which changes the snapshots in progress, brings the work back.
     * <p>
     * The failing delete's removal promotes the completed entry to a finalization that reads the repository afresh, and leaves the
     * queued snapshot owed a reconciliation; the pass that follows runs while the finalization's read is still outstanding. That read
     * then fails. The retained snapshot is kept, and the pass that follows the failed entry's removal starts it.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testThePassLeavesSnapshotsQueuedWhileASnapshotOfTheRepositoryIsFinalizing() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final IndexMetadata otherMetadata = indexMetadata("other");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final ShardId otherShardId = new ShardId(otherMetadata.getIndex(), 0);
        final String dataNodeId = uuid();
        final SnapshotsInProgress.Entry finalizing = snapshotEntry(
            newSnapshotId("finalizing"),
            List.of(new IndexId("other", uuid())),
            Map.of(
                otherShardId,
                new SnapshotsInProgress.ShardSnapshotStatus(dataNodeId, SnapshotsInProgress.ShardState.FAILED, "aborted", null)
            )
        );
        final IndexId preAbandonment = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queued = snapshotEntry(
            newSnapshotId("queued"),
            List.of(preAbandonment),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        assertTrue("the premise: the first entry is complete and waiting to be finalized", finalizing.state().completed());
        final AtomicReference<Object> finalizingOutcome = new AtomicReference<>();
        final AtomicReference<Object> queuedOutcome = new AtomicReference<>();
        listenFor(finalizing.snapshot(), finalizingOutcome);
        listenFor(queued.snapshot(), queuedOutcome);
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));

        runRemoval(
            clusterState(
                List.of(indexMetadata, otherMetadata),
                List.of(startedPrimary(shardId, dataNodeId), startedPrimary(otherShardId, dataNodeId)),
                List.of(finalizing, queued),
                abandoned
            ),
            abandoned
        );
        assertEquals("the premise: the completed entry's finalization is reading the repository", 1, finalizationReads.get());
        assertEquals("the premise: the pass ran while that read was outstanding", 1, reconciliationPasses.get());
        final ClusterState deferred = reconciliationOutput.get();
        assertEquals(
            "the pass must leave the queued snapshot queued while a snapshot of the repository is finalizing",
            SnapshotsInProgress.ShardState.QUEUED,
            entryOf(deferred, 1).shards().get(shardId).state()
        );
        assertEquals("and must not rebind it", List.of(preAbandonment), entryOf(deferred, 1).indices());
        assertTrue("and keeps the debt", reconciliationOwed().contains(REPO));

        final int submittedBefore = submittedSources.size();
        finalizationReadListener.get().onFailure(new RepositoryException(REPO, "the repository read did not answer"));
        assertEquals(
            "a promoted finalization's read failing must fail only that finalization",
            List.of("remove snapshot metadata"),
            submittedSources.subList(submittedBefore, submittedSources.size())
        );
        ClusterState afterFailure = deferred;
        for (int i = submittedBefore; i < submittedSources.size(); i++) {
            final ClusterStateUpdateTask task = submittedTasks.get(i);
            final ClusterState before = afterFailure;
            afterFailure = task.execute(before);
            task.clusterStateProcessed(submittedSources.get(i), before, afterFailure);
        }
        assertEquals(
            "and its removal hands the repository on",
            List.of("remove snapshot metadata", "Run ready deletions"),
            submittedSources.subList(submittedBefore, submittedSources.size())
        );
        assertEquals(
            "only the finalizing entry leaves; the queued snapshot is kept",
            List.of(queued.snapshot()),
            snapshotsOf(afterFailure).entries().stream().map(SnapshotsInProgress.Entry::snapshot).collect(Collectors.toList())
        );
        assertNotNull("the finalizing entry's caller is told", finalizingOutcome.get());
        assertNull("the queued snapshot is not failed", queuedOutcome.get());

        // The finalizing entry leaving is a change to the snapshots in progress, which re-drives the retained debt.
        final ClusterState redriven = reDriveFromClusterState(afterFailure);
        assertNotNull("the retained debt must be re-driven once nothing is finalizing", redriven);
        final SnapshotsInProgress.Entry started = entryOf(redriven, 0);
        assertNotEquals(
            "once nothing is finalizing, the pass rebinds the queued snapshot",
            preAbandonment.getId(),
            started.indices().get(0).getId()
        );
        assertEquals("and starts it", SnapshotsInProgress.ShardState.INIT, started.shards().get(shardId).state());
        assertTrue("and discharges the debt", reconciliationOwed().isEmpty());
        assertNull("and at no point is it failed", queuedOutcome.get());
    }

    /**
     * An entry the reconciliation owes a start is left as it is while an entry created after it has begun on one of the shards it
     * has queued, and so is every entry queued on one of the shards of an entry left that way. A freed shard is handed only to later
     * entries, so an earlier entry started around a later one could not be started on the rest of its shards by anything. Once the
     * later entry has left, the next pass starts the left entries in creation order, and the shard the earlier one finishes is handed
     * to the later one.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnEntryQueuedBehindALaterEntryIsLeftUntilThatEntryLeaves() throws Exception {
        final IndexMetadata a = indexMetadata("a");
        final IndexMetadata b = indexMetadata("b");
        final IndexMetadata c = indexMetadata("c");
        final ShardId aShard = new ShardId(a.getIndex(), 0);
        final ShardId bShard = new ShardId(b.getIndex(), 0);
        final ShardId cShard = new ShardId(c.getIndex(), 0);
        final IndexId aId = new IndexId("a", uuid());
        final IndexId bId = new IndexId("b", uuid());
        final IndexId cId = new IndexId("c", uuid());
        final String dataNodeId = uuid();
        final List<IndexMetadata> indices = List.of(a, b, c);
        final List<IndexRoutingTable> routing = List.of(
            startedPrimary(aShard, dataNodeId),
            startedPrimary(bShard, dataNodeId),
            startedPrimary(cShard, dataNodeId)
        );
        final SnapshotsInProgress.Entry earlier = snapshotEntry(
            newSnapshotId("earlier"),
            List.of(aId, bId),
            Map.of(
                aShard,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED,
                bShard,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED
            )
        );
        final SnapshotsInProgress.Entry laterBegun = snapshotEntry(
            newSnapshotId("later-begun"),
            List.of(aId),
            Map.of(aShard, new SnapshotsInProgress.ShardSnapshotStatus(dataNodeId, "gen-a"))
        );
        final SnapshotsInProgress.Entry latest = snapshotEntry(
            newSnapshotId("latest"),
            List.of(aId, bId, cId),
            Map.of(
                aShard,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED,
                bShard,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED,
                cShard,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED
            )
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, aId, bId, cId));

        final ClusterState left = runRemovalAndReconciliation(
            clusterState(indices, routing, List.of(earlier, laterBegun, latest), abandoned),
            abandoned
        );
        InFlightShardSnapshotStates.forRepo(REPO, snapshotsOf(left).entries());
        assertIdentifiersAreUniqueByName(left);
        assertEquals("an entry queued behind a later one is left unchanged", earlier.shards(), entryOf(left, 0).shards());
        assertEquals("an entry queued behind a later one is left unchanged", earlier.indices(), entryOf(left, 0).indices());
        assertEquals(
            "an entry sharing a queued shard with an entry left for a later one is left whole",
            latest.shards(),
            entryOf(left, 2).shards()
        );
        assertTrue("the debt must be kept while an earlier entry waits on a shard a later one holds", reconciliationOwed().contains(REPO));

        final ClusterState freed = snapshotsService.shardStateExecutor.execute(
            left,
            List.of(
                new SnapshotsService.ShardSnapshotUpdate(
                    laterBegun.snapshot(),
                    aShard,
                    new SnapshotsInProgress.ShardSnapshotStatus(dataNodeId, SnapshotsInProgress.ShardState.SUCCESS, "gen-a1")
                )
            )
        ).resultingState;
        InFlightShardSnapshotStates.forRepo(REPO, snapshotsOf(freed).entries());
        assertIdentifiersAreUniqueByName(freed);
        assertEquals(
            "a shard freed while every entry queued on it is held goes to no one",
            List.of(SnapshotsInProgress.ShardState.QUEUED, SnapshotsInProgress.ShardState.QUEUED),
            List.of(entryOf(freed, 0).shards().get(aShard).state(), entryOf(freed, 2).shards().get(aShard).state())
        );

        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 4L, aId, bId, cId));
        final ClusterState started = reDriveFromClusterState(
            clusterState(indices, routing, List.of(entryOf(freed, 0), entryOf(freed, 2)), null)
        );
        assertNotNull("the later entry leaving must re-drive the debt", started);
        InFlightShardSnapshotStates.forRepo(REPO, snapshotsOf(started).entries());
        assertIdentifiersAreUniqueByName(started);
        assertEquals(
            "the left entries start in creation order once the later one has left",
            List.of(
                SnapshotsInProgress.ShardState.INIT,
                SnapshotsInProgress.ShardState.INIT,
                SnapshotsInProgress.ShardState.QUEUED,
                SnapshotsInProgress.ShardState.QUEUED,
                SnapshotsInProgress.ShardState.INIT
            ),
            List.of(
                entryOf(started, 0).shards().get(aShard).state(),
                entryOf(started, 0).shards().get(bShard).state(),
                entryOf(started, 1).shards().get(aShard).state(),
                entryOf(started, 1).shards().get(bShard).state(),
                entryOf(started, 1).shards().get(cShard).state()
            )
        );
        assertTrue("the left entries start in creation order once the later one has left", reconciliationOwed().isEmpty());

        final ClusterState handedOn = snapshotsService.shardStateExecutor.execute(
            started,
            List.of(
                new SnapshotsService.ShardSnapshotUpdate(
                    earlier.snapshot(),
                    aShard,
                    new SnapshotsInProgress.ShardSnapshotStatus(dataNodeId, SnapshotsInProgress.ShardState.SUCCESS, "gen-a2")
                )
            )
        ).resultingState;
        InFlightShardSnapshotStates.forRepo(REPO, snapshotsOf(handedOn).entries());
        assertIdentifiersAreUniqueByName(handedOn);
        assertEquals(
            "the later entry is handed the shard the earlier one finished",
            SnapshotsInProgress.ShardState.INIT,
            entryOf(handedOn, 1).shards().get(aShard).state()
        );
    }

    /**
     * An entry the reconciliation owes a start is left as it is while an entry created after it has finished on one of the shards it
     * has queued: started, it would run ahead of that entry's result, which is not yet published.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnEarlierEntryDoesNotStartOnAShardALaterEntryHasFinished() throws Exception {
        final IndexMetadata a = indexMetadata("a");
        final IndexMetadata b = indexMetadata("b");
        final IndexMetadata c = indexMetadata("c");
        final ShardId aShard = new ShardId(a.getIndex(), 0);
        final ShardId bShard = new ShardId(b.getIndex(), 0);
        final ShardId cShard = new ShardId(c.getIndex(), 0);
        final IndexId aId = new IndexId("a", uuid());
        final IndexId bId = new IndexId("b", uuid());
        final IndexId cId = new IndexId("c", uuid());
        final String dataNodeId = uuid();
        final SnapshotsInProgress.Entry earlier = snapshotEntry(
            newSnapshotId("earlier"),
            List.of(aId, bId),
            Map.of(
                aShard,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED,
                bShard,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED
            )
        );
        final SnapshotsInProgress.Entry laterPartlyFinished = snapshotEntry(
            newSnapshotId("later-partly-finished"),
            List.of(aId, cId),
            Map.of(
                aShard,
                new SnapshotsInProgress.ShardSnapshotStatus(dataNodeId, SnapshotsInProgress.ShardState.SUCCESS, "gen-a-later"),
                cShard,
                new SnapshotsInProgress.ShardSnapshotStatus(dataNodeId, "gen-c")
            )
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, aId, bId, cId));

        final ClusterState left = runRemovalAndReconciliation(
            clusterState(
                List.of(a, b, c),
                List.of(startedPrimary(aShard, dataNodeId), startedPrimary(bShard, dataNodeId), startedPrimary(cShard, dataNodeId)),
                List.of(earlier, laterPartlyFinished),
                abandoned
            ),
            abandoned
        );
        assertEquals("an earlier entry must not start on a shard a later one has finished", earlier.shards(), entryOf(left, 0).shards());
        InFlightShardSnapshotStates.forRepo(REPO, snapshotsOf(left).entries());
        assertIdentifiersAreUniqueByName(left);
    }

    /**
     * The identifiers an entry left for a later one keeps are pinned like those of any other entry the pass does not rewrite, so an
     * entry that would have to mint for a name the left entry holds waits instead of taking a second identifier for it.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnEntryOfANameAnEntryLeftForALaterOneKeepsWaits() throws Exception {
        final IndexMetadata a = indexMetadata("a");
        final IndexMetadata x = indexMetadata("x");
        final ShardId aShard = new ShardId(a.getIndex(), 0);
        final ShardId xShard = new ShardId(x.getIndex(), 0);
        final IndexId aId = new IndexId("a", uuid());
        final IndexId xOld = new IndexId("x", uuid());
        final String dataNodeId = uuid();
        final SnapshotsInProgress.Entry earlier = snapshotEntry(
            newSnapshotId("earlier"),
            List.of(aId, xOld),
            Map.of(
                aShard,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED,
                xShard,
                SnapshotsInProgress.ShardSnapshotStatus.MISSING
            )
        );
        final SnapshotsInProgress.Entry laterBegun = snapshotEntry(
            newSnapshotId("later-begun"),
            List.of(aId),
            Map.of(aShard, new SnapshotsInProgress.ShardSnapshotStatus(dataNodeId, "gen-a"))
        );
        final SnapshotsInProgress.Entry sameName = snapshotEntry(
            newSnapshotId("same-name"),
            List.of(xOld),
            Map.of(xShard, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        // The repository does not know x, so a pass could only mint for it.
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, aId));

        final ClusterState left = runRemovalAndReconciliation(
            clusterState(
                List.of(a, x),
                List.of(startedPrimary(aShard, dataNodeId), startedPrimary(xShard, dataNodeId)),
                List.of(earlier, laterBegun, sameName),
                abandoned
            ),
            abandoned
        );
        assertIdentifiersAreUniqueByName(left);
        assertEquals("an entry of a name an entry left for a later one keeps waits", sameName.shards(), entryOf(left, 2).shards());
        assertEquals("an entry of a name an entry left for a later one keeps waits", sameName.indices(), entryOf(left, 2).indices());
        assertTrue("and the debt is kept", reconciliationOwed().contains(REPO));
        InFlightShardSnapshotStates.forRepo(REPO, snapshotsOf(left).entries());
    }

    /**
     * A queued shard is kept from the later entries of a pass only when an earlier entry of that pass started it. One that came out
     * missing for the earlier entry comes out missing for the later ones too, and kept from them it would stay queued with nothing
     * running on it to hand it on.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAShardNoEarlierEntryStartedIsNotKeptQueuedForALaterOne() throws Exception {
        final IndexMetadata a = indexMetadata("a");
        final IndexMetadata b = indexMetadata("b");
        final IndexMetadata d = indexMetadata("d");
        final ShardId aShard = new ShardId(a.getIndex(), 0);
        final ShardId bShard = new ShardId(b.getIndex(), 0);
        final ShardId dShard = new ShardId(d.getIndex(), 0);
        final IndexId aId = new IndexId("a", uuid());
        final IndexId bId = new IndexId("b", uuid());
        final IndexId dId = new IndexId("d", uuid());
        final String dataNodeId = uuid();
        final IndexRoutingTable unassignedPrimary = IndexRoutingTable.builder(aShard.getIndex())
            .addIndexShard(
                new IndexShardRoutingTable.Builder(aShard).addShard(
                    TestShardRouting.newShardRouting(aShard, null, true, ShardRoutingState.UNASSIGNED)
                ).build()
            )
            .build();
        final SnapshotsInProgress.Entry earlier = snapshotEntry(
            newSnapshotId("earlier"),
            List.of(aId, dId),
            Map.of(
                aShard,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED,
                dShard,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED
            )
        );
        final SnapshotsInProgress.Entry later = snapshotEntry(
            newSnapshotId("later"),
            List.of(aId, bId),
            Map.of(
                aShard,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED,
                bShard,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED
            )
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, aId, bId, dId));

        final ClusterState reconciled = runRemovalAndReconciliation(
            clusterState(
                List.of(a, b, d),
                List.of(unassignedPrimary, startedPrimary(bShard, dataNodeId), startedPrimary(dShard, dataNodeId)),
                List.of(earlier, later),
                abandoned
            ),
            abandoned
        );
        InFlightShardSnapshotStates.forRepo(REPO, snapshotsOf(reconciled).entries());
        assertIdentifiersAreUniqueByName(reconciled);
        assertEquals(
            "a shard no earlier entry of this pass started is not kept queued for a later one",
            List.of(
                SnapshotsInProgress.ShardState.MISSING,
                SnapshotsInProgress.ShardState.INIT,
                SnapshotsInProgress.ShardState.MISSING,
                SnapshotsInProgress.ShardState.INIT
            ),
            List.of(
                entryOf(reconciled, 1).shards().get(aShard).state(),
                entryOf(reconciled, 1).shards().get(bShard).state(),
                entryOf(reconciled, 0).shards().get(aShard).state(),
                entryOf(reconciled, 0).shards().get(dShard).state()
            )
        );
    }

    /**
     * With no reconciliation owed, a node drop fails the queued shard of an entry behind the failed one, as it always has: the entry
     * holds nothing back for a pass, so only the failure can move it on.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testANodeDropStillFailsAQueuedShardWhenNoReconciliationIsOwed() throws Exception {
        final IndexMetadata a = indexMetadata("a");
        final ShardId aShard = new ShardId(a.getIndex(), 0);
        final IndexId aId = new IndexId("a", uuid());
        final String droppedNodeId = uuid();
        final SnapshotsInProgress.Entry running = snapshotEntry(
            newSnapshotId("running"),
            List.of(aId),
            Map.of(aShard, new SnapshotsInProgress.ShardSnapshotStatus(droppedNodeId, "gen-a"))
        );
        final SnapshotsInProgress.Entry queued = snapshotEntry(
            newSnapshotId("queued"),
            List.of(aId),
            Map.of(aShard, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final ClusterState state = clusterState(a, startedPrimary(aShard, droppedNodeId), List.of(running, queued), null);
        assertTrue("the premise: no reconciliation is owed", reconciliationOwed().isEmpty());

        final ClusterState dropped = nodeDropUpdate(state, droppedNodeId).execute(state);

        assertEquals(
            "with no reconciliation owed, a node drop still fails the queued shard behind the failed one",
            SnapshotsInProgress.ShardState.FAILED,
            entryOf(dropped, 1).shards().get(aShard).state()
        );
        InFlightShardSnapshotStates.forRepo(REPO, snapshotsOf(dropped).entries());
        assertIdentifiersAreUniqueByName(dropped);
    }

    /**
     * A node drop does not copy the failure of the shard it fails into a later entry the reconciliation is holding. That entry has
     * begun nothing, and one of its shards is queued only because of the debt, with nothing running ahead of it; with the failure
     * copied in it would count as begun, no pass would rewrite it, and nothing would ever start that shard. An entry behind the failed
     * shard that has begun still takes the failure. Once the entries ahead of it have left, the pass starts the held entry.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testANodeDropLeavesAnEntryHeldForTheReconciliationQueued() throws Exception {
        final IndexMetadata a = indexMetadata("a");
        final IndexMetadata b = indexMetadata("b");
        final IndexMetadata c = indexMetadata("c");
        final ShardId aShard = new ShardId(a.getIndex(), 0);
        final ShardId bShard = new ShardId(b.getIndex(), 0);
        final ShardId cShard = new ShardId(c.getIndex(), 0);
        final IndexId aId = new IndexId("a", uuid());
        final IndexId bId = new IndexId("b", uuid());
        final IndexId cId = new IndexId("c", uuid());
        final String droppedNodeId = uuid();
        final String survivorNodeId = uuid();
        final SnapshotsInProgress.Entry running = snapshotEntry(
            newSnapshotId("running"),
            List.of(aId),
            Map.of(aShard, new SnapshotsInProgress.ShardSnapshotStatus(droppedNodeId, "gen-a"))
        );
        final SnapshotsInProgress.Entry held = snapshotEntry(
            newSnapshotId("held"),
            List.of(aId, bId),
            Map.of(
                aShard,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED,
                bShard,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED
            )
        );
        final SnapshotsInProgress.Entry begun = snapshotEntry(
            newSnapshotId("begun"),
            List.of(aId, cId),
            Map.of(
                aShard,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED,
                cShard,
                new SnapshotsInProgress.ShardSnapshotStatus(survivorNodeId, SnapshotsInProgress.ShardState.SUCCESS, "gen-c")
            )
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        // The repository does not know a, so the pass defers the held entry, which inherited the running entry's identifier for it.
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, bId, cId));

        final ClusterState left = runRemovalAndReconciliation(
            clusterState(
                List.of(a, b, c),
                List.of(
                    startedPrimary(aShard, droppedNodeId),
                    startedPrimary(bShard, survivorNodeId),
                    startedPrimary(cShard, survivorNodeId)
                ),
                List.of(running, held, begun),
                abandoned
            ),
            abandoned
        );
        assertEquals("the premise: the pass leaves the held entry queued", held.shards(), entryOf(left, 1).shards());
        assertTrue("the premise: the debt is kept", reconciliationOwed().contains(REPO));

        final ClusterState dropped = nodeDropUpdate(left, droppedNodeId).execute(left);
        assertEquals(
            "a node drop must not fail a queued shard of an entry held for the reconciliation",
            held.shards(),
            entryOf(dropped, 1).shards()
        );
        assertEquals(
            "a node drop still fails the queued shard of a begun entry behind the failed one",
            SnapshotsInProgress.ShardState.FAILED,
            entryOf(dropped, 2).shards().get(aShard).state()
        );
        assertEquals(
            "the premise: the drop failed the running shard",
            SnapshotsInProgress.ShardState.FAILED,
            entryOf(dropped, 0).shards().get(aShard).state()
        );
        InFlightShardSnapshotStates.forRepo(REPO, snapshotsOf(dropped).entries());
        assertIdentifiersAreUniqueByName(dropped);

        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 4L, aId, bId, cId));
        final ClusterState started = reDriveFromClusterState(
            clusterState(
                List.of(a, b, c),
                List.of(
                    startedPrimary(aShard, survivorNodeId),
                    startedPrimary(bShard, survivorNodeId),
                    startedPrimary(cShard, survivorNodeId)
                ),
                List.of(entryOf(dropped, 1)),
                null
            )
        );
        assertNotNull("the entries ahead of it leaving must re-drive the debt", started);
        InFlightShardSnapshotStates.forRepo(REPO, snapshotsOf(started).entries());
        assertIdentifiersAreUniqueByName(started);
        assertEquals(
            "the held entry is started by the pass once the entries ahead of it have left",
            List.of(SnapshotsInProgress.ShardState.INIT, SnapshotsInProgress.ShardState.INIT),
            List.of(entryOf(started, 0).shards().get(aShard).state(), entryOf(started, 0).shards().get(bShard).state())
        );
        assertTrue("and the debt is discharged", reconciliationOwed().isEmpty());
    }

    /**
     * The gate on the re-drive, in both directions and on both of its arms. A reconciliation pass reads the repository, which
     * materializes the whole repository index blob before the pass can decide it has nothing to do. That read is made on a pool
     * thread, not on the cluster applier thread the re-drive runs on, but driving a pass from every cluster state application would
     * still spend it at a rate set by routing, membership, mapping and settings changes. The gate keeps those from setting the
     * rate; snapshot progress in any repository still re-drives every owed repository, one pass at a time behind the in-flight
     * guard.
     * <p>
     * The last test is the only cover the deletions-in-progress arm has: routing changes, node joins, mapping updates and setting
     * updates all leave both customs instance-identical.
     * <p>
     * Every test is built on the deferral shape -- a second entry of the repository holds the index name the queued entry would have
     * to mint for -- because it is the only shape a pass leaves the debt standing after, so each test still has a debt for the next one
     * to re-drive.
     */
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
        final SnapshotsInProgress.Entry queued = snapshotEntry(
            newSnapshotId("queued"),
            List.of(shared),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        // A debt an earlier pass retained, which is the state all three tests start from.
        reconciliationOwed().add(REPO);
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));
        final ClusterState state = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(running, queued), null);
        reconciliationInput.set(state);

        // The routing table moved and neither custom did, which is the shape of every event that cannot have released this debt.
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

    /**
     * A reconciliation read is given the repository I/O budget, and a read that outlives it neither wedges the queued snapshots
     * behind it nor is joined by a second read. The budget's expiry arms the next attempt one rung up and keeps the guard and the
     * debt, failing and starting nothing; an attempt that finds the read still out arms the next one without reading; the late
     * answer changes nothing, and gives the repository back; and the attempt after it reads, claims, starts the queued shard and
     * cancels its budget.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAReconciliationReadThatOutlivesItsBudgetIsRetriedWithoutASecondRead() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final String dataNodeId = uuid();
        final IndexId preAbandonment = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queued = snapshotEntry(
            newSnapshotId("queued"),
            List.of(preAbandonment),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));
        final AtomicReference<Object> createOutcome = new AtomicReference<>();
        listenFor(queued.snapshot(), createOutcome);
        // Distinct from every rung of the retry ladder, so a budget can never be taken for a retry.
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

    /**
     * The budget governs a re-read too: when the repository's metadata moved before the update ran, the repository reads again
     * with the same update and failure handler, and that re-read holds the repository as the read out, under the attempt's budget.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testTheBudgetOfAReconciliationReadCoversARereadOfIt() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final String dataNodeId = uuid();
        final SnapshotsInProgress.Entry queued = snapshotEntry(
            newSnapshotId("queued"),
            List.of(new IndexId("idx", uuid())),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));
        final AtomicReference<Object> createOutcome = new AtomicReference<>();
        listenFor(queued.snapshot(), createOutcome);
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

    /**
     * What releases a debt held up by a failing repository read is the read answering, which changes neither of the customs the
     * re-drive watches, so the attempts do not stop.
     * <p>
     * This spends the entire budget on failed reads, asserts that the attempt past the budget is
     * still armed, asserts that a re-drive arriving alongside it cannot stack a second attempt, and then lets
     * the read answer and asserts that the attempt past the budget is the one that rebinds and starts the snapshot.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testReadFailuresPastTheAttemptBudgetStillReconcile() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final String dataNodeId = uuid();
        final IndexId preAbandonment = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queuedEntry = snapshotEntry(
            newSnapshotId("queued"),
            List.of(preAbandonment),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));

        // One failure for the attempt the removal drives and one for every retry the budget allows, so that the attempt after them
        // is the first one that could answer.
        final int budget = attemptsBeforeWarn();
        readFailuresBeforeAnAnswer.set(budget + 1);

        runRemoval(clusterState(indexMetadata, startedPrimary(shardId, dataNodeId), List.of(queuedEntry), abandoned), abandoned);
        assertTrue("a failed read must give its repository back for the next attempt", readsOut().isEmpty());
        assertTrue("and cancel its budget", pendingBudgetTimers.isEmpty());
        for (int attempt = 1; attempt <= budget; attempt++) {
            runArmedRetry();
        }

        assertEquals("a read that failed is not a reconciliation", 0, reconciliationPasses.get());
        assertEquals(
            "the attempt past the budget must still be armed, because nothing else comes back to a debt a read failure holds up",
            1,
            pendingSchedules.size()
        );
        assertTrue("with the debt still recorded", reconciliationOwed().contains(REPO));

        // A re-drive arriving now must not stack a second attempt on the armed one: the repository stays inside the in-flight guard
        // for as long as the chain runs, so the one entry point that could start a second chain declines.
        applyAsClusterManager(
            reconciliationInput.get(),
            ClusterState.builder(reconciliationInput.get()).putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).build()
        );
        assertEquals("a re-drive must not arm a second retry", 1, pendingSchedules.size());
        assertEquals("nor drive a read alongside the armed one", 0, reconciliationPasses.get());

        runArmedRetry();
        assertEquals("the attempt past the budget reconciles once the read answers", 1, reconciliationPasses.get());
        final SnapshotsInProgress.Entry updated = entryOf(reconciliationOutput.get(), 0);
        assertNotEquals(
            "which replaces the identifier the entry resolved before the delete was given up on",
            preAbandonment.getId(),
            updated.indices().get(0).getId()
        );
        assertEquals(SnapshotsInProgress.ShardState.INIT, updated.shards().get(shardId).state());
        assertEquals(dataNodeId, updated.shards().get(shardId).nodeId());
        assertTrue("and discharges the debt", reconciliationOwed().isEmpty());
        assertTrue("leaving no retry armed", pendingSchedules.isEmpty());
    }

    /**
     * An election cannot rebuild a debt whose only record is this set, and a dropped debt makes the identity-rebind predicate read
     * false, after which a shard completion would start the queued entry under the identifier it resolved before the delete was
     * abandoned and its blobs would land under a prefix the abandoned cleanup enumerates.
     * <p>
     * The second half pins the reason keeping it is safe: on a node that is not the elected cluster manager the dangling-snapshot
     * assertion does not consult the debt at all, so a stale one cannot disarm it. Without that early return this test fails on the
     * assertion itself, because it applies a queued shard with no running delete and no debt.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testADemotedNodeKeepsItsReconciliationDebt() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final SnapshotsInProgress.Entry queued = snapshotEntry(
            newSnapshotId("queued"),
            List.of(new IndexId("idx", uuid())),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
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

    /**
     * The chain holds its place on the role, which is what makes keeping the debt possible: it drives no repository work while the
     * role is elsewhere and comes back to the debt once the role returns.
     * <p>
     * It holds its place through the race a release could not survive. {@code ClusterApplierService#applyChanges} runs every applier
     * before it publishes the new state, so for the whole of a re-election's applier pass {@code clusterService.state()} still answers
     * with the pre-election state -- and the resume path, running in that same pass, has already found the queued shard, tried to
     * drive it, and declined because the armed attempt still holds the in-flight guard. An arm that released the guard on that stale
     * read would leave the debt recorded on a node that <em>is</em> cluster manager with no attempt running and no timer armed: the
     * queued snapshot would never be rebound, started or failed, so its listener would never complete and the entry would go on
     * blocking later work on the repository.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnArmedRetryHoldsItsPlaceWhileThisNodeIsNotClusterManager() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final String dataNodeId = uuid();
        final IndexId preAbandonment = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queuedEntry = snapshotEntry(
            newSnapshotId("queued"),
            List.of(preAbandonment),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));
        // One failure, so that the attempt the removal drives arms a retry and the attempt after it could answer.
        readFailuresBeforeAnAnswer.set(1);

        runRemoval(clusterState(indexMetadata, startedPrimary(shardId, dataNodeId), List.of(queuedEntry), abandoned), abandoned);
        assertEquals("the failed read must have armed a retry", 1, pendingSchedules.size());
        final TimeValue armed = pendingSchedules.peekLast().delay();

        // The re-election's applier pass. The resume path finds the queued shard and declines, which is what leaves the armed
        // attempt as the only thing that can come back to this debt.
        final ClusterState reElected = reconciliationInput.get();
        snapshotsService.applyClusterState(new ClusterChangedEvent("test", reElected, withoutLocalClusterManager(reElected)));
        runPendingRepositoryReads();
        assertEquals("the resume path must decline while a loop is in flight", 0, reconciliationPasses.get());
        assertEquals("and must not arm a second attempt alongside the one that is armed", 1, pendingSchedules.size());
        assertTrue("nor hand one to the generic pool", pendingDispatchedDrives.isEmpty());

        // The state that applier pass has not published yet, which is exactly what the armed attempt reads when it fires.
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

        // Re-elected on the same node. What reconciles is the chain the demotion suspended, and it has something to work on only
        // because the demotion kept the debt.
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

    /**
     * The resume path's own feature-flag gate. Both of its arms write the debt -- the delete-owned one straight to the set, the other
     * through {@code reconcileQueuedSnapshots}, which does not check the flag itself -- and the set is read unconditionally by the
     * dangling-snapshot assertion, whose early return is itself gated on the flag. Without the gate, a cluster running default
     * settings would record a debt for every queued shard behind a started delete and leave that assertion disarmed for the
     * repository. The tests that lock the flag on cannot see this gate.
     */
    public void testTheResumePathRecordsNothingWithTheFeatureOff() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final SnapshotsInProgress.Entry queued = snapshotEntry(
            newSnapshotId("queued"),
            List.of(new IndexId("idx", uuid())),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        // Behind a started delete, which is the arm that writes the set directly, and which is also what keeps the
        // dangling-snapshot assertion satisfied on this test while there is no debt to satisfy it instead.
        final ClusterState withDelete = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queued), startedDeletion());

        snapshotsService.applyClusterState(new ClusterChangedEvent("test", withDelete, withoutLocalClusterManager(withDelete)));

        assertTrue("a cluster with the feature off must have no reconciliation debt recorded for it", reconciliationOwed().isEmpty());
        assertTrue("and no repository work driven", pendingRepositoryReads.isEmpty());
        assertTrue("nor armed", pendingSchedules.isEmpty());
    }

    /**
     * Both retry ladders are driven by a timer and never retry at once. A reconciliation read that keeps failing is retried one rung
     * up each time, doubling from two seconds to a thirty-second cap and staying there, and the operator is warned once in the run;
     * the publication of an update that keeps failing to commit while a create is kept queued is retried from one second, doubling
     * to the same cap.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testTheRetryLaddersAreTimedAndNeverImmediate() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final SnapshotsInProgress.Entry queuedEntry = snapshotEntry(
            newSnapshotId("queued"),
            List.of(new IndexId("idx", uuid())),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
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
            for (int attempt = 1; attempt < failures; attempt++) {
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
        runArmedRetry();
        assertTrue("the read that answers discharges the debt", reconciliationOwed().isEmpty());

        final SnapshotsInProgress.Entry queuedCreate = snapshotEntry(
            newSnapshotId("queued-create"),
            List.of(new IndexId("idx", uuid())),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        reconciliationOwed().add(REPO);
        final ClusterState state = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queuedCreate), null);
        reconciliationInput.set(state);
        ClusterStateUpdateTask failTask = failPendingRepoTasksTask();
        final List<TimeValue> publicationRetries = new ArrayList<>();
        for (int attempt = 0; attempt < 6; attempt++) {
            failTask.execute(state);
            failTask.onFailure(FAIL_TASKS_SOURCE, new FailedToCommitClusterStateException("simulated publish failure"));
            final int submitted = submittedTasks.size();
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

    /**
     * With the feature off nothing is owed a start from a fresh read, so the ways into the reconciliation other than the resume path
     * pinned above are not reached either. A failed delete's removal, on the arm every delete takes with the feature off, starts
     * what was queued behind it from the data it carries, and records, drives and arms nothing; and a create is started even with a
     * debt on record.
     */
    public void testAFailedDeleteAndACreateOweNothingWithTheFeatureOff() throws Exception {
        assertFalse(FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING));
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final String dataNodeId = uuid();
        final IndexId preDelete = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queued = snapshotEntry(
            newSnapshotId("queued"),
            List.of(preDelete),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotId deletedBySecond = newSnapshotId("deleted-by-second");
        final SnapshotDeletionsInProgress.Entry first = startedDeletion();
        final SnapshotDeletionsInProgress.Entry second = new SnapshotDeletionsInProgress.Entry(
            List.of(deletedBySecond),
            REPO,
            0L,
            CAPTURED_GEN,
            SnapshotDeletionsInProgress.State.WAITING
        );
        final RepositoryData carried = RepositoryData.EMPTY.withGenId(CAPTURED_GEN)
            .addSnapshot(deletedBySecond, SnapshotState.SUCCESS, Version.CURRENT, ShardGenerations.EMPTY, null, null);
        final AtomicReference<ActionListener<RepositoryData>> secondDelete = new AtomicReference<>();
        doAnswer(invocation -> {
            secondDelete.set(invocation.getArgument(3));
            return null;
        }).when(repository).deleteSnapshots(any(), anyLong(), any(), any());
        final ClusterState withBoth = ClusterState.builder(
            clusterState(indexMetadata, startedPrimary(shardId, dataNodeId), List.of(), null)
        ).putCustom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.of(List.of(first, second))).build();

        // The second delete is dispatched by the production path, which is where its failure's removal is told whether to distrust
        // the data it carries.
        primeRunningDelete(first.uuid());
        final ClusterStateUpdateTask firstRemoval = snapshotsService.createRemoveSnapshotDeletionTask(
            SOURCE,
            0,
            first,
            null,
            carried,
            null,
            false
        );
        final ClusterState secondRunning = firstRemoval.execute(withBoth);
        firstRemoval.clusterStateProcessed(SOURCE, withBoth, secondRunning);
        assertNotNull("the premise: the removal of the first delete dispatched the second", secondDelete.get());
        // A create queued behind the second delete while it runs.
        final ClusterState queuedBehindSecond = ClusterState.builder(secondRunning)
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(List.of(queued)))
            .build();
        final int submittedBefore = submittedTasks.size();
        secondDelete.get().onFailure(new RepositoryException(REPO, "the delete did not complete"));
        assertEquals("the premise: the second delete's failure submits its removal", submittedBefore + 1, submittedTasks.size());
        final ClusterStateUpdateTask secondRemoval = submittedTasks.get(submittedBefore);
        final ClusterState promoted = secondRemoval.execute(queuedBehindSecond);
        secondRemoval.clusterStateProcessed(SOURCE, queuedBehindSecond, promoted);

        assertEquals(
            "with the feature off a failed delete's removal starts what was queued behind it",
            SnapshotsInProgress.ShardState.INIT,
            entryOf(promoted, 0).shards().get(shardId).state()
        );
        assertTrue("and records no reconciliation debt", reconciliationOwed().isEmpty());
        assertTrue("nor drives one", pendingDispatchedDrives.isEmpty() && pendingRepositoryReads.isEmpty());
        assertTrue("nor arms one", pendingSchedules.isEmpty());

        reconciliationOwed().add(REPO);
        final SnapshotsInProgress.Entry inherited = createdEntry(
            clusterState(List.of(indexMetadata), List.of(startedPrimary(shardId, dataNodeId)), List.of(queued), null),
            RepositoryData.EMPTY.withGenId(CAPTURED_GEN)
        );
        assertEquals("the premise: the create inherits the queued entry's identifier", List.of(preDelete), inherited.indices());
        assertEquals(
            "with the feature off a create is started even with a debt on record",
            SnapshotsInProgress.ShardState.INIT,
            inherited.shards().get(shardId).state()
        );
        assertTrue("and nothing is driven or armed for it", pendingDispatchedDrives.isEmpty() && pendingSchedules.isEmpty());
        reconciliationOwed().remove(REPO);
    }

    /**
     * The other half of what nothing drove: an attempt that runs with the debt already gone. An attempt with nothing owed must
     * read nothing -- the repository read is the expensive half of a pass -- and must release the repository so a later debt is not
     * refused as already in flight.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnArmedRetryWithNothingOwedReadsNothingAndReleasesTheRepository() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final String dataNodeId = uuid();
        final IndexId preAbandonment = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queuedEntry = snapshotEntry(
            newSnapshotId("queued"),
            List.of(preAbandonment),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));
        readFailuresBeforeAnAnswer.set(1);

        runRemoval(clusterState(indexMetadata, startedPrimary(shardId, dataNodeId), List.of(queuedEntry), abandoned), abandoned);
        assertEquals("the failed read must have armed a retry", 1, pendingSchedules.size());

        // Discharged while the retry sat on the scheduler, which is what an earlier pass or a failover away and back leaves behind.
        reconciliationOwed().remove(REPO);
        runArmedRetry();

        assertEquals("an attempt with nothing owed must not read the repository", 0, reconciliationPasses.get());
        assertTrue("nor arm another attempt", pendingSchedules.isEmpty());
        assertFalse("and must release the repository", reconcilingRepositories().contains(REPO));

        // A debt recorded afterwards must still be admitted, which it is only if the attempt above released the in-flight guard.
        reconciliationOwed().add(REPO);
        final ClusterState published = reDriveFromClusterState(reconciliationInput.get());
        assertEquals("a later debt must get a loop of its own", 1, reconciliationPasses.get());
        assertEquals(SnapshotsInProgress.ShardState.INIT, entryOf(published, 0).shards().get(shardId).state());
    }

    /**
     * A failover to a <em>different</em> node, with the queued shard still behind a started delete: the resume path records the debt
     * of a repository a delete owns, and only records it, because the debt says the identity is unbound, not that a pass may run
     * now; a pass driven here would re-validate, find the delete and decline. The second half pins that the delete leaving comes back
     * to the record.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAFailoverRecordsADebtForAQueuedShardBehindAStartedDelete() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final String dataNodeId = uuid();
        final IndexId preAbandonment = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queuedEntry = snapshotEntry(
            newSnapshotId("queued"),
            List.of(preAbandonment),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));
        final ClusterState withDelete = clusterState(
            indexMetadata,
            startedPrimary(shardId, dataNodeId),
            List.of(queuedEntry),
            startedDeletion()
        );

        // Elected with the delete still started: previously cluster manager elsewhere, now here.
        snapshotsService.applyClusterState(new ClusterChangedEvent("test", withDelete, withoutLocalClusterManager(withDelete)));

        assertTrue("the debt has to survive a failover, and only this node can record it now", reconciliationOwed().contains(REPO));
        assertTrue("but the delete still owns the repository, so no read is driven for it", pendingRepositoryReads.isEmpty());
        assertTrue("and no retry is armed either", pendingSchedules.isEmpty());
        assertFalse("nor admits a loop for it", reconcilingRepositories().contains(REPO));

        final ClusterState withoutDelete = clusterState(indexMetadata, startedPrimary(shardId, dataNodeId), List.of(queuedEntry), null);
        reconciliationInput.set(withoutDelete);
        applyAsClusterManager(withoutDelete, withDelete);

        assertEquals("the delete leaving is what re-drives the debt recorded behind it", 1, reconciliationPasses.get());
        final SnapshotsInProgress.Entry updated = entryOf(reconciliationOutput.get(), 0);
        assertNotEquals(
            "which replaces the identifier the entry resolved before the delete was given up on",
            preAbandonment.getId(),
            updated.indices().get(0).getId()
        );
        assertEquals(SnapshotsInProgress.ShardState.INIT, updated.shards().get(shardId).state());
        assertTrue("and discharges the debt", reconciliationOwed().isEmpty());
    }

    /**
     * A re-drive that ran the first attempt inline would reach Repository#executeConsistentStateUpdate on the cluster applier thread,
     * and BlobStoreRepository#getRepositoryData answers a cache hit inline, which puts the decompress and parse of the whole
     * repository index blob on the one thread the cluster cannot afford to block.
     * <p>
     * The first three assertions are the property: the thread that applied the state asked a repository for nothing, and the work
     * is outstanding rather than dropped. The next two pin where the attempt went. The harness captures what the service hands to
     * the generic pool apart from what it schedules, so an attempt that arrives among the scheduled retries instead is caught here.
     * A dispatcher the harness does not capture, such as the snapshot pool's executor, would run the attempt for real on another
     * thread.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAReDriveDispatchesTheAttemptInsteadOfReadingOnTheCallingThread() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final SnapshotsInProgress.Entry queuedEntry = snapshotEntry(
            newSnapshotId("queued"),
            List.of(new IndexId("idx", uuid())),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));
        final ClusterState state = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queuedEntry), null);
        reconciliationInput.set(state);
        reconciliationOutput.set(null);
        reconciliationOwed().add(REPO);

        snapshotsService.applyClusterState(
            new ClusterChangedEvent(
                "test",
                state,
                ClusterState.builder(state).putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).build()
            )
        );

        // The property.
        assertTrue(
            "the thread that applied the cluster state must not have asked a repository for anything",
            pendingRepositoryReads.isEmpty()
        );
        assertTrue("the debt stays recorded, so the work is owed", reconciliationOwed().contains(REPO));
        assertTrue("and the guard is held, so a loop is outstanding rather than dropped", reconcilingRepositories().contains(REPO));
        // Where it went -- see the javadoc.
        assertEquals("the attempt must have been handed to the generic pool", 1, pendingDispatchedDrives.size());
        assertTrue("and not scheduled among the retries", pendingSchedules.isEmpty());

        runDispatchedReconciliation();
        assertEquals("the dispatched attempt is the one that reconciles", 1, reconciliationPasses.get());
        assertEquals(SnapshotsInProgress.ShardState.INIT, entryOf(reconciliationOutput.get(), 0).shards().get(shardId).state());
        assertTrue("and discharges the debt", reconciliationOwed().isEmpty());
    }

    /**
     * The entrance the failover resume exists for: this node is newly elected, the cluster state holds a queued shard, and no
     * deletion owns the repository -- so the work is startable now, and the only record that it is owed died with the deposed
     * leader. Deliberately does not prime the debt: inferring it from the cluster state is the whole point, and a primed debt would
     * satisfy the debt assertion below with the resume deleted.
     * <p>
     * Also the failover half of the applier-thread invariant. The resume runs inside the applier pass, so an attempt run inline from
     * it would read the repository on the cluster applier thread -- and, unlike the re-drive, the resume is not gated on the two
     * customs, so it would do so on every election that finds a queued shard and no running delete.
     * <p>
     * The dispatched attempt is run while the role still reads as it did before the election, which is what a pool thread sees
     * until the appliers of the state that elected this node have run, and it must reconcile all the same: a first attempt is not
     * held behind the role check a retry passes.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAFailoverWithNoRunningDeleteDispatchesTheResumedDrive() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final String dataNodeId = uuid();
        final IndexId preAbandonment = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queuedEntry = snapshotEntry(
            newSnapshotId("queued"),
            List.of(preAbandonment),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));
        // No deletion: the queued shard is startable now, which is what distinguishes this from
        // testAFailoverRecordsADebtForAQueuedShardBehindAStartedDelete.
        final ClusterState elected = clusterState(indexMetadata, startedPrimary(shardId, dataNodeId), List.of(queuedEntry), null);
        reconciliationInput.set(elected);
        reconciliationOutput.set(null);

        snapshotsService.applyClusterState(new ClusterChangedEvent("test", elected, withoutLocalClusterManager(elected)));

        // The property: nothing was read on the thread that applied the state.
        assertTrue(
            "the thread that applied the cluster state must not have asked a repository for anything",
            pendingRepositoryReads.isEmpty()
        );
        assertEquals("nor reconciled anything on it", 0, reconciliationPasses.get());
        // The entrance: the debt was inferred, not remembered, and a loop was admitted for it.
        assertTrue("the debt has to be inferred from the cluster state, not remembered", reconciliationOwed().contains(REPO));
        assertTrue("with a loop admitted for it", reconcilingRepositories().contains(REPO));
        assertEquals("and the resumed attempt handed to the generic pool", 1, pendingDispatchedDrives.size());

        // What a pool thread still reads while the election's appliers run.
        when(clusterService.state()).thenReturn(withoutLocalClusterManager(elected));
        runDispatchedReconciliation();
        assertEquals("running the dispatch is what reconciles, whatever the role reads", 1, reconciliationPasses.get());
        final SnapshotsInProgress.Entry updated = entryOf(reconciliationOutput.get(), 0);
        assertNotEquals(
            "rebinding the identifier the entry resolved before the delete was given up on",
            preAbandonment.getId(),
            updated.indices().get(0).getId()
        );
        assertEquals(SnapshotsInProgress.ShardState.INIT, updated.shards().get(shardId).state());
        assertTrue("and discharging the debt", reconciliationOwed().isEmpty());
    }

    /**
     * An attempt that throws before it asks for the repository read. The in-flight guard is held by the chain, not by the attempt, so
     * a throw that escaped would leave the guard held with nothing running or armed behind it -- and every later loop for the
     * repository refused as already in flight, for the life of the node. The throw arms the next attempt instead.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnAttemptThatThrowsArmsItsSuccessor() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final SnapshotsInProgress.Entry queuedEntry = snapshotEntry(
            newSnapshotId("queued"),
            List.of(new IndexId("idx", uuid())),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
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

    /**
     * The arm that runs when an attempt finds the repository gone. Its two releases are asserted separately because each is one
     * line of production and they fail differently. A retained guard refuses every later loop for that name, so a repository
     * registered again under it never reconciles for the life of the node. A retained debt has no writer left that can remove it --
     * the only thing that discharges one is a pass, and a pass needs the repository this arm has just established is gone -- which
     * also leaves the dangling-snapshot assertion's owed-reconciliation arm trivially satisfied for that repository forever.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnAttemptForAMissingRepositoryClearsBothTheGuardAndTheDebt() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final SnapshotsInProgress.Entry queuedEntry = snapshotEntry(
            newSnapshotId("queued"),
            List.of(new IndexId("idx", uuid())),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        final AtomicReference<Object> queuedCreateOutcome = new AtomicReference<>();
        listenFor(queuedEntry.snapshot(), queuedCreateOutcome);
        // One failure, so that the removal's own attempt arms a retry and the repository can go away before that retry fires.
        readFailuresBeforeAnAnswer.set(1);

        runRemoval(clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queuedEntry), abandoned), abandoned);
        assertEquals("the failed read must have armed a retry", 1, pendingSchedules.size());
        assertTrue("which holds the in-flight guard while it is armed", reconcilingRepositories().contains(REPO));
        assertTrue("and the debt it was armed for", reconciliationOwed().contains(REPO));

        // Gone by the time the armed retry fires, which is the state this arm exists for. How it went is deliberately not
        // asserted: the delete-repository API refuses a repository any entry of SnapshotsInProgress names, queued included
        // (RepositoriesService#isRepositoryInUse filters on no state at all), so the route is not the obvious one.
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
        // Paired with the assertion above for the same reason as in the rejected-arm test below: the listener alone cannot see a
        // failure delivered through a cluster state update, because this mock cluster service never runs the task it is given.
        verify(clusterService, never()).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
    }

    /**
     * The arm that runs when the scheduler refuses to arm an attempt, which it does only once it has been shut down. Releasing the
     * guard is the whole of the arm, and the three things it must not do are what make the test worth writing: the debt has to
     * survive, because what was lost is one attempt and not the work; nothing may be armed, because the scheduler that just refused
     * one would refuse its successor; and no queued snapshot may be failed, which is the shape most likely to be reached for here,
     * since a node on its way down looks like a good moment to give up on queued work.
     * <p>
     * The rejection is armed for exactly one schedule so that an attempt this arm should not have armed is captured and asserted on
     * rather than rejected in its turn and never seen.
     * <p>
     * Then what recovers the work the refused arm leaves behind. A scheduler refuses only once it has been shut down, so the node that
     * refused runs nothing more for the repository, and the work is resumed by whichever node is elected next: its resume infers the
     * debt from the queued shard in the cluster state -- the refusing node's record of it does not survive -- and hands a first
     * attempt to the generic pool. The test runs that sequence on one service, forgetting the debt in between, because a successor
     * never had it. The resume on its own is pinned by testAFailoverWithNoRunningDeleteDispatchesTheResumedDrive; what this test adds
     * is that the two compose: the refusal leaves nothing behind that stops the resume.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testARejectedRetryArmReleasesTheGuardAndKeepsTheDebt() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId preAbandonment = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queuedEntry = snapshotEntry(
            newSnapshotId("queued"),
            List.of(preAbandonment),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        final AtomicReference<Object> queuedCreateOutcome = new AtomicReference<>();
        listenFor(queuedEntry.snapshot(), queuedCreateOutcome);
        reconciliationRepositoryData.set(RepositoryData.EMPTY.withGenId(CAPTURED_GEN + 3L));
        // Two failures: the first arms the retry this test runs, the second is the failure that asks for the arm that gets rejected.
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
        // The route the listener above cannot see: failing a repository's pending tasks is a cluster state update, and this mock
        // cluster service never runs the task, so nothing would reach the listener. This catches it instead. It matches the two-argument
        // submit that every task of this service uses bar one -- the shard-state update, which no arm here can reach.
        verify(clusterService, never()).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        assertEquals("and nothing has reconciled yet", 0, reconciliationPasses.get());

        // The next cluster manager never had this node's record of the debt, and applies the state the removal published.
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

    /**
     * A finalization that reads the repository for itself while a reconciliation is owed, and whose read genuinely fails, fails the
     * repository's pending operations, as without the feature, except that a clone of the repository that has begun nothing is kept
     * for the pass.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAQueuedCloneIsKeptWhenAFinalizationsOwnReadFailsWhileAReconciliationIsOwed() throws Exception {
        final IndexMetadata otherMetadata = indexMetadata("other");
        final ShardId otherShardId = new ShardId(otherMetadata.getIndex(), 0);
        final String dataNodeId = uuid();
        final SnapshotsInProgress.Entry finalizing = snapshotEntry(
            newSnapshotId("finalizing"),
            List.of(new IndexId("other", uuid())),
            Map.of(
                otherShardId,
                new SnapshotsInProgress.ShardSnapshotStatus(dataNodeId, SnapshotsInProgress.ShardState.FAILED, "aborted", null)
            )
        );
        final IndexId indexId = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queued = cloneEntry(
            "queued-clone",
            newSnapshotId("clone-source"),
            List.of(indexId),
            Map.of(new RepositoryShardId(indexId, 0), SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final AtomicReference<Object> queuedOutcome = new AtomicReference<>();
        listenFor(queued.snapshot(), queuedOutcome);
        final ClusterState state = clusterState(
            List.of(otherMetadata),
            List.of(startedPrimary(otherShardId, dataNodeId)),
            List.of(finalizing, queued),
            null
        );
        stubLocalNode(state);

        // Elected over the two: the failover records the debt for the clone, and the external-changes update finalizes the
        // completed entry, whose finalization reads the repository for itself.
        snapshotsService.applyClusterState(new ClusterChangedEvent("test", state, withoutLocalClusterManager(state)));
        assertTrue("the premise: a debt is owed for the clone", reconciliationOwed().contains(REPO));
        final int external = IntStream.range(0, submittedSources.size())
            .filter(i -> submittedSources.get(i).startsWith("update snapshot after shards started"))
            .findFirst()
            .orElseThrow();
        final ClusterStateUpdateTask externalChanges = submittedTasks.get(external);
        final ClusterState processed = externalChanges.execute(state);
        externalChanges.clusterStateProcessed(submittedSources.get(external), state, processed);
        assertEquals("the premise: the finalization reads the repository for itself", 1, finalizationReads.get());

        final int before = submittedSources.size();
        finalizationReadListener.get().onFailure(new RepositoryException(REPO, "the repository read failed"));
        assertEquals(
            "a genuine read failure fails the repository's pending tasks",
            List.of(FAIL_TASKS_SOURCE),
            submittedSources.subList(before, submittedSources.size())
        );
        final ClusterStateUpdateTask failTask = submittedTasks.get(before);
        final ClusterState after = failTask.execute(processed);
        assertTrue(
            "a clone that has begun nothing is kept while a reconciliation is owed",
            snapshotsOf(after).entries().stream().anyMatch(entry -> entry.snapshot().equals(queued.snapshot()))
        );
        failTask.clusterStateProcessed(FAIL_TASKS_SOURCE, processed, after);
        assertNull("and its caller is not answered", queuedOutcome.get());
    }

    /**
     * The same state with this node no longer the elected cluster manager, which is how the demotion tests are built. The node keeps
     * its identity and the cluster simply has no cluster manager, so nothing else about the state moves.
     */
    private static ClusterState withoutLocalClusterManager(ClusterState state) {
        return ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).clusterManagerNodeId(null).build()).build();
    }

    /**
     * Removes a delete that was given up on, then runs the reconciliation its removal owes, and returns the state that
     * reconciliation published. The removal task's {@code execute} and {@code clusterStateProcessed} are run in that order and
     * against that pair of states, which is what the cluster state service does.
     */
    private ClusterState runRemovalAndReconciliation(ClusterState currentState, SnapshotDeletionsInProgress.Entry removed)
        throws Exception {
        runRemoval(currentState, removed);
        assertNotNull("the removal must have owed and driven a reconciliation", reconciliationOutput.get());
        return reconciliationOutput.get();
    }

    /**
     * The removal half of the above, without the assertion that a pass published: the test that makes every repository read fail has
     * no published state to assert on until the last one answers.
     */
    private void runRemoval(ClusterState currentState, SnapshotDeletionsInProgress.Entry removed) throws Exception {
        runRemovalTask(currentState, removed);
        runDispatchedReconciliation();
    }

    /**
     * The same, except that the attempt the removal dispatched is run and its repository read is left out, unanswered.
     */
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

    /**
     * One repository read of {@code Repository#executeConsistentStateUpdate}, answered the way the stub in {@code setUp} answers it: a
     * failure while {@link #readFailuresBeforeAnAnswer} lasts, and otherwise the update built from
     * {@link #reconciliationRepositoryData}, run against {@link #reconciliationInput}. While {@link #metadataMovesBeforeExecute}
     * lasts the update is built and dropped and the same read is asked for again, which runs neither the update nor its callback.
     */
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

    /** Runs the one time budget the service has armed, which is how a test makes it expire. */
    private void runBudgetTimer() {
        assertEquals("exactly one budget may be armed", 1, pendingBudgetTimers.size());
        pendingBudgetTimers.poll().task().run();
    }

    /** Sets the repository I/O time budget the way an operator does, through the cluster settings the service listens to. */
    private void setBudget(TimeValue budget) {
        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put("snapshot.repository.io_timeout", budget.getStringRep()).build());
    }

    /** The repositories the service has a reconciliation read out for. */
    @SuppressForbidden(reason = "the service's record of reconciliation reads out has no test seam")
    @SuppressWarnings("unchecked")
    private Set<Repository> readsOut() throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("reconciliationReadsOut");
        field.setAccessible(true);
        return (Set<Repository>) field.get(snapshotsService);
    }

    /**
     * Answers every repository read the service has asked for, including any a read's own completion asks for in turn.
     */
    private void runPendingRepositoryReads() {
        while (pendingRepositoryReads.isEmpty() == false) {
            pendingRepositoryReads.poll().run();
        }
    }

    /**
     * Runs the retry the service armed after a failed repository read, and answers the read that retry asks for. Exactly one retry
     * may be armed at a time, which is the property that keeps a repository that never answers from accumulating timers.
     */
    private void runArmedRetry() {
        assertEquals("exactly one retry may be armed for a repository", 1, pendingSchedules.size());
        runOnlyPendingSchedule();
        runPendingRepositoryReads();
    }

    /**
     * Runs the one schedule the service has asked for and returns it, so that a test can check the delay and the pool it was asked
     * for. Exactly one may be pending: with two, which one ran would be an accident of the order they were asked for in.
     */
    private Schedule runOnlyPendingSchedule() {
        assertEquals("exactly one schedule may be pending", 1, pendingSchedules.size());
        final Schedule schedule = pendingSchedules.poll();
        schedule.task().run();
        return schedule;
    }

    /**
     * Runs the first attempt a drive handed to the generic pool, if it handed one over, and then answers every repository read that
     * followed. Tolerant of there being nothing to run, because tests asserting that a drive declined come through here too;
     * {@link #runArmedRetry()} is the strict form, whose subject is a retry.
     */
    private void runDispatchedReconciliation() {
        if (pendingDispatchedDrives.isEmpty() == false) {
            pendingDispatchedDrives.poll().run();
        }
        runPendingRepositoryReads();
    }

    /**
     * Applies the given state as an ordinary cluster state change on a node that is already cluster manager, which is what re-drives
     * a debt a reconciliation pass deliberately retained, and returns the state the pass that followed published.
     */
    private ClusterState reDriveFromClusterState(ClusterState state) {
        reconciliationInput.set(state);
        reconciliationOutput.set(null);
        // The event has to carry a change to the snapshots in progress, because that is what production re-drives on: a pass reads
        // the repository, and nothing but a change to that custom or to the deletions in progress can have released the debt. A
        // cluster state change that touches neither deliberately does not re-drive, so passing the same state on both sides would
        // assert the opposite of the production rule.
        applyAsClusterManager(state, ClusterState.builder(state).putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).build());
        return reconciliationOutput.get();
    }

    /**
     * Applies {@code state} over {@code previous} on a node that is cluster manager in both, and answers every repository read that
     * follows. What {@code previous} differs from {@code state} in is the whole subject of the gate tests: the re-drive is meant to
     * happen on a change to the snapshots or the deletions in progress and on nothing else.
     */
    private void applyAsClusterManager(ClusterState state, ClusterState previous) {
        snapshotsService.applyClusterState(new ClusterChangedEvent("test", state, previous));
        runDispatchedReconciliation();
    }

    /**
     * Applies {@code state} over the same state with one more node, which is how the cluster manager sees that node leave, and
     * returns the snapshot update the departure submits, not yet run.
     */
    private ClusterStateUpdateTask nodeDropUpdate(ClusterState state, String droppedNodeId) {
        final DiscoveryNode dropped = new DiscoveryNode(droppedNodeId, buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState withNode = ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).add(dropped)).build();
        final int submittedBefore = submittedTasks.size();
        applyAsClusterManager(state, withNode);
        assertEquals("the premise: the departure submits one snapshot update", submittedBefore + 1, submittedTasks.size());
        return submittedTasks.get(submittedBefore);
    }

    /**
     * Creates a snapshot of index {@code idx} against the given state and repository data through the production create path, and
     * returns the entry it published. The stubs are the ones {@code createSnapshot} reads on its way to the identity decision under
     * test, and nothing else about the path is replaced.
     */
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

    /**
     * Marks the delete about to be removed as running against the repository, and the repository as inside its operation loop.
     * Both are what the production dispatcher would have established before the removal task was submitted, and the task asserts
     * on both. Reflection because neither has a seam a test can use -- see {@link PromotedSnapshotDeleteTests#primeRunningDelete}.
     */
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

    /**
     * The service's record of which repositories are owed a start from a fresh repository read. Asserted on directly, and primed
     * directly by the test that needs a debt an earlier delete left behind, because the set is private and the only production
     * entry point that writes it also does repository work.
     */
    @SuppressForbidden(reason = "the service's reconciliation debt has no test seam")
    @SuppressWarnings("unchecked")
    private Set<String> reconciliationOwed() throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("reconciliationOwed");
        field.setAccessible(true);
        return (Set<String>) field.get(snapshotsService);
    }

    /**
     * The service's in-flight guard. Asserted on directly by the tests about a suspended chain, because "an attempt is running or
     * armed" is exactly what membership means and a test that only counted passes and timers could not tell a held entry from a
     * released one.
     */
    @SuppressForbidden(reason = "the service's in-flight reconciliation guard has no test seam")
    @SuppressWarnings("unchecked")
    private Set<String> reconcilingRepositories() throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("reconcilingRepositories");
        field.setAccessible(true);
        return (Set<String>) field.get(snapshotsService);
    }

    /**
     * Registers a completion listener for a snapshot the way the create path does, so that a test can assert a queued create was
     * neither failed nor completed. Reflection rather than a new seam: {@code addListener} and the map behind it are both private, and
     * widening production to make a test possible is the wrong trade -- see {@link #reconciliationOwed()}.
     * <p>
     * The listener is deliberately left uncompleted, since that is what the tests using it assert. {@code assertAllListenersResolved}
     * would call that a leak, so a test registering one must not be moved onto a harness that runs it; no unit suite does.
     */
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

    /** Registers a listener for a delete the way a delete request does, counting each answer it gets. */
    @SuppressForbidden(reason = "the service's delete listeners have no test seam")
    @SuppressWarnings("unchecked")
    private void listenForDelete(String deleteUuid, AtomicInteger answers) throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("snapshotDeletionListeners");
        field.setAccessible(true);
        ((Map<String, List<ActionListener<Void>>>) field.get(snapshotsService)).computeIfAbsent(deleteUuid, uuid -> new ArrayList<>())
            .add(ActionListener.wrap(ignored -> answers.incrementAndGet(), e -> answers.incrementAndGet()));
    }

    /** How many times this node has failed its snapshot operations over. */
    @SuppressForbidden(reason = "the service's failover count has no test seam")
    private long failovers() throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("failovers");
        field.setAccessible(true);
        return ((AtomicLong) field.get(snapshotsService)).get();
    }

    /**
     * The attempt at which the service warns that a repository's queued snapshots still cannot be reconciled. Read from the constant
     * so that the test driving the attempt count past it moves with the production value instead of pinning a literal.
     */
    @SuppressForbidden(reason = "the reconciliation warn boundary has no test seam")
    private static int attemptsBeforeWarn() throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("QUEUED_SNAPSHOT_RECONCILE_ATTEMPTS_BEFORE_WARN");
        field.setAccessible(true);
        return field.getInt(null);
    }

    /**
     * A cluster state holding the given snapshots and one delete of this repository. The index metadata and routing table are
     * nullable so that a scenario can leave the index out of the cluster state altogether, which is how a shard resolves to
     * missing.
     */
    private static ClusterState clusterState(
        IndexMetadata indexMetadata,
        IndexRoutingTable indexRoutingTable,
        List<SnapshotsInProgress.Entry> snapshots,
        SnapshotDeletionsInProgress.Entry deletion
    ) {
        return clusterState(
            indexMetadata == null ? List.of() : List.of(indexMetadata),
            indexRoutingTable == null ? List.of() : List.of(indexRoutingTable),
            snapshots,
            deletion
        );
    }

    /**
     * A cluster state holding the given snapshots, the given indices, and at most one delete of this repository. Either index list
     * may be empty, which is how a shard resolves to missing, and the deletion may be null for a scenario that has no delete left.
     * The repository itself is always registered, because the create path refuses a repository the metadata does not name.
     */
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

    /**
     * Repository data that holds the given identifiers, which is what finalizing a snapshot of those indices leaves behind and what
     * lets a later reconciliation pass resolve their names authoritatively instead of minting for them.
     */
    private static RepositoryData repositoryDataHolding(long generation, IndexId... indices) {
        final ShardGenerations.Builder shardGenerations = ShardGenerations.builder();
        for (IndexId indexId : indices) {
            shardGenerations.put(indexId, 0, "gen-" + indexId.getName());
        }
        return RepositoryData.EMPTY.withGenId(generation)
            .addSnapshot(newSnapshotId("finalized"), SnapshotState.SUCCESS, Version.CURRENT, shardGenerations.build(), null, null);
    }

    /**
     * Asserts the invariant every test here turns on: across the entries of this repository, one index name carries exactly one
     * identifier. {@code RepositoryData} and the service's in-flight lookup both build their name-keyed maps with no merge function,
     * so a second live identifier for one name is not a degraded outcome but an {@code IllegalStateException} at the next
     * finalization and at every later create or clone for the repository.
     */
    private static void assertIdentifiersAreUniqueByName(ClusterState state) {
        final Map<String, Set<String>> identifiersByName = new HashMap<>();
        for (SnapshotsInProgress.Entry entry : snapshotsOf(state).entries()) {
            if (entry.repository().equals(REPO) == false) {
                continue;
            }
            for (IndexId indexId : entry.indices()) {
                identifiersByName.computeIfAbsent(indexId.getName(), name -> new HashSet<>()).add(indexId.getId());
            }
        }
        for (Map.Entry<String, Set<String>> byName : identifiersByName.entrySet()) {
            assertEquals(
                "index name [" + byName.getKey() + "] must carry exactly one live identifier but carries " + byName.getValue(),
                1,
                byName.getValue().size()
            );
        }
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
            // Must not be the unknown generation: an entry carrying that is treated as aborted before it started.
            CAPTURED_GEN,
            shards,
            Map.of(),
            Version.CURRENT,
            false
        );
    }

    /** The same, for a partial snapshot, which is the only kind whose index may be deleted while it waits. */
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

    /**
     * A clone that has begun nothing is kept, as a queued create the reconciler owes a start is, when the repository's pending tasks
     * are failed while a reconciliation is owed. A clone is a snapshot its caller asked for, and failing it because the work it was
     * queued behind failed is the outcome this arrangement exists to prevent. Its caller is left waiting, and a pass is admitted for
     * the repository.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAQueuedCloneIsKeptWhileAReconciliationIsOwed() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId indexId = new IndexId("idx", uuid());

        final SnapshotsInProgress.Entry queuedCreate = snapshotEntry(
            newSnapshotId("queued-create"),
            List.of(indexId),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotsInProgress.Entry queuedClone = cloneEntry(
            "queued-clone",
            newSnapshotId("clone-source"),
            List.of(indexId),
            Map.of(new RepositoryShardId(indexId, 0), SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );

        final AtomicReference<Object> cloneOutcome = new AtomicReference<>();
        final AtomicReference<Object> createOutcome = new AtomicReference<>();
        listenFor(queuedClone.snapshot(), cloneOutcome);
        listenFor(queuedCreate.snapshot(), createOutcome);
        reconciliationOwed().add(REPO);

        final ClusterState state = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queuedCreate, queuedClone), null);
        enterRepositoryLoop();
        final ClusterStateUpdateTask failTask = failPendingRepoTasksTask();
        final ClusterState after = failTask.execute(state);
        failTask.clusterStateProcessed(FAIL_TASKS_SOURCE, state, after);

        assertEquals(
            "the queued clone must be kept",
            List.of(queuedCreate.snapshot(), queuedClone.snapshot()),
            snapshotsOf(after).entries().stream().map(SnapshotsInProgress.Entry::snapshot).collect(Collectors.toList())
        );
        assertNull("the queued clone must be kept, and its caller left waiting", cloneOutcome.get());
        assertNull("the retained create's caller must not be told anything yet", createOutcome.get());
        assertEquals("and one pass is admitted for the repository", 1, pendingDispatchedDrives.size());
        assertTrue("nothing is handed to the snapshot pool", pendingSnapshotTasks.isEmpty());
    }

    /**
     * A queued create the reconciler owes a start is kept, its caller is left waiting, and a pass is admitted for the repository. It
     * is not failed because the delete ahead of it could not be read: a repository read failing is precisely the condition
     * reconciliation retries through.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAQueuedCreateOwedARebindIsRetainedAndReconciled() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final SnapshotsInProgress.Entry queuedCreate = snapshotEntry(
            newSnapshotId("queued-create"),
            List.of(new IndexId("idx", uuid())),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final AtomicReference<Object> createOutcome = new AtomicReference<>();
        listenFor(queuedCreate.snapshot(), createOutcome);
        reconciliationOwed().add(REPO);

        final ClusterState state = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queuedCreate), null);
        reconciliationInput.set(state);
        enterRepositoryLoop();
        final ClusterStateUpdateTask failTask = failPendingRepoTasksTask();
        final int readsBefore = pendingRepositoryReads.size();
        final ClusterState after = failTask.execute(state);
        failTask.clusterStateProcessed(FAIL_TASKS_SOURCE, state, after);

        assertEquals("the entry must survive", 1, snapshotsOf(after).entries().size());
        assertNull("and its caller must still be waiting", createOutcome.get());
        assertTrue(
            "a pass must be admitted for the repository, because the retained entry has no other owner",
            reconcilingRepositories().contains(REPO)
        );
        assertEquals("and its attempt handed to the generic pool, not read here", readsBefore, pendingRepositoryReads.size());
        assertEquals("exactly once", 1, pendingDispatchedDrives.size());
        pendingDispatchedDrives.poll().run();
        assertTrue("which, run, is the attempt that reads the repository", pendingRepositoryReads.size() > readsBefore);
    }

    /**
     * The retained-creates flag is per invocation, not per task instance. A second {@code execute} that retains nothing must not
     * inherit the first one's answer and re-arm a pass for work that is no longer there.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRetainedQueuedCreatesIsNotStickyAcrossExecute() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final SnapshotsInProgress.Entry queuedCreate = snapshotEntry(
            newSnapshotId("queued-create"),
            List.of(new IndexId("idx", uuid())),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        reconciliationOwed().add(REPO);
        final ClusterState withCreate = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queuedCreate), null);
        reconciliationInput.set(withCreate);

        enterRepositoryLoop();
        final ClusterStateUpdateTask failTask = failPendingRepoTasksTask();
        failTask.execute(withCreate);

        // Second invocation of the same instance, with nothing left to retain.
        final ClusterState empty = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(), null);
        final ClusterState after = failTask.execute(empty);
        failTask.clusterStateProcessed(FAIL_TASKS_SOURCE, empty, after);

        assertFalse(
            "an execute that retained nothing must admit no loop, whatever an earlier execute of the same task retained",
            reconcilingRepositories().contains(REPO)
        );
        assertTrue("nor hand an attempt to the generic pool", pendingDispatchedDrives.isEmpty());
    }

    /**
     * A publication that never commits must not turn the decision to keep a queued create into a failure. The give-up arm's
     * fallback fails every completion listener on the node, including the retained create's, while its entry stays queued
     * because nothing published -- and the debt is not cleared either, so a later pass would run that snapshot to completion
     * after its caller had been told it failed. Retrying the publication is the only answer that is not a lie.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRetainingCreatesSurvivesAPublicationFailure() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final SnapshotsInProgress.Entry queuedCreate = snapshotEntry(
            newSnapshotId("queued-create"),
            List.of(new IndexId("idx", uuid())),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final AtomicReference<Object> createOutcome = new AtomicReference<>();
        listenFor(queuedCreate.snapshot(), createOutcome);
        reconciliationOwed().add(REPO);

        final ClusterState state = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queuedCreate), null);
        reconciliationInput.set(state);
        final ClusterStateUpdateTask failTask = failPendingRepoTasksTask();
        failTask.execute(state);
        final int submitsBefore = submittedTasks.size();

        failTask.onFailure(FAIL_TASKS_SOURCE, new FailedToCommitClusterStateException("simulated publish failure"));

        assertNull("a publication that did not commit must not fail the create it retained", createOutcome.get());
        assertTrue("the publication must be retried instead", pendingSchedules.isEmpty() == false);
        assertEquals("and nothing else may be submitted in its place", submitsBefore, submittedTasks.size());
    }

    /**
     * The same for the removal of a failed delete, on the attempt past which a publication that simply failed would be given
     * up on. The removal's {@code execute} defers the queued create and records the debt, and a publication that does not
     * commit takes that record back. Whether to keep retrying has to be decided from what was owed before it was taken back:
     * when this removal recorded the only debt on the node, asking afterwards answers that nothing is owed, and the give-up
     * arm then fails the very create this removal had just left queued.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testARemovalThatRecordedTheOnlyDebtKeepsRetryingItsPublication() throws Exception {
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final SnapshotsInProgress.Entry queuedCreate = snapshotEntry(
            newSnapshotId("queued-create"),
            List.of(new IndexId("idx", uuid())),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final AtomicReference<Object> createOutcome = new AtomicReference<>();
        listenFor(queuedCreate.snapshot(), createOutcome);
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion();
        assertTrue("the premise: nothing is owed before the removal runs", reconciliationOwed().isEmpty());

        final ClusterStateUpdateTask task = snapshotsService.createRemoveSnapshotDeletionTask(
            SOURCE,
            SnapshotsService.SNAPSHOT_CLEANUP_RETRIES_SETTING.get(Settings.EMPTY),
            removed,
            new RepositoryException(REPO, "the delete did not complete"),
            RepositoryData.EMPTY.withGenId(CAPTURED_GEN),
            null,
            true
        );
        task.execute(clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queuedCreate), removed));
        assertTrue("the premise: the removal deferred the create and recorded the only debt", reconciliationOwed().contains(REPO));

        task.onFailure(SOURCE, new FailedToCommitClusterStateException("simulated publish failure"));

        assertNull("a publication that did not commit must not fail the create the removal deferred", createOutcome.get());
        assertEquals("the publication must be retried instead", 1, pendingSchedules.size());
    }

    /**
     * A removal whose publication failed, with nothing owed and the delete not given up on, arms a bounded retry. A failover this
     * node handles before that retry runs has already answered and released the delete, so the retry submits nothing.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testABoundedRemovalRetryArmedBeforeAFailoverSubmitsNothingAfterIt() throws Exception {
        final ClusterStateUpdateTask task = snapshotsService.createRemoveSnapshotDeletionTask(
            SOURCE,
            0,
            startedDeletion(),
            new RepositoryException(REPO, "the delete did not complete"),
            RepositoryData.EMPTY.withGenId(CAPTURED_GEN),
            null,
            true
        );
        assertTrue("the premise: nothing is owed, so the retry is bounded", reconciliationOwed().isEmpty());
        task.onFailure(SOURCE, new FailedToCommitClusterStateException("simulated publish failure"));
        assertEquals("the premise: the failed publication arms a retry", 1, pendingSchedules.size());

        snapshotsService.createRemoveFailedSnapshotTask(
            "remove snapshot metadata",
            0,
            new Snapshot(REPO, newSnapshotId("other")),
            new RepositoryException(REPO, "failed"),
            null,
            null
        ).onFailure("remove snapshot metadata", new NotClusterManagerException("no longer cluster manager"));
        final int submittedBefore = submittedTasks.size();
        pendingSchedules.poll().task().run();

        assertEquals("a bounded retry armed before a failover must submit nothing after it", submittedBefore, submittedTasks.size());
    }

    /**
     * A removal whose publication failed to commit can still have taken effect: a node re-elected on the state it last accepted
     * already lacks the delete. The removal's retry then finds the delete gone, here the only one, and changes nothing, answers the
     * delete once and fails nothing over.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testARemovalRetryThatFindsTheOnlyDeleteGoneAnswersItOnce() throws Exception {
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion();
        final AtomicInteger answers = new AtomicInteger();
        listenForDelete(removed.uuid(), answers);
        final ClusterState s1 = removeThenRetry(removed, clusterState(List.of(), List.of(), List.of(), removed));
        final long failoversBefore = failovers();
        final ClusterStateUpdateTask retry = submittedTasks.get(submittedTasks.size() - 1);

        assertSame("a retry that finds its delete gone must change nothing", s1, retry.execute(s1));
        retry.clusterStateProcessed(SOURCE, s1, s1);
        assertEquals("and must answer the delete once", 1, answers.get());
        assertEquals("without failing anything over", failoversBefore, failovers());
    }

    /** The same with a waiting delete the removal promoted: the retry hands the repository on, so that delete is re-driven. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testARemovalRetryThatFindsItsDeleteGoneReDrivesTheDeleteItPromoted() throws Exception {
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion();
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
        final ClusterStateUpdateTask retry = submittedTasks.get(submittedTasks.size() - 1);

        retry.clusterStateProcessed(SOURCE, s1, retry.execute(s1));
        final int handOn = submittedSources.indexOf("Run ready deletions");
        assertTrue("a retry that finds its delete gone must hand the repository on", handOn >= 0);
        final ClusterStateUpdateTask runReady = submittedTasks.get(handOn);
        runReady.clusterStateProcessed("Run ready deletions", s1, runReady.execute(s1));
        verify(repositoriesService, times(1).description("so the delete its removal promoted is re-driven")).getRepositoryData(
            anyString(),
            any()
        );
    }

    /**
     * Executes the removal of the given delete against the given state, fails its publication to commit, and runs the retry that
     * arms, which submits the removal again. Returns the state the first execution produced.
     */
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

    /**
     * A kept clone is started by the pass the fail-pending task admits for it, on this node and from the shard generation that pass
     * read, and the shard clone it starts is handed to the snapshot pool, which clones the shard from that generation.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testThePassStartsAKeptCloneFromItsRead() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId indexId = new IndexId("idx", uuid());
        final RepositoryShardId repoShardId = new RepositoryShardId(indexId, 0);
        final SnapshotId source = newSnapshotId("clone-source");
        final SnapshotsInProgress.Entry clone = cloneEntry(
            "kept-clone",
            source,
            List.of(indexId),
            Map.of(repoShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final AtomicReference<Object> cloneOutcome = new AtomicReference<>();
        listenFor(clone.snapshot(), cloneOutcome);
        reconciliationOwed().add(REPO);
        final ClusterState state = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(clone), null);
        stubLocalNode(state);
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, indexId));

        final ClusterState afterFailure = failPendingTasks(state);
        reconciliationInput.set(afterFailure);
        reconciliationOutput.set(null);
        runDispatchedReconciliation();

        final SnapshotsInProgress.ShardSnapshotStatus started = cloneStatus(reconciliationOutput.get(), clone, repoShardId);
        assertNotNull("the pass must start the kept clone from its read", started);
        assertEquals("the pass must start the kept clone from its read", SnapshotsInProgress.ShardState.INIT, started.state());
        assertEquals("the pass must start the kept clone from its read", state.nodes().getLocalNodeId(), started.nodeId());
        assertEquals("the pass must start the kept clone from its read", "gen-idx", started.generation());
        assertTrue("and discharges the debt", reconciliationOwed().isEmpty());
        assertEquals("the started shard clone must be dispatched to the snapshot pool", 1, pendingSnapshotTasks.size());
        pendingSnapshotTasks.poll().run();
        assertEquals(
            "which clones the shard from the generation the pass read",
            List.of(new CloneCall(source, clone.snapshot().getSnapshotId(), repoShardId, "gen-idx", cloneCalls.get(0).listener())),
            cloneCalls
        );
        assertNull("and its caller is still waiting", cloneOutcome.get());
        assertTrue(pendingSnapshotTasks.isEmpty());
    }

    /**
     * A kept clone queued on a shard an earlier entry of the same pass starts stays queued: the pass hands out each repository shard
     * once, in creation order, whichever kind of entry takes it.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAKeptCloneWaitsForAShardAnEarlierEntryOfThePassStarts() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId indexId = new IndexId("idx", uuid());
        final RepositoryShardId repoShardId = new RepositoryShardId(indexId, 0);
        final SnapshotsInProgress.Entry create = snapshotEntry(
            newSnapshotId("earlier-create"),
            List.of(indexId),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotsInProgress.Entry clone = cloneEntry(
            "later-clone",
            newSnapshotId("clone-source"),
            List.of(indexId),
            Map.of(repoShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        reconciliationOwed().add(REPO);
        final ClusterState state = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(create, clone), null);
        stubLocalNode(state);
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, indexId));

        final ClusterState afterFailure = failPendingTasks(state);
        reconciliationInput.set(afterFailure);
        reconciliationOutput.set(null);
        runDispatchedReconciliation();
        final ClusterState reconciled = reconciliationOutput.get();

        assertNotNull("the premise: the retained create is owed a pass", reconciled);
        assertOneActiveOperationPerShard(reconciled);
        assertEquals(
            "the earlier create takes the shard",
            SnapshotsInProgress.ShardState.INIT,
            entryFor(reconciled, create.snapshot()).shards().get(shardId).state()
        );
        final SnapshotsInProgress.ShardSnapshotStatus cloneStatus = cloneStatus(reconciled, clone, repoShardId);
        assertNotNull("and the kept clone stays queued behind it", cloneStatus);
        assertEquals("and the kept clone stays queued behind it", SnapshotsInProgress.ShardState.QUEUED, cloneStatus.state());
        assertTrue(pendingSnapshotTasks.isEmpty());
    }

    /**
     * A kept clone created before a queued create of the same shard takes the shard first, and the create is started when the clone's
     * shard completes: the order in which a completing shard hands itself on is creation order, so the pass follows it.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAKeptCloneCreatedFirstTakesTheShardFirst() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId indexId = new IndexId("idx", uuid());
        final RepositoryShardId repoShardId = new RepositoryShardId(indexId, 0);
        final SnapshotsInProgress.Entry clone = cloneEntry(
            "earlier-clone",
            newSnapshotId("clone-source"),
            List.of(indexId),
            Map.of(repoShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotsInProgress.Entry create = snapshotEntry(
            newSnapshotId("later-create"),
            List.of(indexId),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        reconciliationOwed().add(REPO);
        final ClusterState state = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(clone, create), null);
        stubLocalNode(state);
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, indexId));

        final ClusterState reconciled = reDriveFromClusterState(state);

        assertOneActiveOperationPerShard(reconciled);
        assertEquals(
            "the clone created first takes the shard first",
            SnapshotsInProgress.ShardState.INIT,
            cloneStatus(reconciled, clone, repoShardId).state()
        );
        assertEquals(
            "the clone created first takes the shard first",
            SnapshotsInProgress.ShardState.QUEUED,
            entryFor(reconciled, create.snapshot()).shards().get(shardId).state()
        );
        assertEquals("its shard clone is dispatched", 1, pendingSnapshotTasks.size());
        pendingSnapshotTasks.clear();

        final ClusterState handedOn = snapshotsService.shardStateExecutor.execute(
            reconciled,
            List.of(
                new SnapshotsService.ShardSnapshotUpdate(
                    clone.snapshot(),
                    repoShardId,
                    new SnapshotsInProgress.ShardSnapshotStatus(
                        state.nodes().getLocalNodeId(),
                        SnapshotsInProgress.ShardState.SUCCESS,
                        "gen-cloned"
                    )
                )
            )
        ).resultingState;
        assertOneActiveOperationPerShard(handedOn);
        assertEquals(
            "the create starts when the clone ahead of it completes",
            SnapshotsInProgress.ShardState.INIT,
            entryFor(handedOn, create.snapshot()).shards().get(shardId).state()
        );
        assertTrue(pendingSnapshotTasks.isEmpty());
    }

    /**
     * Characterisation. A kept clone the pass starts on a repository shard that a removed entry's shard clone is still cloning waits for
     * that clone: this node clones a repository shard one operation at a time, and the running clone's own update, once processed, is
     * what starts the next one.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAKeptCloneBehindARemovedEntrysRunningCloneStartsWhenThatCloneReports() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId indexId = new IndexId("idx", uuid());
        final RepositoryShardId repoShardId = new RepositoryShardId(indexId, 0);
        final SnapshotId source = newSnapshotId("clone-source");
        final SnapshotsInProgress.Entry running = cloneEntry(
            "running-clone",
            source,
            List.of(indexId),
            Map.of(repoShardId, new SnapshotsInProgress.ShardSnapshotStatus(uuid(), "gen-running"))
        );
        final SnapshotsInProgress.Entry kept = cloneEntry(
            "kept-clone",
            source,
            List.of(indexId),
            Map.of(repoShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        reconciliationOwed().add(REPO);
        final ClusterState state = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(running, kept), null);
        stubLocalNode(state);
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, indexId));

        // The running clone's shard clone is under way, and its answer is held.
        snapshotsService.runReadyClone(running.snapshot(), source, running.clones().get(repoShardId), repoShardId, repository, false);
        assertEquals(1, runPendingSnapshotTasks());
        assertEquals("the premise: the running clone is cloning the repository shard", 1, cloneCalls.size());

        // Its entry is failed with the repository's pending tasks, and the pass that follows starts the kept clone.
        final ClusterState afterFailure = failPendingTasks(state);
        reconciliationInput.set(afterFailure);
        reconciliationOutput.set(null);
        runDispatchedReconciliation();
        final ClusterState reconciled = reconciliationOutput.get();
        assertEquals(SnapshotsInProgress.ShardState.INIT, cloneStatus(reconciled, kept, repoShardId).state());
        assertEquals("the pass dispatches the kept clone's shard clone", 1, runPendingSnapshotTasks());
        assertEquals("which does not clone the repository shard while another clone of it is still running", 1, cloneCalls.size());

        // The running clone answers, and its update is processed.
        cloneCalls.get(0).listener().onResponse("gen-cloned");
        assertEquals("the running clone's answer submits its update", 1, submittedShardUpdates.size());
        final ShardStateSubmission submission = submittedShardUpdates.get(0);
        final ClusterState afterUpdate = snapshotsService.shardStateExecutor.execute(
            reconciled,
            List.of(submission.update())
        ).resultingState;
        submission.listener().clusterStateProcessed("update snapshot state", reconciled, afterUpdate);
        assertEquals("that update starts the kept clone's shard clone", 1, runPendingSnapshotTasks());
        assertEquals("exactly once", 2, cloneCalls.size());
        assertEquals(kept.snapshot().getSnapshotId(), cloneCalls.get(1).target());
        assertEquals("from the generation the pass read", "gen-idx", cloneCalls.get(1).generation());
        assertTrue(pendingSnapshotTasks.isEmpty());
    }

    /**
     * A clone this node is still preparing, whose shard clone list is still empty, has begun nothing and is kept like any other. One
     * whose list is empty but that no preparation on this node will ever fill in is failed, because nothing else would fill it in.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnInitializingCloneIsKeptOnlyWhileThisNodeIsPreparingIt() throws Exception {
        captureSnapshotExecutor = true;
        final IndexId indexId = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry preparing = cloneEntry(
            "preparing-clone",
            newSnapshotId("clone-source"),
            List.of(indexId),
            Map.of()
        );
        final SnapshotsInProgress.Entry unprepared = cloneEntry(
            "unprepared-clone",
            newSnapshotId("clone-source"),
            List.of(indexId),
            Map.of()
        );
        final AtomicReference<Object> preparingOutcome = new AtomicReference<>();
        final AtomicReference<Object> unpreparedOutcome = new AtomicReference<>();
        listenFor(preparing.snapshot(), preparingOutcome);
        listenFor(unprepared.snapshot(), unpreparedOutcome);
        initializingClones().add(preparing.snapshot());
        reconciliationOwed().add(REPO);
        final ClusterState state = clusterState(List.of(), List.of(), List.of(preparing, unprepared), null);

        final ClusterState after = failPendingTasks(state);

        assertNotNull("a clone this node is still preparing must be kept", entryFor(after, preparing.snapshot()));
        assertNull("a clone this node is still preparing must be kept", preparingOutcome.get());
        assertNull("an initializing clone this node is not preparing is failed", entryFor(after, unprepared.snapshot()));
        assertNotNull("an initializing clone this node is not preparing is failed", unpreparedOutcome.get());
        assertTrue(pendingSnapshotTasks.isEmpty());
    }

    /**
     * A clone prepared while a reconciliation is owed publishes every shard clone queued, and the pass then starts them. Started by its
     * own preparation a shard clone would count as begun, and a failed read of a finalization the pass waits for would fail it.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testACloneMadeReadyWhileAReconciliationIsOwedLeavesItsShardClonesQueued() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId indexId = new IndexId("idx", uuid());
        final RepositoryShardId repoShardId = new RepositoryShardId(indexId, 0);
        final SnapshotId source = newSnapshotId("clone-source");
        final SnapshotsInProgress.Entry preparing = cloneEntry("preparing-clone", source, List.of(indexId), Map.of());
        final AtomicReference<Object> outcome = new AtomicReference<>();
        listenFor(preparing.snapshot(), outcome);
        initializingClones().add(preparing.snapshot());
        reconciliationOwed().add(REPO);
        final ClusterState state = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(preparing), null);
        stubLocalNode(state);
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, indexId));

        final ClusterState afterFailure = failPendingTasks(state);
        assertNotNull("the clone being prepared is kept", entryFor(afterFailure, preparing.snapshot()));
        final ClusterState ready = prepareClone(preparing, afterFailure, source, indexId, false);

        final SnapshotsInProgress.ShardSnapshotStatus prepared = cloneStatus(ready, preparing, repoShardId);
        assertNotNull("a clone prepared while a reconciliation is owed leaves every shard clone queued", prepared);
        assertEquals(
            "a clone prepared while a reconciliation is owed leaves every shard clone queued",
            SnapshotsInProgress.ShardState.QUEUED,
            prepared.state()
        );
        assertTrue("a clone prepared while a reconciliation is owed leaves every shard clone queued", pendingSnapshotTasks.isEmpty());

        // The pass the fail-pending task admitted runs next, and starts it.
        reconciliationInput.set(ready);
        reconciliationOutput.set(null);
        runDispatchedReconciliation();
        assertEquals(
            "the pass starts the clone",
            SnapshotsInProgress.ShardState.INIT,
            cloneStatus(reconciliationOutput.get(), preparing, repoShardId).state()
        );
        assertEquals("and dispatches its shard clone", 1, pendingSnapshotTasks.size());
        pendingSnapshotTasks.clear();
        assertNull("its caller is still waiting", outcome.get());
    }

    /**
     * A clone whose source turns out to be a shallow copy is prepared as without the feature while a reconciliation is owed: it is
     * started.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAShallowCloneIsPreparedAsBeforeWhileAReconciliationIsOwed() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId indexId = new IndexId("idx", uuid());
        final RepositoryShardId repoShardId = new RepositoryShardId(indexId, 0);
        final SnapshotId source = newSnapshotId("clone-source");
        final SnapshotsInProgress.Entry preparing = cloneEntry("preparing-clone", source, List.of(indexId), Map.of());
        initializingClones().add(preparing.snapshot());
        reconciliationOwed().add(REPO);
        final ClusterState state = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(preparing), null);
        stubLocalNode(state);
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, indexId));

        final ClusterState ready = prepareClone(preparing, state, source, indexId, true);

        assertEquals("a shallow clone is started", SnapshotsInProgress.ShardState.INIT, cloneStatus(ready, preparing, repoShardId).state());
        assertEquals("and its shard clone dispatched", 1, pendingSnapshotTasks.size());
        pendingSnapshotTasks.clear();
    }

    /**
     * The removal of a given-up delete records the reconciliation debt for a clone queued behind it that has begun nothing, with no
     * queued create to record it for: the finalization read the removal can force would otherwise fail that clone.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testTheRemovalOfAGivenUpDeleteRecordsTheDebtForAQueuedClone() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId indexId = new IndexId("idx", uuid());
        final RepositoryShardId repoShardId = new RepositoryShardId(indexId, 0);
        final SnapshotId source = newSnapshotId("clone-source");
        final SnapshotsInProgress.Entry running = cloneEntry(
            "running-clone",
            source,
            List.of(indexId),
            Map.of(repoShardId, new SnapshotsInProgress.ShardSnapshotStatus(uuid(), "gen-running"))
        );
        final SnapshotsInProgress.Entry queued = cloneEntry(
            "queued-clone",
            source,
            List.of(indexId),
            Map.of(repoShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, indexId));
        final ClusterState state = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(running, queued), abandoned);
        stubLocalNode(state);

        runRemoval(state, abandoned);

        assertEquals(
            "the queued clone stays queued behind the running one",
            SnapshotsInProgress.ShardState.QUEUED,
            cloneStatus(reconciliationOutput.get(), queued, repoShardId).state()
        );
        assertTrue(pendingSnapshotTasks.isEmpty());
    }

    /**
     * A clone that has begun nothing, queued behind a running clone when a given-up delete's removal forces the finalization read of a
     * completed snapshot, is kept when that read fails: the read's failure fails that finalization alone, so the running clone is
     * not failed with it either, and neither clone's caller is answered.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testACloneQueuedBehindAFailedRecoveryReadIsKept() throws Exception {
        runCloneQueuedBehindAFailedRecoveryRead(false);
    }

    /**
     * The same, with the running clone's shard completing while the debt is owed and before the read fails. The completion does not
     * start the queued clone, which the pass starts once nothing is finalizing.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testACloneQueuedBehindAFailedRecoveryReadIsNotStartedByACompletingShard() throws Exception {
        runCloneQueuedBehindAFailedRecoveryRead(true);
    }

    private void runCloneQueuedBehindAFailedRecoveryRead(boolean runningCloneCompletesFirst) throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final IndexMetadata otherMetadata = indexMetadata("other");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final ShardId otherShardId = new ShardId(otherMetadata.getIndex(), 0);
        final String dataNodeId = uuid();
        final SnapshotsInProgress.Entry finalizing = snapshotEntry(
            newSnapshotId("finalizing"),
            List.of(new IndexId("other", uuid())),
            Map.of(
                otherShardId,
                new SnapshotsInProgress.ShardSnapshotStatus(dataNodeId, SnapshotsInProgress.ShardState.FAILED, "aborted", null)
            )
        );
        final IndexId indexId = new IndexId("idx", uuid());
        final RepositoryShardId repoShardId = new RepositoryShardId(indexId, 0);
        final SnapshotId source = newSnapshotId("clone-source");
        final SnapshotsInProgress.Entry running = cloneEntry(
            "running-clone",
            source,
            List.of(indexId),
            Map.of(repoShardId, new SnapshotsInProgress.ShardSnapshotStatus(uuid(), "gen-running"))
        );
        final SnapshotsInProgress.Entry queued = cloneEntry(
            "queued-clone",
            source,
            List.of(indexId),
            Map.of(repoShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final AtomicReference<Object> finalizingOutcome = new AtomicReference<>();
        final AtomicReference<Object> runningOutcome = new AtomicReference<>();
        final AtomicReference<Object> queuedOutcome = new AtomicReference<>();
        listenFor(finalizing.snapshot(), finalizingOutcome);
        listenFor(running.snapshot(), runningOutcome);
        listenFor(queued.snapshot(), queuedOutcome);
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, indexId));
        final ClusterState state = clusterState(
            List.of(indexMetadata, otherMetadata),
            List.of(startedPrimary(shardId, dataNodeId), startedPrimary(otherShardId, dataNodeId)),
            List.of(finalizing, running, queued),
            abandoned
        );
        stubLocalNode(state);

        // The removal, with no check of its own on the debt: whether it records one is what this test is about.
        primeRunningDelete(abandoned.uuid());
        final ClusterStateUpdateTask removal = snapshotsService.createRemoveSnapshotDeletionTask(
            SOURCE,
            0,
            abandoned,
            new RepositoryException(REPO, "the delete did not complete"),
            RepositoryData.EMPTY.withGenId(CAPTURED_GEN),
            null,
            true
        );
        final ClusterState removed = removal.execute(state);
        reconciliationInput.set(removed);
        removal.clusterStateProcessed(SOURCE, state, removed);
        runDispatchedReconciliation();
        assertEquals("the premise: the completed entry's finalization is reading the repository", 1, finalizationReads.get());

        ClusterState current = removed;
        if (runningCloneCompletesFirst) {
            current = snapshotsService.shardStateExecutor.execute(
                current,
                List.of(
                    new SnapshotsService.ShardSnapshotUpdate(
                        running.snapshot(),
                        repoShardId,
                        new SnapshotsInProgress.ShardSnapshotStatus(
                            state.nodes().getLocalNodeId(),
                            SnapshotsInProgress.ShardState.SUCCESS,
                            "gen-cloned"
                        )
                    )
                )
            ).resultingState;
        }

        final int submittedBefore = submittedSources.size();
        finalizationReadListener.get().onFailure(new RepositoryException(REPO, "the repository read did not answer"));
        assertEquals(
            "the premise: the read's failure fails that finalization alone",
            List.of("remove snapshot metadata"),
            submittedSources.subList(submittedBefore, submittedSources.size())
        );
        ClusterState afterFailure = current;
        for (int i = submittedBefore; i < submittedSources.size(); i++) {
            final ClusterStateUpdateTask task = submittedTasks.get(i);
            final ClusterState before = afterFailure;
            afterFailure = task.execute(before);
            task.clusterStateProcessed(submittedSources.get(i), before, afterFailure);
        }

        final String kept = "the clone queued behind the failed recovery read must be kept";
        final List<Snapshot> left = snapshotsOf(afterFailure).entries()
            .stream()
            .map(SnapshotsInProgress.Entry::snapshot)
            .collect(Collectors.toList());
        assertTrue(kept, left.contains(queued.snapshot()));
        assertFalse("the finalizing snapshot is removed", left.contains(finalizing.snapshot()));
        assertNotNull("and its caller is told", finalizingOutcome.get());
        assertNull(kept, queuedOutcome.get());
        assertFalse("and the running clone is not failed with the finalization", runningOutcome.get() instanceof Exception);
        assertEquals(kept, SnapshotsInProgress.ShardState.QUEUED, cloneStatus(afterFailure, queued, repoShardId).state());
    }

    /**
     * A removal of a given-up delete whose publication fails keeps retrying it when the only debt it recorded was for a clone queued
     * behind the delete. Given up on, the fallback would fail that clone, which the removal had just left queued.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testARemovalThatRecordedTheOnlyDebtForAQueuedCloneKeepsRetryingItsPublication() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId indexId = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queuedClone = cloneEntry(
            "queued-clone",
            newSnapshotId("clone-source"),
            List.of(indexId),
            Map.of(new RepositoryShardId(indexId, 0), SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final AtomicReference<Object> cloneOutcome = new AtomicReference<>();
        listenFor(queuedClone.snapshot(), cloneOutcome);
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion();

        final ClusterStateUpdateTask task = snapshotsService.createRemoveSnapshotDeletionTask(
            SOURCE,
            SnapshotsService.SNAPSHOT_CLEANUP_RETRIES_SETTING.get(Settings.EMPTY),
            removed,
            new RepositoryException(REPO, "the delete did not complete"),
            RepositoryData.EMPTY.withGenId(CAPTURED_GEN),
            null,
            true
        );
        task.execute(clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queuedClone), removed));
        task.onFailure(SOURCE, new FailedToCommitClusterStateException("simulated publish failure"));

        assertEquals("the publication must be retried instead", 1, pendingSchedules.size());
        assertNull("and the clone the removal left queued is not failed", cloneOutcome.get());
        assertTrue(pendingSnapshotTasks.isEmpty());
    }

    /**
     * A new cluster manager infers the debt for a clone that has begun nothing, as it does for a queued shard snapshot, and drives the
     * pass that starts it: whatever kept the clone on the previous cluster manager took the only record of the debt with it.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAFailoverDrivesAPassForACloneThatHasBegunNothing() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId indexId = new IndexId("idx", uuid());
        final RepositoryShardId repoShardId = new RepositoryShardId(indexId, 0);
        final SnapshotsInProgress.Entry queuedClone = cloneEntry(
            "queued-clone",
            newSnapshotId("clone-source"),
            List.of(indexId),
            Map.of(repoShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, indexId));
        final ClusterState elected = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queuedClone), null);
        stubLocalNode(elected);
        reconciliationInput.set(elected);
        reconciliationOutput.set(null);

        snapshotsService.applyClusterState(new ClusterChangedEvent("test", elected, withoutLocalClusterManager(elected)));

        assertEquals("a failover must dispatch a pass for a clone that has begun nothing", 1, pendingDispatchedDrives.size());
        runDispatchedReconciliation();
        assertEquals(
            "which starts it",
            SnapshotsInProgress.ShardState.INIT,
            cloneStatus(reconciliationOutput.get(), queuedClone, repoShardId).state()
        );
        assertEquals("and dispatches its shard clone", 1, pendingSnapshotTasks.size());
        pendingSnapshotTasks.clear();
        assertTrue("and discharges the debt", reconciliationOwed().isEmpty());
    }

    /**
     * A new cluster manager records the debt for a clone that has begun nothing behind a running delete without driving a pass, as it
     * does for a queued shard snapshot, and failing the repository's pending tasks then keeps the clone.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testACloneRecordedBehindARunningDeleteIsKeptWhenThatDeleteFails() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId indexId = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry queuedClone = cloneEntry(
            "queued-clone",
            newSnapshotId("clone-source"),
            List.of(indexId),
            Map.of(new RepositoryShardId(indexId, 0), SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final AtomicReference<Object> cloneOutcome = new AtomicReference<>();
        listenFor(queuedClone.snapshot(), cloneOutcome);
        final ClusterState withDelete = clusterState(
            indexMetadata,
            startedPrimary(shardId, uuid()),
            List.of(queuedClone),
            startedDeletion()
        );
        stubLocalNode(withDelete);

        snapshotsService.applyClusterState(new ClusterChangedEvent("test", withDelete, withoutLocalClusterManager(withDelete)));
        final boolean recorded = reconciliationOwed().contains(REPO);
        final int drivesAtTheFailover = pendingDispatchedDrives.size();

        final ClusterState after = failPendingTasks(withDelete);

        assertNotNull("a clone recorded behind a running delete must be kept", entryFor(after, queuedClone.snapshot()));
        assertNull("a clone recorded behind a running delete must be kept", cloneOutcome.get());
        assertTrue("the failover records the debt for it", recorded);
        assertEquals("without driving a pass while the delete owns the repository", 0, drivesAtTheFailover);
        assertEquals("and the fail-pending task admits one", 1, pendingDispatchedDrives.size());
        assertTrue(pendingSnapshotTasks.isEmpty());
    }

    /** A new cluster manager infers no debt for a clone that has begun: no pass could start it or change it. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAFailoverRecordsNoDebtForACloneThatHasBegun() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata a = indexMetadata("a");
        final IndexMetadata b = indexMetadata("b");
        final ShardId aShard = new ShardId(a.getIndex(), 0);
        final ShardId bShard = new ShardId(b.getIndex(), 0);
        final IndexId aId = new IndexId("a", uuid());
        final IndexId bId = new IndexId("b", uuid());
        final SnapshotsInProgress.Entry begun = cloneEntry(
            "begun-clone",
            newSnapshotId("clone-source"),
            List.of(aId, bId),
            Map.of(
                new RepositoryShardId(aId, 0),
                new SnapshotsInProgress.ShardSnapshotStatus(uuid(), "gen-a"),
                new RepositoryShardId(bId, 0),
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED
            )
        );
        final ClusterState elected = clusterState(
            List.of(a, b),
            List.of(startedPrimary(aShard, uuid()), startedPrimary(bShard, uuid())),
            List.of(begun),
            null
        );
        stubLocalNode(elected);

        snapshotsService.applyClusterState(new ClusterChangedEvent("test", elected, withoutLocalClusterManager(elected)));

        assertTrue("a clone that has begun is owed no pass after a failover", reconciliationOwed().isEmpty());
        assertTrue("a clone that has begun is owed no pass after a failover", pendingDispatchedDrives.isEmpty());
        assertTrue(pendingSnapshotTasks.isEmpty());
    }

    /**
     * With the feature off a new cluster manager records no debt and infers nothing for a queued clone; a queued clone is failed
     * even with a debt on record; and a clone's preparation starts its shard clones.
     */
    public void testAQueuedCloneIsHandledAsTheBaseHandlesItWithTheFeatureOff() throws Exception {
        assertFalse("the premise: the feature is off", FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING));
        captureSnapshotExecutor = true;
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId indexId = new IndexId("idx", uuid());
        final RepositoryShardId repoShardId = new RepositoryShardId(indexId, 0);
        final SnapshotId source = newSnapshotId("clone-source");
        final SnapshotsInProgress.Entry queuedClone = cloneEntry(
            "queued-clone",
            source,
            List.of(indexId),
            Map.of(repoShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotsInProgress.Entry anotherQueuedClone = cloneEntry(
            "another-queued-clone",
            source,
            List.of(indexId),
            Map.of(repoShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final ClusterState elected = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(anotherQueuedClone), null);
        stubLocalNode(elected);
        snapshotsService.applyClusterState(new ClusterChangedEvent("test", elected, withoutLocalClusterManager(elected)));
        assertTrue("a failover records no debt for a queued clone", reconciliationOwed().isEmpty());
        assertTrue("a failover infers nothing for a queued clone", pendingDispatchedDrives.isEmpty());
        assertTrue("a failover infers nothing for a queued clone", reconcilingRepositories().isEmpty());
        assertTrue(pendingSnapshotTasks.isEmpty());

        final AtomicReference<Object> cloneOutcome = new AtomicReference<>();
        listenFor(queuedClone.snapshot(), cloneOutcome);
        reconciliationOwed().add(REPO);
        final ClusterState state = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(queuedClone), null);
        stubLocalNode(state);

        final ClusterState after = failPendingTasks(state);
        assertNull("with the feature off a queued clone is failed", entryFor(after, queuedClone.snapshot()));
        assertNotNull("with the feature off a queued clone is failed", cloneOutcome.get());
        assertTrue("and no pass is admitted for it", pendingDispatchedDrives.isEmpty());

        final SnapshotsInProgress.Entry preparing = cloneEntry("preparing-clone", source, List.of(indexId), Map.of());
        initializingClones().add(preparing.snapshot());
        final ClusterState withPreparing = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(preparing), null);
        stubLocalNode(withPreparing);
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, indexId));
        final ClusterState ready = prepareClone(preparing, withPreparing, source, indexId, false);
        assertEquals(
            "a clone's preparation starts its shard clones",
            SnapshotsInProgress.ShardState.INIT,
            cloneStatus(ready, preparing, repoShardId).state()
        );
        assertEquals("and dispatches them", 1, pendingSnapshotTasks.size());
        pendingSnapshotTasks.clear();
        assertTrue("and nothing is dispatched for a reconciliation", pendingDispatchedDrives.isEmpty());
    }

    /** A queued clone known to be a shallow copy keeps its existing outcome while a reconciliation is owed: it is failed. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAQueuedShallowCloneKeepsItsExistingOutcome() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId indexId = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry shallowClone = cloneEntry(
            "shallow-clone",
            newSnapshotId("clone-source"),
            List.of(indexId),
            Map.of(new RepositoryShardId(indexId, 0), SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        ).withRemoteStoreIndexShallowCopy(true);
        final AtomicReference<Object> cloneOutcome = new AtomicReference<>();
        listenFor(shallowClone.snapshot(), cloneOutcome);
        reconciliationOwed().add(REPO);
        final ClusterState state = clusterState(indexMetadata, startedPrimary(shardId, uuid()), List.of(shallowClone), null);

        final ClusterState after = failPendingTasks(state);

        assertNull("a shallow clone keeps its existing outcome", entryFor(after, shallowClone.snapshot()));
        assertNotNull("a shallow clone keeps its existing outcome", cloneOutcome.get());
        assertTrue(pendingSnapshotTasks.isEmpty());
    }

    /**
     * A kept clone the pass starts is not failed by the finalization of an entry the same pass completed: that entry is finalized with
     * the data the pass read, and its failure removes it alone.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAKeptCloneThePassStartsIsNotFailedByAnotherEntrysFinalization() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final IndexMetadata otherMetadata = indexMetadata("other");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final ShardId otherShardId = new ShardId(otherMetadata.getIndex(), 0);
        final ShardId deletedIndexShardId = new ShardId(new Index("gone", uuid()), 0);
        final String dataNodeId = uuid();
        final SnapshotsInProgress.Entry completedByThePass = partialSnapshotEntry(
            newSnapshotId("completed-by-the-pass"),
            List.of(new IndexId("gone", uuid())),
            Map.of(deletedIndexShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final IndexId idx = new IndexId("idx", uuid());
        final SnapshotsInProgress.Entry startedByThePass = snapshotEntry(
            newSnapshotId("started-by-the-pass"),
            List.of(idx),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotsInProgress.Entry queuedBehindIt = snapshotEntry(
            newSnapshotId("queued-behind-it"),
            List.of(idx),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final IndexId otherId = new IndexId("other", uuid());
        final RepositoryShardId otherRepoShardId = new RepositoryShardId(otherId, 0);
        final SnapshotsInProgress.Entry keptClone = cloneEntry(
            "kept-clone",
            newSnapshotId("clone-source"),
            List.of(otherId),
            Map.of(otherRepoShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final AtomicReference<Object> completedOutcome = new AtomicReference<>();
        final AtomicReference<Object> cloneOutcome = new AtomicReference<>();
        listenFor(completedByThePass.snapshot(), completedOutcome);
        listenFor(keptClone.snapshot(), cloneOutcome);
        final SnapshotDeletionsInProgress.Entry abandoned = startedDeletion();
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, otherId));
        final ClusterState state = clusterState(
            List.of(indexMetadata, otherMetadata),
            List.of(startedPrimary(shardId, dataNodeId), startedPrimary(otherShardId, dataNodeId)),
            List.of(completedByThePass, startedByThePass, queuedBehindIt, keptClone),
            abandoned
        );
        stubLocalNode(state);

        final ClusterState reconciled = runRemovalAndReconciliation(state, abandoned);
        final SnapshotsInProgress.ShardSnapshotStatus started = cloneStatus(reconciled, keptClone, otherRepoShardId);
        assertEquals("the pass starts the kept clone", SnapshotsInProgress.ShardState.INIT, started == null ? null : started.state());
        assertEquals("and dispatches its shard clone", 1, pendingSnapshotTasks.size());
        pendingSnapshotTasks.clear();
        assertTrue("the premise: the pass completed the entry of the deleted index", entryOf(reconciled, 0).state().completed());

        // The completed entry's finalization fails, whichever repository call it is waiting on.
        final int submittedBefore = submittedSources.size();
        final RepositoryException failure = new RepositoryException(REPO, "the finalization did not complete");
        if (finalizationReadListener.get() != null) {
            finalizationReadListener.get().onFailure(failure);
        } else {
            finalizationListener.get().onFailure(failure);
        }
        assertEquals("the failure submits one task", submittedBefore + 1, submittedSources.size());
        final ClusterStateUpdateTask task = submittedTasks.get(submittedBefore);
        final ClusterState after = task.execute(reconciled);
        task.clusterStateProcessed(submittedSources.get(submittedBefore), reconciled, after);

        assertNull("a kept clone the pass started must not be failed by another entry's finalization", cloneOutcome.get());
        assertNotNull(
            "a kept clone the pass started must not be failed by another entry's finalization",
            entryFor(after, keptClone.snapshot())
        );
        assertEquals(
            "only the entry whose finalization failed leaves",
            List.of(startedByThePass.snapshot(), queuedBehindIt.snapshot(), keptClone.snapshot()),
            snapshotsOf(after).entries().stream().map(SnapshotsInProgress.Entry::snapshot).collect(Collectors.toList())
        );
        assertNotNull("and its caller is told", completedOutcome.get());
        assertTrue(pendingSnapshotTasks.isEmpty());
    }

    /**
     * A clone that has begun nothing is not started by a completing shard while a reconciliation is owed: started so it would count as
     * begun, and a failed finalization read would fail it.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testACompletingShardDoesNotStartAQueuedCloneWhileAReconciliationIsOwed() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata indexMetadata = indexMetadata("idx");
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexId indexId = new IndexId("idx", uuid());
        final RepositoryShardId repoShardId = new RepositoryShardId(indexId, 0);
        final SnapshotId source = newSnapshotId("clone-source");
        final String dataNodeId = uuid();
        final ClusterState empty = clusterState(indexMetadata, startedPrimary(shardId, dataNodeId), List.of(), null);
        final String localNodeId = empty.nodes().getLocalNodeId();
        final SnapshotsInProgress.Entry running = cloneEntry(
            "running-clone",
            source,
            List.of(indexId),
            Map.of(repoShardId, new SnapshotsInProgress.ShardSnapshotStatus(localNodeId, "gen-running"))
        );
        final SnapshotsInProgress.Entry create = snapshotEntry(
            newSnapshotId("queued-create"),
            List.of(indexId),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotsInProgress.Entry queuedClone = cloneEntry(
            "queued-clone",
            source,
            List.of(indexId),
            Map.of(repoShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final ClusterState state = withSnapshots(empty, List.of(running, create, queuedClone));
        stubLocalNode(state);

        final ClusterState freed = SnapshotsService.executeShardSnapshotUpdates(
            state,
            List.of(
                new SnapshotsService.ShardSnapshotUpdate(
                    running.snapshot(),
                    repoShardId,
                    new SnapshotsInProgress.ShardSnapshotStatus(localNodeId, SnapshotsInProgress.ShardState.SUCCESS, "gen-running-1")
                )
            ),
            repository -> true
        ).resultingState;
        assertEquals(
            "a completing shard must not start a clone that has begun nothing while a reconciliation is owed",
            SnapshotsInProgress.ShardState.QUEUED,
            cloneStatus(freed, queuedClone, repoShardId).state()
        );
        assertTrue(pendingSnapshotTasks.isEmpty());
    }

    /**
     * A kept clone queued on a shard an entry created after it has begun or finished on is left as it is, as a create the pass owes a
     * start is, and so is every later entry queued on one of its shards; the debt is kept for them.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAKeptCloneQueuedBehindALaterEntryIsLeftWithEveryEntrySharingItsShards() throws Exception {
        captureSnapshotExecutor = true;
        final IndexMetadata a = indexMetadata("a");
        final IndexMetadata c = indexMetadata("c");
        final ShardId aShard = new ShardId(a.getIndex(), 0);
        final ShardId cShard = new ShardId(c.getIndex(), 0);
        final IndexId aId = new IndexId("a", uuid());
        final IndexId cId = new IndexId("c", uuid());
        final String dataNodeId = uuid();
        final SnapshotsInProgress.Entry keptClone = cloneEntry(
            "kept-clone",
            newSnapshotId("clone-source"),
            List.of(aId),
            Map.of(new RepositoryShardId(aId, 0), SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );
        final SnapshotsInProgress.Entry laterBegun = snapshotEntry(
            newSnapshotId("later-begun"),
            List.of(aId, cId),
            Map.of(
                aShard,
                new SnapshotsInProgress.ShardSnapshotStatus(dataNodeId, SnapshotsInProgress.ShardState.SUCCESS, "gen-a-later"),
                cShard,
                new SnapshotsInProgress.ShardSnapshotStatus(dataNodeId, "gen-c")
            )
        );
        final SnapshotsInProgress.Entry latest = snapshotEntry(
            newSnapshotId("latest"),
            List.of(aId, cId),
            Map.of(
                aShard,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED,
                cShard,
                SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED
            )
        );
        reconciliationOwed().add(REPO);
        reconciliationRepositoryData.set(repositoryDataHolding(CAPTURED_GEN + 3L, aId, cId));
        final ClusterState state = clusterState(
            List.of(a, c),
            List.of(startedPrimary(aShard, dataNodeId), startedPrimary(cShard, dataNodeId)),
            List.of(keptClone, laterBegun, latest),
            null
        );
        stubLocalNode(state);

        final ClusterState left = reDriveFromClusterState(state);

        final String message = "a clone queued behind a later entry is left, with every entry sharing its shards, and the debt kept";
        assertEquals(message, keptClone.clones(), entryFor(left, keptClone.snapshot()).clones());
        assertEquals(message, latest.shards(), entryFor(left, latest.snapshot()).shards());
        assertTrue(message, reconciliationOwed().contains(REPO));
        InFlightShardSnapshotStates.forRepo(REPO, snapshotsOf(left).entries());
        assertOneActiveOperationPerShard(left);
        assertIdentifiersAreUniqueByName(left);
        assertTrue(pendingSnapshotTasks.isEmpty());
    }

    /**
     * Runs the service's own fail-pending-repository-tasks update over the given state, as the cluster state service runs it, and
     * returns the state it produced.
     */
    private ClusterState failPendingTasks(ClusterState state) throws Exception {
        enterRepositoryLoop();
        final ClusterStateUpdateTask failTask = failPendingRepoTasksTask();
        final ClusterState after = failTask.execute(state);
        failTask.clusterStateProcessed(FAIL_TASKS_SOURCE, state, after);
        return after;
    }

    /**
     * Runs a clone's preparation to its end: the source snapshot's info and index metadata are read on the snapshot pool with the
     * repository data read in between, and the shard clones are then published by a consistent state update over {@code state}.
     * Returns the state that update published.
     */
    private ClusterState prepareClone(
        SnapshotsInProgress.Entry clone,
        ClusterState state,
        SnapshotId source,
        IndexId indexId,
        boolean shallow
    ) throws Exception {
        when(repository.getSnapshotInfo(source)).thenReturn(
            new SnapshotInfo(source, List.of(indexId.getName()), List.of(), 0L, null, 1L, 1, List.of(), false, Map.of(), shallow)
        );
        when(repository.getSnapshotIndexMetaData(any(), any(), any())).thenReturn(indexMetadata(indexId.getName()));
        startCloning(clone);
        assertEquals("the source snapshot's info is read on the snapshot pool", 1, runPendingSnapshotTasks());
        finalizationReadListener.getAndSet(null).onResponse(repositoryDataHolding(CAPTURED_GEN, indexId));
        assertEquals("and its index metadata too", 1, runPendingSnapshotTasks());
        reconciliationInput.set(state);
        reconciliationOutput.set(null);
        runPendingRepositoryReads();
        assertNotNull("the preparation must publish its shard clones", reconciliationOutput.get());
        return reconciliationOutput.get();
    }

    /**
     * A clone's preparation. Reflection, because it is a private method and the path that reaches it needs a source snapshot a live
     * repository holds; the tests using it are about what its last step publishes.
     */
    @SuppressForbidden(reason = "a clone's preparation is a private method")
    private void startCloning(SnapshotsInProgress.Entry clone) throws Exception {
        final Method method = SnapshotsService.class.getDeclaredMethod("startCloning", Repository.class, SnapshotsInProgress.Entry.class);
        method.setAccessible(true);
        method.invoke(snapshotsService, repository, clone);
    }

    /** The service's record of the clones this node is preparing. */
    @SuppressForbidden(reason = "the service's record of the clones this node is preparing has no test seam")
    @SuppressWarnings("unchecked")
    private Set<Snapshot> initializingClones() throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("initializingClones");
        field.setAccessible(true);
        return (Set<Snapshot>) field.get(snapshotsService);
    }

    /** Makes the cluster service name the given state's local node as its own, which is what a shard clone reports as. */
    private void stubLocalNode(ClusterState state) {
        when(clusterService.localNode()).thenReturn(
            new DiscoveryNode(state.nodes().getLocalNodeId(), buildNewFakeTransportAddress(), Version.CURRENT)
        );
    }

    /** Runs every snapshot-pool task the service handed over, including any a task hands over in turn, and says how many ran. */
    private int runPendingSnapshotTasks() {
        int ran = 0;
        while (pendingSnapshotTasks.isEmpty() == false) {
            pendingSnapshotTasks.poll().run();
            ran++;
        }
        return ran;
    }

    /**
     * Asserts that no shard of this repository has more than one shard snapshot or shard clone running on it, by index name and shard
     * number, across every entry.
     */
    private static void assertOneActiveOperationPerShard(ClusterState state) {
        final Map<String, Integer> active = new HashMap<>();
        for (SnapshotsInProgress.Entry entry : snapshotsOf(state).entries()) {
            entry.shards().forEach((id, status) -> {
                if (status.isActive()) {
                    active.merge(id.getIndexName() + "/" + id.id(), 1, Integer::sum);
                }
            });
            entry.clones().forEach((id, status) -> {
                if (status.isActive()) {
                    active.merge(id.indexName() + "/" + id.shardId(), 1, Integer::sum);
                }
            });
        }
        active.forEach(
            (shard, count) -> assertEquals("one active operation per repository shard, but [" + shard + "] has " + count, 1, (int) count)
        );
    }

    /** A clone of this repository from the given source, holding the given shard clones. */
    private static SnapshotsInProgress.Entry cloneEntry(
        String name,
        SnapshotId source,
        List<IndexId> indices,
        Map<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> clones
    ) {
        return SnapshotsInProgress.startClone(new Snapshot(REPO, newSnapshotId(name)), source, indices, 0L, CAPTURED_GEN, Version.CURRENT)
            .withClones(clones);
    }

    /** The entry of the given snapshot, or null if the state is null or the entry has left it. */
    private static SnapshotsInProgress.Entry entryFor(ClusterState state, Snapshot snapshot) {
        return state == null ? null : snapshotsOf(state).snapshot(snapshot);
    }

    /** The status of the given clone's shard clone, or null if the clone is not in the state. */
    private static SnapshotsInProgress.ShardSnapshotStatus cloneStatus(
        ClusterState state,
        SnapshotsInProgress.Entry clone,
        RepositoryShardId shardId
    ) {
        final SnapshotsInProgress.Entry entry = entryFor(state, clone.snapshot());
        return entry == null ? null : entry.clones().get(shardId);
    }

    /** The same state holding the given snapshots in place of its own. */
    private static ClusterState withSnapshots(ClusterState state, List<SnapshotsInProgress.Entry> snapshots) {
        return ClusterState.builder(state).putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(snapshots)).build();
    }

    /** The source string the service submits its fail-pending-tasks update under. */
    private static final String FAIL_TASKS_SOURCE = "fail repo tasks for [" + REPO + "]";

    /**
     * Marks the repository as inside its operation loop, which {@code leaveRepoLoop} asserts on. The production dispatcher
     * establishes this before any task that releases the loop is submitted; a test that builds such a task directly has to do
     * the same.
     */
    @SuppressForbidden(reason = "the service's repository-loop bookkeeping has no test seam")
    @SuppressWarnings("unchecked")
    private void enterRepositoryLoop() throws Exception {
        final Field repositoryLoop = SnapshotsService.class.getDeclaredField("currentlyFinalizing");
        repositoryLoop.setAccessible(true);
        ((Set<String>) repositoryLoop.get(snapshotsService)).add(REPO);
    }

    /**
     * The service's own fail-pending-repository-tasks update.
     * <p>
     * Built by reflection, and the reason is worth stating rather than hiding: the class is a private inner class, so a test
     * cannot name it, and every production path that submits one needs a promoted delete or a finalization in flight -- a great
     * deal of arrangement whose only purpose would be to obtain an instance. The tests above are about what its {@code execute}
     * decides, not about how it comes to be submitted, and the two production call sites that do submit it are already covered
     * elsewhere. Reflection on a private member is the established answer in this suite; this is the same answer applied to a
     * constructor.
     */
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
