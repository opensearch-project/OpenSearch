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
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
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

import static org.opensearch.repositories.blobstore.BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY;
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

/**
 * Unit tests for what {@code SnapshotsService}'s delete-removal cluster state update hands to the work it promotes out
 * of the queue: another queued delete, or a queued snapshot finalization.
 * <p>
 * The proposition under test is a single one. The removal task carries the {@link RepositoryData} that belonged to the
 * delete it is removing. On the success path that data is what the repository returned, so it is current and may be
 * handed on. On the failure path it is what was read <em>before</em> the failed pass ran, and a pass that failed after
 * committing a new repository generation leaves that data superseded -- so the promoted work must read the repository
 * afresh instead. Both arms of both promotion paths are covered here, because an implementation that re-reads
 * unconditionally is as wrong as one that never re-reads: it changes the healthy path for every cluster.
 * <p>
 * The re-read is gated on the snapshot resilience feature flag, because a delete is only ever given up on while that flag
 * is enabled, and re-reading on every ordinary delete failure would change what a cluster running default settings does.
 * The failure-path tests below therefore lock the flag on. One further test pins the other side of that gate: with the flag
 * at its default the failure path promotes from the captured data, as without the feature; the success path carries no gate.
 * <p>
 * The delete is driven through the package-private task factory rather than through a live cluster so that the
 * generation reaching the repository is directly observable. Two pieces of the service's bookkeeping have to be primed
 * by reflection first -- see {@link #primeRunningDelete} -- because the task asserts on both and neither has a test
 * seam.
 */
public class PromotedSnapshotDeleteTests extends OpenSearchTestCase {

    private static final String REPO = "test-repo";

    /** The source string the production dispatcher submits this task under. */
    private static final String SOURCE = "remove snapshot deletion metadata";

    /**
     * The generation of the {@link RepositoryData} the removal task carries -- what the pass that just ended saw. Every
     * other generation in these tests is derived from it and deliberately different, so that the value reaching the
     * repository names its own source.
     */
    private static final long CAPTURED_GEN = 5L;

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private RepositoriesService repositoriesService;
    private Repository repository;
    private SnapshotsService snapshotsService;

    /** Repository reads issued by the promoted <em>delete</em>, which routes through {@link RepositoriesService}. */
    private final AtomicInteger promotedDeleteReads = new AtomicInteger();

    /** Repository reads issued by the promoted <em>finalization</em>, which routes through {@link Repository}. */
    private final AtomicInteger promotedFinalizationReads = new AtomicInteger();

    /** The listeners of the finalization reads, in the order they were asked for, left unanswered for a test to answer. */
    private final Deque<ActionListener<RepositoryData>> finalizationReadListeners = new ArrayDeque<>();

    /** The generation the last finalization was written against. */
    private final AtomicLong finalizedGeneration = new AtomicLong(Long.MIN_VALUE);

    /** A failure the next fresh read of the repository answers with instead of {@link #freshRepositoryData}. */
    private final AtomicReference<Exception> failNextFreshRead = new AtomicReference<>();

    private final AtomicInteger deleteDispatches = new AtomicInteger();

    /** Dispatches through each of the two arms. The sum is {@link #deleteDispatches}; these say which one ran. */
    private final AtomicInteger wideDispatches = new AtomicInteger();
    private final AtomicInteger narrowDispatches = new AtomicInteger();

    /** The listener the narrow overload was handed, so a test can fail it the way an unbudgeted repository would. */
    private final AtomicReference<ActionListener<RepositoryData>> dispatchedNarrowListener = new AtomicReference<>();

    /** The entrypoint a test hands to {@link #supportAbandonment()}. Built in {@code setUp}. */
    private Repository.AbandonableSnapshotDelete abandonableEntrypoint;

    /**
     * Calls reaching the shallow-copy lock-file delete entrypoint. Counted separately from {@link #deleteDispatches}
     * because the two are different entrypoints with different contracts: one carries an attempt and one cannot.
     */
    private final AtomicInteger shallowDeleteDispatches = new AtomicInteger();

    /** The listener the lock-file entrypoint was last handed, so a test can fail it the way a repository would. */
    private final AtomicReference<ActionListener<RepositoryData>> dispatchedShallowListener = new AtomicReference<>();

    /**
     * Calls reaching the shallow-copy pinned-timestamp delete entrypoint. Separate from
     * {@link #shallowDeleteDispatches} because the shallow arm routes to one or the other on the snapshot's pinned
     * timestamp, so a test that did not distinguish them could not tell which it had reached.
     */
    private final AtomicInteger shallowPinnedDispatches = new AtomicInteger();
    private final AtomicLong dispatchedGeneration = new AtomicLong(Long.MIN_VALUE);

    /** Source strings and tasks handed to {@link ClusterService#submitStateUpdateTask}, in order. */
    private final List<String> submittedSources = new ArrayList<>();
    private final List<ClusterStateUpdateTask> submittedTasks = new ArrayList<>();

    /** What a fresh repository read answers with. Set per scenario. */
    private final AtomicReference<RepositoryData> freshRepositoryData = new AtomicReference<>();

    /** The attempt the dispatched delete was actually handed, so a test asserts on the repository's own instance. */
    private final AtomicReference<SnapshotDeletionAttempt> dispatchedAttempt = new AtomicReference<>();

    /** The listener the dispatched delete was handed, so a test can fail it the way a repository would. */
    private final AtomicReference<ActionListener<RepositoryData>> dispatchedListener = new AtomicReference<>();

    /** The snapshots the abandonment-observing entrypoint was asked to delete. */
    private final AtomicReference<List<SnapshotId>> dispatchedSnapshotIds = new AtomicReference<>();

    /**
     * Timers armed on the generic pool and their delays. Captured rather than run, so that "a budget was armed" is an
     * assertion on a queue and expiry is something a test triggers deliberately instead of waiting for. Nothing here
     * advances a clock; the captured runnable <em>is</em> the expiry.
     */
    private final Deque<Runnable> pendingTimers = new ArrayDeque<>();

    /**
     * Set to make the next generic-pool schedule throw, which is what a node on its way down does. One shot, so a successor
     * the rejection arm should not have armed would be captured and asserted on rather than rejected in its turn.
     */
    private final AtomicBoolean rejectNextTimerSchedule = new AtomicBoolean();
    private final AtomicReference<TimeValue> lastTimerDelay = new AtomicReference<>();

    /**
     * Whether the dispatched attempt already reported abandonment at the moment each cluster state update was submitted.
     * Recorded at submit time because the ordering is the point: the update that removes the delete entry is what releases
     * the snapshots queued behind it, and they are started from a fresh read, so the repository must already be refusing
     * new destructive work by then. A test that only checked both facts at the end could not tell the order apart.
     */
    private final List<Boolean> abandonedAtSubmit = new ArrayList<>();

    /** What the intercepted schedule hands back. Cancelling a timer this suite never started is a no-op by construction. */
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

        repository = mock(Repository.class);
        when(repository.getMetadata()).thenReturn(new RepositoryMetadata(REPO, "mock", Settings.EMPTY));
        // Records the dispatch and then leaves the listener unanswered, which is what a repository call that is still
        // working looks like. Answering it would drive a second removal task and obscure the one observation wanted.
        // The abandonment-observing entrypoint this repository offers when a test says it does. Held in a field so a test can
        // stub the accessor with it, and so the identity a decorator would forward is stable.
        abandonableEntrypoint = (snapshotIds, repositoryStateId, repositoryMetaVersion, deletion, listener) -> {
            deleteDispatches.incrementAndGet();
            wideDispatches.incrementAndGet();
            dispatchedGeneration.set(repositoryStateId);
            dispatchedSnapshotIds.set(List.copyOf(snapshotIds));
            dispatchedAttempt.set(deletion);
            dispatchedListener.set(listener);
        };
        // The narrow overload, which is what an unsupported repository is dispatched through. Counted separately so a test can
        // assert WHICH arm ran, and into the shared total so the tests that only care that a delete was dispatched still do.
        doAnswer(invocation -> {
            deleteDispatches.incrementAndGet();
            narrowDispatches.incrementAndGet();
            dispatchedGeneration.set(invocation.<Long>getArgument(1));
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
        // Recorded and left unanswered, like the wide overload above. This is the entrypoint the shallow-copy arm reaches
        // for a snapshot with no pinned timestamp, and a test that never reaches it cannot assert anything about budgets.
        doAnswer(invocation -> {
            shallowDeleteDispatches.incrementAndGet();
            dispatchedGeneration.set(invocation.<Long>getArgument(1));
            dispatchedShallowListener.set(invocation.getArgument(4));
            return null;
        }).when(repository).deleteSnapshotsAndReleaseLockFiles(any(), anyLong(), any(), any(), any());
        // The other shallow-copy entrypoint.
        doAnswer(invocation -> {
            shallowPinnedDispatches.incrementAndGet();
            return null;
        }).when(repository).deleteSnapshotsWithPinnedTimestamp(any(), anyLong(), any(), any(), any(), any());

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

        // Generic-pool schedules are the repository I/O budget timers and are captured rather than run, so that arming is
        // observable and expiry is deliberate. Everything on any other pool is delegated untouched, so a schedule this
        // suite has no business swallowing still behaves exactly as it did.
        final ThreadPool interceptedThreadPool = spy(threadPool);
        doAnswer(invocation -> {
            if (ThreadPool.Names.GENERIC.equals(invocation.getArgument(2)) == false) {
                return invocation.callRealMethod();
            }
            if (rejectNextTimerSchedule.compareAndSet(true, false)) {
                throw new OpenSearchRejectedExecutionException("the generic pool is shutting down");
            }
            lastTimerDelay.set(invocation.getArgument(1));
            pendingTimers.add(invocation.getArgument(0));
            // A real handle, not null: the wrapper stores what schedule returns and cancels it when the call answers, so a
            // null here turns an answered budgeted delete into a NullPointerException instead of an assertion.
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

    /**
     * Three generations are in play and all three differ: the data the task carries is at {@code CAPTURED_GEN}, the
     * queued entry recorded {@code CAPTURED_GEN + 2}, and the repository actually holds {@code CAPTURED_GEN + 4}
     * because the failed pass committed before it stopped. Only the last of the three may reach the repository, so the
     * assertion names its own source.
     * <p>
     * No timeout and no abandonment appear anywhere in this test, which is the point. The promoted delete's exposure to
     * a superseded generation follows from the delete having failed at all, not from how it failed, so the unit stands
     * on its own merits and is testable without any of the timeout machinery.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testFailedDeleteRereadsTheRepositoryAndDispatchesTheGenerationItRead() throws Exception {
        final SnapshotId promotedSnapshot = newSnapshotId("promoted");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(promotedSnapshot, CAPTURED_GEN + 2L);

        final long committedGen = CAPTURED_GEN + 4L;
        freshRepositoryData.set(repositoryDataWith(committedGen, promotedSnapshot));

        runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, queued)),
            SnapshotsInProgress.EMPTY,
            new RepositoryException(REPO, "the delete did not complete"),
            repositoryDataWith(CAPTURED_GEN, promotedSnapshot)
        );

        assertEquals("a failed delete must hand the promoted delete a fresh repository read", 1, promotedDeleteReads.get());
        assertEquals("the promoted delete must still be dispatched", 1, deleteDispatches.get());
        assertEquals(
            "the promoted delete must run against the generation the repository actually holds",
            committedGen,
            dispatchedGeneration.get()
        );
        // The snapshots are still present in what the read returned, so the already-applied short circuit must not be
        // taken and there is nothing for this path to publish.
        assertEquals("work that is still outstanding must be dispatched, not withdrawn", 0, submittedSources.size());
    }

    /**
     * The other side of the flag gate, and the regression lock on it.
     * <p>
     * The inputs are the failure test's inputs exactly, and only the flag differs, so the pair reads as one statement about the
     * gate rather than two statements about two scenarios. The fresh read is armed with a distinguishable generation for the
     * same reason the success tests arm one: if a read were issued anyway, the dispatch assertion below would name it.
     */
    public void testFailedDeletePromotesFromCapturedDataWithTheFeatureOff() throws Exception {
        assertFalse(FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING));

        final SnapshotId promotedSnapshot = newSnapshotId("promoted");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(promotedSnapshot, CAPTURED_GEN + 2L);

        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, promotedSnapshot));

        runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, queued)),
            SnapshotsInProgress.EMPTY,
            new RepositoryException(REPO, "the delete did not complete"),
            repositoryDataWith(CAPTURED_GEN, promotedSnapshot)
        );

        assertEquals("with the feature off a failed delete must not add a repository read", 0, promotedDeleteReads.get());
        assertEquals("the promoted delete must still be dispatched", 1, deleteDispatches.get());
        assertEquals(
            "the promoted delete must run against the data this task carried, as without the feature",
            CAPTURED_GEN,
            dispatchedGeneration.get()
        );
    }

    /**
     * Pins the success arm, which does not re-read: a re-read there would give every healthy delete queue an extra
     * repository round trip and discard data the repository had just returned.
     * <p>
     * The queued entry records a generation of its own, different from the captured data's, so an implementation that
     * ignored the captured data and passed {@code readyDeletion.repositoryStateId()} instead would not satisfy the
     * dispatch assertion. The wider {@code deleteSnapshotsFromRepository} overload asserts nothing about generation
     * equality, so the mismatch is safe to construct.
     */
    public void testSuccessfulDeleteHandsItsOwnFreshDataToThePromotedDelete() throws Exception {
        assertFalse(FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING));

        final SnapshotId promotedSnapshot = newSnapshotId("promoted");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(promotedSnapshot, CAPTURED_GEN + 2L);

        // A successful delete's listener is answered with the data the repository wrote, so this is current, and the
        // removed delete's own snapshot is absent from it -- which the success variant of the task asserts.
        final RepositoryData afterSuccessfulDelete = repositoryDataWith(CAPTURED_GEN, promotedSnapshot);
        // Deliberately distinguishable: if a read were issued anyway, the generation below would be this one.
        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, promotedSnapshot));

        runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, queued)),
            SnapshotsInProgress.EMPTY,
            null,
            afterSuccessfulDelete
        );

        assertEquals("a successful delete must not add a repository read", 0, promotedDeleteReads.get());
        assertEquals("the promoted delete must still be dispatched", 1, deleteDispatches.get());
        assertEquals(
            "the promoted delete must run against the data the completed delete returned",
            CAPTURED_GEN,
            dispatchedGeneration.get()
        );
    }

    /**
     * The only test here that pins the re-read helper specifically rather than
     * pinning "something re-read". The two overloads of {@code deleteSnapshotsFromRepository} both read the repository
     * and both dispatch the generation they read; the re-read helper differs in one further behaviour, which is this
     * one: it compares what came back against the snapshots the promoted entry wants deleted, and when they are already
     * gone it withdraws the entry from the cluster state instead of issuing a second physical delete. Neither overload
     * can reproduce that, so this is what stops the test being satisfied by calling an overload instead.
     * <p>
     * The withdrawal is asserted by running the submitted cluster state update, not by counting submissions: a counting
     * assertion could not tell this task from the {@code FailPendingRepoTasksTask} the same method submits when the read
     * itself fails, which would produce the identical read-once, dispatch-never pair. The source string is checked for
     * the same reason.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testPromotedDeleteIssuesNoSecondDeleteWhenTheAbandonedCallAlreadyApplied() throws Exception {
        final SnapshotId promotedSnapshot = newSnapshotId("promoted");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(promotedSnapshot, CAPTURED_GEN);

        // The call that was given up on reached the repository after all: its snapshots are gone from what a fresh read
        // answers with, and the repository has moved on.
        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 7L, newSnapshotId("unrelated")));

        final ClusterState afterRemoval = runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, queued)),
            SnapshotsInProgress.EMPTY,
            new RepositoryException(REPO, "the delete did not complete"),
            repositoryDataWith(CAPTURED_GEN, promotedSnapshot)
        );

        assertEquals("the promoted delete must read before deciding there is work", 1, promotedDeleteReads.get());
        assertEquals("work the abandoned call already applied must not be issued a second time", 0, deleteDispatches.get());

        assertEquals("exactly one cluster state update belongs on this path", 1, submittedSources.size());
        assertEquals("and it must be the entry's own removal, not the failed-read fallback", SOURCE, submittedSources.get(0));
        final ClusterState afterWithdrawal = submittedTasks.get(0).execute(afterRemoval);
        final SnapshotDeletionsInProgress remaining = afterWithdrawal.custom(
            SnapshotDeletionsInProgress.TYPE,
            SnapshotDeletionsInProgress.EMPTY
        );
        assertTrue(
            "the promoted entry must leave the cluster state, which is what releases its waiting listeners",
            remaining.getEntries().stream().noneMatch(entry -> entry.uuid().equals(queued.uuid()))
        );
    }

    /**
     * The re-drive's partial case. The delete being removed committed one of the snapshots the queued delete also names and
     * then failed in its cleanup, and a failed delete's removal does not prune the snapshots it committed from the entries
     * queued behind it. So the promoted entry still names a snapshot the repository no longer holds, alongside one it does.
     * Handing the repository both fails the whole delete, because a snapshot that is not there cannot be removed, and leaves
     * the surviving one in place; only the snapshots the fresh read still holds may be dispatched.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAPromotedDeleteIsDispatchedOnlyTheSnapshotsTheFreshReadStillHolds() throws Exception {
        supportAbandonment();
        final SnapshotId committed = newSnapshotId("committed");
        final SnapshotId surviving = newSnapshotId("surviving");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(committed);
        final SnapshotDeletionsInProgress.Entry queued = new SnapshotDeletionsInProgress.Entry(
            List.of(surviving, committed),
            REPO,
            0L,
            CAPTURED_GEN,
            SnapshotDeletionsInProgress.State.WAITING
        );
        // The failed delete committed the next generation, which no longer holds the snapshot it removed.
        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 1L, surviving));

        runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, queued)),
            SnapshotsInProgress.EMPTY,
            new RepositoryException(REPO, "cleanup failed"),
            repositoryDataWith(CAPTURED_GEN, committed, surviving)
        );

        assertEquals("the promoted delete must read the repository afresh", 1, promotedDeleteReads.get());
        assertEquals("and must be dispatched, because one of its snapshots is still there", 1, wideDispatches.get());
        assertEquals("against the generation the read returned", CAPTURED_GEN + 1L, dispatchedGeneration.get());
        assertEquals("with only the snapshots that read still holds", List.of(surviving), dispatchedSnapshotIds.get());
    }

    /**
     * {@code endSnapshot} takes a {@code @Nullable RepositoryData} and reads the repository itself when it is given
     * null. Two other callers rely on that: the one that finalizes
     * snapshots which completed while a delete was running, and the one that finalizes an entry after a shard-level
     * state update. So the promoted finalization is told to re-read by being handed null, and a read reaching the
     * repository is how this test observes it; answering that read then finalizes the snapshot at the generation it
     * returned.
     * <p>
     * The scenario carries a queued delete as well as the completed snapshot, so that the zero dispatches asserted
     * below is a property rather than an absence: a completed snapshot entry counts as writing to the repository, which
     * is what keeps the queued delete waiting and leaves finalization the only promoted work. That is the mutual
     * exclusion the promotion step's own assertion relies on, and with the queued entry present this test would fail on
     * an {@code AssertionError} if it stopped holding.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testFailedDeleteMakesAPromotedFinalizationReReadToo() throws Exception {
        final SnapshotId promotedSnapshot = newSnapshotId("promoted");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(newSnapshotId("queued"), CAPTURED_GEN);
        // A snapshot whose shards all reached a completed state while the delete was running. It needs no shard
        // entries: an entry with no shards is built in state SUCCESS, which is a completed state, and a completed
        // entry for this repository is what puts the task on its promoted-finalization branch.
        final SnapshotsInProgress.Entry completedSnapshot = completedSnapshotEntry(promotedSnapshot);

        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, promotedSnapshot));

        runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, queued)),
            SnapshotsInProgress.of(List.of(completedSnapshot)),
            new RepositoryException(REPO, "the delete did not complete"),
            repositoryDataWith(CAPTURED_GEN, promotedSnapshot)
        );

        // Handing the captured data on instead would take the other branch of endSnapshot, finalize straight away and
        // read nothing. The finalize stub is what stays unanswered: finalization must not proceed past it in a test that
        // owns no repository.
        assertEquals("a promoted finalization must be told to re-read by being handed null", 1, promotedFinalizationReads.get());
        finalizationReadListeners.poll().onResponse(repositoryDataWith(CAPTURED_GEN + 4L));
        assertEquals("and is finalized from that read", CAPTURED_GEN + 4L, finalizedGeneration.get());
        assertEquals("the queued delete must stay waiting behind the finalization", 0, deleteDispatches.get());
    }

    /**
     * The finalization arm's other direction, and the reason the class's central claim is about both arms rather than
     * one. If the promotion step regressed to handing {@code endSnapshot} a bare null, every healthy delete queue that
     * promotes a finalization would gain a repository round trip and discard data the repository had just returned --
     * the defect the delete arm's success test guards against, on the finalization arm.
     * <p>
     * Zero is discriminating here: given non-null data {@code endSnapshot} goes straight to {@code finalizeSnapshotEntry},
     * which issues no repository read at all.
     */
    public void testSuccessfulDeleteMakesThePromotedFinalizationIssueNoRepositoryRead() throws Exception {
        assertFalse(FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING));

        final SnapshotId promotedSnapshot = newSnapshotId("promoted");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(newSnapshotId("queued"), CAPTURED_GEN);
        final SnapshotsInProgress.Entry completedSnapshot = completedSnapshotEntry(promotedSnapshot);

        // Deliberately distinguishable: if a read were issued anyway, the counter below would move.
        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, promotedSnapshot));

        runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, queued)),
            SnapshotsInProgress.of(List.of(completedSnapshot)),
            null,
            // Empty rather than merely current: the success variant asserts the removed delete's snapshots are gone
            // from the data it carries.
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

    /**
     * The load-bearing test. A repository that implements only the narrower delete overload cannot observe the attempt --
     * the interface default hands its implementation every argument except that one -- so budgeting the wait against it
     * would answer the caller, release the snapshots queued behind the delete and start them from a fresh read, while the
     * delete went on rewriting the repository from the view that read replaced. The mock here reports no support, which is
     * the same answer the interface default gives such an implementation.
     * <p>
     * Removing the capability check at the dispatch site must fail this test: a timer would be armed, and every assertion
     * below describes something that only an expiry can do.
     * <p>
     * The repository then fails the call itself with {@link OpenSearchTimeoutException}, which is shared vocabulary, not a
     * signal: any repository may raise one from its own request deadline. Recognising expiry by testing the failure for that
     * type would record an unbudgeted call as given up on, and a given-up delete is not joined by a request for the same
     * snapshots and keeps its removal retrying. Expiry is recognised structurally instead: only the wrapper this service built
     * runs the hook that records it, and an unsupported repository is handed no wrapper at all.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testNoBudgetIsArmedAgainstARepositoryThatCannotObserveAbandonment() throws Exception {
        assertTrue(
            "the premise of this test: a repository offering no abandonment-observing entrypoint",
            repository.abandonableSnapshotDelete().isEmpty()
        );
        final SnapshotId promotedSnapshot = newSnapshotId("promoted");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(promotedSnapshot, CAPTURED_GEN);
        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, promotedSnapshot));

        runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, queued)),
            SnapshotsInProgress.EMPTY,
            new RepositoryException(REPO, "the delete did not complete"),
            repositoryDataWith(CAPTURED_GEN, promotedSnapshot)
        );

        assertEquals("the delete must go through the narrow overload, as without the feature", 1, narrowDispatches.get());
        assertEquals("and not through the abandonment-observing entrypoint", 0, wideDispatches.get());
        assertNull("no attempt may be created for a repository that cannot observe one", dispatchedAttempt.get());
        assertTrue("no budget may be armed against a repository that will not stop for it", pendingTimers.isEmpty());
        assertTrue(
            "and the promoted delete holds the ownership claim it took on dispatch -- this suite's only check of that",
            runningDeletions().contains(queued.uuid())
        );
        assertTrue(
            "with no cluster state update at all: the entry removal that releases queued work is an expiry's doing",
            submittedSources.isEmpty()
        );

        dispatchedNarrowListener.get().onFailure(new OpenSearchTimeoutException("the repository gave up on its own request"));
        assertTrue("an unbudgeted call failing with the expiry's type must not be recorded as given up on", abandonedDeletes().isEmpty());
        assertEquals(List.of(SOURCE), submittedSources);
    }

    /**
     * The other side of the gate. Against a repository that does observe the attempt the budget is armed, and on expiry the
     * attempt is abandoned <em>before</em> the update that removes the delete entry is submitted -- that update is what
     * releases the snapshots queued behind the delete, and they are started from a fresh read of the repository, so the
     * ordering is the safety property rather than an incidental one.
     * <p>
     * The attempt asserted on is the instance the repository was handed, not one this test made, because handing over a
     * different object would satisfy any assertion about abandonment while leaving the repository holding a live attempt.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testASupportedRepositoryIsBudgetedAndAbandonedBeforeQueuedWorkIsReleased() throws Exception {
        supportAbandonment();
        final SnapshotId promotedSnapshot = newSnapshotId("promoted");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(promotedSnapshot, CAPTURED_GEN);
        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, promotedSnapshot));

        runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, queued)),
            SnapshotsInProgress.EMPTY,
            new RepositoryException(REPO, "the delete did not complete"),
            repositoryDataWith(CAPTURED_GEN, promotedSnapshot)
        );

        assertEquals("a supported repository must have its wait budgeted", 1, pendingTimers.size());
        assertEquals("at the configured budget, not some other delay", snapshotsService.repositoryIoTimeout(), lastTimerDelay.get());
        assertFalse("and the attempt is live until that budget expires", dispatchedAttempt.get().isAbandoned());

        pendingTimers.poll().run();

        assertTrue("expiry must declare the repository's own attempt abandoned", dispatchedAttempt.get().isAbandoned());
        assertEquals("expiry removes the delete entry, and that is the only update it may submit", 1, submittedSources.size());
        assertEquals(SOURCE, submittedSources.get(0));
        assertTrue(
            "and the attempt must already be abandoned when that update is submitted, because it is what releases the"
                + " snapshots queued behind this delete",
            abandonedAtSubmit.get(0)
        );
    }

    /**
     * With the flag off the delete takes the narrow overload with no budget, whatever the repository reports. The capability is set
     * to the answer that <em>would</em> arm a budget, so that the test turns on the flag gate alone; a repository reporting no
     * support could not arm one either way, which is why that combination is not a separate test.
     */
    public void testNoBudgetIsArmedWithTheFeatureOffEvenWhenSupported() throws Exception {
        assertFalse(FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING));
        supportAbandonment();
        final SnapshotId promotedSnapshot = newSnapshotId("promoted");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(promotedSnapshot, CAPTURED_GEN);
        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, promotedSnapshot));

        runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, queued)),
            SnapshotsInProgress.EMPTY,
            new RepositoryException(REPO, "the delete did not complete"),
            repositoryDataWith(CAPTURED_GEN, promotedSnapshot)
        );

        assertEquals("with the feature off the delete takes the narrow overload", 1, narrowDispatches.get());
        assertEquals("and the entrypoint this repository does offer is not used at all", 0, wideDispatches.get());
        assertNull("nor is an attempt created, which the single-call-site form could not express", dispatchedAttempt.get());
        assertTrue("and nothing may be armed, because the feature is off", pendingTimers.isEmpty());
    }

    /**
     * The capability says nothing about the shallow-copy entrypoints, and must not be spent on them. Those take no attempt
     * at all, so there is nothing for a repository to observe on those paths and no budget belongs on one.
     * <p>
     * The test has to <em>reach</em> a shallow-copy delete for its budget assertion to mean anything, which is why
     * {@code getSnapshotInfo} is stubbed. Left unstubbed the mock answers null, the shallow arm throws inside its own
     * try, its {@code catch} swallows that, and neither shallow entrypoint is called -- so "no budget was armed" would
     * hold because nothing was reached, and a budget on either shallow listener would leave the test green. The
     * dispatch counter below is what distinguishes the two, and a pinned timestamp of zero is what routes the snapshot
     * to the lock-file entrypoint rather than the pinned-timestamp one.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testASupportedRepositoryIsNotBudgetedOnTheShallowCopyPath() throws Exception {
        supportAbandonment();
        when(repository.getMetadata()).thenReturn(
            new RepositoryMetadata(REPO, "mock", Settings.builder().put(REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true).build())
        );
        final SnapshotId promotedSnapshot = newSnapshotId("promoted");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(promotedSnapshot, CAPTURED_GEN);
        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, promotedSnapshot));
        // A real SnapshotInfo, because the class is final and cannot be mocked. Only its pinned timestamp is read
        // here, and the four-argument constructor leaves it at zero, which is what routes the snapshot to the
        // lock-file entrypoint rather than the pinned-timestamp one. Nothing else about it is asserted on.
        when(repository.getSnapshotInfo(promotedSnapshot)).thenReturn(
            new SnapshotInfo(promotedSnapshot, List.of(), List.of(), SnapshotState.SUCCESS)
        );

        runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, queued)),
            SnapshotsInProgress.EMPTY,
            new RepositoryException(REPO, "the delete did not complete"),
            repositoryDataWith(CAPTURED_GEN, promotedSnapshot)
        );

        assertEquals("the test has to reach a shallow-copy delete to be saying anything", 1, shallowDeleteDispatches.get());
        assertEquals("a shallow-copy delete does not go through the overload that carries an attempt", 0, deleteDispatches.get());
        assertTrue("and gets no budget, whatever the repository reports about the full-copy path", pendingTimers.isEmpty());
    }

    /**
     * Primes the bookkeeping, builds the removal task through the package-private factory, runs its {@code execute}
     * against the given cluster state and then its {@code clusterStateProcessed} against the result -- which is the
     * order and the pair of arguments the cluster state service uses.
     *
     * @return the state {@code execute} produced, which is also what the promotion step saw
     */
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
            // Set explicitly and not left to a default: the readiness computation this task performs reads this custom
            // without one and asserts it is present.
            .putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress)
            .putCustom(SnapshotDeletionsInProgress.TYPE, deletions)
            .build();

        // The value both full-copy arms pass.
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

    /**
     * The delete claims the service is holding. Read directly for the same reason {@link #primeRunningDelete} writes it
     * directly: the field is private, on a private nested class whose type a test cannot name.
     * <p>
     * Kept even though a dispatch count of one already entails membership -- the dispatch sits inside
     * {@code startDeletion}, which is the add. Implied is not observed: this is the only place in this file that reads
     * the claim at all, so deleting it would leave nothing watching the property.
     */
    @SuppressForbidden(reason = "the service's running-delete bookkeeping has no test seam")
    @SuppressWarnings("unchecked")
    private Set<String> runningDeletions() throws Exception {
        final Field operations = SnapshotsService.class.getDeclaredField("repositoryOperations");
        operations.setAccessible(true);
        final Object repositoryOperations = operations.get(snapshotsService);
        final Field running = repositoryOperations.getClass().getDeclaredField("runningDeletions");
        running.setAccessible(true);
        return (Set<String>) running.get(repositoryOperations);
    }

    /**
     * Marks the delete about to be removed as running against the repository, and the repository as inside its
     * operation loop. Both are what the production dispatcher would have established before this task was ever
     * submitted, and the task asserts on both: its completion asserts that it released a claim that was held, and its
     * promoted-finalization branch asserts that it left a loop that was entered.
     * <p>
     * Reflection because neither has a seam a test can use: {@code currentlyFinalizing} is a private field, and
     * {@code runningDeletions} is a private field of a private nested class whose type a test cannot even name. No
     * production entry point reachable from here establishes either without also doing repository work. Widening those
     * two fields, or their nested class, would remove this method and the suppression with it.
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

    private static SnapshotId newSnapshotId(String name) {
        return new SnapshotId(name, UUIDs.randomBase64UUID());
    }

    /**
     * The pinned-timestamp twin of the test above: a non-zero pinned timestamp routes the shallow arm to {@code
     * deleteSnapshotsWithPinnedTimestamp}, which the test above cannot reach. Without both, "the shallow-copy paths are unbudgeted"
     * is half a claim.
     * <p>
     * Same guard against vacuity: the dispatch counter is what distinguishes "no budget was attached" from "nothing ran".
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testASupportedRepositoryIsNotBudgetedOnTheShallowPinnedTimestampPath() throws Exception {
        supportAbandonment();
        when(repository.getMetadata()).thenReturn(
            new RepositoryMetadata(REPO, "mock", Settings.builder().put(REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true).build())
        );
        final SnapshotId promotedSnapshot = newSnapshotId("promoted");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(promotedSnapshot, CAPTURED_GEN);
        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, promotedSnapshot));
        // A NON-zero pinned timestamp, which is the whole difference from the test above: it is what routes the snapshot to
        // the pinned-timestamp entrypoint instead of the lock-file one.
        when(repository.getSnapshotInfo(promotedSnapshot)).thenReturn(
            new SnapshotInfo(promotedSnapshot, List.of(), List.of(), 0L, null, 0L, 0, List.of(), null, Map.of(), null, 4321L)
        );

        runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, queued)),
            SnapshotsInProgress.EMPTY,
            new RepositoryException(REPO, "the delete did not complete"),
            repositoryDataWith(CAPTURED_GEN, promotedSnapshot)
        );

        assertEquals("the test has to reach the pinned-timestamp delete to be saying anything", 1, shallowPinnedDispatches.get());
        assertEquals("and not the lock-file one", 0, shallowDeleteDispatches.get());
        assertEquals("a shallow-copy delete does not go through the attempt-carrying entrypoint", 0, wideDispatches.get());
        assertTrue("and gets no budget, whatever the repository reports about the full-copy path", pendingTimers.isEmpty());
    }

    /**
     * A shallow-copy delete that fails keeps the continuation it has without the feature. The shallow-copy entrypoints take no
     * attempt and are never budgeted, so the removal of a failed one hands the delete queued behind it the data it carries, as
     * without the feature, and does not read the repository afresh first.
     * <p>
     * The first removal is a successful one, so that the delete under test is dispatched through the lock-file entrypoint with
     * the data that removal carried. The test then fails that dispatch the way the repository would and runs the removal the
     * failure submits.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAFailedShallowCopyDeletePromotesTheQueuedDeleteFromTheDataItCarried() throws Exception {
        when(repository.getMetadata()).thenReturn(
            new RepositoryMetadata(REPO, "mock", Settings.builder().put(REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true).build())
        );
        final SnapshotId failingSnapshot = newSnapshotId("failing");
        final SnapshotId promotedSnapshot = newSnapshotId("promoted");
        // Pinned timestamps of zero, which route both deletes to the lock-file entrypoint.
        when(repository.getSnapshotInfo(failingSnapshot)).thenReturn(
            new SnapshotInfo(failingSnapshot, List.of(), List.of(), SnapshotState.SUCCESS)
        );
        when(repository.getSnapshotInfo(promotedSnapshot)).thenReturn(
            new SnapshotInfo(promotedSnapshot, List.of(), List.of(), SnapshotState.SUCCESS)
        );
        final SnapshotDeletionsInProgress.Entry first = startedDeletion(newSnapshotId("first"));
        final SnapshotDeletionsInProgress.Entry failing = waitingDeletion(failingSnapshot, CAPTURED_GEN);
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(promotedSnapshot, CAPTURED_GEN);
        final RepositoryData carried = repositoryDataWith(CAPTURED_GEN, failingSnapshot, promotedSnapshot);
        // Distinguishable: a fresh read, if one were issued, would dispatch this generation instead.
        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, failingSnapshot, promotedSnapshot));

        final ClusterState afterFirst = runRemoval(
            first,
            SnapshotDeletionsInProgress.of(List.of(first, failing)),
            SnapshotsInProgress.EMPTY,
            null,
            carried
        );
        assertEquals("the premise: the delete under test went through the lock-file entrypoint", 1, shallowDeleteDispatches.get());
        final SnapshotDeletionsInProgress.Entry started = afterFirst.<SnapshotDeletionsInProgress>custom(SnapshotDeletionsInProgress.TYPE)
            .getEntries()
            .get(0);
        assertEquals("the premise: the entry now running is the delete under test", failing.uuid(), started.uuid());

        dispatchedShallowListener.get().onFailure(new RepositoryException(REPO, "the shallow-copy delete did not complete"));
        assertEquals("its failure submits exactly its own removal", List.of(SOURCE), submittedSources);
        final ClusterState current = ClusterState.builder(afterFirst)
            .putCustom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.of(List.of(started, queued)))
            .build();
        final ClusterStateUpdateTask removal = submittedTasks.get(0);
        final ClusterState newState = removal.execute(current);
        removal.clusterStateProcessed(SOURCE, current, newState);

        assertEquals("the removal of a failed shallow-copy delete must not read the repository afresh", 0, promotedDeleteReads.get());
        assertEquals("the queued delete must be dispatched, through the lock-file entrypoint", 2, shallowDeleteDispatches.get());
        assertEquals("against the generation of the data the removal carried", CAPTURED_GEN, dispatchedGeneration.get());
    }

    /**
     * A budgeted repository that answers with the expiry's own exception type, before the budget expires. The timer then fires
     * and must do nothing: the wrapper is already resolved, so its one-shot guard bars the hook, and nothing may be recorded
     * as given up on for a delete the repository itself answered.
     * <p>
     * This is the test that makes the wrapper's single done-flag load-bearing rather than incidental. Re-deriving expiry from
     * the failure type reddens it: the type is shared vocabulary and the repository just used it.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testABudgetedRepositoryAnsweringWithATimeoutIsNotAbandonedByTheTimer() throws Exception {
        supportAbandonment();
        final SnapshotId promotedSnapshot = newSnapshotId("promoted");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(promotedSnapshot, CAPTURED_GEN);
        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, promotedSnapshot));

        runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, queued)),
            SnapshotsInProgress.EMPTY,
            new RepositoryException(REPO, "the delete did not complete"),
            repositoryDataWith(CAPTURED_GEN, promotedSnapshot)
        );
        assertEquals("the premise: this delete was budgeted", 1, pendingTimers.size());

        // The repository answers first, with the very type the budget's own expiry uses.
        dispatchedListener.get().onFailure(new OpenSearchTimeoutException("the repository gave up on its own request"));
        // And only then does the timer fire.
        pendingTimers.poll().run();

        assertFalse("a timer that lost the race must not abandon an answered attempt", dispatchedAttempt.get().isAbandoned());
        assertTrue("and no delete may be recorded as given up on", abandonedDeletes().isEmpty());
        assertEquals("the entry is removed exactly once, by the repository's own failure", 1, submittedSources.size());
        assertEquals(SOURCE, submittedSources.get(0));
    }

    /**
     * A budget that could not be armed at all, because the pool refused the schedule. The wrapper degrades to the caller's own
     * listener, so the delete still runs -- unbudgeted is the flag-off behaviour and is not a failure -- but an unbudgeted call must
     * never be recorded as given up on, and there is no timer that could do it.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testARejectedScheduleLeavesTheDeleteUnbudgetedAndUnabandoned() throws Exception {
        supportAbandonment();
        rejectNextTimerSchedule.set(true);
        final SnapshotId promotedSnapshot = newSnapshotId("promoted");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(promotedSnapshot, CAPTURED_GEN);
        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, promotedSnapshot));

        runRemoval(
            removed,
            SnapshotDeletionsInProgress.of(List.of(removed, queued)),
            SnapshotsInProgress.EMPTY,
            new RepositoryException(REPO, "the delete did not complete"),
            repositoryDataWith(CAPTURED_GEN, promotedSnapshot)
        );

        assertEquals("the delete must still have been dispatched through the observing entrypoint", 1, wideDispatches.get());
        assertTrue("no timer may have been armed, because the schedule was refused", pendingTimers.isEmpty());
        assertFalse("and nothing may be abandoned without a budget to expire", dispatchedAttempt.get().isAbandoned());
        assertTrue("nor recorded as given up on", abandonedDeletes().isEmpty());
    }

    /**
     * A budgeted delete whose generation committed before its budget expired is answered with success when the budget expires,
     * with a warning that its cleanup did not finish, and its removal distrusts the data it carries: the delete it promotes
     * reads the repository afresh.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testACommittedDeleteWhoseBudgetExpiresIsAnsweredWithSuccessAndAWarning() throws Exception {
        final BudgetedDelete budgeted = budgetedDelete();
        final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
        assertTrue(attempt.claimCommit());
        attempt.committed(repositoryDataWith(CAPTURED_GEN + 5L, budgeted.remaining));

        pendingTimers.poll().run();

        final ClusterState after = runSubmittedRemoval(budgeted.state);
        assertEquals("the delete must be answered with success", SUCCESS, budgeted.outcome.get());
        assertEquals(
            "the success removal must take the deleted snapshot out of the waiting delete",
            List.of(budgeted.remaining),
            deletionsOf(after).getEntries().get(0).getSnapshots()
        );
        assertEquals(
            "and must distrust the data it carries, so the delete it promotes reads the repository",
            budgeted.readsBefore + 1,
            promotedDeleteReads.get()
        );
        assertWarnings(budgeted.warning());
    }

    /** A budgeted delete that fails after its generation committed is answered with success and a warning, not a failure. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testACommittedDeleteThatFailsAfterItsCommitIsAnsweredWithSuccess() throws Exception {
        final BudgetedDelete budgeted = budgetedDelete();
        final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
        assertTrue(attempt.claimCommit());
        attempt.committed(repositoryDataWith(CAPTURED_GEN + 5L, budgeted.remaining));

        dispatchedListener.get().onFailure(new RepositoryException(REPO, "a step after the commit failed"));

        runSubmittedRemoval(budgeted.state);
        assertEquals("a delete whose generation committed must be answered with success", SUCCESS, budgeted.outcome.get());
        assertEquals("and its removal must distrust the data it carries", budgeted.readsBefore + 1, promotedDeleteReads.get());
        assertWarnings(budgeted.warning());
    }

    /** A budget that expires while the commit is in flight answers nothing until the commit's outcome is known. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnExpiryWhileTheCommitIsInFlightWaitsForTheCommit() throws Exception {
        final BudgetedDelete budgeted = budgetedDelete();
        final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
        assertTrue(attempt.claimCommit());

        pendingTimers.poll().run();
        assertTrue("nothing may be submitted while the commit is in flight", submittedTasks.isEmpty());
        assertNull("and the delete must not be answered yet", budgeted.outcome.get());

        attempt.committed(repositoryDataWith(CAPTURED_GEN + 5L, budgeted.remaining));
        runSubmittedRemoval(budgeted.state);
        assertEquals("once the commit took effect the delete is answered with success", SUCCESS, budgeted.outcome.get());
        assertWarnings(budgeted.warning());
    }

    /** The same, when the commit in flight does not take effect: the delete fails with the commit's own failure. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnExpiryWhileTheCommitIsInFlightFailsWithTheCommitsFailure() throws Exception {
        final BudgetedDelete budgeted = budgetedDelete();
        final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
        assertTrue(attempt.claimCommit());

        pendingTimers.poll().run();
        assertTrue("nothing may be submitted while the commit is in flight", submittedTasks.isEmpty());

        final Exception publication = new RepositoryException(REPO, "the commit was not published");
        attempt.commitUnconfirmed(publication);
        runSubmittedRemoval(budgeted.state);
        assertSame("the delete must fail with the commit's own failure, not a timeout", publication, budgeted.outcome.get());
    }

    /**
     * A committed delete whose removal cannot be published because this node stopped being cluster manager is still answered
     * with success, without a warning header.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testACommittedDeleteIsAnsweredWithSuccessWhenItsRemovalLosesTheClusterManager() throws Exception {
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

    /** The same when an unrelated update loses the cluster manager while the committed delete is still running. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testACommittedDeleteIsAnsweredWithSuccessWhenAnotherUpdateLosesTheClusterManager() throws Exception {
        final BudgetedDelete budgeted = budgetedDelete();
        final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
        assertTrue(attempt.claimCommit());
        attempt.committed(repositoryDataWith(CAPTURED_GEN + 5L, budgeted.remaining));

        final ClusterStateUpdateTask unrelated = snapshotsService.createRemoveSnapshotDeletionTask(
            SOURCE,
            0,
            startedDeletion(newSnapshotId("unrelated")),
            new RepositoryException(REPO, "an unrelated delete failed"),
            RepositoryData.EMPTY,
            null,
            true
        );
        unrelated.onFailure(SOURCE, new NotClusterManagerException("no longer cluster manager"));
        assertEquals("a delete whose generation committed must be answered with success", SUCCESS, budgeted.outcome.get());
    }

    /** The same when a failed repository read fails every pending operation of the repository. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testACommittedDeleteIsAnsweredWithSuccessWhenThePendingOperationsOfItsRepositoryAreFailed() throws Exception {
        final BudgetedDelete budgeted = budgetedDelete();
        final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
        assertTrue(attempt.claimCommit());
        attempt.committed(repositoryDataWith(CAPTURED_GEN + 5L, budgeted.remaining));

        final ClusterStateUpdateTask failPending = failPendingRepoTasksTask();
        final ClusterState after = failPending.execute(budgeted.state);
        // Applied before the update's own callback runs, as its publication applies it on this node first.
        snapshotsService.applyClusterState(new ClusterChangedEvent(SOURCE, after, budgeted.state));
        failPending.clusterStateProcessed(SOURCE, budgeted.state, after);
        assertEquals("a delete whose generation committed must be answered with success", SUCCESS, budgeted.outcome.get());
    }

    /**
     * The same when this node stopped being cluster manager and applied a state from which another cluster manager had removed
     * the delete's entry, before the delete's own removal failed because this node is no longer cluster manager.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testACommittedDeleteIsAnsweredWithSuccessAfterAnotherClusterManagerRemovedItsEntry() throws Exception {
        final BudgetedDelete budgeted = budgetedDelete();
        final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
        assertTrue(attempt.claimCommit());
        attempt.committed(repositoryDataWith(CAPTURED_GEN + 5L, budgeted.remaining));

        final SnapshotDeletionsInProgress.Entry started = deletionsOf(budgeted.state).getEntries()
            .stream()
            .filter(entry -> entry.state() == SnapshotDeletionsInProgress.State.STARTED)
            .findFirst()
            .orElseThrow();
        final ClusterState demoted = ClusterState.builder(budgeted.state)
            .nodes(DiscoveryNodes.builder(budgeted.state.nodes()).clusterManagerNodeId(null))
            .putCustom(SnapshotDeletionsInProgress.TYPE, deletionsOf(budgeted.state).withRemovedEntry(started.uuid()))
            .build();
        snapshotsService.applyClusterState(new ClusterChangedEvent(SOURCE, demoted, budgeted.state));

        pendingTimers.poll().run();
        assertEquals(1, submittedTasks.size());
        final ClusterStateUpdateTask removal = submittedTasks.remove(0);
        removal.onFailure(SOURCE, new NotClusterManagerException("no longer cluster manager"));
        assertEquals("a delete whose generation committed must be answered with success", SUCCESS, budgeted.outcome.get());
    }

    /**
     * A node removal makes a cluster manager that holds no loop for the repository re-run its started delete. With the feature
     * on, the re-run reads the repository first, and when that read no longer holds the delete's snapshots it records the
     * reconciliation debt before it removes the entry, so a create queued behind the delete stays queued for the reconciliation
     * pass rather than being started from the removal. It answers the delete as done, with no warning: its snapshots are gone.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testARerunOfAnAppliedDeleteLeavesTheCreatesBehindItForReconciliation() throws Exception {
        final Rerun rerun = rerunAfterANodeRemoval();
        assertEquals("nothing may be dispatched for snapshots the repository no longer holds", 0, deleteDispatches.get());
        final AtomicReference<Object> outcome = new AtomicReference<>();
        listenForDelete(rerun.delete.uuid(), outcome);

        final ClusterState removed = runSubmittedRemoval(rerun.processed);
        assertEquals(
            "the create queued behind the delete must stay queued",
            SnapshotsInProgress.ShardState.QUEUED,
            createOf(removed).shards().get(rerun.createShard).state()
        );
        assertTrue("the debt must be recorded for the reconciliation pass", reconciliationOwed().contains(REPO));
        // With no warning: the test framework fails a test that leaves one behind.
        assertEquals("and the delete is answered as done", SUCCESS, outcome.get());
    }

    /**
     * On election the new cluster manager re-runs a started delete from a fresh read with the feature on, and removes it
     * without dispatching anything when that read no longer holds its snapshots.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnElectedClusterManagerRerunsAStartedDeleteFromAFreshRead() throws Exception {
        final ClusterState processed = rerunAfterAnElection();
        assertEquals("the re-run must dispatch nothing", 0, deleteDispatches.get());
        assertEquals("and must remove the entry", List.of(SOURCE), submittedSources);
        assertNotNull(processed);
    }

    /** With the feature off the elected cluster manager re-runs the started delete unchanged. */
    public void testAnElectedClusterManagerRerunsAStartedDeleteAsTheBaseDoesWithTheFeatureOff() throws Exception {
        rerunAfterAnElection();
        assertEquals("the re-run must dispatch the delete", 1, deleteDispatches.get());
    }

    /** A started delete, the node state of a re-run of it, and the external-changes update that re-ran it, already processed. */
    private static final class Rerun {
        final SnapshotDeletionsInProgress.Entry delete;
        final ShardId createShard;
        final ClusterState processed;

        Rerun(SnapshotDeletionsInProgress.Entry delete, ShardId createShard, ClusterState processed) {
            this.delete = delete;
            this.createShard = createShard;
            this.processed = processed;
        }
    }

    /**
     * A data node leaves while a snapshot of another repository runs a shard on it, which is what makes the cluster manager
     * process external changes without an election, and so without the failover path that rebuilds the reconciliation debt.
     * The repository holds a started delete whose snapshot a fresh read no longer holds, and a create queued behind it.
     */
    private Rerun rerunAfterANodeRemoval() throws Exception {
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
        return new Rerun(delete, createShard, runSubmittedExternalChanges(after));
    }

    /** A cluster manager elected over a started delete whose snapshot a fresh read no longer holds. */
    private ClusterState rerunAfterAnElection() throws Exception {
        final DiscoveryNode clusterManager = newNode("cluster-manager");
        final SnapshotDeletionsInProgress.Entry delete = startedDeletion(newSnapshotId("gone"));
        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN));
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
        final ClusterState previous = ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder(state.nodes()).clusterManagerNodeId(null))
            .build();
        snapshotsService.applyClusterState(new ClusterChangedEvent("test", state, previous));
        return runSubmittedExternalChanges(state);
    }

    /** Runs the external-changes update the service submitted, as the cluster state service does. */
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
        return state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
            .entries()
            .stream()
            .filter(entry -> entry.repository().equals(REPO))
            .findFirst()
            .orElseThrow();
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

    @SuppressForbidden(reason = "the service's reconciliation debt has no test seam")
    @SuppressWarnings("unchecked")
    private Set<String> reconciliationOwed() throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("reconciliationOwed");
        field.setAccessible(true);
        return (Set<String>) field.get(snapshotsService);
    }

    /**
     * A promoted delete's own re-read failing fails that delete alone, through its own removal: nothing fails the repository's
     * other pending operations, and a create and a clone queued behind it stay queued with their callers not answered.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAPromotedDeleteWhoseReReadFailsIsFailedAlone() throws Exception {
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry promoted = waitingDeletion(newSnapshotId("promoted"), CAPTURED_GEN);
        final SnapshotsInProgress.Entry create = queuedCreate("queued-create");
        final SnapshotsInProgress.Entry clone = queuedClone("queued-clone");
        final Exception injected = new RepositoryException(REPO, "injected read failure");
        failNextFreshRead.set(injected);
        final AtomicReference<Object> promotedOutcome = new AtomicReference<>();
        listenForDelete(promoted.uuid(), promotedOutcome);

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

    /** The same for a started delete re-run by an elected cluster manager: its read failing fails that delete alone. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAReRunDeleteWhoseReadFailsIsFailedAlone() throws Exception {
        final Exception injected = new RepositoryException(REPO, "injected read failure");
        failNextFreshRead.set(injected);
        final DiscoveryNode clusterManager = newNode("cluster-manager");
        final SnapshotDeletionsInProgress.Entry delete = startedDeletion(newSnapshotId("gone"));
        final SnapshotsInProgress.Entry create = queuedCreate("queued-create");
        final ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(
                DiscoveryNodes.builder()
                    .add(clusterManager)
                    .localNodeId(clusterManager.getId())
                    .clusterManagerNodeId(clusterManager.getId())
            )
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(List.of(create)))
            .putCustom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.of(List.of(delete)))
            .build();
        final AtomicReference<Object> outcome = new AtomicReference<>();
        listenForDelete(delete.uuid(), outcome);
        snapshotsService.applyClusterState(
            new ClusterChangedEvent(
                "test",
                state,
                ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).clusterManagerNodeId(null)).build()
            )
        );
        final ClusterState processed = runSubmittedExternalChanges(state);

        assertEquals("a re-run delete whose read fails is failed alone", List.of(SOURCE), submittedSources);
        final ClusterState after = runSubmittedRemoval(processed);
        assertSame("with the read's own failure", injected, outcome.get());
        assertEquals(
            "and the create queued behind it stays",
            List.of(create.snapshot()),
            snapshotsOf(after).entries().stream().map(SnapshotsInProgress.Entry::snapshot).collect(Collectors.toList())
        );
    }

    /**
     * A re-run's read issued before a failover this node handles does not act after it: that failover answered and released
     * the delete, and the re-run of the next election removes it, once.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAReRunReadIssuedBeforeAFailoverDoesNotActAfterIt() throws Exception {
        final Deque<ActionListener<RepositoryData>> reads = new ArrayDeque<>();
        doAnswer(invocation -> {
            reads.add(invocation.getArgument(1));
            return null;
        }).when(repositoriesService).getRepositoryData(anyString(), any());
        final ClusterState state = rerunAfterAnElection();
        failOver();
        snapshotsService.applyClusterState(
            new ClusterChangedEvent(
                "test",
                state,
                ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).clusterManagerNodeId(null)).build()
            )
        );
        runSubmittedExternalChanges(state);
        assertEquals("the premise: one re-run read before the failover and one after it", 2, reads.size());

        reads.removeLast().onResponse(repositoryDataWith(CAPTURED_GEN));
        runSubmittedRemoval(state);
        reads.removeFirst().onResponse(repositoryDataWith(CAPTURED_GEN));
        assertFalse("a read issued before a failover must not remove the delete again after it", submittedSources.contains(SOURCE));
    }

    /** Each promoted finalization answers for its own read: the first read failing fails only the first finalization. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testEachPromotedFinalizationAnswersForItsOwnRead() throws Exception {
        final SnapshotsInProgress.Entry first = completedSnapshotEntry(newSnapshotId("first"));
        final SnapshotsInProgress.Entry second = completedSnapshotEntry(newSnapshotId("second"));
        final ClusterState state = runRemovalPromoting(List.of(first, second));
        assertEquals(1, promotedFinalizationReads.get());

        finalizationReadListeners.poll().onFailure(new RepositoryException(REPO, "the read did not answer"));
        assertEquals("each promoted finalization answers for its own read", List.of("remove snapshot metadata"), submittedSources);
        runSubmitted("remove snapshot metadata", state);
        assertEquals("and the next one reads for itself, once the first one's removal has published", 2, promotedFinalizationReads.get());
        finalizationReadListeners.poll().onResponse(repositoryDataWith(CAPTURED_GEN + 4L));
        assertEquals("the second is finalized with the data its own read returned", CAPTURED_GEN + 4L, finalizedGeneration.get());
    }

    /** A promotion with nothing left to finalize releases the repository. */
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
        if (submittedSources.contains("Run ready deletions")) {
            runSubmitted("Run ready deletions", state);
        }
        assertTrue("a promotion with nothing to finalize releases the repository", repositoryLoop().isEmpty());
    }

    /** A delete promoted after a finalization whose read failed, with no data to hand on, reads for itself. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testADeletePromotedWithNoDataToHandOnReadsForItself() throws Exception {
        final SnapshotId deleted = newSnapshotId("deleted");
        final SnapshotsInProgress.Entry completed = completedSnapshotEntry(newSnapshotId("completed"));
        final SnapshotDeletionsInProgress.Entry behind = waitingDeletion(deleted, CAPTURED_GEN);
        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 4L, deleted));
        final ClusterState state = runRemovalPromoting(List.of(completed), behind);
        final int readsBefore = promotedDeleteReads.get();

        finalizationReadListeners.poll().onFailure(new RepositoryException(REPO, "the read did not answer"));
        final ClusterState removedFinalization = runSubmitted("remove snapshot metadata", state);
        runSubmitted("Run ready deletions", removedFinalization);
        assertEquals("a delete promoted with no data to hand on reads for itself", readsBefore + 1, promotedDeleteReads.get());
        assertEquals("and is dispatched once", 1, deleteDispatches.get());
        assertEquals("at the generation its read returned", CAPTURED_GEN + 4L, dispatchedGeneration.get());
    }

    /**
     * Removes a failed delete whose removal promotes the given completed snapshots to finalization, with an optional delete
     * waiting behind them, and returns the state the removal published.
     */
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

    /** Runs the one submitted update with the given source against the given state, as the cluster state service does. */
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

    @SuppressForbidden(reason = "the service's repository-loop bookkeeping has no test seam")
    @SuppressWarnings("unchecked")
    private Set<String> repositoryLoop() throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("currentlyFinalizing");
        field.setAccessible(true);
        return (Set<String>) field.get(snapshotsService);
    }

    /**
     * A delete requested after this node gave up on the one it repeats is not joined to it: the request gets an entry of its own,
     * queued behind the given-up one, and the removal of the given-up delete then starts it from a fresh read, without the
     * request being answered with the other delete's outcome.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testADeleteRequestedAfterTheOneItRepeatsWasGivenUpOnGetsAnEntryOfItsOwn() throws Exception {
        final SnapshotId snapshot = newSnapshotId("repeated");
        final SnapshotDeletionsInProgress.Entry givenUp = startedDeletion(snapshot);
        abandonedDeletes().add(givenUp.uuid());
        primeRunningDelete(givenUp.uuid());
        final AtomicReference<Object> outcome = new AtomicReference<>();

        final ClusterState requested = requestDelete(givenUp, snapshot, outcome);
        final List<SnapshotDeletionsInProgress.Entry> entries = deletionsOf(requested).getEntries();
        assertEquals("a delete issued after the one it repeats timed out must not be answered with that timeout", 2, entries.size());
        final SnapshotDeletionsInProgress.Entry own = entries.get(1);
        assertNotEquals(givenUp.uuid(), own.uuid());
        assertEquals(SnapshotDeletionsInProgress.State.WAITING, own.state());

        freshRepositoryData.set(repositoryDataWith(CAPTURED_GEN + 1L, snapshot));
        final ClusterState removed = runRemoval(
            givenUp,
            deletionsOf(requested),
            SnapshotsInProgress.EMPTY,
            new OpenSearchTimeoutException("the delete timed out"),
            repositoryDataWith(CAPTURED_GEN, snapshot)
        );
        assertNull("the request is not answered with the given-up delete's outcome", outcome.get());
        assertEquals(SnapshotDeletionsInProgress.State.STARTED, deletionsOf(removed).getEntries().get(0).state());
        assertEquals("and its own delete starts from a fresh read", 1, promotedDeleteReads.get());
    }

    /** A delete requested for the snapshots of a delete this node is still waiting on joins it. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testADeleteRequestedForALiveDeleteJoinsIt() throws Exception {
        final SnapshotId snapshot = newSnapshotId("repeated");
        final SnapshotDeletionsInProgress.Entry live = startedDeletion(snapshot);
        primeRunningDelete(live.uuid());
        final ClusterState requested = requestDelete(live, snapshot, new AtomicReference<>());
        assertEquals("a request for a live delete joins it", List.of(live), deletionsOf(requested).getEntries());
    }

    /** Runs a request to delete the given snapshot against a state holding the given started delete of it. */
    private ClusterState requestDelete(SnapshotDeletionsInProgress.Entry started, SnapshotId snapshot, AtomicReference<Object> outcome)
        throws Exception {
        final String localNodeId = UUIDs.randomBase64UUID();
        final ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(DiscoveryNodes.builder().localNodeId(localNodeId).clusterManagerNodeId(localNodeId).build())
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
            .putCustom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.of(List.of(started)))
            .build();
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

    /**
     * A finalization whose own repository read exceeds its budget fails that finalization alone, with the budget's own timeout,
     * and its removal hands the repository on: the next finalization reads for itself, and the create and the delete queued
     * behind them are neither failed nor answered.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAFinalizationWhoseOwnReadTimedOutFailsOnlyThatFinalization() throws Exception {
        final Finalizing finalizing = finalizingAfterANodeRemoval();

        pendingTimers.poll().run();
        assertEquals(
            "a finalization whose own read timed out must fail only that finalization",
            List.of(REMOVE_SNAPSHOT),
            submittedSources
        );
        final ClusterState after = runSubmitted(REMOVE_SNAPSHOT, finalizing.state);
        assertEquals(
            "the finalization is failed with its read's own timeout",
            "[get repository data for [" + REPO + "]] timed out after [" + snapshotsService.repositoryIoTimeout() + "]",
            ((Exception) finalizing.firstOutcome.get()).getMessage()
        );
        assertEquals("and the next one reads for itself", 2, promotedFinalizationReads.get());
        assertNull("the create queued behind them is not answered", finalizing.createOutcome.get());
        assertNull("nor is the delete", finalizing.deleteOutcome.get());
        assertTrue(snapshotsOf(after).entries().stream().anyMatch(entry -> entry.snapshot().equals(finalizing.create.snapshot())));
        assertEquals(
            List.of(finalizing.delete.uuid()),
            deletionsOf(after).getEntries().stream().map(d -> d.uuid()).collect(Collectors.toList())
        );
    }

    /** The repository is handed on only once the removal of the timed-out finalization has published. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testTheRepositoryIsHandedOnOnlyOnceTheRemovalHasPublished() throws Exception {
        final Finalizing finalizing = finalizingAfterANodeRemoval();
        pendingTimers.poll().run();
        assertEquals("the expired read submits the finalization's own removal", List.of(REMOVE_SNAPSHOT), submittedSources);
        final int index = submittedSources.indexOf(REMOVE_SNAPSHOT);
        submittedSources.remove(index);
        final ClusterStateUpdateTask removal = submittedTasks.remove(index);

        removal.onFailure(REMOVE_SNAPSHOT, new FailedToCommitClusterStateException("simulated publish failure"));
        assertEquals("the repository is handed on only once the removal has published", 1, promotedFinalizationReads.get());
        assertFalse(submittedSources.contains("Run ready deletions"));

        pendingTimers.poll().run();
        runSubmitted(REMOVE_SNAPSHOT, finalizing.state);
        assertEquals("once it has, the next finalization reads for itself", 2, promotedFinalizationReads.get());
    }

    /** A genuine failure of a finalization's own read fails the repository's pending operations, as without the feature. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAGenuineReadFailureFailsThePendingOperationsAsBefore() throws Exception {
        finalizingAfterANodeRemoval();
        finalizationReadListeners.poll().onFailure(new RepositoryException(REPO, "the read failed"));
        assertEquals(
            "a genuine read failure fails the repository's pending tasks, as without the feature",
            List.of("fail repo tasks for [" + REPO + "]"),
            submittedSources
        );
    }

    /** A budget armed before this node failed its snapshot operations over does not act after it. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnExpiryArmedBeforeAFailoverDoesNotActAfterIt() throws Exception {
        final Finalizing finalizing = finalizingAfterANodeRemoval();
        snapshotsService.createRemoveFailedSnapshotTask(
            REMOVE_SNAPSHOT,
            0,
            new Snapshot("other-repo", newSnapshotId("other")),
            new RepositoryException("other-repo", "failed"),
            null,
            null
        ).onFailure(REMOVE_SNAPSHOT, new NotClusterManagerException("no longer cluster manager"));
        assertTrue("the premise: the failover released the repository", repositoryLoop().isEmpty());
        // A later finalization takes the repository and starts its own read.
        final SnapshotsInProgress.Entry later = completedSnapshotEntry(newSnapshotId("later"));
        final ClusterState retaken = ClusterState.builder(finalizing.state)
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(List.of(finalizing.first, later)))
            .build();
        snapshotsService.applyClusterState(
            new ClusterChangedEvent(
                "test",
                retaken,
                ClusterState.builder(retaken).nodes(DiscoveryNodes.builder(retaken.nodes()).clusterManagerNodeId(null)).build()
            )
        );
        runSubmittedExternalChanges(retaken);
        assertTrue("the premise: the later finalization holds the repository", repositoryLoop().contains(REPO));

        pendingTimers.poll().run();
        assertEquals("the expired read submits one update", 1, submittedSources.size());
        final ClusterState after = runSubmitted(submittedSources.get(0), retaken);
        assertTrue(
            "an expiry armed before a failover must not act after it",
            snapshotsOf(after).entries().stream().anyMatch(entry -> entry.snapshot().equals(finalizing.first.snapshot()))
                && repositoryLoop().contains(REPO)
                && submittedSources.contains("Run ready deletions") == false
        );
    }

    /**
     * A delete's budget armed before this node failed its snapshot operations over does not act after it, even once this node is
     * cluster manager again and has re-run the delete: the re-run's entry, claim and attempt are left alone, and the callers that
     * joined the re-run are not answered. It still records the call it was armed for as past its budget, because that call is
     * still running on this node.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testADeleteBudgetArmedBeforeAFailoverDoesNotActAfterIt() throws Exception {
        final BudgetedDelete budgeted = budgetedDelete();
        final SnapshotDeletionsInProgress.Entry started = startedDeletionIn(budgeted.state);
        failOver();
        reElect(budgeted.state);
        final SnapshotDeletionAttempt rerun = dispatchedAttempt.get();
        final AtomicReference<Object> joiner = new AtomicReference<>();
        listenForDelete(started.uuid(), joiner);

        pendingTimers.poll().run();
        assertTrue("a budget armed before a failover must not act after it", submittedTasks.isEmpty());
        assertFalse(
            "a budget armed before a failover must not record the re-run as given up on",
            abandonedDeletes().contains(started.uuid())
        );
        assertEquals(
            SnapshotDeletionsInProgress.State.STARTED,
            deletionsOf(budgeted.state).getEntries()
                .stream()
                .filter(entry -> entry.uuid().equals(started.uuid()))
                .findFirst()
                .orElseThrow()
                .state()
        );
        assertTrue("the re-run's claim is held", runningDeletions().contains(started.uuid()));
        assertFalse("and its attempt is untouched", rerun.isAbandoned());
        assertNull("and the callers that joined the re-run are not answered", joiner.get());
        verify(repositoriesService, times(1).description("a budget armed before a failover must still record its running call"))
            .callPastBudget(any(AtomicBoolean.class), eq(REPO));
        assertSame("and leaves the re-run's attempt in place", rerun, budgetedAttempts().get(started.uuid()));
    }

    /**
     * A caller that joins a delete whose live attempt a failover kept is answered with success at the next failover, if that
     * attempt has committed and no re-run has replaced it.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testADeleteThatCommitsAfterAFailoverIsAnsweredWithSuccessOnTheNextOne() throws Exception {
        final BudgetedDelete budgeted = budgetedDelete();
        final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
        final SnapshotDeletionsInProgress.Entry started = startedDeletionIn(budgeted.state);
        failOver();

        final AtomicReference<Object> joiner = new AtomicReference<>();
        listenForDelete(started.uuid(), joiner);
        assertTrue(attempt.claimCommit());
        attempt.committed(repositoryDataWith(CAPTURED_GEN + 5L, budgeted.remaining));
        failOver();

        assertEquals("a delete whose generation committed after a failover must be answered with success", SUCCESS, joiner.get());
    }

    /** A delete whose budget fires past a failover while its commit is in flight keeps no attempt once that commit fails. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnAttemptWhoseCommitFailsAfterAFencedExpiryIsNotKept() throws Exception {
        final BudgetedDelete budgeted = budgetedDelete();
        final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
        final String uuid = startedDeletionIn(budgeted.state).uuid();
        assertTrue(attempt.claimCommit());
        failOver();
        pendingTimers.poll().run();
        assertSame("the premise: a commit in flight keeps the attempt", attempt, budgetedAttempts().get(uuid));

        attempt.commitUnconfirmed(new RepositoryException(REPO, "the commit was not published"));
        assertNull("an attempt whose commit failed after a fenced expiry must not be kept", budgetedAttempts().get(uuid));
    }

    /** A delete whose budget fires past a failover before anything was committed keeps no attempt. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnAttemptThatExpiresPastAFailoverUncommittedIsNotKept() throws Exception {
        final BudgetedDelete budgeted = budgetedDelete();
        final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
        final String uuid = startedDeletionIn(budgeted.state).uuid();
        failOver();
        assertSame("the premise: the failover keeps a live attempt", attempt, budgetedAttempts().get(uuid));

        pendingTimers.poll().run();
        assertNull("an attempt that expires past a failover with nothing committed must not be kept", budgetedAttempts().get(uuid));
    }

    /** A delete whose repository call fails past a failover before anything was committed keeps no attempt. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnAttemptWhoseCallFailsPastAFailoverUncommittedIsNotKept() throws Exception {
        final BudgetedDelete budgeted = budgetedDelete();
        final SnapshotDeletionAttempt attempt = dispatchedAttempt.get();
        final String uuid = startedDeletionIn(budgeted.state).uuid();
        failOver();
        assertSame("the premise: the failover keeps a live attempt", attempt, budgetedAttempts().get(uuid));

        dispatchedListener.get().onFailure(new RepositoryException(REPO, "the delete failed"));
        assertNull("an attempt whose call failed past a failover with nothing committed must not be kept", budgetedAttempts().get(uuid));
    }

    private static SnapshotDeletionsInProgress.Entry startedDeletionIn(ClusterState state) {
        return deletionsOf(state).getEntries()
            .stream()
            .filter(entry -> entry.state() == SnapshotDeletionsInProgress.State.STARTED)
            .findFirst()
            .orElseThrow();
    }

    /** Has this node fail its snapshot operations over, as any update that finds it no longer cluster manager does. */
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

    /** Makes this node cluster manager again over the given state, which re-runs its started delete. */
    private void reElect(ClusterState state) throws Exception {
        final int dispatchesBefore = deleteDispatches.get();
        snapshotsService.applyClusterState(
            new ClusterChangedEvent(
                "test",
                state,
                ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).clusterManagerNodeId(null)).build()
            )
        );
        runSubmittedExternalChanges(state);
        assertEquals("the premise: the re-run dispatched the delete again", dispatchesBefore + 1, deleteDispatches.get());
        assertEquals("with a budget of its own", 2, pendingTimers.size());
    }

    @SuppressForbidden(reason = "the service's budgeted attempts have no test seam")
    @SuppressWarnings("unchecked")
    private Map<String, SnapshotDeletionAttempt> budgetedAttempts() throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("budgetedAttempts");
        field.setAccessible(true);
        return (Map<String, SnapshotDeletionAttempt>) field.get(snapshotsService);
    }

    private static final String REMOVE_SNAPSHOT = "remove snapshot metadata";

    /** Two completed snapshots, a queued create and a waiting delete of the repository, with the first one's read outstanding. */
    private static final class Finalizing {
        final ClusterState state;
        final SnapshotsInProgress.Entry first;
        final SnapshotsInProgress.Entry create;
        final SnapshotDeletionsInProgress.Entry delete;
        final AtomicReference<Object> firstOutcome;
        final AtomicReference<Object> createOutcome;
        final AtomicReference<Object> deleteOutcome;

        Finalizing(
            ClusterState state,
            SnapshotsInProgress.Entry first,
            SnapshotsInProgress.Entry create,
            SnapshotDeletionsInProgress.Entry delete,
            AtomicReference<Object> firstOutcome,
            AtomicReference<Object> createOutcome,
            AtomicReference<Object> deleteOutcome
        ) {
            this.state = state;
            this.first = first;
            this.create = create;
            this.delete = delete;
            this.firstOutcome = firstOutcome;
            this.createOutcome = createOutcome;
            this.deleteOutcome = deleteOutcome;
        }
    }

    /**
     * A cluster manager finalizes two completed snapshots of the repository: the first reads the repository with its budget
     * armed, and the second is queued behind it. A create and a delete of the repository wait behind them. Triggered by a data
     * node leaving while a snapshot of another repository runs a shard on it, rather than by an election, whose rebuild of the
     * reconciliation debt would start a reconciliation read, with a budget of its own, beside the one under test.
     */
    private Finalizing finalizingAfterANodeRemoval() throws Exception {
        final DiscoveryNode clusterManager = newNode("cluster-manager");
        final DiscoveryNode leaving = newNode("leaving");
        final ShardId otherShard = new ShardId(new Index("other-idx", UUIDs.randomBase64UUID()), 0);
        final ShardId otherLiveShard = new ShardId(otherShard.getIndex(), 1);
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
        final SnapshotsInProgress.Entry first = completedSnapshotEntry(newSnapshotId("first"));
        final SnapshotsInProgress.Entry second = completedSnapshotEntry(newSnapshotId("second"));
        final SnapshotsInProgress.Entry create = queuedCreate("queued-create");
        final SnapshotDeletionsInProgress.Entry delete = waitingDeletion(newSnapshotId("waiting"), CAPTURED_GEN);
        final ClusterState before = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(
                DiscoveryNodes.builder()
                    .add(clusterManager)
                    .add(leaving)
                    .localNodeId(clusterManager.getId())
                    .clusterManagerNodeId(clusterManager.getId())
            )
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(List.of(other, first, second, create)))
            .putCustom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.of(List.of(delete)))
            .build();
        final ClusterState after = ClusterState.builder(before)
            .nodes(DiscoveryNodes.builder(before.nodes()).remove(leaving.getId()))
            .build();
        final AtomicReference<Object> firstOutcome = new AtomicReference<>();
        final AtomicReference<Object> createOutcome = new AtomicReference<>();
        final AtomicReference<Object> deleteOutcome = new AtomicReference<>();
        listenForSnapshot(first.snapshot(), firstOutcome);
        listenForSnapshot(create.snapshot(), createOutcome);
        listenForDelete(delete.uuid(), deleteOutcome);
        snapshotsService.applyClusterState(new ClusterChangedEvent("test", after, before));
        final ClusterState processed = runSubmittedExternalChanges(after);
        assertEquals("the premise: the first finalization reads the repository", 1, promotedFinalizationReads.get());
        assertEquals("with its budget armed", 1, pendingTimers.size());
        return new Finalizing(processed, first, create, delete, firstOutcome, createOutcome, deleteOutcome);
    }

    @SuppressForbidden(reason = "the service's snapshot completion listeners have no test seam")
    @SuppressWarnings("unchecked")
    private void listenForSnapshot(Snapshot snapshot, AtomicReference<Object> outcome) throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("snapshotCompletionListeners");
        field.setAccessible(true);
        final Map<Snapshot, List<ActionListener<Tuple<RepositoryData, SnapshotInfo>>>> listeners = (Map<
            Snapshot,
            List<ActionListener<Tuple<RepositoryData, SnapshotInfo>>>>) field.get(snapshotsService);
        listeners.computeIfAbsent(snapshot, ignored -> new ArrayList<>()).add(ActionListener.wrap(outcome::set, outcome::set));
    }

    private static final String SUCCESS = "success";

    /** A budgeted delete of one snapshot, dispatched and still running, with a second delete waiting behind it. */
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

    /**
     * Removes a failed delete, which promotes a delete of one snapshot through the budgeted entrypoint with a delete of that
     * snapshot and another waiting behind it, and listens for the promoted delete's answer.
     */
    private BudgetedDelete budgetedDelete() throws Exception {
        supportAbandonment();
        final SnapshotId deleted = newSnapshotId("deleted");
        final SnapshotId remaining = newSnapshotId("remaining");
        final SnapshotDeletionsInProgress.Entry removed = startedDeletion(newSnapshotId("removed"));
        final SnapshotDeletionsInProgress.Entry queued = waitingDeletion(deleted, CAPTURED_GEN);
        final SnapshotDeletionsInProgress.Entry behind = new SnapshotDeletionsInProgress.Entry(
            List.of(deleted, remaining),
            REPO,
            0L,
            CAPTURED_GEN,
            SnapshotDeletionsInProgress.State.WAITING
        );
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
        final AtomicReference<Object> outcome = new AtomicReference<>();
        listenForDelete(queued.uuid(), outcome);
        return new BudgetedDelete(state, deleted, remaining, outcome, promotedDeleteReads.get());
    }

    /** Runs the one update submitted since, against the given state, as the cluster state service does. */
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
    private void listenForDelete(String deleteUuid, AtomicReference<Object> outcome) throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("snapshotDeletionListeners");
        field.setAccessible(true);
        ((Map<String, List<ActionListener<Void>>>) field.get(snapshotsService)).computeIfAbsent(deleteUuid, uuid -> new ArrayList<>())
            .add(ActionListener.wrap(ignored -> outcome.set(SUCCESS), outcome::set));
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

    /**
     * The service's record of which deletes were given up on, read directly because it is private: it decides whether a request
     * joins a running delete and whether the delete's removal keeps retrying.
     */
    @SuppressForbidden(reason = "the service's abandoned-delete record has no test seam")
    @SuppressWarnings("unchecked")
    private Set<String> abandonedDeletes() throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("abandonedDeletes");
        field.setAccessible(true);
        return (Set<String>) field.get(snapshotsService);
    }

    /**
     * Makes this repository advertise the abandonment-observing entrypoint built in {@code setUp}. A Mockito mock answers an
     * {@code Optional}-returning method with {@link Optional#empty()}, so the unsupported case is the default and needs no
     * stubbing at all -- which is also the answer the interface default gives.
     */
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

    /**
     * A queued delete. Its recorded generation is a parameter because it is a distinct source of the value that can
     * reach the repository -- {@code Entry#started()} preserves it -- and holding it apart from the captured data's
     * generation is what makes the dispatch assertions name one source each.
     */
    private static SnapshotDeletionsInProgress.Entry waitingDeletion(SnapshotId snapshotId, long recordedGeneration) {
        return new SnapshotDeletionsInProgress.Entry(
            List.of(snapshotId),
            REPO,
            0L,
            recordedGeneration,
            SnapshotDeletionsInProgress.State.WAITING
        );
    }

    /**
     * A snapshot entry for this repository in a completed state, which is what the removal task promotes to
     * finalization. An entry built with no shards is completed, since every shard it has is.
     */
    private static SnapshotsInProgress.Entry completedSnapshotEntry(SnapshotId snapshotId) {
        final SnapshotsInProgress.Entry entry = SnapshotsInProgress.startedEntry(
            new Snapshot(REPO, snapshotId),
            // No global state, so that a finalization which does run reaches the cheap metadata path and needs nothing
            // of the empty cluster state this test builds.
            false,
            false,
            Collections.emptyList(),
            Collections.emptyList(),
            0L,
            // Must not be the unknown generation: an entry carrying that is treated as aborted before it started and
            // never reaches the repository-data decision this test is about.
            CAPTURED_GEN,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Version.CURRENT,
            false
        );
        assert entry.state().completed() : "an entry with no shards must be completed for this test to mean anything";
        assert entry.repositoryStateId() != RepositoryData.UNKNOWN_REPO_GEN;
        return entry;
    }

    /** Repository data at the given generation that records the given snapshots as present. */
    private static RepositoryData repositoryDataWith(long generation, SnapshotId... snapshotIds) {
        RepositoryData data = RepositoryData.EMPTY;
        for (SnapshotId snapshotId : snapshotIds) {
            data = data.addSnapshot(snapshotId, SnapshotState.SUCCESS, Version.CURRENT, ShardGenerations.EMPTY, null, null);
        }
        return data.withGenId(generation);
    }
}
