/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.shard;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.canmatch.CanMatchPreFilterPhase;
import org.opensearch.analytics.exec.canmatch.SortSpec;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskState;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.exec.task.TaskRunner;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.TargetResolver;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ShardSortBounds;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The pre-send veto: a shard whose whole range sits outside the top-{@code K} the coordinator holds is
 * never dispatched, and its task is settled {@code SKIPPED} so the stage still completes.
 *
 * <p>Runs against the real {@link ShardTaskRunner}, {@link PendingExecutions} and
 * {@link org.opensearch.analytics.exec.canmatch.TopNGate}; only the transport is stubbed, and the stub
 * follows the production admission sequence (take a permit, ask the veto, send only if it says yes).
 * The permit hand-off is most of what can break here, so it is deliberately real.
 *
 * <p>Lives in the {@code shard} package because the gate accessors are package-private.
 */
public class SortVetoDispatchTests extends OpenSearchTestCase {

    private static final String SORT_COLUMN = "@timestamp";

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    /**
     * The shards the gate rules out never reach the wire and are settled {@code SKIPPED} — a request
     * that was never sent has no transport callback, so an unsettled task would hang the stage.
     */
    public void testEliminatedShardsAreNeverSentAndAreSettledSkipped() {
        Fixture f = new Fixture(1, 3, bounds(millis(10, 20), millis(90, 100), millis(10, 20)));
        f.armWithBar(50L);

        f.dispatchAll();

        assertEquals("only the shard that can still contribute was sent", List.of(1), f.sent);
        assertEquals(StageTaskState.SKIPPED, f.taskState(0));
        assertEquals(StageTaskState.SKIPPED, f.taskState(2));

        f.complete(1);

        assertEquals("a part-skipped stage succeeds", StageExecution.State.SUCCEEDED, f.exec.getState());
        assertNull("skipping is not a failure", f.exec.getFailure());
    }

    /** A shard whose max exactly ties the bar may hold a tying row, so it is still dispatched. */
    public void testShardTyingTheBarIsStillDispatched() {
        Fixture f = new Fixture(1, 2, bounds(millis(10, 50), millis(10, 49)));
        f.armWithBar(50L);

        f.dispatchAll();

        assertEquals("max == bar is kept; max < bar is dropped", List.of(0), f.sent);
        assertEquals(StageTaskState.SKIPPED, f.taskState(1));
    }

    /** Fewer than {@code K} keys collected means there is no bar yet, so every shard is still needed. */
    public void testGateWithFewerThanKKeysDispatchesEveryShard() {
        Fixture f = new Fixture(2, 2, bounds(millis(10, 20), millis(10, 20)));
        f.setupSortGate();
        f.exec.topNGate().offer(50L);   // one key against K = 2

        f.dispatchAll();

        assertFalse("setup: the gate is not armed", f.exec.topNGate().isArmed());
        assertEquals(List.of(0, 1), f.sent);
        assertNoneSkipped(f);
    }

    /** A shard the check learned nothing about is unjudgeable, so it is kept. */
    public void testShardWithoutBoundsIsDispatched() {
        Fixture f = new Fixture(1, 2, bounds(millis(10, 20), null));
        f.armWithBar(50L);

        f.dispatchAll();

        assertEquals("the shard with no statistics is kept", List.of(1), f.sent);
        assertEquals(StageTaskState.SKIPPED, f.taskState(0));
    }

    /** No bounded sort — the common shape — means no gate, and the dispatch path is untouched. */
    public void testUngatedQueryDispatchesEveryShard() {
        Fixture f = new Fixture(null, 2, bounds(millis(10, 20), millis(10, 20)));

        f.dispatchAll();

        assertNull("setup: no gate", f.exec.topNGate());
        assertEquals(List.of(0, 1), f.sent);
        assertNoneSkipped(f);
    }

    /**
     * The verdict is taken when the slot frees, not at submission. Two shards, one permit: while the
     * second waits, the first raises the bar past it — so it is dropped, though at submission it wasn't.
     */
    public void testVerdictUsesTheBarAtSlotFreeTimeNotAtSubmissionTime() {
        Fixture f = new Fixture(1, 1, bounds(millis(90, 100), millis(10, 20)));
        f.setupSortGate();

        f.dispatchAll();
        assertEquals("no keys yet, so nothing is eliminable — first sent, second queued", List.of(0), f.sent);
        assertEquals("the queued task has not been judged yet", StageTaskState.RUNNING, f.taskState(1));

        f.exec.topNGate().offer(50L);   // the first shard's rows set the bar
        f.complete(0);                    // its slot frees, and the queued task is judged now

        assertEquals("still only one send", List.of(0), f.sent);
        assertEquals(StageTaskState.SKIPPED, f.taskState(1));
        assertEquals(StageExecution.State.SUCCEEDED, f.exec.getState());
    }

    /**
     * A skip must forward its permit, not consume it: with one permit and three eliminable shards
     * queued ahead of one the gate keeps, the kept shard still gets sent. A leaked permit hangs the query.
     */
    public void testRunOfSkipsStillLetsALaterKeptShardThrough() {
        Fixture f = new Fixture(1, 1, bounds(millis(90, 100), millis(10, 20), millis(10, 20), millis(10, 20), millis(95, 200)));
        f.setupSortGate();

        f.dispatchAll();
        assertEquals("one permit, so only the first is in flight", List.of(0), f.sent);

        f.exec.topNGate().offer(50L);
        f.complete(0);

        assertEquals("three skips forwarded the permit to the shard behind them", List.of(0, 4), f.sent);
        assertEquals(StageTaskState.SKIPPED, f.taskState(1));
        assertEquals(StageTaskState.SKIPPED, f.taskState(2));
        assertEquals(StageTaskState.SKIPPED, f.taskState(3));

        f.complete(4);
        assertEquals(StageExecution.State.SUCCEEDED, f.exec.getState());
        assertNull(f.exec.getFailure());
    }

    /** Every shard eliminated: the stage succeeds on skips alone, with nothing left to wait for. */
    public void testStageWithEveryShardSkippedStillSucceeds() {
        Fixture f = new Fixture(1, 2, bounds(millis(10, 20), millis(10, 20)));
        f.armWithBar(50L);

        f.dispatchAll();

        assertTrue("nothing on the wire", f.sent.isEmpty());
        assertEquals(StageExecution.State.SUCCEEDED, f.exec.getState());
        assertNull(f.exec.getFailure());
    }

    /**
     * A veto firing after cancel must not re-settle a task cancel already settled: whichever terminal
     * lands first owns the counter decrement, or the stage's bookkeeping goes wrong.
     */
    public void testSkipDoesNotResettleATaskCancelAlreadyTook() {
        Fixture f = new Fixture(1, 1, bounds(millis(90, 100), millis(10, 20)));
        f.setupSortGate();

        f.dispatchAll();
        assertEquals(List.of(0), f.sent);

        f.exec.topNGate().offer(50L);
        f.exec.cancel("test");
        f.complete(0);   // frees the slot, so the queued task is judged and vetoed

        assertEquals("the vetoed shard is still never sent", List.of(0), f.sent);
        assertEquals("cancel settled it first, so the skip left it alone", StageTaskState.CANCELLED, f.taskState(1));
        assertEquals(StageExecution.State.CANCELLED, f.exec.getState());
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static ShardSortBounds millis(long min, long max) {
        return new ShardSortBounds(min, max, false, ShardSortBounds.VALUE_KIND_INT64_MILLIS);
    }

    /**
     * Per-shard bounds in shard order; a {@code null} entry means no statistics for that shard.
     * {@code Arrays.asList} because {@code List.of} rejects nulls.
     */
    private static List<ShardSortBounds> bounds(ShardSortBounds... perShard) {
        return Arrays.asList(perShard);
    }

    private static void assertNoneSkipped(Fixture f) {
        for (StageTask task : f.exec.tasks()) {
            assertNotSame("no task should have been skipped here", StageTaskState.SKIPPED, task.state());
        }
    }

    /**
     * One stage over N shard targets on a single node, its real runner, and a transport stub that
     * follows the production admission sequence and holds each permit until {@link Fixture#complete}.
     * All targets share one node, so {@code permitsPerNode} is the queue depth these tests drive.
     */
    private final class Fixture {

        final ShardFragmentStageExecution exec;
        /** Shard ids that reached the send path, in send order. */
        final List<Integer> sent = new ArrayList<>();

        private final SortSpec sortSpec;
        private final List<ExecutionTarget> targets = new ArrayList<>();
        private final Map<ExecutionTarget, ShardSortBounds> boundsByTarget = new IdentityHashMap<>();
        private final Map<Integer, PendingExecutions> permitByShard = new LinkedHashMap<>();
        private final Map<Integer, ActionListener<Void>> handleByShard = new LinkedHashMap<>();

        Fixture(int limit, int permitsPerNode, List<ShardSortBounds> perShardBounds) {
            this(new SortSpec(SORT_COLUMN, true, limit), permitsPerNode, perShardBounds);
        }

        Fixture(SortSpec sortSpec, int permitsPerNode, List<ShardSortBounds> perShardBounds) {
            this.sortSpec = sortSpec;

            // Built before any when(...) — a stubbing can't be nested inside another's arguments.
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getId()).thenReturn("test-node");
            for (int i = 0; i < perShardBounds.size(); i++) {
                ExecutionTarget target = new ShardExecutionTarget(node, new ShardId("idx", "_na_", i), i);
                targets.add(target);
                if (perShardBounds.get(i) != null) {
                    boundsByTarget.put(target, perShardBounds.get(i));
                }
            }
            AnalyticsQueryTask parentTask = mock(AnalyticsQueryTask.class);
            ClusterState clusterState = mock(ClusterState.class);

            // No plan alternatives → no can-match round-trip; targets are published as-is so these
            // tests can set up the gate by hand.
            Stage stage = mock(Stage.class);
            when(stage.getStageId()).thenReturn(0);
            when(stage.getSortSpec()).thenReturn(sortSpec);
            TargetResolver resolver = mock(TargetResolver.class);
            when(resolver.resolve(any(ClusterState.class), any())).thenReturn(targets);
            when(stage.getTargetResolver()).thenReturn(resolver);

            QueryContext config = mock(QueryContext.class);
            when(config.parentTask()).thenReturn(parentTask);
            when(config.maxConcurrentShardRequestsPerNode()).thenReturn(permitsPerNode);
            when(config.bufferAllocator()).thenReturn(allocator);

            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.state()).thenReturn(clusterState);

            AnalyticsSearchTransportService dispatcher = mock(AnalyticsSearchTransportService.class);
            doAnswer(invocation -> {
                FragmentExecutionRequest request = invocation.getArgument(0);
                PendingExecutions pending = invocation.getArgument(4);
                BooleanSupplier stillNeeded = invocation.getArgument(5);
                int shard = request.getShardId().id();
                pending.tryRun(() -> {
                    if (stillNeeded != null && stillNeeded.getAsBoolean() == false) {
                        return false;   // vetoed: the permit moves to the next queued task
                    }
                    sent.add(shard);
                    permitByShard.put(shard, pending);   // held until complete(shard)
                    return true;
                });
                return null;
            }).when(dispatcher).dispatchFragmentStreaming(any(), any(), any(), any(), any(), any());

            Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder = t -> new FragmentExecutionRequest(
                "test-query",
                0,
                t.shardId(),
                List.of(new FragmentExecutionRequest.PlanAlternative("test-backend", new byte[0], List.of()))
            );
            this.exec = new ShardFragmentStageExecution(stage, config, new NoopSink(), clusterService, requestBuilder, dispatcher);
        }

        /** Sets up a gate over the fixture's bounds, as the can-match completion would. */
        void setupSortGate() {
            exec.setupSortGate(sortSpec, new CanMatchPreFilterPhase.ShardCheckResult(targets, boundsByTarget));
            assertNotNull("setup: the fixture's bounds must set up a gate", exec.topNGate());
        }

        /** Sets up the gate and fills it to {@code K} with {@code bar}, making that value the bar. */
        void armWithBar(long bar) {
            setupSortGate();
            for (int i = 0; i < sortSpec.limit(); i++) {
                exec.topNGate().offer(bar);
            }
            assertTrue("setup: the gate must be armed", exec.topNGate().isArmed());
            assertEquals("setup: the bar is what the test says it is", bar, exec.topNGate().bottom());
        }

        /** Mirrors {@code QueryScheduler.scheduleStage}: publish, then run every task through the runner. */
        void dispatchAll() {
            exec.start(ActionListener.wrap(v -> {}, e -> {}));
            assertEquals("setup: the stage published one task per target", targets.size(), exec.tasks().size());
            @SuppressWarnings("unchecked")
            TaskRunner<StageTask> runner = (TaskRunner<StageTask>) exec.taskRunner();
            for (StageTask task : exec.tasks()) {
                handleByShard.put(task.id().partitionId(), handleFor(task));
                task.transitionTo(StageTaskState.RUNNING);
                runner.run(task, handleByShard.get(task.id().partitionId()));
            }
        }

        /** A shard's stream finishes: its task settles and its permit frees, popping the next queued task. */
        void complete(int shard) {
            PendingExecutions pending = permitByShard.remove(shard);
            assertNotNull("shard " + shard + " was never sent, so it holds no permit to release", pending);
            handleByShard.get(shard).onResponse(null);
            pending.finishAndRunNext();
        }

        StageTaskState taskState(int shard) {
            for (StageTask task : exec.tasks()) {
                if (task.id().partitionId() == shard) {
                    return task.state();
                }
            }
            throw new AssertionError("no task for shard " + shard);
        }

        private ActionListener<Void> handleFor(StageTask task) {
            return ActionListener.wrap(v -> {
                task.transitionTo(StageTaskState.FINISHED);
                exec.onTaskTerminal(task, null);
            }, cause -> {
                task.transitionTo(StageTaskState.FAILED);
                exec.onTaskTerminal(task, cause);
            });
        }
    }

    /** Nothing is fed on the dispatch path these tests drive — the streams never produce a batch. */
    private static final class NoopSink implements ExchangeSink {
        @Override
        public void feed(VectorSchemaRoot batch) {
            throw new AssertionError("no batch should reach the sink in a dispatch-only test");
        }

        @Override
        public void close() {}
    }
}
