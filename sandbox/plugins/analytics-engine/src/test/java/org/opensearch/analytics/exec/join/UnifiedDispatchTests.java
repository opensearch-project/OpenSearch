/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.QueryScheduler;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.exec.stage.StageMetrics;
import org.opensearch.analytics.exec.stage.StageStateListener;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link UnifiedDispatch}'s broadcast-capture phase — specifically the cancel-before-run
 * race the single general dispatcher inherited from the deleted {@code BroadcastDispatch}. The end-to-end
 * dispatch (capture → inject → shuffle promotion → transport) is covered by the cluster ITs
 * ({@code BroadcastJoinIT} / {@code GeneralSchedulerJoinIT}); this pins the listener-before-cancel-callback
 * ordering that a JVM test can assert deterministically.
 */
public class UnifiedDispatchTests extends OpenSearchTestCase {

    /**
     * Regression for the "task cancelled before the capture-phase listener is installed" race. If
     * {@link AnalyticsQueryTask#setOnCancelCallback(Runnable)} replays its callback synchronously because the
     * task was already cancelled (user cancel landed during DAG rewrite), the cancel callback drives the
     * build execution to CANCELLED. The state listener must already be wired by then — otherwise
     * {@code addStateListener} (which does not replay terminal states) never sees the transition and
     * {@code terminal.onFailure} never fires, leaving the query hung. {@link UnifiedDispatch} installs every
     * build listener BEFORE registering the cancel callback for exactly this reason.
     */
    public void testRunFailsTerminalWhenTaskCancelledBeforeRun() {
        // 1) A parent task that's already cancelled — setOnCancelCallback will replay synchronously.
        AnalyticsQueryTask cancelledTask = new AnalyticsQueryTask(/* id */ 1L,
            "transport",
            "analytics_query",
            "qid",
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        cancelledTask.cancel("user cancel during DAG rewrite");
        assertTrue("precondition: task must be cancelled", cancelledTask.isCancelled());

        // 2) A fake StageExecution that records listeners and translates cancel(...) into a CANCELLED
        // state-listener fire — the same contract AbstractStageExecution honors.
        FakeStageExecution buildExec = new FakeStageExecution(/* stageId */ 1);
        StageExecutionBuilder builder = mock(StageExecutionBuilder.class);
        when(builder.buildSubGraphWithSink(any(Stage.class), any(ExchangeSink.class), any(QueryContext.class), any())).thenReturn(
            new StageExecutionBuilder.SubGraph(buildExec, List.of(buildExec))
        );

        QueryScheduler scheduler = mock(QueryScheduler.class);
        when(scheduler.getStageExecutionBuilder()).thenReturn(builder);
        QueryContext ctx = mock(QueryContext.class);
        when(ctx.parentTask()).thenReturn(cancelledTask);
        when(ctx.queryId()).thenReturn("qid");

        // 3) A DAG with a single BROADCAST_BUILD stage under the coordinator root, so UnifiedDispatch enters
        // the capture phase (collectBuildStages finds the build) and exercises the cancel wiring.
        Stage build = newRoleStage(/* stageId */ 1, Stage.StageRole.BROADCAST_BUILD);
        Stage root = newRoleStage(/* stageId */ 2, Stage.StageRole.COORDINATOR_REDUCE, build);
        QueryDAG dag = new QueryDAG("qid", root);

        AtomicReference<Throwable> failure = new AtomicReference<>();
        ActionListener<Iterable<VectorSchemaRoot>> terminal = ActionListener.wrap(v -> {
            throw new AssertionError("expected onFailure, got onResponse");
        }, failure::set);

        UnifiedDispatch dispatch = new UnifiedDispatch(
            scheduler,
            mock(ClusterService.class),
            mock(CapabilityRegistry.class),
            /* preferMetadataDriver */ false,
            /* sortMergeJoinMinRows */ Long.MAX_VALUE
        );
        dispatch.run(ctx, dag, buildStage -> mock(ExchangeSink.class), /* queryExecutionSink */ null, terminal);

        assertNotNull(
            "terminal.onFailure must fire when the task was cancelled before the capture phase installed its "
                + "callbacks; otherwise the query would hang",
            failure.get()
        );
        // The capture phase observes the already-cancelled task and bails before scheduling any leaf — the
        // build's leaves are never dispatched.
        verify(scheduler, never()).scheduleStage(any());
        assertFalse("build leaf must not start for an already-cancelled task", buildExec.startInvoked);
    }

    private static Stage newRoleStage(int stageId, Stage.StageRole role, Stage... children) {
        Stage stage = new Stage(stageId, null, List.of(children), null, null, null);
        stage.setRole(role);
        return stage;
    }

    /**
     * Regression for cancellation landing BETWEEN capture waves (nested broadcast). The capture-phase cancel
     * callback is installed once, by the first wave, and reads {@code activeRoots} so it can cancel whichever
     * wave is in flight. But there is a window: after wave N's builds have all SUCCEEDED and its
     * {@code isCancelled()} check has passed, and before wave N+1 publishes its own roots, {@code activeRoots}
     * still holds wave N's roots — which are terminal. {@code AbstractStageExecution#cancel} no-ops on a
     * terminal state (its {@code transitionTo} returns false), so the callback fires no listener. Wave N+1 then
     * observes the cancellation in its own {@code isCancelled()} branch and returns without scheduling — and
     * without cancelling its fresh roots or failing the terminal. Nothing completes the query: it hangs.
     */
    public void testRunFailsTerminalWhenTaskCancelledBetweenWaves() {
        AnalyticsQueryTask task = new AnalyticsQueryTask(/* id */ 1L, "transport", "analytics_query", "qid", TaskId.EMPTY_TASK_ID, Map.of()
        );

        FakeStageExecution innerExec = new FakeStageExecution(/* stageId */ 0);
        FakeStageExecution outerExec = new FakeStageExecution(/* stageId */ 1);

        StageExecutionBuilder builder = mock(StageExecutionBuilder.class);
        AtomicInteger buildCalls = new AtomicInteger();
        when(builder.buildSubGraphWithSink(any(Stage.class), any(ExchangeSink.class), any(QueryContext.class), any())).thenAnswer(
            invocation -> {
                if (buildCalls.getAndIncrement() == 0) {
                    return new StageExecutionBuilder.SubGraph(innerExec, List.of(innerExec));
                }
                // Wave 1 is being built. Cancel NOW: wave 0 has already succeeded and passed its
                // isCancelled() check, but wave 1 has not yet published its roots, so the one-shot
                // callback still points at wave 0's terminal roots and cancels nothing.
                task.cancel("user cancel between capture waves");
                return new StageExecutionBuilder.SubGraph(outerExec, List.of(outerExec));
            }
        );

        QueryScheduler scheduler = mock(QueryScheduler.class);
        when(scheduler.getStageExecutionBuilder()).thenReturn(builder);
        QueryContext ctx = mock(QueryContext.class);
        when(ctx.parentTask()).thenReturn(task);
        when(ctx.queryId()).thenReturn("qid");

        // Nested broadcast: build 1's own build is build 0, so collectBuildWaves yields two waves,
        // innermost (build 0) first.
        Stage innerBuild = newRoleStage(/* stageId */ 0, Stage.StageRole.BROADCAST_BUILD);
        Stage outerBuild = newRoleStage(/* stageId */ 1, Stage.StageRole.BROADCAST_BUILD, innerBuild);
        Stage root = newRoleStage(/* stageId */ 2, Stage.StageRole.COORDINATOR_REDUCE, outerBuild);
        QueryDAG dag = new QueryDAG("qid", root);

        AtomicReference<Throwable> failure = new AtomicReference<>();
        ActionListener<Iterable<VectorSchemaRoot>> terminal = ActionListener.wrap(v -> {
            throw new AssertionError("expected onFailure, got onResponse");
        }, failure::set);

        UnifiedDispatch dispatch = new UnifiedDispatch(
            scheduler,
            mock(ClusterService.class),
            mock(CapabilityRegistry.class),
            /* preferMetadataDriver */ false,
            /* sortMergeJoinMinRows */ Long.MAX_VALUE
        );
        dispatch.run(ctx, dag, buildStage -> new FakeCaptureSink(), /* queryExecutionSink */ null, terminal);

        // Drive wave 0 to SUCCEEDED. Its listener advances to wave 1, whose subgraph construction cancels
        // the task in the window described above.
        innerExec.succeed();

        assertTrue("precondition: wave 1 must have been reached", buildCalls.get() >= 2);
        assertNotNull(
            "terminal must complete when cancellation lands between capture waves; otherwise the query "
                + "hangs — the one-shot cancel callback still points at wave 0's terminal roots, and wave 1 "
                + "returns from its isCancelled() branch without cancelling its own roots or failing",
            failure.get()
        );
    }

    /** Capture-sink stand-in exposing the {@code ipcBytesFuture()} that {@code extractIpcBytes} reflects on. */
    static final class FakeCaptureSink implements ExchangeSink {
        @Override
        public void feed(VectorSchemaRoot batch) {}

        @Override
        public void close() {}

        public CompletableFuture<byte[]> ipcBytesFuture() {
            return CompletableFuture.completedFuture(new byte[] { 1, 2, 3 });
        }
    }

    /**
     * Hand-rolled StageExecution stand-in. Captures listeners; cancel(...) fires CANCELLED on any listeners
     * present at cancel-time — the same contract AbstractStageExecution honors. Listeners attached AFTER
     * cancel see nothing, which is the bug surface this test guards.
     */
    private static final class FakeStageExecution implements StageExecution {
        private final int stageId;
        private final List<StageStateListener> listeners = new ArrayList<>();
        private volatile State state = State.CREATED;
        private volatile boolean startInvoked = false;

        FakeStageExecution(int stageId) {
            this.stageId = stageId;
        }

        @Override
        public int getStageId() {
            return stageId;
        }

        @Override
        public State getState() {
            return state;
        }

        @Override
        public StageMetrics getMetrics() {
            return null;
        }

        @Override
        public void start(ActionListener<Void> onStarted) {
            startInvoked = true;
            // Synchronous fake: materialisation completes inline, so notify immediately (the
            // contract real stages honour — deferred stages notify on their async completion).
            onStarted.onResponse(null);
        }

        @Override
        public synchronized void addStateListener(StageStateListener listener) {
            listeners.add(listener);
        }

        @Override
        public Exception getFailure() {
            return null;
        }

        @Override
        public synchronized void cancel(String reason) {
            if (isTerminal(state)) {
                return;
            }
            State previous = state;
            state = State.CANCELLED;
            for (StageStateListener l : listeners) {
                l.onStateChange(previous, State.CANCELLED);
            }
        }

        /** Drives SUCCEEDED, firing listeners — the transition the capture phase keys its wave advance off. */
        synchronized void succeed() {
            if (isTerminal(state)) {
                return;
            }
            State previous = state;
            state = State.SUCCEEDED;
            for (StageStateListener l : List.copyOf(listeners)) {
                l.onStateChange(previous, State.SUCCEEDED);
            }
        }

        private static boolean isTerminal(State s) {
            return s == State.SUCCEEDED || s == State.FAILED || s == State.CANCELLED;
        }
    }
}
