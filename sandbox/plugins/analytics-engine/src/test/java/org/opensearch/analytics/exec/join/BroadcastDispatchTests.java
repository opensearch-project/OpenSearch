/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.QueryScheduler;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.exec.stage.StageStateListener;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.spi.BroadcastInjectionInstructionNode;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link BroadcastDispatch#enrichProbeAlternatives} — the plan-mutation step.
 * The end-to-end dispatch path (two passes, walker, transport) is verified in the
 * integration test added alongside Commit 6.
 */
public class BroadcastDispatchTests extends OpenSearchTestCase {

    public void testEnrichAppendsBroadcastInstructionPerAlternative() {
        Stage probe = newStage(/* stageId */ 7);
        StagePlan altA = new StagePlan(null, "df").withInstructions(List.of(new ShardScanInstructionNode()));
        StagePlan altB = new StagePlan(null, "df").withInstructions(List.of(new ShardScanInstructionNode()));
        probe.setPlanAlternatives(List.of(altA, altB));

        byte[] ipcBytes = new byte[] { 1, 2, 3, 4 };
        BroadcastDispatch.enrichProbeAlternatives(probe, /* buildStageId */ 3, /* buildSideIndex */ 1, ipcBytes);

        List<StagePlan> enriched = probe.getPlanAlternatives();
        assertEquals(2, enriched.size());
        for (StagePlan plan : enriched) {
            List<InstructionNode> instr = plan.instructions();
            assertEquals("existing scan instruction must still be first", 2, instr.size());
            assertTrue("first instruction preserved", instr.get(0) instanceof ShardScanInstructionNode);
            assertTrue("second (appended) instruction is broadcast", instr.get(1) instanceof BroadcastInjectionInstructionNode);
            BroadcastInjectionInstructionNode bn = (BroadcastInjectionInstructionNode) instr.get(1);
            assertEquals("broadcast-3", bn.getNamedInputId());
            assertEquals(1, bn.getBuildSideIndex());
            assertArrayEquals(ipcBytes, bn.getBroadcastData());
        }
    }

    public void testEnrichAppendsBroadcastWhenAlternativeHasNoExistingInstructions() {
        Stage probe = newStage(/* stageId */ 2);
        // Plan alternative with no instructions (null-bytes / empty): ensure append still works
        // and produces a one-element list.
        StagePlan alt = new StagePlan(null, "df");
        probe.setPlanAlternatives(List.of(alt));

        BroadcastDispatch.enrichProbeAlternatives(probe, /* buildStageId */ 5, /* buildSideIndex */ 0, new byte[] { 9 });

        StagePlan out = probe.getPlanAlternatives().get(0);
        assertEquals(1, out.instructions().size());
        assertTrue(out.instructions().get(0) instanceof BroadcastInjectionInstructionNode);
        assertEquals("broadcast-5", ((BroadcastInjectionInstructionNode) out.instructions().get(0)).getNamedInputId());
    }

    private static Stage newStage(int stageId) {
        // The planner-dag Stage constructor requires non-null childStages (copies into immutable).
        return new Stage(stageId, null, List.of(), null, null, null);
    }

    /**
     * Regression for the "task cancelled before pass 1 listener install" race. If
     * {@link AnalyticsQueryTask#setOnCancelCallback(Runnable)} replays its callback synchronously
     * because the task was already cancelled (e.g. user cancel landed during DAG rewrite), the
     * cancel-callback drives buildExec to CANCELLED. The listener must already be wired by then,
     * otherwise {@code addStateListener} (which does not replay terminal states) would never see
     * the transition and {@code terminal.onFailure} would never fire — leaving the query hung.
     */
    public void testRunFailsTerminalWhenTaskCancelledBeforeRun() {
        // 1) Build a parent task that's already cancelled. setOnCancelCallback will then replay
        //    synchronously — that's exactly the scenario this test exercises.
        AnalyticsQueryTask cancelledTask = new AnalyticsQueryTask(
            /* id */ 1L,
            "transport",
            "analytics_query",
            "qid",
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        cancelledTask.cancel("user cancel during DAG rewrite");
        assertTrue("precondition: task must be cancelled", cancelledTask.isCancelled());

        // 2) Build a fake StageExecution that records listeners and translates cancel(...) into
        //    a CANCELLED state-listener fire — the same contract AbstractStageExecution honors.
        FakeStageExecution buildExec = new FakeStageExecution(/* stageId */ 1);
        StageExecutionBuilder builder = mock(StageExecutionBuilder.class);
        when(builder.buildWithSink(any(Stage.class), any(ExchangeSink.class), any(QueryContext.class))).thenReturn(buildExec);

        QueryScheduler scheduler = mock(QueryScheduler.class);
        QueryContext ctx = mock(QueryContext.class);
        when(ctx.parentTask()).thenReturn(cancelledTask);

        Stage build = newRoleStage(/* stageId */ 1, Stage.StageRole.BROADCAST_BUILD);
        Stage probe = newRoleStage(/* stageId */ 2, Stage.StageRole.BROADCAST_PROBE);
        Stage root = newRoleStage(/* stageId */ 3, Stage.StageRole.COORDINATOR_REDUCE);

        AtomicReference<Throwable> failure = new AtomicReference<>();
        ActionListener<Iterable<org.apache.arrow.vector.VectorSchemaRoot>> terminal = ActionListener.wrap(
            v -> { throw new AssertionError("expected onFailure, got onResponse"); },
            failure::set
        );

        // 3) Run dispatch. setOnCancelCallback should fire the cancel synchronously — but only
        //    after the listener is installed, so the resulting CANCELLED transition fires the
        //    listener and propagates to terminal.onFailure.
        QueryDAG rewritten = new QueryDAG("qid", root);
        new BroadcastDispatch(builder, scheduler).run(
            ctx,
            rewritten,
            build,
            probe,
            root,
            () -> mock(ExchangeSink.class),
            terminal
        );

        assertNotNull(
            "terminal.onFailure must fire when the task was cancelled before BroadcastDispatch.run installed its callbacks; "
                + "instead the query would hang",
            failure.get()
        );
        // start() must NOT have been invoked — the dispatcher should bail out once it observes
        // the task is already cancelled.
        assertFalse("buildExec.start() must not run for an already-cancelled task", buildExec.startInvoked);
    }

    private static Stage newRoleStage(int stageId, Stage.StageRole role) {
        Stage stage = new Stage(stageId, null, List.of(), null, null, null);
        stage.setRole(role);
        return stage;
    }

    /**
     * Hand-rolled StageExecution stand-in. Captures listeners; cancel(...) fires CANCELLED on
     * any listeners present at cancel-time — same contract AbstractStageExecution honors.
     * Listeners attached AFTER cancel see nothing, which is the bug surface this test guards.
     */
    private static final class FakeStageExecution implements StageExecution {
        private final int stageId;
        private final java.util.List<StageStateListener> listeners = new java.util.ArrayList<>();
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
        public org.opensearch.analytics.exec.stage.StageMetrics getMetrics() {
            return null;
        }

        @Override
        public void start() {
            startInvoked = true;
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
        public boolean failFromChild(Exception cause) {
            return false;
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

        private static boolean isTerminal(State s) {
            return s == State.SUCCEEDED || s == State.FAILED || s == State.CANCELLED;
        }
    }
}
