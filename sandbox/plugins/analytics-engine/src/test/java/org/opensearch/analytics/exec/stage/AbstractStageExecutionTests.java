/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.exec.stage.coordinator.LocalStageTask;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AbstractStageExecution}. Uses a minimal test-only
 * concrete subclass that adds no behavior beyond what the base provides.
 */
public class AbstractStageExecutionTests extends OpenSearchTestCase {

    // ─── test-only concrete subclass ────────────────────────────────────

    private static class TestStageExecution extends AbstractStageExecution {

        List<StageTask> materializeReturn = List.of();
        RuntimeException materializeThrow;

        TestStageExecution(Stage stage) {
            this(stage, mock(AnalyticsQueryTask.class));
        }

        TestStageExecution(Stage stage, AnalyticsQueryTask parentTask) {
            super(stage, null, List.of(), parentTask);
        }

        public void doTransitionTo(StageExecution.State target) {
            transitionTo(target);
        }

        public void doCaptureFailure(Exception e) {
            captureFailure(e);
        }

        @Override
        protected List<StageTask> materializeTasks() {
            if (materializeThrow != null) throw materializeThrow;
            return materializeReturn;
        }
    }

    // ─── helpers ────────────────────────────────────────────────────────

    private static Stage mockStage(int stageId) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(stageId);
        return stage;
    }

    // ─── tests ─────────────────────────────────────────────────────────

    public void testInitialStateIsCreated() {
        TestStageExecution exec = new TestStageExecution(mockStage(0));
        assertEquals(StageExecution.State.CREATED, exec.getState());
    }

    public void testTransitionToFiresListenersWithFromAndTo() {
        TestStageExecution exec = new TestStageExecution(mockStage(1));

        List<StageExecution.State> fromCapture = new ArrayList<>();
        List<StageExecution.State> toCapture = new ArrayList<>();
        exec.addStateListener((from, to) -> {
            fromCapture.add(from);
            toCapture.add(to);
        });

        exec.doTransitionTo(StageExecution.State.RUNNING);

        assertEquals(1, fromCapture.size());
        assertEquals(StageExecution.State.CREATED, fromCapture.get(0));
        assertEquals(StageExecution.State.RUNNING, toCapture.get(0));
    }

    public void testTransitionToNoOpsOnSameState() {
        TestStageExecution exec = new TestStageExecution(mockStage(2));

        List<StageExecution.State> toCapture = new ArrayList<>();
        exec.addStateListener((from, to) -> toCapture.add(to));

        // Transition to CREATED (the current state) — should be a no-op
        exec.doTransitionTo(StageExecution.State.CREATED);

        assertTrue("listener should not fire on same-state transition", toCapture.isEmpty());
    }

    public void testMultipleListenersAllFireInRegistrationOrder() {
        TestStageExecution exec = new TestStageExecution(mockStage(3));

        List<Integer> callOrder = new ArrayList<>();
        exec.addStateListener((from, to) -> callOrder.add(1));
        exec.addStateListener((from, to) -> callOrder.add(2));
        exec.addStateListener((from, to) -> callOrder.add(3));

        exec.doTransitionTo(StageExecution.State.RUNNING);

        assertEquals(List.of(1, 2, 3), callOrder);
    }

    public void testListenerExceptionIsSwallowedAndSubsequentListenersStillFire() {
        TestStageExecution exec = new TestStageExecution(mockStage(4));

        List<Integer> callOrder = new ArrayList<>();
        exec.addStateListener((from, to) -> { throw new RuntimeException("boom"); });
        exec.addStateListener((from, to) -> callOrder.add(2));
        exec.addStateListener((from, to) -> callOrder.add(3));

        exec.doTransitionTo(StageExecution.State.RUNNING);

        assertEquals("second and third listeners must still fire", List.of(2, 3), callOrder);
        assertEquals("state must have transitioned despite listener exception", StageExecution.State.RUNNING, exec.getState());
    }

    public void testCaptureFailureIsIdempotent() {
        TestStageExecution exec = new TestStageExecution(mockStage(5));

        Exception first = new RuntimeException("first");
        Exception second = new RuntimeException("second");
        exec.doCaptureFailure(first);
        exec.doCaptureFailure(second);

        assertSame("getFailure() must return the first captured exception", first, exec.getFailure());
    }

    public void testGetFailureNullWhenNoFailureCaptured() {
        TestStageExecution exec = new TestStageExecution(mockStage(6));

        exec.doTransitionTo(StageExecution.State.CANCELLED);

        assertNull("getFailure() must be null when captureFailure was never called", exec.getFailure());
    }

    /** Base owns the start template: materializeTasks → publishTasksAndStart. */
    public void testStartPublishesMaterializedTasksAndTransitionsToRunning() {
        TestStageExecution exec = new TestStageExecution(mockStage(7));
        StageTask task = new LocalStageTask(new StageTaskId(7, 0), l -> l.onResponse(null));
        exec.materializeReturn = List.of(task);

        exec.start();

        assertEquals(StageExecution.State.RUNNING, exec.getState());
        assertEquals(1, exec.tasks().size());
        assertSame(task, exec.tasks().get(0));
    }

    /** Empty task list short-circuits CREATED → SUCCEEDED — concrete stages don't need the branch. */
    public void testStartWithEmptyMaterializedTasksTransitionsStraightToSucceeded() {
        TestStageExecution exec = new TestStageExecution(mockStage(8));
        exec.materializeReturn = List.of();  // explicit

        exec.start();

        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        assertTrue("no tasks published", exec.tasks().isEmpty());
    }

    /** materializeTasks() throwing must mark the stage FAILED with the thrown cause, not leak. */
    public void testStartTransitionsToFailedWhenMaterializeThrows() {
        TestStageExecution exec = new TestStageExecution(mockStage(9));
        RuntimeException boom = new RuntimeException("resolve failed");
        exec.materializeThrow = boom;

        exec.start();

        assertEquals(StageExecution.State.FAILED, exec.getState());
        assertSame("captured cause is the thrown exception", boom, exec.getFailure());
    }

    /**
     * The {@code onTerminalTransition} hook must fire BEFORE state listeners — that's the
     * point of the hook (stage-internal cleanup that has to precede the listener cascade
     * which may tear down query-level resources the cleanup needs).
     */
    public void testOnTerminalTransitionHookFiresBeforeStateListeners() {
        List<String> order = new ArrayList<>();
        TestStageExecution exec = new TestStageExecution(mockStage(10)) {
            @Override
            protected void onTerminalTransition(State terminal) {
                order.add("hook:" + terminal);
            }
        };
        exec.addStateListener((from, to) -> order.add("listener:" + to));

        exec.doTransitionTo(StageExecution.State.SUCCEEDED);

        assertEquals(List.of("hook:SUCCEEDED", "listener:SUCCEEDED"), order);
    }

    /**
     * Engine-internal cancel (e.g. cascade from a child failure, sibling-cancel sweep)
     * must propagate to the query-level {@link org.opensearch.tasks.CancellableTask} so
     * OpenSearch's task cancellation service tears down any in-flight data-node work.
     * Lives on the base because it applies to every stage with remote tasks; the
     * {@code isCancelled} guard makes it idempotent on the return trip from external
     * cancel.
     */
    public void testCancelPropagatesToParentTaskWhenProvided() {
        AnalyticsQueryTask parentTask = mock(AnalyticsQueryTask.class);
        when(parentTask.isCancelled()).thenReturn(false);

        TestStageExecution exec = new TestStageExecution(mockStage(11), parentTask);
        exec.cancel("reason");

        verify(parentTask).cancel("reason");
    }

    /** No double-cancel when the parent task is already cancelled (external-cancel path). */
    public void testCancelSkipsParentTaskWhenAlreadyCancelled() {
        AnalyticsQueryTask parentTask = mock(AnalyticsQueryTask.class);
        when(parentTask.isCancelled()).thenReturn(true);

        TestStageExecution exec = new TestStageExecution(mockStage(12), parentTask);
        exec.cancel("reason");

        verify(parentTask, never()).cancel(any());
    }
}
