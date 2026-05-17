/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AbstractStageExecution}. Uses a minimal test-only
 * concrete subclass that adds no behavior beyond what the base provides.
 */
public class AbstractStageExecutionTests extends OpenSearchTestCase {

    // ─── test-only concrete subclass ────────────────────────────────────

    private static final class TestStageExecution extends AbstractStageExecution {

        TestStageExecution(Stage stage) {
            super(stage);
        }

        public void doTransitionTo(StageExecution.State target) {
            transitionTo(target);
        }

        public void doCaptureFailure(Exception e) {
            captureFailure(e);
        }

        @Override
        public void start() {
            // no-op
        }

        @Override
        public void cancel(String reason) {
            // no-op
        }

        @Override
        public void onTaskTerminal(StageTask task, Exception cause) {
            // no-op
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
}
