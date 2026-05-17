/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageMetrics;
import org.opensearch.analytics.exec.stage.StageStateListener;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link StageListenerWiring} — the cascade's three behaviours:
 * scheduler dispatch on all-SUCCEEDED, direct propagation on FAILED / CANCELLED,
 * and the metadata channel that ferries each child's {@code publishedMetadata}
 * to the parent's {@code consumeChildMetadata} before scheduling.
 */
public class StageListenerWiringTests extends OpenSearchTestCase {

    public void testSchedulesParentOnceAllChildrenSucceed() {
        StageExecution parent = mock(StageExecution.class);
        FakeChild childA = new FakeChild(1);
        FakeChild childB = new FakeChild(2);

        AtomicReference<StageExecution> scheduled = new AtomicReference<>();
        Consumer<StageExecution> scheduler = scheduled::set;

        StageListenerWiring.wire(scheduler, parent, List.of(childA, childB));
        childA.fireSucceeded();
        assertNull("not scheduled until last child succeeds", scheduled.get());
        childB.fireSucceeded();
        assertSame("parent scheduled after all-children-SUCCEEDED", parent, scheduled.get());
    }

    public void testHandsOffPublishedMetadataBeforeScheduling() {
        StageExecution parent = mock(StageExecution.class);
        FakeChild childA = new FakeChild(7, "broadcast-bytes-A");
        FakeChild childB = new FakeChild(8, "stats-from-B");
        FakeChild childC = new FakeChild(9, null);  // publishes nothing

        AtomicReference<Map<Integer, Object>> consumed = new AtomicReference<>();
        // Capture metadata at the moment the cascade hands it off — i.e., before scheduling.
        org.mockito.Mockito.doAnswer(inv -> {
            consumed.set(new HashMap<>(inv.getArgument(0)));
            return null;
        }).when(parent).consumeChildMetadata(any());

        AtomicReference<StageExecution> scheduled = new AtomicReference<>();
        Consumer<StageExecution> scheduler = stage -> {
            // Verify the metadata handoff happened BEFORE this scheduler runs.
            assertNotNull("metadata should be handed off before scheduling", consumed.get());
            assertEquals("broadcast-bytes-A", consumed.get().get(7));
            assertEquals("stats-from-B", consumed.get().get(8));
            assertFalse("null-publishing child must not appear in map", consumed.get().containsKey(9));
            scheduled.set(stage);
        };

        StageListenerWiring.wire(scheduler, parent, List.of(childA, childB, childC));
        childA.fireSucceeded();
        childB.fireSucceeded();
        childC.fireSucceeded();
        assertNotNull("scheduler should have run", consumed.get());
    }

    public void testFailedChildPropagatesDirectlyToParent() {
        StageExecution parent = mock(StageExecution.class);
        FakeChild failing = new FakeChild(1);
        failing.failure = new RuntimeException("kaboom");

        Consumer<StageExecution> scheduler = stage -> fail("must NOT schedule on child failure");

        StageListenerWiring.wire(scheduler, parent, List.of(failing));
        failing.fire(StageExecution.State.FAILED);

        verify(parent).failWithCause(failing.failure);
        verify(parent, never()).consumeChildMetadata(any());
    }

    /**
     * Early-termination contract: when a parent cancels its own child (e.g. coordinator-side
     * LIMIT satisfied, stop the shard stream), the child transitions to CANCELLED. The
     * cascade must NOT propagate that cancel back to the parent — the parent is the one
     * who issued the cancel and must stay RUNNING.
     */
    public void testCancelledChildIsNotPropagatedToParent() {
        StageExecution parent = mock(StageExecution.class);
        FakeChild cancelled = new FakeChild(5);  // no failure recorded

        Consumer<StageExecution> scheduler = stage -> fail("must NOT schedule on child cancellation");

        StageListenerWiring.wire(scheduler, parent, List.of(cancelled));
        cancelled.fire(StageExecution.State.CANCELLED);

        verify(parent, never()).cancel(any());
        verify(parent, never()).failWithCause(any());
        verify(parent, never()).consumeChildMetadata(any());
    }

    /**
     * Sibling-cancel sweep: when one child fails and the parent transitions to FAILED,
     * any siblings still running must be cancelled so they don't keep producing into a
     * sink whose owner has terminated.
     */
    public void testSiblingsAreCancelledWhenParentReachesFailedTerminal() {
        FakeChild failing = new FakeChild(1);
        failing.failure = new RuntimeException("kaboom");
        FakeChild stillRunning = new FakeChild(2);
        FakeChild alreadyDone = new FakeChild(3);
        alreadyDone.fakeState = StageExecution.State.SUCCEEDED;

        FakeParent parent = new FakeParent(99);
        Consumer<StageExecution> scheduler = stage -> {};

        StageListenerWiring.wire(scheduler, parent, List.of(failing, stillRunning, alreadyDone));
        failing.fire(StageExecution.State.FAILED);

        assertEquals("parent must reach FAILED from child failure", StageExecution.State.FAILED, parent.fakeState);
        assertNotNull("still-running sibling must have been cancelled", stillRunning.cancelReason);
        assertNull("already-terminal sibling must not be re-cancelled", alreadyDone.cancelReason);
    }

    /**
     * Minimal child stub: append listeners + fire transitions; carries optional metadata + failure;
     * records the {@code cancel(reason)} that the sibling-sweep test asserts against. Mutable
     * {@link #fakeState} is queried by the cascade's parent-listener via {@link #getState()}.
     */
    private static final class FakeChild implements StageExecution {
        private final int stageId;
        private final Object metadata;
        private final List<StageStateListener> listeners = new ArrayList<>();
        State fakeState = State.RUNNING;
        Exception failure;
        String cancelReason;

        FakeChild(int stageId) {
            this(stageId, null);
        }

        FakeChild(int stageId, Object metadata) {
            this.stageId = stageId;
            this.metadata = metadata;
        }

        void fireSucceeded() {
            fire(State.SUCCEEDED);
        }

        void fire(State terminal) {
            State previous = fakeState;
            fakeState = terminal;
            for (StageStateListener l : listeners)
                l.onStateChange(previous, terminal);
        }

        @Override
        public int getStageId() {
            return stageId;
        }

        @Override
        public State getState() {
            return fakeState;
        }

        @Override
        public StageMetrics getMetrics() {
            return new StageMetrics();
        }

        @Override
        public void start() {}

        @Override
        public void addStateListener(StageStateListener listener) {
            listeners.add(listener);
        }

        @Override
        public Exception getFailure() {
            return failure;
        }

        @Override
        public boolean failWithCause(Exception cause) {
            return false;
        }

        @Override
        public void cancel(String reason) {
            cancelReason = reason;
        }

        @Override
        public void onTaskTerminal(StageTask task, Exception cause) {}

        @Override
        public Object publishedMetadata() {
            return metadata;
        }
    }

    /**
     * Parent stub for tests that exercise the parent-state listener installed by
     * {@link StageListenerWiring#wire}: {@link #failWithCause(Exception)} and
     * {@link #cancel(String)} actually transition {@link #fakeState} and fire listeners,
     * which is what triggers the sibling-cancel sweep.
     */
    private static final class FakeParent implements StageExecution {
        private final int stageId;
        private final List<StageStateListener> listeners = new ArrayList<>();
        State fakeState = State.RUNNING;
        Exception capturedFailure;

        FakeParent(int stageId) {
            this.stageId = stageId;
        }

        private void transitionTo(State target) {
            State previous = fakeState;
            fakeState = target;
            for (StageStateListener l : listeners)
                l.onStateChange(previous, target);
        }

        @Override
        public int getStageId() {
            return stageId;
        }

        @Override
        public State getState() {
            return fakeState;
        }

        @Override
        public StageMetrics getMetrics() {
            return new StageMetrics();
        }

        @Override
        public void start() {}

        @Override
        public void addStateListener(StageStateListener listener) {
            listeners.add(listener);
        }

        @Override
        public Exception getFailure() {
            return capturedFailure;
        }

        @Override
        public boolean failWithCause(Exception cause) {
            capturedFailure = cause;
            transitionTo(State.FAILED);
            return true;
        }

        @Override
        public void cancel(String reason) {
            transitionTo(State.CANCELLED);
        }

        @Override
        public void onTaskTerminal(StageTask task, Exception cause) {}
    }
}
