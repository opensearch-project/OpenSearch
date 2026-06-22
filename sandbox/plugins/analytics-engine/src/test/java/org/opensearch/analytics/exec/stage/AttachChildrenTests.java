/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link StageExecution#attachChildren} — the cascade's three behaviours:
 * scheduler dispatch on all-SUCCEEDED, direct propagation on FAILED / CANCELLED,
 * and the metadata channel that ferries each child's {@code publishedMetadata}
 * to the parent's {@code consumeChildMetadata} before scheduling.
 */
public class AttachChildrenTests extends OpenSearchTestCase {

    public void testSchedulesParentOnceAllChildrenSucceed() {
        StageExecution parent = mock(StageExecution.class, CALLS_REAL_METHODS);
        FakeChild childA = new FakeChild(1);
        FakeChild childB = new FakeChild(2);

        AtomicReference<StageExecution> scheduled = new AtomicReference<>();
        Consumer<StageExecution> scheduler = scheduled::set;

        parent.attachChildren(List.of(childA, childB), scheduler);
        childA.fireSucceeded();
        assertNull("not scheduled until last child succeeds", scheduled.get());
        childB.fireSucceeded();
        assertSame("parent scheduled after all-children-SUCCEEDED", parent, scheduled.get());
    }

    public void testHandsOffPublishedMetadataBeforeScheduling() {
        StageExecution parent = mock(StageExecution.class, CALLS_REAL_METHODS);
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

        parent.attachChildren(List.of(childA, childB, childC), scheduler);
        childA.fireSucceeded();
        childB.fireSucceeded();
        childC.fireSucceeded();
        assertNotNull("scheduler should have run", consumed.get());
    }

    public void testFailedChildPropagatesDirectlyToParent() {
        StageExecution parent = mock(StageExecution.class, CALLS_REAL_METHODS);
        FakeChild failing = new FakeChild(1);
        failing.failure = new RuntimeException("kaboom");

        Consumer<StageExecution> scheduler = stage -> fail("must NOT schedule on child failure");

        parent.attachChildren(List.of(failing), scheduler);
        failing.fire(StageExecution.State.FAILED);

        verify(parent).failWithCause(failing.failure);
        verify(parent, never()).consumeChildMetadata(any());
    }

    /**
     * Cancelled-child contract: the cascade must close the parent's per-child input (so a parent
     * reduce drain blocked on {@code streamNext} sees EOF and unwinds) but must NOT propagate cancel
     * to the parent's state or schedule it.
     */
    public void testCancelledChildClosesInputButDoesNotPropagateToParent() {
        StageExecution parent = mock(StageExecution.class, CALLS_REAL_METHODS);
        FakeChild cancelled = new FakeChild(5);  // no failure recorded

        Consumer<StageExecution> scheduler = stage -> fail("must NOT schedule on child cancellation");

        parent.attachChildren(List.of(cancelled), scheduler);
        cancelled.fire(StageExecution.State.CANCELLED);

        // EOF released to the reduce input (the leak fix).
        verify(parent).closeChildInput(eq(5));
        // State is NOT propagated and the parent is NOT scheduled.
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

        parent.attachChildren(List.of(failing, stillRunning, alreadyDone), scheduler);
        failing.fire(StageExecution.State.FAILED);

        assertEquals("parent must reach FAILED from child failure", StageExecution.State.FAILED, parent.fakeState);
        assertNotNull("still-running sibling must have been cancelled", stillRunning.cancelReason);
        assertNull("already-terminal sibling must not be re-cancelled", alreadyDone.cancelReason);
    }

    /**
     * Eager (streaming) parents must be scheduled as soon as the first child transitions
     * to RUNNING — they need to run concurrently with their children's feeds (e.g. a
     * streaming reduce whose drain pulls native output while children push batches).
     * Waiting for all-children-SUCCEEDED would deadlock on a bounded input mpsc.
     */
    public void testEagerParentSchedulesOnFirstChildRunning() {
        StageExecution parent = mock(StageExecution.class, CALLS_REAL_METHODS);
        org.mockito.Mockito.when(parent.schedulesEagerly()).thenReturn(true);
        FakeChild childA = new FakeChild(1);
        childA.fakeState = StageExecution.State.CREATED;
        FakeChild childB = new FakeChild(2);
        childB.fakeState = StageExecution.State.CREATED;

        AtomicReference<StageExecution> scheduled = new AtomicReference<>();
        Consumer<StageExecution> scheduler = scheduled::set;

        parent.attachChildren(List.of(childA, childB), scheduler);

        assertNull("not scheduled until any child enters RUNNING", scheduled.get());
        childA.fire(StageExecution.State.RUNNING);
        assertSame("eager parent scheduled on first child RUNNING", parent, scheduled.get());

        // Subsequent RUNNING transitions on other children must not re-schedule.
        scheduled.set(null);
        childB.fire(StageExecution.State.RUNNING);
        assertNull("subsequent child RUNNING must not re-schedule", scheduled.get());
    }

    /**
     * Per-input EOF hook fires on every child SUCCEEDED, regardless of scheduling mode.
     * Backends without per-child resources inherit the default {@code closeChildInput}
     * no-op; this test guards against re-introducing an eager-mode gate that would
     * silently drop the signal for a future buffered multi-input backend.
     */
    public void testCloseChildInputFiresOnEveryChildSucceededRegardlessOfMode() {
        StageExecution defaultParent = mock(StageExecution.class, CALLS_REAL_METHODS);
        FakeChild a = new FakeChild(11);
        FakeChild b = new FakeChild(22);
        defaultParent.attachChildren(List.of(a, b), stage -> {});
        a.fireSucceeded();
        b.fireSucceeded();
        verify(defaultParent).closeChildInput(eq(11));
        verify(defaultParent).closeChildInput(eq(22));

        StageExecution eagerParent = mock(StageExecution.class, CALLS_REAL_METHODS);
        org.mockito.Mockito.when(eagerParent.schedulesEagerly()).thenReturn(true);
        FakeChild c = new FakeChild(33);
        c.fakeState = StageExecution.State.CREATED;
        eagerParent.attachChildren(List.of(c), stage -> {});
        c.fire(StageExecution.State.RUNNING);
        c.fire(StageExecution.State.SUCCEEDED);
        verify(eagerParent).closeChildInput(eq(33));
    }

    /**
     * Default (non-streaming) parents keep today's contract: scheduled only when all
     * children SUCCEEDED. A child reaching RUNNING must not trigger the parent.
     */
    public void testDefaultParentDoesNotScheduleOnChildRunning() {
        StageExecution parent = mock(StageExecution.class, CALLS_REAL_METHODS);
        // default schedulesEagerly() == false
        FakeChild child = new FakeChild(1);
        child.fakeState = StageExecution.State.CREATED;

        AtomicReference<StageExecution> scheduled = new AtomicReference<>();
        Consumer<StageExecution> scheduler = scheduled::set;

        parent.attachChildren(List.of(child), scheduler);
        child.fire(StageExecution.State.RUNNING);

        assertNull("default-mode parent must NOT schedule on child RUNNING", scheduled.get());
        child.fire(StageExecution.State.SUCCEEDED);
        assertSame("default-mode parent scheduled on all-SUCCEEDED", parent, scheduled.get());
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
     * {@link StageExecution#attachChildren}: {@link #failWithCause(Exception)} and
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
