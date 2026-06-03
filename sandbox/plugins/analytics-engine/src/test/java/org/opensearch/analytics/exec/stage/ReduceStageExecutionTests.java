/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.RowProducingSink;
import org.opensearch.analytics.exec.stage.coordinator.ReduceStageExecution;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.exec.task.TaskRunner;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.CancellableExchangeSink;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.MultiInputExchangeSink;
import org.opensearch.analytics.spi.ReducingExchangeSink;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ReduceStageExecution}. Exercises both scheduling modes:
 * <ul>
 *   <li><b>Buffered</b> — sink's {@code supportsEagerScheduling()} returns false
 *   (default). Task body invokes {@code reduce()}, terminal transition invokes
 *   {@code close()}.</li>
 *   <li><b>Streaming</b> — sink's {@code supportsEagerScheduling()} returns true,
 *   sink implements {@link MultiInputExchangeSink}. {@code closeChildInput} closes
 *   the corresponding per-child wrapper.</li>
 * </ul>
 */
public class ReduceStageExecutionTests extends OpenSearchTestCase {

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

    // ── Buffered mode (default supportsEagerScheduling = false) ──────────

    public void testBufferedSchedulesEagerlyIsFalse() {
        CapturingReducingSink backend = new CapturingReducingSink();
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());
        assertFalse(exec.schedulesEagerly());
    }

    public void testBufferedStartInvokesReduceAndTerminalInvokesClose() {
        CapturingReducingSink backend = new CapturingReducingSink();
        CapturingSink downstream = new CapturingSink();
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, downstream);

        scheduleAndDispatch(exec);

        assertEquals("reduce invoked once from task body", 1, backend.reduceCalls.get());
        assertTrue("close invoked from onTerminalTransition", backend.closed);
        assertFalse("downstream is not closed by reduce execution", downstream.closed);
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
    }

    public void testBufferedInputSinkReturnsBackendSinkForAnyChildId() {
        CapturingReducingSink backend = new CapturingReducingSink();
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        assertSame(backend, exec.inputSink(0));
        assertSame(backend, exec.inputSink(7));
        assertSame(backend, exec.inputSink(42));
    }

    public void testOutputSourceReturnsDownstreamWhenItImplementsExchangeSource() {
        RowProducingSink downstream = new RowProducingSink();
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), new CapturingReducingSink(), downstream);
        assertSame(downstream, exec.outputSource());
    }

    public void testOutputSourceThrowsWhenDownstreamDoesNotImplementExchangeSource() {
        ReduceStageExecution exec = new ReduceStageExecution(
            stageWithId(0),
            mockContext(),
            new CapturingReducingSink(),
            new CapturingSink()
        );
        expectThrows(UnsupportedOperationException.class, exec::outputSource);
    }

    public void testStartTransitionsToFailedWhenReduceFiresOnFailure() {
        RuntimeException boom = new RuntimeException("reduce blew up");
        CapturingReducingSink backend = new CapturingReducingSink();
        backend.reduceFailure = boom;
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        scheduleAndDispatch(exec);

        assertEquals(StageExecution.State.FAILED, exec.getState());
        assertSame(boom, exec.getFailure());
        assertTrue("close still fires from onTerminalTransition on FAILED", backend.closed);
    }

    public void testStartIsNoopAfterTerminalTransition() {
        CapturingReducingSink backend = new CapturingReducingSink();
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        exec.cancel("test cancellation");
        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertEquals("close fired once from cancel", 1, backend.closeCount);

        scheduleAndDispatch(exec);  // start() returns no-op; no task body runs because state is already terminal
        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertEquals("backend not re-closed by no-op start()", 1, backend.closeCount);
        assertEquals("reduce was never invoked — cancel preceded body", 0, backend.reduceCalls.get());
    }

    public void testFailFromChildClosesBackendSinkAndTransitions() {
        CapturingReducingSink backend = new CapturingReducingSink();
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        Exception cause = new RuntimeException("child failed");
        boolean transitioned = exec.failWithCause(cause);

        assertTrue(transitioned);
        assertEquals(StageExecution.State.FAILED, exec.getState());
        assertSame(cause, exec.getFailure());
        assertTrue(backend.closed);
    }

    public void testCancelClosesBackendSink() {
        CapturingReducingSink backend = new CapturingReducingSink();
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        exec.cancel("user requested");

        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertTrue(backend.closed);
    }

    public void testCancelAfterFailFromChildDoesNotDoubleCloseBackendSink() {
        CapturingReducingSink backend = new CapturingReducingSink();
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        exec.failWithCause(new RuntimeException("child failed"));
        assertEquals(StageExecution.State.FAILED, exec.getState());
        assertEquals("first close fires from failWithCause's terminal transition", 1, backend.closeCount);

        exec.cancel("late external cancel");

        assertEquals("first terminal wins; cancel does not re-transition", StageExecution.State.FAILED, exec.getState());
        assertEquals("backend close fires exactly once across both calls", 1, backend.closeCount);
    }

    public void testFailFromChildAfterCancelDoesNotDoubleCloseBackendSink() {
        CapturingReducingSink backend = new CapturingReducingSink();
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        exec.cancel("external cancel first");
        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertEquals("first close fires from cancel", 1, backend.closeCount);

        boolean transitioned = exec.failWithCause(new RuntimeException("late child failure"));

        assertFalse("transition rejected — already terminal", transitioned);
        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertEquals("backend close fires exactly once across both calls", 1, backend.closeCount);
    }

    // ── CancellableExchangeSink interaction ────────────────────────────────

    public void testCancelCallsCancellableExchangeSinkCancelBeforeClose() {
        CancellableCapturingSink backend = new CancellableCapturingSink();
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        exec.cancel("user requested");

        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertTrue("cancel() must be called on CancellableExchangeSink", backend.cancelCalled);
        assertTrue("close() must still be called after cancel()", backend.closed);
        assertTrue("cancel() must fire before close()", backend.cancelCalledBeforeClose);
    }

    public void testFailFromChildCallsCancellableExchangeSinkCancelBeforeClose() {
        CancellableCapturingSink backend = new CancellableCapturingSink();
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        exec.failWithCause(new RuntimeException("child failed"));

        assertEquals(StageExecution.State.FAILED, exec.getState());
        assertTrue("cancel() must be called on FAILED transition", backend.cancelCalled);
        assertTrue("close() must still be called", backend.closed);
        assertTrue("cancel() before close()", backend.cancelCalledBeforeClose);
    }

    public void testSuccessDoesNotCallCancellableExchangeSinkCancel() {
        CancellableCapturingSink backend = new CancellableCapturingSink();
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        scheduleAndDispatch(exec);

        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        assertFalse("cancel() must NOT be called on SUCCEEDED", backend.cancelCalled);
        assertTrue("close() is still called on terminal", backend.closed);
    }

    // ── Streaming mode (supportsEagerScheduling = true) ───────────────────

    public void testStreamingSchedulesEagerlyIsTrue() {
        StreamingFakeSink backend = new StreamingFakeSink();
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());
        assertTrue(exec.schedulesEagerly());
    }

    public void testStreamingStartInvokesReduceAndTransitionsToSucceeded() {
        StreamingFakeSink backend = new StreamingFakeSink();
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        scheduleAndDispatch(exec);

        assertEquals(1, backend.reduceCalls.get());
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        assertTrue("close still fires in onTerminalTransition for streaming", backend.closed);
    }

    public void testStreamingStartTransitionsToFailedWhenReduceFires() {
        RuntimeException boom = new RuntimeException("drain failed");
        StreamingFakeSink backend = new StreamingFakeSink();
        backend.reduceFailure = boom;
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        scheduleAndDispatch(exec);

        assertEquals(StageExecution.State.FAILED, exec.getState());
        assertSame(boom, exec.getFailure());
        assertTrue(backend.closed);
    }

    public void testStreamingCloseChildInputClosesPerChildWrapper() {
        StreamingFakeSink backend = new StreamingFakeSink();
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        exec.closeChildInput(7);
        exec.closeChildInput(11);

        assertEquals("each closeChildInput closed exactly one per-child wrapper", List.of(7, 11), backend.closedChildIds);
    }

    public void testBufferedCloseChildInputIsNoopBecauseNotMultiInput() {
        CapturingReducingSink backend = new CapturingReducingSink();
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        // No-op for non-MultiInput sinks — must not affect closed state.
        exec.closeChildInput(0);
        assertFalse("closeChildInput on non-MultiInput sink must not close backend", backend.closed);
    }

    /**
     * Regression: when a child stage transitions to FAILED, the cascade must call
     * {@code closeChildInput(childId)} BEFORE propagating the failure. Without this,
     * the reduce drain hangs forever waiting for input from the dead child's partition
     * stream (the sender never gets closed, so the native receiver never sees EOF).
     */
    public void testChildFailureClosesChildInputBeforeFailingParent() {
        StreamingFakeSink backend = new StreamingFakeSink();
        ReduceStageExecution exec = new ReduceStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        // Create a fake child stage that we can fail manually.
        int childStageId = 5;
        Stage childStageDef = mock(Stage.class);
        when(childStageDef.getStageId()).thenReturn(childStageId);
        when(childStageDef.getChildStages()).thenReturn(List.of());
        FakeChildExecution child = new FakeChildExecution(childStageDef);

        // Wire the cascade: parent observes child state transitions.
        exec.attachChildren(List.of(child), r -> {});

        // Fail the child — the cascade should call closeChildInput(5) then failWithCause.
        child.failWith(new RuntimeException("shard exploded"));

        assertTrue("closeChildInput must be called for the failed child", backend.closedChildIds.contains(childStageId));
        assertEquals(StageExecution.State.FAILED, exec.getState());
        assertNotNull(exec.getFailure());
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private Stage stageWithId(int id) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(id);
        when(stage.getChildStages()).thenReturn(List.of());
        return stage;
    }

    private static ExecutorService inlineExecutor() {
        return mock(ExecutorService.class, invocation -> {
            if (invocation.getMethod().getName().equals("execute")) {
                ((Runnable) invocation.getArgument(0)).run();
                return null;
            }
            return null;
        });
    }

    private static QueryContext mockContext() {
        QueryContext ctx = mock(QueryContext.class);
        when(ctx.reduceExecutor()).thenReturn(inlineExecutor());
        when(ctx.schedulerExecutor()).thenReturn(inlineExecutor());
        when(ctx.parentTask()).thenReturn(mock(AnalyticsQueryTask.class));
        return ctx;
    }

    private static void scheduleAndDispatch(ReduceStageExecution exec) {
        exec.start();
        @SuppressWarnings("unchecked")
        TaskRunner<StageTask> dispatcher = (TaskRunner<StageTask>) exec.taskRunner();
        if (dispatcher == null) return;
        for (StageTask task : exec.tasks()) {
            task.transitionTo(StageTaskState.RUNNING);
            dispatcher.run(task, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    task.transitionTo(StageTaskState.FINISHED);
                    exec.onTaskTerminal(task, null);
                }

                @Override
                public void onFailure(Exception cause) {
                    task.transitionTo(StageTaskState.FAILED);
                    exec.onTaskTerminal(task, cause);
                }
            });
        }
    }

    /** Buffered-mode reducing sink — overrides supportsEagerScheduling to false. */
    private static class CapturingReducingSink implements ReducingExchangeSink {
        final List<VectorSchemaRoot> fed = new ArrayList<>();
        final AtomicInteger reduceCalls = new AtomicInteger();
        RuntimeException reduceFailure;
        boolean closed = false;
        int closeCount = 0;

        @Override
        public boolean supportsEagerScheduling() {
            return false;
        }

        @Override
        public void feed(VectorSchemaRoot batch) {
            fed.add(batch);
        }

        @Override
        public void reduce(ActionListener<Void> listener) {
            reduceCalls.incrementAndGet();
            if (reduceFailure != null) {
                listener.onFailure(reduceFailure);
            } else {
                listener.onResponse(null);
            }
        }

        @Override
        public void close() {
            closed = true;
            closeCount++;
            for (VectorSchemaRoot batch : fed) {
                batch.close();
            }
            fed.clear();
        }
    }

    /**
     * Streaming-mode reducing sink — re-inherits the default supportsEagerScheduling=true
     * from ReducingExchangeSink (overriding the parent CapturingReducingSink's buffered
     * override); implements MultiInputExchangeSink for per-child wrapper EOF.
     */
    private static final class StreamingFakeSink extends CapturingReducingSink implements MultiInputExchangeSink {
        final List<Integer> closedChildIds = new ArrayList<>();
        final Map<Integer, ExchangeSink> childWrappers = new HashMap<>();

        @Override
        public boolean supportsEagerScheduling() {
            return true;
        }

        @Override
        public ExchangeSink sinkForChild(int childStageId) {
            return childWrappers.computeIfAbsent(childStageId, id -> new ExchangeSink() {
                @Override
                public void feed(VectorSchemaRoot batch) {
                    batch.close();
                }

                @Override
                public void close() {
                    closedChildIds.add(id);
                }
            });
        }
    }

    /** CancellableExchangeSink variant that records cancel/close ordering. */
    private static class CancellableCapturingSink extends CapturingReducingSink implements CancellableExchangeSink {
        boolean cancelCalled = false;
        boolean cancelCalledBeforeClose = false;

        @Override
        public void cancel() {
            cancelCalled = true;
            cancelCalledBeforeClose = !closed;
        }
    }

    /** Bare ExchangeSink for the downstream slot. */
    private static final class CapturingSink implements ExchangeSink {
        boolean closed = false;

        @Override
        public void feed(VectorSchemaRoot batch) {
            batch.close();
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    /** Minimal child stage that can be manually failed to trigger the parent cascade. */
    private static final class FakeChildExecution implements StageExecution {
        private final Stage stage;
        private final List<StageStateListener> listeners = new ArrayList<>();
        private State state = State.CREATED;
        private Exception failure;

        FakeChildExecution(Stage stage) {
            this.stage = stage;
        }

        void failWith(Exception cause) {
            this.failure = cause;
            State prev = this.state;
            this.state = State.FAILED;
            for (StageStateListener l : listeners) {
                l.onStateChange(prev, State.FAILED);
            }
        }

        @Override
        public int getStageId() {
            return stage.getStageId();
        }

        @Override
        public State getState() {
            return state;
        }

        @Override
        public StageMetrics getMetrics() {
            return new StageMetrics();
        }

        @Override
        public Exception getFailure() {
            return failure;
        }

        @Override
        public void start() {
            state = State.RUNNING;
        }

        @Override
        public void addStateListener(StageStateListener listener) {
            listeners.add(listener);
        }

        @Override
        public void cancel(String reason) {
            state = State.CANCELLED;
        }
    }
}
