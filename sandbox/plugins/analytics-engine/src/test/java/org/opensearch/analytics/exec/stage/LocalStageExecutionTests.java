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
import org.opensearch.analytics.exec.task.TaskRunner;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link LocalStageExecution} covering the placeholder
 * lifecycle: inputSink delegates to the backend sink, start closes both
 * sinks and transitions, failWithCause/cancel close the backend sink.
 */
public class LocalStageExecutionTests extends OpenSearchTestCase {

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

    public void testStartClosesBackendSinkAndTransitionsToSucceeded() {
        CapturingSink backend = new CapturingSink();
        CapturingSink downstream = new CapturingSink();
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), mockContext(), backend, downstream);

        scheduleAndDispatch(exec);

        assertTrue("backend sink closed", backend.closed);
        // Downstream is NOT closed by start() — its lifecycle is owned by the walker,
        // which still needs to read the buffered batches via outputSource().readResult().
        assertFalse("downstream must not be closed by LocalStageExecution.start()", downstream.closed);
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
    }

    public void testInputSinkReturnsBackendSinkForAnyChildId() {
        CapturingSink backend = new CapturingSink();
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        assertSame(backend, exec.inputSink(0));
        assertSame(backend, exec.inputSink(7));
        assertSame(backend, exec.inputSink(42));
    }

    public void testOutputSourceReturnsDownstreamWhenItImplementsExchangeSource() {
        RowProducingSink downstream = new RowProducingSink();
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), mockContext(), new CapturingSink(), downstream);
        assertSame(downstream, exec.outputSource());
    }

    public void testOutputSourceThrowsWhenDownstreamDoesNotImplementExchangeSource() {
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), mockContext(), new CapturingSink(), new CapturingSink());
        expectThrows(UnsupportedOperationException.class, exec::outputSource);
    }

    public void testStartTransitionsToFailedWhenCloseThrows() {
        RuntimeException boom = new RuntimeException("close blew up");
        ExchangeSink backend = new ExchangeSink() {
            @Override
            public void feed(VectorSchemaRoot batch) {}

            @Override
            public void close() {
                throw boom;
            }
        };
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        scheduleAndDispatch(exec);

        assertEquals(StageExecution.State.FAILED, exec.getState());
        assertSame(boom, exec.getFailure());
    }

    public void testStartIsNoopAfterTerminalTransition() {
        CapturingSink backend = new CapturingSink();
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        exec.cancel("test cancellation");
        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertTrue(backend.closed);

        backend.closed = false;
        scheduleAndDispatch(exec);  // start() returns no-op; dispatcher dispatches no tasks because state is already terminal
        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertFalse("backend sink not re-closed by start()", backend.closed);
    }

    public void testFailFromChildClosesBackendSinkAndTransitions() {
        CapturingSink backend = new CapturingSink();
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        Exception cause = new RuntimeException("child failed");
        boolean transitioned = exec.failWithCause(cause);

        assertTrue(transitioned);
        assertEquals(StageExecution.State.FAILED, exec.getState());
        assertSame(cause, exec.getFailure());
        assertTrue(backend.closed);
    }

    public void testCancelClosesBackendSink() {
        CapturingSink backend = new CapturingSink();
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        exec.cancel("user requested");

        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertTrue(backend.closed);
    }

    /**
     * Cascade-driven double-close: a child failure causes failWithCause to close
     * the backend sink and transition FAILED. A subsequent external cancel (e.g.
     * task-cancel after the cascade has already failed the stage) must not close
     * the sink a second time — streaming reduce sinks block on draining the
     * native pipeline, and concurrent closes serialize on the sink's feedLock.
     */
    public void testCancelAfterFailFromChildDoesNotDoubleCloseBackendSink() {
        CapturingSink backend = new CapturingSink();
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        exec.failWithCause(new RuntimeException("child failed"));
        assertEquals(StageExecution.State.FAILED, exec.getState());
        assertEquals("first close fires from failWithCause", 1, backend.closeCount);

        exec.cancel("late external cancel");

        assertEquals("first terminal wins; cancel does not transition", StageExecution.State.FAILED, exec.getState());
        assertEquals("backend sink close fires exactly once across both calls", 1, backend.closeCount);
    }

    /**
     * Symmetric case: external cancel runs first, then a child-failure cascade
     * fires failWithCause on the already-CANCELLED stage. failWithCause's
     * transitionTo returns false but must not close the sink a second time.
     */
    public void testFailFromChildAfterCancelDoesNotDoubleCloseBackendSink() {
        CapturingSink backend = new CapturingSink();
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), mockContext(), backend, new CapturingSink());

        exec.cancel("external cancel first");
        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertEquals("first close fires from cancel", 1, backend.closeCount);

        boolean transitioned = exec.failWithCause(new RuntimeException("late child failure"));

        assertFalse("transition rejected — already terminal", transitioned);
        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertEquals("backend sink close fires exactly once across both calls", 1, backend.closeCount);
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private Stage stageWithId(int id) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(id);
        when(stage.getChildStages()).thenReturn(List.of());
        return stage;
    }

    /**
     * Synchronous executor — runs submitted Runnables on the caller thread so dispatch
     * completes before {@link #scheduleAndDispatch} returns. Lets tests assert on
     * terminal stage state immediately after dispatch.
     */
    private static ExecutorService inlineExecutor() {
        return mock(ExecutorService.class, invocation -> {
            if (invocation.getMethod().getName().equals("execute")) {
                ((Runnable) invocation.getArgument(0)).run();
                return null;
            }
            return null;
        });
    }

    /** QueryContext mock whose localTaskExecutor() returns a synchronous executor. */
    private static QueryContext mockContext() {
        QueryContext ctx = mock(QueryContext.class);
        when(ctx.localTaskExecutor()).thenReturn(inlineExecutor());
        return ctx;
    }

    /**
     * Mirrors {@code QueryExecution.scheduleStage} for unit-test purposes — calls
     * start() to materialise + transition, then iterates the stage's tasks via its
     * dispatcher with a scheduler-side listener.
     */
    private static void scheduleAndDispatch(LocalStageExecution exec) {
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

    /** ExchangeSink that records feed/close calls and releases held batches on close. */
    private static final class CapturingSink implements ExchangeSink {
        final List<VectorSchemaRoot> fed = new ArrayList<>();
        boolean closed = false;
        int closeCount = 0;

        @Override
        public void feed(VectorSchemaRoot batch) {
            fed.add(batch);
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
}
