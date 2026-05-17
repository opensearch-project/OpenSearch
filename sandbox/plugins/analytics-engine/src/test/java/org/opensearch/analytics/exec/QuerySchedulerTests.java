/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.exec.stage.ShardStageTask;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.exec.stage.StageMetrics;
import org.opensearch.analytics.exec.stage.StageStateListener;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskId;
import org.opensearch.analytics.exec.stage.StageTaskState;
import org.opensearch.analytics.exec.task.TaskRunner;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link QueryScheduler#scheduleStage} and {@link QueryScheduler#handleFor}.
 * Exercises dispatch + retry seam in isolation, using a fake stage + fake runner — no real
 * graph, no Guice wiring.
 */
public class QuerySchedulerTests extends OpenSearchTestCase {

    private QueryScheduler scheduler;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        scheduler = new QueryScheduler(mock(StageExecutionBuilder.class));
    }

    /** Happy path: scheduler iterates tasks, transitions each to RUNNING, runs them. */
    public void testScheduleStageRunsEachTaskAfterTransitioningRunning() {
        FakeStage stage = new FakeStage(makeTask(0), makeTask(1));
        scheduler.scheduleStage(stage);

        assertEquals("each task should have been dispatched once", 2, stage.runner.dispatched.size());
        for (StageTask t : stage.tasks()) {
            assertEquals("task transitioned to RUNNING before dispatch", StageTaskState.RUNNING, t.state());
        }
    }

    /** Scheduler must skip dispatch when start() transitions the stage straight to a terminal. */
    public void testScheduleStageSkipsDispatchWhenStageEntersTerminalDuringStart() {
        FakeStage stage = new FakeStage();  // empty tasks
        stage.startTarget = StageExecution.State.SUCCEEDED;  // start() goes CREATED→SUCCEEDED directly
        scheduler.scheduleStage(stage);
        assertTrue("no tasks dispatched", stage.runner.dispatched.isEmpty());
    }

    /** handle.onCompleted → task FINISHED + stage.onTaskTerminal(task, null). */
    public void testHandleOnCompletedTransitionsTaskAndCallsOnTerminal() {
        FakeStage stage = new FakeStage(makeTask(0));
        scheduler.scheduleStage(stage);
        ActionListener<Void> handle = stage.runner.dispatched.get(0).handle;
        handle.onResponse(null);

        assertEquals(StageTaskState.FINISHED, stage.tasks().get(0).state());
        assertEquals("onTaskTerminal called with no cause", 1, stage.terminalCalls.get());
        assertNull(stage.lastTerminalCause.get());
    }

    /** Default path (no retry): handle.onFailed → task FAILED + stage.onTaskTerminal(task, cause). */
    public void testHandleOnFailedPropagatesWhenNoRetryAvailable() {
        FakeStage stage = new FakeStage(makeTask(0));
        scheduler.scheduleStage(stage);
        ActionListener<Void> handle = stage.runner.dispatched.get(0).handle;

        RuntimeException cause = new RuntimeException("boom");
        handle.onFailure(cause);

        assertEquals(StageTaskState.FAILED, stage.tasks().get(0).state());
        assertSame(cause, stage.lastTerminalCause.get());
    }

    /**
     * Retry-aware stage with exhaustion: first onFailed → retargetForRetry returns alternate →
     * scheduler dispatches alternate via runner with the SAME handle. Second onFailed →
     * retarget returns empty → propagates to onTaskTerminal. Pins the contract that BOTH
     * the original attempt and the retry attempt end in FAILED — neither is left as RUNNING.
     */
    public void testRetryExhaustionMarksBothOriginalAndRetryAttemptsFailed() {
        StageTask originalTask = makeTask(0);
        FakeStage stage = new FakeStage(originalTask);
        StageTask retryTask = makeTask(0);  // same slot id — retry is a new attempt at the same partition
        stage.retryQueue.add(Optional.of(retryTask));  // first onFailed → retry
        stage.retryQueue.add(Optional.empty());        // second onFailed → give up

        scheduler.scheduleStage(stage);
        assertEquals("initial dispatch", 1, stage.runner.dispatched.size());
        ActionListener<Void> handle = stage.runner.dispatched.get(0).handle;

        RuntimeException first = new RuntimeException("first attempt failed");
        handle.onFailure(first);

        assertEquals("retry attempt dispatched via same runner", 2, stage.runner.dispatched.size());
        assertSame(retryTask, stage.runner.dispatched.get(1).task);
        assertSame("retry reuses the original handle", handle, stage.runner.dispatched.get(1).handle);
        assertEquals("original attempt marked FAILED when superseded by retry", StageTaskState.FAILED, originalTask.state());
        assertEquals("retry attempt transitioned to RUNNING", StageTaskState.RUNNING, retryTask.state());
        assertEquals("no terminal yet — stage stays RUNNING during retry", 0, stage.terminalCalls.get());

        // Second failure on the retry — stage has exhausted alternates.
        RuntimeException second = new RuntimeException("retry attempt failed");
        handle.onFailure(second);

        assertEquals("no further dispatch", 2, stage.runner.dispatched.size());
        assertEquals("terminal propagation after retry exhausted", 1, stage.terminalCalls.get());
        assertSame("second failure propagated as terminal cause", second, stage.lastTerminalCause.get());
        assertEquals("retry attempt marked FAILED on exhaustion", StageTaskState.FAILED, retryTask.state());
    }

    /**
     * Retry then success: original attempt fails, retry succeeds. Pins the contract
     * that the superseded original ends in FAILED and the retry ends in FINISHED — neither
     * is left in RUNNING. This was the gap that motivated tracking the current attempt
     * in the handle.
     */
    public void testRetrySuccessTransitionsCorrectTaskStates() {
        StageTask originalTask = makeTask(0);
        FakeStage stage = new FakeStage(originalTask);
        StageTask retryTask = makeTask(0);
        stage.retryQueue.add(Optional.of(retryTask));

        scheduler.scheduleStage(stage);
        ActionListener<Void> handle = stage.runner.dispatched.get(0).handle;

        // First attempt fails → dispatch retry
        handle.onFailure(new RuntimeException("first attempt failed"));
        assertEquals(StageTaskState.FAILED, originalTask.state());
        assertEquals(StageTaskState.RUNNING, retryTask.state());

        // Retry succeeds — retry attempt transitions to FINISHED, original stays FAILED.
        handle.onResponse(null);
        assertEquals("retry attempt marked FINISHED on success", StageTaskState.FINISHED, retryTask.state());
        assertEquals("superseded original attempt stays FAILED", StageTaskState.FAILED, originalTask.state());
        assertEquals("terminal callback fired exactly once", 1, stage.terminalCalls.get());
        assertNull("success path passes null cause to onTaskTerminal", stage.lastTerminalCause.get());
    }

    // ─── helpers ──────────────────────────────────────────────────────────

    private static StageTask makeTask(int partitionId) {
        return new ShardStageTask(new StageTaskId(0, partitionId), new ExecutionTarget(null) {
        });
    }

    /** Records every run() invocation with its handle so tests can drive lifecycle manually. */
    private static final class CapturingRunner implements TaskRunner<StageTask> {
        final List<Dispatch> dispatched = new ArrayList<>();

        @Override
        public void run(StageTask task, ActionListener<Void> handle) {
            dispatched.add(new Dispatch(task, handle));
        }

        record Dispatch(StageTask task, ActionListener<Void> handle) {
        }
    }

    /**
     * Test stage implementing {@link StageExecution} directly. Bypasses AbstractStageExecution
     * so the test isolates scheduler behavior from state-machine machinery.
     */
    private static final class FakeStage implements StageExecution {
        final CapturingRunner runner = new CapturingRunner();
        final List<StageTask> tasks;
        final AtomicInteger terminalCalls = new AtomicInteger();
        final AtomicReference<Exception> lastTerminalCause = new AtomicReference<>();
        final List<Optional<StageTask>> retryQueue = new ArrayList<>();
        State state = State.CREATED;
        State startTarget = State.RUNNING;  // override to simulate empty-target → SUCCEEDED, etc.

        FakeStage(StageTask... tasks) {
            this.tasks = List.of(tasks);
        }

        @Override
        public int getStageId() {
            return 0;
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
        public void start() {
            state = startTarget;
        }

        @Override
        public void addStateListener(StageStateListener listener) {}

        @Override
        public Exception getFailure() {
            return lastTerminalCause.get();
        }

        @Override
        public void cancel(String reason) {}

        @Override
        public List<StageTask> tasks() {
            return tasks;
        }

        @Override
        public TaskRunner taskRunner() {
            return runner;
        }

        @Override
        public void onTaskTerminal(StageTask task, Exception cause) {
            terminalCalls.incrementAndGet();
            lastTerminalCause.set(cause);
        }

        @Override
        public Optional<StageTask> retargetForRetry(StageTask failed, Exception cause) {
            return retryQueue.isEmpty() ? Optional.empty() : retryQueue.remove(0);
        }
    }
}
