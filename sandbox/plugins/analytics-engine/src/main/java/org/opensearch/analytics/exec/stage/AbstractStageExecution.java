/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.analytics.backend.AnalyticsOperationListener;
import org.opensearch.analytics.exec.task.TaskRunner;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.common.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Shared mechanics for every {@link StageExecution} variant: state CAS, listener
 * fire loop, failure capture, metrics, default {@code tasks}/{@code runner} accessors,
 * default {@code onTaskTerminal}/{@code cancel}/{@code failWithCause} impls.
 *
 * <p>Subclasses typically implement only {@link #start()} (materialise tasks +
 * {@link #publishTasksAndStart}) and override {@link #cancel}/{@link #failWithCause}
 * if they need stage-specific cleanup ordered relative to terminal listeners.
 *
 * @opensearch.internal
 */
abstract class AbstractStageExecution implements StageExecution {

    private static final Logger logger = LogManager.getLogger(AbstractStageExecution.class);

    protected final Stage stage;
    protected final StageMetrics metrics;
    protected TaskRunner<?> runner = TaskRunner.NONE;
    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);
    private final AtomicReference<Exception> failure = new AtomicReference<>();
    private final List<StageStateListener> stateListeners = new ArrayList<>();
    private final List<AnalyticsOperationListener> operationListeners;
    private final String queryId;
    private final AtomicInteger pendingTaskTerminal = new AtomicInteger(0);
    private volatile List<StageTask> tasks = Collections.emptyList();

    protected AbstractStageExecution(Stage stage) {
        this(stage, null, List.of());
    }

    protected AbstractStageExecution(Stage stage, String queryId, List<AnalyticsOperationListener> operationListeners) {
        this.stage = stage;
        this.queryId = queryId;
        this.operationListeners = operationListeners;
        this.metrics = new StageMetrics();
    }

    @Override
    public final int getStageId() {
        return stage.getStageId();
    }

    @Override
    public final State getState() {
        return state.get();
    }

    @Override
    public final StageMetrics getMetrics() {
        return metrics;
    }

    @Override
    @Nullable
    public final Exception getFailure() {
        return failure.get();
    }

    @Override
    public final void addStateListener(StageStateListener listener) {
        stateListeners.add(listener);
    }

    @Override
    public final List<StageTask> tasks() {
        return tasks;
    }

    @Override
    public TaskRunner<?> taskRunner() {
        return runner;
    }

    /**
     * Default lifecycle on per-task terminal: fast-fail on any failure, otherwise
     * decrement the pending counter and transition to SUCCEEDED/FAILED once every
     * task is terminal. Retry (when {@link #retargetForRetry} is overridden)
     * intercepts BEFORE this fires — see {@code QueryScheduler.handleFor}.
     */
    @Override
    public void onTaskTerminal(StageTask task, @Nullable Exception cause) {
        if (cause != null) {
            captureFailure(cause);
            transitionTo(State.FAILED);
            return;
        }
        if (recordTaskTerminal(null)) {
            Exception captured = getFailure();
            transitionTo(captured != null ? State.FAILED : State.SUCCEEDED);
        }
    }

    /**
     * Default cancel: idempotent transition to CANCELLED + sweep tasks. Stage-specific
     * cleanup is best done by overriding this method (close resources before calling
     * super.cancel if cleanup must happen before terminal listeners fire).
     */
    @Override
    public void cancel(String reason) {
        if (transitionTo(State.CANCELLED) == false) return;
        for (StageTask t : tasks) {
            t.transitionTo(StageTaskState.CANCELLED);
        }
    }

    /**
     * Default fail-with-cause: capture, transition to FAILED. Stage-specific cleanup
     * is best done by overriding this method (close resources before calling super if
     * cleanup must happen before terminal listeners fire — they may tear down
     * query-level resources the cleanup needs).
     */
    @Override
    public boolean failWithCause(Exception cause) {
        captureFailure(cause);
        return transitionTo(State.FAILED);
    }

    /**
     * Attempt to transition to {@code target}. No-op + returns {@code false} when the
     * current state is already terminal or equal to {@code target}. On a successful
     * transition: stamps metrics, fires state listeners (registration order), fires
     * operation listeners. CREATED→terminal also synthesises the onStageStart event so
     * observers see a start before the terminal.
     */
    protected final boolean transitionTo(State target) {
        State previous;
        do {
            previous = state.get();
            if (previous.isTerminal() || previous == target) {
                return false;
            }
        } while (state.compareAndSet(previous, target) == false);

        if (previous == State.CREATED) {
            metrics.recordStart();
        }
        if (target.isTerminal()) {
            metrics.recordEnd();
        }

        for (StageStateListener l : stateListeners) {
            try {
                l.onStateChange(previous, target);
            } catch (Exception e) {
                logger.warn(
                    new ParameterizedMessage(
                        "[StageExecution] listener threw for stage {} transition {} -> {}",
                        getStageId(),
                        previous,
                        target
                    ),
                    e
                );
            }
        }

        fireOperationListeners(previous, target);
        return true;
    }

    private void fireOperationListeners(State previous, State target) {
        if (operationListeners.isEmpty() || queryId == null) return;
        String stageType = getClass().getSimpleName();
        int sid = getStageId();
        // Synthesise onStageStart when a stage skips RUNNING (CREATED → terminal),
        // so observers always see a start before any terminal event.
        if (previous == State.CREATED && target.isTerminal()) {
            fireSingle(target, sid, stageType, l -> l.onStageStart(queryId, sid, stageType));
        }
        switch (target) {
            case RUNNING -> fireSingle(target, sid, stageType, l -> l.onStageStart(queryId, sid, stageType));
            case SUCCEEDED -> fireSingle(
                target,
                sid,
                stageType,
                l -> l.onStageSuccess(
                    queryId,
                    sid,
                    metrics.getEndTimeMs() > 0 && metrics.getStartTimeMs() > 0
                        ? (metrics.getEndTimeMs() - metrics.getStartTimeMs()) * 1_000_000L
                        : 0,
                    metrics.getRowsProcessed()
                )
            );
            case FAILED -> fireSingle(target, sid, stageType, l -> l.onStageFailure(queryId, sid, getFailure()));
            case CANCELLED -> fireSingle(target, sid, stageType, l -> l.onStageCancelled(queryId, sid, "stage cancelled"));
            default -> {
            }
        }
    }

    private void fireSingle(State target, int sid, String stageType, java.util.function.Consumer<AnalyticsOperationListener> call) {
        for (AnalyticsOperationListener l : operationListeners) {
            try {
                call.accept(l);
            } catch (Exception e) {
                logger.warn(new ParameterizedMessage("[StageExecution] operation listener threw for stage {} -> {}", sid, target), e);
            }
        }
    }

    /** Idempotent — only the first non-null failure is retained. */
    protected final void captureFailure(Exception e) {
        failure.compareAndSet(null, e);
    }

    /**
     * Subclasses call from {@link #start()} after materialising their task list.
     * No-op if state has advanced past CREATED (re-entry guard — defends against
     * a double-call corrupting bookkeeping).
     */
    protected final void publishTasksAndStart(List<StageTask> ts) {
        if (getState() != State.CREATED) return;
        this.tasks = List.copyOf(ts);
        pendingTaskTerminal.set(ts.size());
        transitionTo(State.RUNNING);
    }

    /**
     * Captures {@code cause}, decrements pending counter, returns {@code true} iff
     * every task is now terminal. Caller decides FAILED vs SUCCEEDED.
     */
    protected final boolean recordTaskTerminal(@Nullable Exception cause) {
        if (cause != null) {
            captureFailure(cause);
        }
        int remaining = pendingTaskTerminal.decrementAndGet();
        assert remaining >= 0 : "task terminal counter went negative: " + remaining;
        return remaining == 0;
    }

    @Override
    public abstract void start();
}
