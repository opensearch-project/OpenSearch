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
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.exec.task.TaskRunner;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.common.Nullable;
import org.opensearch.core.action.ActionListener;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Shared mechanics for every {@link StageExecution} variant: state CAS, listener
 * fire loop, failure capture, metrics, default {@code tasks}/{@code runner} accessors,
 * default {@code onTaskTerminal}/{@code cancel}/{@code failWithCause} impls, and the
 * {@link #start} template (materialise → publish → transition).
 *
 * <p>Subclasses implement {@link #materializeTasks()} (what tasks to run) and
 * optionally {@link #onTerminalTransition(State)} (pre-listener cleanup). The base
 * drives every state transition.
 *
 * @opensearch.internal
 */
public abstract class AbstractStageExecution implements StageExecution {

    private static final Logger logger = LogManager.getLogger(AbstractStageExecution.class);

    protected final Stage stage;
    protected final StageMetrics metrics;
    protected TaskRunner<?> runner = TaskRunner.NONE;
    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);
    private final AtomicReference<Exception> failure = new AtomicReference<>();
    // state listeners are for stage to stage wiring.
    private final List<StageStateListener> stateListeners = new CopyOnWriteArrayList<>();
    // operation listeners - for metrics / observability callbacks.
    private final List<AnalyticsOperationListener> operationListeners;
    private final String queryId;
    private final AnalyticsQueryTask parentTask;
    private final AtomicInteger pendingTaskTerminal = new AtomicInteger(0);
    private volatile List<StageTask> tasks = Collections.emptyList();

    protected AbstractStageExecution(
        Stage stage,
        String queryId,
        List<AnalyticsOperationListener> operationListeners,
        AnalyticsQueryTask parentTask
    ) {
        this.stage = stage;
        this.queryId = queryId;
        this.operationListeners = operationListeners;
        this.parentTask = parentTask;
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
        // Defensive wrap so callers (QueryScheduler dispatch loop, tests) can't mutate the
        // stage-owned task list. Cheap — just a view wrapper, not a copy.
        return Collections.unmodifiableList(tasks);
    }

    @Override
    public TaskRunner<?> taskRunner() {
        return runner;
    }

    /**
     * Template: materialise task list (possibly async), publish, transition, then signal
     * {@code onStarted}. Subclasses customise via {@link #materializeTasks()} for synchronous
     * work or {@link #materializeTasksAsync} for deferred work (e.g. a can-match round-trip).
     *
     * <p>{@code onStarted.onResponse} fires after the post-materialisation transition (RUNNING
     * when there is work, terminal otherwise); {@code onStarted.onFailure} fires if
     * materialisation failed (the stage is FAILED by then). For the synchronous path all of this
     * happens inline before this method returns; for the async path it happens on the completion
     * thread — so callers must act via {@code onStarted}, not by polling {@link #getState()}
     * after the call.
     */
    @Override
    public final void start(ActionListener<Void> onStarted) {
        try {
            materializeTasksAsync(ActionListener.wrap(tasks -> {
                publishTasksAndStart(tasks);
                onStarted.onResponse(null);
            }, cause -> {
                failWithCause(cause);
                onStarted.onFailure(cause);
            }));
        } catch (Exception e) {
            failWithCause(e);
            onStarted.onFailure(e);
        }
    }

    /**
     * Build this stage's task list. Called once from {@link #start}. Return empty for
     * "nothing to do" — the base will short-circuit straight to SUCCEEDED. Throw to mark
     * the stage FAILED (e.g. target resolution failure).
     *
     * <p>Default sync path; override {@link #materializeTasksAsync} instead when work must
     * be deferred (network call, lock wait, etc.).
     */
    protected abstract List<StageTask> materializeTasks();

    /**
     * Async variant of {@link #materializeTasks()}. Default runs the sync method and invokes
     * the listener inline. Override when the work needs to defer publication — e.g.
     * {@code ShardFragmentStageExecution} dispatches a can-match round-trip before publishing.
     *
     * <p>Implementations MUST eventually call exactly one of {@code listener.onResponse}
     * or {@code listener.onFailure}. Synchronous exceptions thrown before either are caught
     * by {@link #start} and treated as failures.
     */
    protected void materializeTasksAsync(ActionListener<List<StageTask>> listener) {
        try {
            listener.onResponse(materializeTasks());
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Pre-listener cleanup hook. Fires inside {@link #transitionTo} on any terminal
     * transition (SUCCEEDED / FAILED / CANCELLED), BEFORE state listeners run. Use this
     * for stage-internal cleanup that must precede the listener cascade — e.g., closing
     * a backend sink before a query-level listener tears down the per-query allocator.
     * Default no-op. Exceptions are swallowed (logged) so cleanup failures don't block
     * listener delivery.
     */
    protected void onTerminalTransition(State terminal) {}

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
     * Ensure if this stage is cancelled, our parent stage is also cancelled if not already
     * This does NOT mean individual StageTask cancellation fails the parent stages/query,
     * This only handles if a full child stage is cancelled, task management is handled
     * separately in
     */
    @Override
    public void cancel(String reason) {
        if (transitionTo(State.CANCELLED) == false) return;
        for (StageTask t : tasks) {
            t.transitionTo(StageTaskState.CANCELLED);
        }
        if (parentTask.isCancelled() == false) {
            parentTask.cancel(reason);
        }
    }

    /** Capture cause, transition to FAILED. Stage-internal cleanup runs via {@link #onTerminalTransition}. */
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
            try {
                onTerminalTransition(target);
            } catch (Exception e) {
                logger.warn(
                    new ParameterizedMessage("[StageExecution] onTerminalTransition threw for stage {} -> {}", getStageId(), target),
                    e
                );
            }
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
                    stageType,
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
     * Hands the materialised task list to the lifecycle: empty → straight to SUCCEEDED
     * (no work to do); otherwise publish + transition to RUNNING. Re-entry guard:
     * no-op if state has advanced past CREATED.
     */
    private void publishTasksAndStart(List<StageTask> ts) {
        if (getState() != State.CREATED) return;
        if (ts.isEmpty()) {
            transitionTo(State.SUCCEEDED);
            return;
        }
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

}
