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
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.common.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Consolidates the shared mechanics of every {@link StageExecution} variant:
 * state CAS, listener fire loop, failure capture, metrics, and stage-id
 * accessors. Subclasses implement only {@link #start()} and
 * {@link #cancel(String)}.
 *
 * <p>Listener registration is append-only and must happen before {@link #start()}
 * is called (during {@code PlanWalker.walk()} construction phase). After that
 * point the list is effectively frozen.
 *
 * @opensearch.internal
 */
abstract class AbstractStageExecution implements StageExecution {

    protected final Stage stage;
    protected final StageMetrics metrics;
    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);
    private final AtomicReference<Exception> failure = new AtomicReference<>();
    private final List<StageStateListener> stateListeners = new ArrayList<>();
    private final List<AnalyticsOperationListener> operationListeners;
    private final String queryId;
    private static final Logger logger = LogManager.getLogger(AbstractStageExecution.class);

    protected AbstractStageExecution(Stage stage) {
        this(stage, null, List.of());
    }

    protected AbstractStageExecution(Stage stage, String queryId, List<AnalyticsOperationListener> operationListeners) {
        this.stage = stage;
        this.queryId = queryId;
        this.operationListeners = operationListeners;
        this.metrics = new StageMetrics(stage.getStageId());
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

    /**
     * Attempt to transition the execution to the given target state. If the
     * current state is already terminal ({@link State#SUCCEEDED},
     * {@link State#FAILED}, {@link State#CANCELLED}) or equal to {@code target},
     * the call is a no-op and returns {@code false} — no listeners fire, no
     * state change. On a successful transition, metrics are updated and all
     * registered listeners fire in registration order with the
     * {@code (previous, target)} pair.
     *
     * <p><b>Metrics are driven entirely from this method.</b> Subclasses must
     * not call {@code metrics.recordStart()} or {@code metrics.recordEnd()}
     * directly. Start time is stamped on the <i>first</i> transition out of
     * {@link State#CREATED} (whether that target is {@code RUNNING} or a
     * direct terminal), and end time is stamped on any transition into a
     * terminal state. Stages that skip {@code RUNNING} (e.g. empty-target
     * scans that go straight from {@code CREATED} to {@code SUCCEEDED}) get
     * both stamps on the same call.
     *
     * <p>Callers should still gate terminal side effects (closing resources,
     * firing external callbacks) on the return value:
     * <pre>
     *     if (transitionTo(State.FAILED)) {
     *         ctx.close();
     *     }
     * </pre>
     *
     * @return {@code true} if the transition happened, {@code false} otherwise
     */
    protected final boolean transitionTo(State target) {
        State previous;
        do {
            previous = state.get();
            if (isTerminal(previous) || previous == target) {
                return false;
            }
        } while (state.compareAndSet(previous, target) == false);

        // Metrics: start is stamped on the first transition out of CREATED;
        // end is stamped on any transition into a terminal state. Both fire
        // on direct CREATED→terminal transitions.
        if (previous == State.CREATED) {
            metrics.recordStart();
        }
        if (isTerminal(target)) {
            metrics.recordEnd();
        }

        // Transition succeeded. Fire state listeners.
        for (StageStateListener l : stateListeners) {
            try {
                l.onStateChange(previous, target);
            } catch (Exception e) {
                logger.warn(
                    new ParameterizedMessage("[StageExecution] listener threw for stage {} transition {} -> {}", getStageId(), previous, target),
                    e
                );
            }
        }

        // Fire operation listeners for observability.
        fireOperationListeners(previous, target);

        return true;
    }

    private void fireOperationListeners(State previous, State target) {
        if (operationListeners.isEmpty() || queryId == null) return;
        String stageType = getClass().getSimpleName();
        int sid = getStageId();
        for (AnalyticsOperationListener l : operationListeners) {
            try {
                switch (target) {
                    case RUNNING -> l.onStageStart(queryId, sid, stageType);
                    case SUCCEEDED -> l.onStageSuccess(
                        queryId,
                        sid,
                        metrics.getEndTimeMs() > 0 && metrics.getStartTimeMs() > 0
                            ? (metrics.getEndTimeMs() - metrics.getStartTimeMs()) * 1_000_000L
                            : 0,
                        metrics.getRowsProcessed()
                    );
                    case FAILED -> l.onStageFailure(queryId, sid, getFailure());
                    case CANCELLED -> l.onStageCancelled(queryId, sid, "stage cancelled");
                    default -> {
                    }
                }
            } catch (Exception e) {
                logger.warn(new ParameterizedMessage("[StageExecution] operation listener threw for stage {} -> {}", sid, target), e);
            }
        }
    }

    private static boolean isTerminal(State s) {
        return s == State.SUCCEEDED || s == State.FAILED || s == State.CANCELLED;
    }

    /**
     * Records a failure exception. Idempotent — only the first non-null
     * failure is retained. Does NOT transition state; the caller is
     * responsible for following up with {@code transitionTo(State.FAILED)}.
     */
    protected final void captureFailure(Exception e) {
        failure.compareAndSet(null, e);
    }

    /**
     * Captures the given failure and attempts to transition to {@link State#FAILED}.
     * Called from the per-parent listener in {@code PlanWalker} when a child fails —
     * propagates the child's cause upward so the root terminal listener surfaces
     * the actual exception, not a synthetic "CANCELLED" message.
     *
     * <p>The failure is captured via {@link #captureFailure} BEFORE the transition
     * attempt. If the transition is rejected (e.g., this execution was already
     * FAILED due to a different child failing first), the capture has already
     * happened — the FIRST failure to reach {@code captureFailure} wins the
     * {@code AtomicReference.compareAndSet}.
     *
     * @return {@code true} if the transition happened, {@code false} otherwise
     */
    public boolean failFromChild(Exception cause) {
        captureFailure(cause);
        return transitionTo(State.FAILED);
    }

    @Override
    public abstract void start();

    @Override
    public abstract void cancel(String reason);
}
