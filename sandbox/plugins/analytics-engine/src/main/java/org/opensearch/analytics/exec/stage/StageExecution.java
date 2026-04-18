/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.common.Nullable;

/**
 * One-shot execution unit for a single stage. Provides state, metrics,
 * lifecycle control, state-change observation, and a
 * cancellation hook. Implementations:
 * <ul>
 *   <li>{@link ShardFragmentStageExecution} — fan-out scan dispatch to data nodes</li>
 *   <li>{@link LocalStageExecution} — coordinator-local execution, backend-provided stage</li>
 * </ul>
 *
 * <p>Tracked in {@code PlanWalker.executions} for the duration of
 * execution so that {@code DefaultPlanExecutor} can push cancellation
 * to in-flight stages on failure.
 *
 * @opensearch.internal
 */
public interface StageExecution {

    int getStageId();

    State getState();

    StageMetrics getMetrics();

    /**
     * Begins execution of this stage. Transitions state from
     * {@link State#CREATED} to {@link State#RUNNING} and initiates
     * the stage-specific dispatch logic (shard fan-out, shuffle write,
     * local compute, etc.).
     *
     * <p>Must be called at most once. Behaviour is undefined if called
     * after the execution has already reached a terminal state.
     */
    void start();

    /**
     * Registers an observer that will be notified on every state
     * transition. Listeners are fired synchronously from within the
     * execution's {@code transitionTo(...)} helper; implementations
     * must be non-blocking and exception-safe.
     *
     * <p>Listeners must be registered during the setup phase, before
     * {@link #start()} is called. Registration after {@code start()}
     * is unsupported.
     *
     * @param listener the observer to register
     */
    void addStateListener(StageStateListener listener);

    /**
     * Returns the exception that caused this execution to fail, or
     * {@code null} if the execution has not failed. Non-null only when
     * {@link #getState()} is {@link State#FAILED}.
     *
     * @return the captured failure, or {@code null}
     */
    @Nullable
    Exception getFailure();

    /**
     * Captures the given failure and attempts to transition to {@link State#FAILED}.
     * Used by the per-parent listener in {@code PlanWalker} to propagate a child's
     * failure upward so the root terminal listener surfaces the actual exception.
     *
     * @return {@code true} if the transition happened, {@code false} otherwise
     */
    boolean failFromChild(Exception cause);

    /**
     * Idempotent. Transitions state to CANCELLED, tears down
     * stage-owned resources (in-flight transport, sinks, drain threads).
     */
    void cancel(String reason);

    enum State {
        CREATED,
        RUNNING,
        SUCCEEDED,
        FAILED,
        CANCELLED
    }
}
