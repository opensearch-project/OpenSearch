/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.exec.task.TaskRunner;
import org.opensearch.common.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * One-shot execution unit for a single stage.
 *
 * @opensearch.internal
 */
public interface StageExecution {

    int getStageId();

    State getState();

    StageMetrics getMetrics();

    /** CREATED → RUNNING; initiates stage-specific dispatch logic. Called at most once. */
    void start();

    /** Append-only; register before {@link #start()}. Fired synchronously on every transition. */
    void addStateListener(StageStateListener listener);

    /** Non-null only when state is {@link State#FAILED}. */
    @Nullable
    Exception getFailure();

    /** Capture {@code cause} and transition to FAILED. Returns true if the transition happened. */
    default boolean failWithCause(Exception cause) {
        return false;
    }

    /** Idempotent. Transitions to CANCELLED and tears down stage-owned resources. */
    void cancel(String reason);

    // ── Scheduler-driven dispatch hooks ───────────────────────────────────

    /** Empty until {@link #start()} populates from resolved targets. */
    default List<StageTask> tasks() {
        return List.of();
    }

    /** Default {@link TaskRunner#NONE} — stages with runnable tasks override. */
    default TaskRunner<?> taskRunner() {
        return TaskRunner.NONE;
    }

    /** Per-task terminal callback. Captures failure / drives the stage's terminal transition. */
    default void onTaskTerminal(StageTask task, @Nullable Exception cause) {}

    /**
     * Per-stage retry seam. Return a fresh task pointing at an alternate target, or
     * {@code Optional.empty()} to give up (default). Owns the entire retry policy —
     * attempt budget, retryable exception classes, alternate selection. The scheduler's
     * {@code handleFor} consults this on every failure and dispatches the alternate if any.
     */
    default Optional<StageTask> retargetForRetry(StageTask failed, Exception cause) {
        return Optional.empty();
    }

    // ── Cross-stage metadata channel ──────────────────────────────────────

    /**
     * Control-plane payload (broadcast bytes, per-shard stats) handed to the parent via
     * {@link #consumeChildMetadata} before the parent is scheduled. Distinct from data flow
     * through the pre-wired {@code ExchangeSink}. Default null (nothing published).
     */
    @Nullable
    default Object publishedMetadata() {
        return null;
    }

    /** Invoked by the cascade right before parent dispatch. Empty map when no children published. */
    default void consumeChildMetadata(Map<Integer, Object> metadataByChildStageId) {}

    /** Lifecycle states a stage execution moves through. */
    enum State {
        CREATED,
        RUNNING,
        SUCCEEDED,
        FAILED,
        CANCELLED;

        public boolean isTerminal() {
            return this == SUCCEEDED || this == FAILED || this == CANCELLED;
        }
    }
}
