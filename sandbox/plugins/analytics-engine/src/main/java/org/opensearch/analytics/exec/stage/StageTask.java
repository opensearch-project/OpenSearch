/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.planner.dag.ExecutionTarget;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A single dispatchable unit within a {@link StageExecution}. Wraps an
 * {@link ExecutionTarget} (the already-resolved node + shards + fragment bytes) with
 * mutable lifecycle state so the scheduler can track per-partition progress.
 *
 * <p>One stage produces N tasks: one per shard for SOURCE stages, one per hash
 * partition for HASH_PARTITIONED, one total for COORDINATOR. State transitions are
 * observed by {@link TaskTracker} — which in turn drives stage readiness.
 *
 * @opensearch.internal
 */
public final class StageTask {

    private final StageTaskId id;
    private final ExecutionTarget target;
    private final AtomicReference<StageTaskState> state = new AtomicReference<>(StageTaskState.CREATED);
    private volatile long startedAtMs;
    private volatile long finishedAtMs;

    public StageTask(StageTaskId id, ExecutionTarget target) {
        this.id = id;
        this.target = target;
    }

    public StageTaskId id() {
        return id;
    }

    public ExecutionTarget target() {
        return target;
    }

    public StageTaskState state() {
        return state.get();
    }

    /** Wall-clock millis stamped on the first successful transition to {@link StageTaskState#RUNNING}, or 0 if never dispatched. */
    public long startedAtMs() {
        return startedAtMs;
    }

    /** Wall-clock millis stamped on the first successful terminal transition, or 0 if still running. */
    public long finishedAtMs() {
        return finishedAtMs;
    }

    /**
     * Attempts to transition this task to {@code target}. Returns false if the task is
     * already in a terminal state — callers must gate terminal side effects on the return
     * value, just like {@link AbstractStageExecution#transitionTo}.
     *
     * <p>On a successful transition, wall-clock stamps are recorded: {@code startedAtMs}
     * on the first entry into {@link StageTaskState#RUNNING}, {@code finishedAtMs} on
     * the first entry into any terminal state. Rejected transitions never rewrite the
     * stamps.
     */
    public boolean transitionTo(StageTaskState target) {
        StageTaskState prev;
        do {
            prev = state.get();
            if (prev.isTerminal() || prev == target) return false;
        } while (state.compareAndSet(prev, target) == false);
        long now = System.currentTimeMillis();
        if (target == StageTaskState.RUNNING && startedAtMs == 0L) {
            startedAtMs = now;
        }
        if (target.isTerminal() && finishedAtMs == 0L) {
            finishedAtMs = now;
        }
        return true;
    }
}
