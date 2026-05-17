/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A single dispatchable unit within a {@link StageExecution}. Sealed so the variant set
 * stays known at compile time — each variant carries the payload its transport needs:
 * {@link ShardStageTask} an {@link org.opensearch.analytics.planner.dag.ExecutionTarget}
 * (routing key for shard-fragment dispatch), {@link LocalStageTask} a {@link Runnable}
 * body executed on the per-query virtual-thread executor. Future variants (shuffle,
 * metadata, broadcast) will extend the {@code permits} list.
 *
 * <p>Per-attempt state and timestamps are common to all variants and live on this base.
 *
 * @opensearch.internal
 */
public abstract sealed class StageTask permits ShardStageTask, LocalStageTask {

    private final StageTaskId id;
    private final AtomicReference<StageTaskState> state = new AtomicReference<>(StageTaskState.CREATED);
    private volatile long startedAtMs;
    private volatile long finishedAtMs;

    protected StageTask(StageTaskId id) {
        this.id = id;
    }

    public StageTaskId id() {
        return id;
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
