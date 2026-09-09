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
import org.opensearch.core.action.ActionListener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * One-shot execution unit for a single stage.
 *
 * @opensearch.internal
 */
public interface StageExecution {

    int getStageId();

    State getState();

    StageMetrics getMetrics();

    /**
     * Starts the stage: materialise its task list, publish it, and transition. Called at most once.
     *
     * <p>Materialisation may be deferred behind a network round-trip (e.g. the can-match
     * pre-filter), so the stage can still be CREATED when this method returns and only reach its
     * post-materialisation state later, on the completion thread. {@code onStarted} signals when
     * materialisation has completed: {@code onResponse} after the stage has transitioned (RUNNING
     * when there is work, or a terminal state such as SUCCEEDED for empty targets), {@code
     * onFailure} if materialisation failed (the stage is already FAILED by then). Callers dispatch
     * by checking {@code getState() == RUNNING} inside {@code onResponse}. Pass {@link
     * ActionListener#wrap} no-ops when the caller does not need the signal.
     */
    void start(ActionListener<Void> onStarted);

    /** Append-only; register before {@link #start(ActionListener)}. Fired synchronously on every transition. */
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

    /** Empty until {@link #start} populates from resolved targets. */
    default List<StageTask> tasks() {
        return List.of();
    }

    /** Default {@link TaskRunner#NONE} — stages with runnable tasks override. */
    default TaskRunner<?> taskRunner() {
        return TaskRunner.NONE;
    }

    /**
     * Dispatch the stage's tasks. Default implementation iterates {@link #tasks()} eagerly
     * — one {@code runner.run} call per task up front. Stages may override to dispatch with a
     * different cadence.
     *
     * <p>{@code handleForFactory} is the scheduler's per-task listener builder (the same one
     * that carries retry / terminal logic); the scheduler owns the listener it hands them.
     */
    default void dispatchTasks(java.util.function.BiFunction<StageExecution, StageTask, ActionListener<Void>> handleForFactory) {
        @SuppressWarnings("unchecked")
        TaskRunner<StageTask> runner = (TaskRunner<StageTask>) taskRunner();
        for (StageTask task : tasks()) {
            task.transitionTo(StageTaskState.RUNNING);
            runner.run(task, handleForFactory.apply(this, task));
        }
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

    /**
     * Default {@code false}: parent scheduled on all-children-SUCCEEDED. {@code true} (eager):
     * scheduled on first-child-RUNNING — required for streaming-reduce shapes whose drain
     * must run concurrently with feeds to avoid deadlocking on a bounded input mpsc.
     */
    default boolean schedulesEagerly() {
        return false;
    }

    /**
     * Per-input EOF hook fired by the cascade on every child SUCCEEDED (independent of
     * {@link #schedulesEagerly()}). Default no-op; streaming reduce overrides to close the
     * just-finished child's sender. Failure paths fall through to the parent's terminal
     * close, which tears everything down regardless.
     */
    default void closeChildInput(int childStageId) {}

    // ── Cascade wiring (called at graph build time) ────────────────────────

    /**
     * Wires the child→parent state cascade and the reverse parent→sibling cancel sweep.
     * Called once at graph build time, before any child has transitioned out of CREATED.
     *
     * <p>Per-child child→parent listener:
     * <ul>
     *   <li>RUNNING — eager parents ({@link #schedulesEagerly}) schedule on the first
     *   child to enter RUNNING so their work can run concurrently with children's feeds.
     *   <li>SUCCEEDED — invokes {@link #closeChildInput} (ungated; no-op for backends with
     *   no per-child resources); decrements a counter; on zero, collects
     *   {@link #publishedMetadata} from each child and hands off via {@link #consumeChildMetadata};
     *   default-mode parents are scheduled here (eager parents already scheduled).
     *   <li>FAILED — invokes {@link #closeChildInput} then propagates via {@link #failWithCause}.
     *   <li>CANCELLED — invokes {@link #closeChildInput} (so a parent reduce drain sees EOF and
     *   unwinds) then propagates {@link #cancel} to the parent so it can't strand in RUNNING.
     * </ul>
     *
     * <p>Parent→sibling cancel sweep: on FAILED / CANCELLED, sweep still-running children.
     *
     * <p>Thread-safe under the documented contracts: counters are atomic; child state reads +
     * {@link #cancel} are idempotent; this runs during graph build (before any transition
     * fires), so listener registration never races with firing.
     */
    default void attachChildren(List<? extends StageExecution> children, Consumer<StageExecution> scheduler) {
        if (children.isEmpty()) return;

        boolean eager = schedulesEagerly();
        AtomicInteger pending = new AtomicInteger(children.size());
        AtomicInteger eagerScheduled = new AtomicInteger(0);  // fires scheduler.accept at most once for eager mode
        for (StageExecution child : children) {
            int childId = child.getStageId();
            child.addStateListener((from, to) -> {
                switch (to) {
                    case RUNNING -> {
                        if (eager && eagerScheduled.compareAndSet(0, 1)) {
                            scheduler.accept(this);
                        }
                    }
                    case SUCCEEDED -> {
                        closeChildInput(childId);  // per-input EOF; no-op when not multi-input
                        if (pending.decrementAndGet() == 0) {
                            Map<Integer, Object> metadata = new HashMap<>();
                            for (StageExecution c : children) {
                                Object payload = c.publishedMetadata();
                                if (payload != null) {
                                    metadata.put(c.getStageId(), payload);
                                }
                            }
                            consumeChildMetadata(metadata);
                            // Eager parents already scheduled on first child RUNNING; default-mode schedules here.
                            if (eager == false) {
                                scheduler.accept(this);
                            }
                        }
                    }
                    case FAILED -> {
                        closeChildInput(childId);
                        Exception cause = child.getFailure();
                        failWithCause(
                            cause != null
                                ? cause
                                : new RuntimeException("child stage " + child.getStageId() + " failed without recorded cause")
                        );
                    }
                    case CANCELLED -> {
                        closeChildInput(childId);
                        // A cancelled child can't produce a complete result, so the parent must reach
                        // terminal too — otherwise pending never drains and it strands in RUNNING (the
                        // phantom-task leak). Idempotent; no-op if the parent is already terminal.
                        cancel("child stage " + childId + " cancelled");
                    }
                    default -> {
                    }
                }
            });
        }

        // child.cancel is a no-op when already terminal, so this is safe under top-down sweeps.
        addStateListener((from, to) -> {
            if (to == State.FAILED || to == State.CANCELLED) {
                for (StageExecution child : children) {
                    if (child.getState().isTerminal() == false) {
                        child.cancel("parent " + getStageId() + " " + to);
                    }
                }
            }
        });
    }

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
