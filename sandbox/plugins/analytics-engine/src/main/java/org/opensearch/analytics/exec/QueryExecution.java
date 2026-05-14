/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.analytics.exec.stage.DataProducer;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.tasks.CancellableTask;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-query execution. Holds a pre-built {@link ExecutionGraph}, a query-level
 * state machine (CREATED → RUNNING → SUCCEEDED|FAILED|CANCELLED), and end-to-end
 * resource cleanup. The state machine drives both single-fire listener
 * notification and a single-fire {@link #close()} that releases the terminal
 * sink and the per-query buffer allocator. Caller-side cleanup (e.g.
 * {@code TaskManager.unregister}) is wrapped onto the listener via
 * {@code ActionListener.runAfter} — the state machine's single-fire guarantee
 * makes that the natural attachment point.
 *
 * <p>{@link #cancelAll(String)} is the fast-path external cancellation entry
 * (parent-task cancel, timeout) — it cancels every stage and drives the query
 * to CANCELLED. {@link #close()} is the end-of-life cleanup, called
 * automatically from any terminal state transition.
 *
 * <p>Construction wires a root-stage listener that drives query-level
 * transitions. {@link #start()} dispatches the leaf stages, triggering the
 * event-driven cascade up to the root. Build the graph with
 * {@link ExecutionGraph#build} before constructing.
 *
 * @opensearch.internal
 */
public class QueryExecution {

    private static final Logger logger = LogManager.getLogger(QueryExecution.class);

    private final QueryContext config;
    private final ExecutionGraph graph;
    private final ActionListener<Iterable<VectorSchemaRoot>> listener;
    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** Lifecycle states a query execution moves through. */
    public enum State {
        CREATED,
        RUNNING,
        SUCCEEDED,
        FAILED,
        CANCELLED
    }

    public QueryExecution(QueryContext config, ExecutionGraph graph, ActionListener<Iterable<VectorSchemaRoot>> listener) {
        this.config = config;
        this.graph = graph;
        this.listener = listener;
        // Mirror the root stage's terminal state into the query state machine.
        graph.rootExecution().addStateListener((from, to) -> {
            switch (to) {
                case SUCCEEDED -> transitionTo(State.SUCCEEDED);
                case FAILED -> transitionTo(State.FAILED);
                case CANCELLED -> transitionTo(State.CANCELLED);
                // CREATED / RUNNING — no query-level transition; the query is
                // already past CREATED by the time the root reports RUNNING.
                default -> {
                }
            }
        });
    }

    /** Start execution by dispatching leaf stages bottom-up. */
    public void start() {
        if (transitionTo(State.RUNNING) == false) {
            return;
        }
        for (StageExecution leaf : graph.leaves()) {
            leaf.start();
        }
    }

    /**
     * Top-down cancel. Calls {@code cancel(reason)} on every stage execution
     * (per-stage state CAS makes it a no-op when already terminal), then
     * transitions the query itself to {@link State#CANCELLED}. Idempotent.
     */
    public void cancelAll(String reason) {
        for (StageExecution exec : graph.allExecutions()) {
            try {
                exec.cancel(reason);
            } catch (Exception e) {
                logger.warn(
                    new ParameterizedMessage("[QueryExecution] cancel threw for stageId={} state={}", exec.getStageId(), exec.getState()),
                    e
                );
            }
        }
        transitionTo(State.CANCELLED);
    }

    /** Current query-level state. */
    public State getState() {
        return state.get();
    }

    /**
     * Single-fire query-level cleanup: closes the terminal sink and the
     * per-query buffer allocator. Each step runs under {@link #runQuietly}
     * so a failure in one does not skip the other. Called automatically from
     * any terminal state transition; safe to call directly.
     */
    public void close() {
        if (closed.compareAndSet(false, true) == false) return;
        runQuietly("terminal sink close", this::closeTerminalSink);
        // TODO: Re-evaluate this per query child allocator
        runQuietly("buffer allocator close", config::closeBufferAllocator);
    }

    // ─── Internal: query-level state machine ─────────────────────────────

    /**
     * Attempt to transition to {@code target}. No-op + returns {@code false} if
     * the current state is already terminal or equal to {@code target}. On a
     * successful transition into a terminal state, fires the user-facing
     * completion listener exactly once and runs {@link #close()}.
     */
    private boolean transitionTo(State target) {
        State previous;
        do {
            previous = state.get();
            if (isTerminal(previous) || previous == target) {
                return false;
            }
        } while (state.compareAndSet(previous, target) == false);

        if (isTerminal(target)) {
            try {
                fireListener(target);
            } finally {
                close();
            }
        }
        return true;
    }

    private void fireListener(State terminal) {
        if (terminal == State.SUCCEEDED) {
            DataProducer producer = (DataProducer) graph.rootExecution();
            listener.onResponse(producer.outputSource().readResult());
        } else {
            listener.onFailure(terminalCause(terminal));
        }
    }

    /**
     * Resolves the failure cause for FAILED/CANCELLED terminals. A live parent-task
     * cancellation always wins — surfacing "query cancelled" to the user instead of
     * the stage's downstream FAILED cause keeps the message accurate. Otherwise
     * propagate the captured stage failure, with a synthetic fallback when no
     * cause was captured (e.g. CANCELLED with no upstream failure).
     */
    private Exception terminalCause(State terminal) {
        if (config.parentTask() instanceof CancellableTask ct && ct.isCancelled()) {
            return new TaskCancelledException("query cancelled");
        }
        StageExecution rootExec = graph.rootExecution();
        Exception failure = rootExec.getFailure();
        return failure != null ? failure : new RuntimeException("Stage " + rootExec.getStageId() + " " + terminal);
    }

    /**
     * Releases any batches buffered in the terminal sink. Arrow's allocator and
     * FFI close paths throw {@code IllegalStateException} on leak / double-release;
     * {@link #runQuietly} swallows so a cleanup failure does not replace the
     * primary stage failure en route to the completion listener.
     */
    private void closeTerminalSink() {
        StageExecution rootExec = graph.rootExecution();
        if (rootExec instanceof DataProducer producer && producer.outputSource() instanceof ExchangeSink sink) {
            sink.close();
        }
    }

    private void runQuietly(String label, Runnable action) {
        try {
            action.run();
        } catch (Exception e) {
            logger.warn(new ParameterizedMessage("[QueryExecution] {} threw for query {}", label, config.queryId()), e);
        }
    }

    private static boolean isTerminal(State s) {
        return s == State.SUCCEEDED || s == State.FAILED || s == State.CANCELLED;
    }
}
