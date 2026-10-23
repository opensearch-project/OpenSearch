/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.analytics.exec.stage.DataProducer;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.transport.stream.StreamException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Per-query execution: query-level state machine (CREATED → RUNNING → terminal),
 * single-fire listener + cleanup, root-state mirror that drives query terminal from
 * the root stage's terminal. {@link #cancelAll} is the external cancel entry; {@link #close}
 * is end-of-life cleanup fired automatically from any terminal transition.
 *
 * @opensearch.internal
 */
public class QueryExecution {

    private static final Logger logger = LogManager.getLogger(QueryExecution.class);

    private final QueryContext config;
    private final ExecutionGraph graph;
    private final Consumer<StageExecution> scheduler;
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

    public QueryExecution(
        QueryContext config,
        ExecutionGraph graph,
        Consumer<StageExecution> scheduler,
        ActionListener<Iterable<VectorSchemaRoot>> listener
    ) {
        this.config = config;
        this.graph = graph;
        this.scheduler = scheduler;
        this.listener = listener;
        graph.rootExecution().addStateListener(this::mirrorRootStateToQuery);
    }

    /** Start execution by scheduling leaf stages bottom-up. */
    public void start() {
        if (transitionTo(State.RUNNING) == false) {
            return;
        }
        for (StageExecution leaf : graph.leaves()) {
            scheduler.accept(leaf);
        }
    }

    /** Mirrors root terminal → query terminal. CREATED/RUNNING ignored. */
    private void mirrorRootStateToQuery(StageExecution.State from, StageExecution.State to) {
        switch (to) {
            case SUCCEEDED -> transitionTo(State.SUCCEEDED);
            case FAILED -> transitionTo(State.FAILED);
            case CANCELLED -> transitionTo(State.CANCELLED);
            default -> {
            }
        }
    }

    /**
     * Top-down cancel. Idempotent. The explicit final {@code transitionTo} is a safety net
     * for the case where {@code root.cancel} throws and the root-state mirror never fires.
     * Bottom-up cancel (external task cancel → leaves fail → cascade) reaches the same
     * terminal — the state CAS reconciles whichever wins.
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

    public State getState() {
        return state.get();
    }

    /** Returns the execution graph for post-execution inspection (e.g. profiling). */
    public ExecutionGraph getGraph() {
        return graph;
    }

    /**
     * Single-fire cleanup — closes terminal sink + per-query context. Each step under
     * {@link #runQuietly} so a failure in one doesn't skip the other. Auto-fired from any
     * terminal transition; safe to call directly.
     */
    public void close() {
        if (closed.compareAndSet(false, true) == false) return;
        runQuietly("terminal sink close", this::closeTerminalSink);
        logAllocatorState();
        runQuietly("query context close", config::close);
    }

    private void logAllocatorState() {
        if (!config.ownsAllocator()) return;
        BufferAllocator allocator = config.bufferAllocator();
        long allocated = allocator.getAllocatedMemory();
        if (allocated > 0) {
            logger.warn("[query-{}] Arrow allocator closing with {}B still allocated — potential leak", config.queryId(), allocated);
        } else {
            logger.debug("[query-{}] Arrow allocator closed cleanly", config.queryId());
        }
    }

    // ─── Internal: query-level state machine ─────────────────────────────

    /** On terminal transition: fires user listener exactly once + runs {@link #close()}. */
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
     * Resolves the exception handed to the completion listener on a non-success terminal.
     *
     * <p>A captured stage failure is the <em>true</em> cause and wins, even when the parent
     * task is also cancelled: a query self-cancels its remaining stages as a consequence of
     * the first stage failure (e.g. a {@code ReduceSizeExceededException} or an Arrow OOM),
     * so reporting only {@code TaskCancelledException} would mask the actionable reason —
     * the user sees "query cancelled" instead of "raise the buffer limit / narrow the query".
     * The root stage's failure is preferred; otherwise the first captured failure across the
     * graph (the failing stage is often a child reduce/shard stage that cascaded up).
     *
     * <p>Two memory-pressure failures are re-typed to {@link CircuitBreakingException} (HTTP 429)
     * before being returned, so back-pressure is reported as 429 and {@code FailAwareWeightedRouting}
     * skips replica retry (preventing retry storms):
     * <ul>
     *   <li><b>Breaker buried in the cause chain</b> — a {@link CircuitBreakingException} wrapped by an
     *       outer exception is unwrapped and surfaced directly so {@code status()} = 429 (PR #22275).
     *   <li><b>Arrow allocator exhaustion</b> — {@link OutOfMemoryException} from {@code BufferAllocator}
     *       is allocation back-pressure (rejected against {@code native.allocator.pool.query.max}), NOT a
     *       JVM {@code OutOfMemoryError}; {@link #arrowOomAsBreaker} translates it to a 429, covering both
     *       the in-process path (class identity intact) and the cross-Flight-RPC path, where the wire
     *       envelope strips the class to a {@code StreamException} carrying Arrow's
     *       {@code "Unable to allocate buffer"} marker.
     * </ul>
     *
     * <p>Only when NO stage captured a failure is the terminal a genuine external cancel
     * (client disconnect, admin task cancel) — then {@code TaskCancelledException} is the
     * honest answer. The final synthetic fallback covers a CANCELLED/FAILED terminal with no
     * recorded cause at all.
     *
     * <p>TODO: replace this reactive translation with proactive circuit-breaking via Arrow's
     * {@code AllocationListener} — wired to register against the parent breaker, so the budget check
     * happens at allocation request time and a {@code CircuitBreakingException} is raised natively
     * (no unwrap dance). Until then, this is the chokepoint.
     */
    private Exception terminalCause(State terminal) {
        Exception failure = firstStageFailure();
        if (failure != null) {
            Throwable breaker = ExceptionsHelper.unwrap(failure, CircuitBreakingException.class);
            if (breaker != null) {
                return (CircuitBreakingException) breaker;
            }
            CircuitBreakingException arrowOom = arrowOomAsBreaker(failure);
            if (arrowOom != null) {
                return arrowOom;
            }
            return failure;
        }
        if (config.parentTask() instanceof CancellableTask ct && ct.isCancelled()) {
            return new TaskCancelledException("query cancelled");
        }
        return new RuntimeException("Stage " + graph.rootExecution().getStageId() + " " + terminal);
    }

    /** Root stage's captured failure if present, else the first captured failure across the graph. */
    private Exception firstStageFailure() {
        Exception rootFailure = graph.rootExecution().getFailure();
        if (rootFailure != null) {
            return rootFailure;
        }
        for (StageExecution exec : graph.allExecutions()) {
            Exception failure = exec.getFailure();
            if (failure != null) {
                return failure;
            }
        }
        return null;
    }

    /**
     * If {@code failure}'s cause chain carries an Arrow allocator exhaustion — either as a real
     * {@link OutOfMemoryException} (in-process) or as a {@link StreamException} whose message contains
     * Arrow's allocator-refusal marker (post-Flight-RPC, where the wire envelope stripped the class) —
     * return a fresh {@link CircuitBreakingException} (HTTP 429). Otherwise {@code null}.
     *
     * <p>Arrow's {@code OutOfMemoryException} is the allocator's budget-refusal signal, not a JVM OOM,
     * so it belongs in the same 429/back-pressure class as {@code CircuitBreakingException}. The string
     * marker is Arrow's stable {@code BaseAllocator.wrapForeignAllocation} message ("Unable to allocate
     * buffer ... due to memory limit"); this is the only producer of that exact prefix in Arrow Java.
     */
    private static CircuitBreakingException arrowOomAsBreaker(Throwable failure) {
        Throwable arrowOom = ExceptionsHelper.unwrap(failure, OutOfMemoryException.class);
        if (arrowOom != null) {
            return wrapAsBreaker("native memory allocation rejected: " + arrowOom.getMessage(), failure);
        }
        Throwable streamFailure = ExceptionsHelper.unwrap(failure, StreamException.class);
        if (streamFailure instanceof StreamException se && carriesArrowOomMessage(se)) {
            return wrapAsBreaker("native memory allocation rejected: " + se.getMessage(), failure);
        }
        return null;
    }

    private static boolean carriesArrowOomMessage(StreamException se) {
        String msg = se.getMessage();
        return msg != null && msg.contains("Unable to allocate buffer");
    }

    private static CircuitBreakingException wrapAsBreaker(String message, Throwable cause) {
        CircuitBreakingException cbe = new CircuitBreakingException(message, CircuitBreaker.Durability.TRANSIENT);
        cbe.initCause(cause);
        return cbe;
    }

    /** Releases buffered terminal-sink batches. Arrow leak/double-release surfaces via {@link #runQuietly}. */
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
