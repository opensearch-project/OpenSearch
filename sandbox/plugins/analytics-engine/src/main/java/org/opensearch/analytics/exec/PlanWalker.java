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
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-query walker that owns the execution graph. Two-phase lifecycle:
 * <ol>
 *   <li>{@link #build()} — walks the DAG, constructs all {@link StageExecution}
 *       instances, wires listeners, and returns an inspectable
 *       {@link ExecutionGraph}. No stages are started.</li>
 *   <li>{@link #start(ExecutionGraph)} — starts leaf stages, triggering
 *       the event-driven cascade.</li>
 * </ol>
 *
 * <p>The split enables EXPLAIN: call {@link #build()} to get the graph,
 * inspect it via {@link ExecutionGraph#explain()}, and optionally call
 * {@link #start(ExecutionGraph)} to execute.
 *
 * <p>The legacy {@link #walk()} method calls both phases for backward
 * compatibility.
 *
 * @opensearch.internal
 */
public class PlanWalker {

    private static final Logger logger = LogManager.getLogger(PlanWalker.class);

    private final QueryContext config;
    private final StageExecutionBuilder stageExecutionBuilder;
    private final AtomicBoolean terminalFired = new AtomicBoolean(false);
    private final ActionListener<Iterable<VectorSchemaRoot>> completionListener;
    private volatile ExecutionGraph graph;

    public PlanWalker(
        QueryContext config,
        StageExecutionBuilder stageExecutionBuilder,
        ActionListener<Iterable<VectorSchemaRoot>> listener
    ) {
        this.config = config;
        this.stageExecutionBuilder = stageExecutionBuilder;
        this.completionListener = listener;
    }

    /**
     * Phase 1: Build the execution graph without starting any stages.
     * All stages are in {@link StageExecution.State#CREATED} state.
     * The graph is inspectable for EXPLAIN.
     *
     * <p>The completion listener is NOT wired here — call {@link #wireCompletion()}
     * after build succeeds and before exposing the walker to cancellation triggers.
     * Build-time failures bubble unchecked; partial-graph cleanup runs in finally
     * to release any native handles backend factories had already allocated.
     *
     * @return the fully-wired execution graph
     */
    public ExecutionGraph build() {
        Map<Integer, StageExecution> executions = new HashMap<>();
        Stage rootStage = config.dag().rootStage();
        boolean success = false;
        try {
            StageExecution rootExec = stageExecutionBuilder.buildRootExecution(rootStage, config);
            executions.put(rootStage.getStageId(), rootExec);

            buildChildrenRecursively(executions, rootExec, rootStage);

            List<StageExecution> leaves = findLeaves(executions, rootStage);
            this.graph = new ExecutionGraph(config.queryId(), executions, rootExec, leaves);
            success = true;
            return this.graph;
        } finally {
            if (!success) cancelPartialBuild(executions);
        }
    }

    /**
     * Wires the completion listener on the root execution. Must be called after
     * {@link #build()} succeeds and before {@link #start(ExecutionGraph)} or any
     * external cancellation trigger ({@link #cancelAll(String)}). The listener
     * fires once on the root's terminal transition (SUCCEEDED / FAILED /
     * CANCELLED) — see {@link #wireCompletionListener(StageExecution)}.
     */
    public void wireCompletion() {
        ExecutionGraph g = this.graph;
        if (g == null) {
            throw new IllegalStateException("wireCompletion must be called after build() succeeds");
        }
        wireCompletionListener(g.rootExecution());
    }

    /**
     * Cancels every stage built before {@link #build()} threw. If a stage's cancel itself
     * throws, subsequent stages still get a chance to cancel — the first cancel failure
     * is rethrown at the end with later ones attached as suppressed.
     */
    private void cancelPartialBuild(Map<Integer, StageExecution> executions) {
        String reason = "stage-build failure";
        IllegalStateException primary = null;
        for (StageExecution exec : executions.values()) {
            try {
                exec.cancel(reason);
            } catch (IllegalStateException t) {
                if (primary == null) {
                    primary = t;
                } else {
                    primary.addSuppressed(t);
                }
            }
        }
        assert allTerminal(executions) : "cancelPartialBuild left non-terminal stages";
        if (primary != null) throw primary;
    }

    private static boolean allTerminal(Map<Integer, StageExecution> executions) {
        for (StageExecution exec : executions.values()) {
            StageExecution.State s = exec.getState();
            if (s != StageExecution.State.SUCCEEDED && s != StageExecution.State.FAILED && s != StageExecution.State.CANCELLED) {
                return false;
            }
        }
        return true;
    }

    /**
     * Start execution by dispatching leaf stages.
     * Must be called after {@link #build()}.
     *
     * @param executionGraph the graph returned by {@link #build()}
     */
    public void start(ExecutionGraph executionGraph) {
        for (StageExecution leaf : executionGraph.leaves()) {
            leaf.start();
        }
    }

    /**
     * Legacy single-call entry point. Builds the graph, wires the completion
     * listener, and starts execution in one shot.
     */
    public void walk() {
        ExecutionGraph g = build();
        wireCompletion();
        start(g);
    }

    /**
     * Top-down cancel: iterates all executions and cancels any in
     * {@code RUNNING} or {@code CREATED} state.
     */
    public void cancelAll(String reason) {
        ExecutionGraph g = this.graph;
        if (g == null) return;
        for (StageExecution exec : g.allExecutions()) {
            StageExecution.State state = exec.getState();
            if (state == StageExecution.State.RUNNING || state == StageExecution.State.CREATED) {
                try {
                    exec.cancel(reason);
                } catch (Exception e) {
                    logger.warn(new ParameterizedMessage("[PlanWalker] cancel threw for stageId={} state={}", exec.getStageId(), state), e);
                }
            }
        }
    }

    /**
     * Returns the built execution graph, or null if {@link #build()} hasn't been called.
     */
    public ExecutionGraph getGraph() {
        return graph;
    }

    public String getQueryId() {
        return config.queryId();
    }

    public QueryContext getConfig() {
        return config;
    }

    public Task getParentTask() {
        return config.parentTask();
    }

    public StageExecution executionFor(int stageId) {
        ExecutionGraph g = this.graph;
        return g != null ? g.executionFor(stageId) : null;
    }

    public Collection<StageExecution> activeExecutions() {
        ExecutionGraph g = this.graph;
        if (g == null) return List.of();
        return g.allExecutions().stream().filter(e -> e.getState() == StageExecution.State.RUNNING).toList();
    }

    public Collection<StageExecution> allExecutions() {
        ExecutionGraph g = this.graph;
        return g != null ? g.allExecutions() : List.of();
    }

    // ─── Internal graph construction ────────────────────────────────────

    private void buildChildrenRecursively(Map<Integer, StageExecution> executions, StageExecution parentExec, Stage parentStage) {
        List<Stage> children = parentStage.getChildStages();
        if (children.isEmpty()) {
            return;
        }

        AtomicInteger pendingChildren = new AtomicInteger(children.size());

        for (Stage child : children) {
            StageExecution childExec = stageExecutionBuilder.buildExecution(child, parentExec, config);
            executions.put(child.getStageId(), childExec);

            childExec.addStateListener((from, to) -> {
                switch (to) {
                    case SUCCEEDED -> {
                        if (pendingChildren.decrementAndGet() == 0) {
                            parentExec.start();
                        }
                    }
                    case FAILED, CANCELLED -> {
                        Exception cause = childExec.getFailure();
                        if (cause != null) {
                            parentExec.failFromChild(cause);
                        } else {
                            parentExec.cancel("child " + childExec.getStageId() + " " + to);
                        }
                    }
                    default -> {
                    }
                }
            });
            buildChildrenRecursively(executions, childExec, child);
        }
    }

    private void wireCompletionListener(StageExecution rootExec) {
        if ((rootExec instanceof DataProducer) == false) {
            throw new IllegalStateException("Root execution " + rootExec.getClass().getSimpleName() + " does not implement DataProducer");
        }
        final DataProducer producer = (DataProducer) rootExec;
        rootExec.addStateListener((from, to) -> {
            switch (to) {
                case SUCCEEDED -> fireTerminal(() -> completionListener.onResponse(producer.outputSource().readResult()));
                case FAILED, CANCELLED -> {
                    // Release any batches the root producer already accumulated before
                    // teardown. Success path closes batches in batchesToRows; failure path
                    // skips that, so without this the VSRs sitting in the terminal sink
                    // leak into the per-query allocator close check.
                    if (producer.outputSource() instanceof ExchangeSink sink) {
                        try {
                            sink.close();
                        } catch (IllegalStateException e) {
                            // Arrow's allocator and FFI close paths throw IllegalStateException
                            // on leak / double-release. Don't let a close failure replace the
                            // primary stage failure en route to the completion listener.
                            logger.warn(
                                new ParameterizedMessage("[PlanWalker] terminal sink close failed on {}", to),
                                e
                            );
                        }
                    }
                    Exception failure = rootExec.getFailure();
                    if (config.parentTask() instanceof CancellableTask ct && ct.isCancelled()) {
                        fireTerminal(() -> completionListener.onFailure(new TaskCancelledException("query cancelled")));
                    } else if (failure != null) {
                        fireTerminal(() -> completionListener.onFailure(failure));
                    } else {
                        fireTerminal(() -> completionListener.onFailure(new RuntimeException("Stage " + rootExec.getStageId() + " " + to)));
                    }
                }
                default -> {
                }
            }
        });
    }

    private static List<StageExecution> findLeaves(Map<Integer, StageExecution> executions, Stage rootStage) {
        final List<StageExecution> leaves = new ArrayList<>();
        collectLeaves(executions, rootStage, leaves);
        return leaves;
    }

    private static void collectLeaves(Map<Integer, StageExecution> executions, Stage stage, List<StageExecution> leaves) {
        if (stage.getChildStages().isEmpty()) {
            StageExecution exec = executions.get(stage.getStageId());
            if (exec != null) {
                leaves.add(exec);
            }
        } else {
            for (Stage child : stage.getChildStages()) {
                collectLeaves(executions, child, leaves);
            }
        }
    }

    private void fireTerminal(Runnable action) {
        if (terminalFired.compareAndSet(false, true)) {
            action.run();
        }
    }
}
