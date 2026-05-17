/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.exec.stage.StageMetrics;
import org.opensearch.analytics.planner.dag.Stage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Topology snapshot of a fully-wired execution graph — root, leaves, every stage
 * execution with its state listeners and cascade already attached. No stage has
 * been started; all are in {@link StageExecution.State#CREATED}.
 *
 * <p>Pure data: dispatch is the caller's concern (typically {@link QueryScheduler}).
 *
 * @opensearch.internal
 */
public class ExecutionGraph {

    private final Map<Integer, StageExecution> executions;
    private final StageExecution rootExecution;
    private final List<StageExecution> leaves;
    private final String queryId;

    ExecutionGraph(String queryId, Map<Integer, StageExecution> executions, StageExecution rootExecution, List<StageExecution> leaves) {
        this.queryId = queryId;
        this.executions = executions;
        this.rootExecution = rootExecution;
        this.leaves = leaves;
    }

    /**
     * Builds a fully-wired graph for {@code context}'s DAG. The {@code scheduler} callback
     * is plumbed into each parent's cascade listener — fired when all of a parent's children
     * have SUCCEEDED. Build-time failures bubble unchecked; partial cleanup runs in a
     * {@code finally} to release native handles that backend factories already allocated.
     */
    public static ExecutionGraph build(QueryContext context, StageExecutionBuilder builder, Consumer<StageExecution> scheduler) {
        Map<Integer, StageExecution> executions = new HashMap<>();
        Stage rootStage = context.dag().rootStage();
        boolean success = false;
        try {
            StageExecution rootExec = builder.buildRootExecution(rootStage, context);
            executions.put(rootStage.getStageId(), rootExec);
            List<StageExecution> leaves = new ArrayList<>();

            buildChildrenRecursively(scheduler, executions, builder, rootExec, rootStage, context);
            collectLeaves(executions, rootStage, leaves);
            success = true;
            return new ExecutionGraph(context.queryId(), executions, rootExec, leaves);
        } finally {
            if (!success) cancelPartialBuild(executions);
        }
    }

    private static void cancelPartialBuild(Map<Integer, StageExecution> executions) {
        IllegalStateException primary = null;
        for (StageExecution stage : executions.values()) {
            try {
                stage.cancel("stage-build failure");
            } catch (IllegalStateException t) {
                if (primary == null) {
                    primary = t;
                } else {
                    primary.addSuppressed(t);
                }
            }
        }
        if (primary != null) throw primary;
    }

    private static void buildChildrenRecursively(
        Consumer<StageExecution> scheduler,
        Map<Integer, StageExecution> executions,
        StageExecutionBuilder builder,
        StageExecution parentExec,
        Stage parentStage,
        QueryContext config
    ) {
        List<Stage> children = parentStage.getChildStages();
        if (children.isEmpty()) {
            return;
        }
        List<StageExecution> childExecs = new ArrayList<>(children.size());
        for (Stage child : children) {
            StageExecution childExec = builder.buildExecution(child, parentExec, config);
            executions.put(child.getStageId(), childExec);
            childExecs.add(childExec);
            buildChildrenRecursively(scheduler, executions, builder, childExec, child, config);
        }
        StageListenerWiring.wire(scheduler, parentExec, childExecs);
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

    /** The query this graph belongs to. */
    public String queryId() {
        return queryId;
    }

    /** The root stage execution. */
    public StageExecution rootExecution() {
        return rootExecution;
    }

    /** All leaf executions (stages with no children). */
    public List<StageExecution> leaves() {
        return Collections.unmodifiableList(leaves);
    }

    /** All stage executions in the graph. */
    public Collection<StageExecution> allExecutions() {
        return Collections.unmodifiableCollection(executions.values());
    }

    /** Number of stages in the graph. */
    public int stageCount() {
        return executions.size();
    }

    /** Human-readable summary for EXPLAIN: per-stage type / state / elapsed / rows. */
    public String explain() {
        StringBuilder sb = new StringBuilder();
        sb.append("ExecutionGraph[queryId=").append(queryId);
        sb.append(", stages=").append(executions.size());
        sb.append(", leaves=").append(leaves.size()).append("]\n");
        for (StageExecution exec : executions.values()) {
            sb.append("  Stage ").append(exec.getStageId());
            sb.append(" [").append(exec.getClass().getSimpleName()).append("]");
            sb.append(" state=").append(exec.getState());
            StageMetrics m = exec.getMetrics();
            if (m.getStartTimeMs() > 0) {
                sb.append(" elapsed=").append(m.getEndTimeMs() - m.getStartTimeMs()).append("ms");
                sb.append(" rows=").append(m.getRowsProcessed());
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}
