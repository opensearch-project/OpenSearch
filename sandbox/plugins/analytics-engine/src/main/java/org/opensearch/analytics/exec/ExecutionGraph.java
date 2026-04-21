/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageMetrics;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Inspectable snapshot of a fully-wired execution graph. Built by
 * {@link PlanWalker#build()}, consumed by {@link PlanWalker#start(ExecutionGraph)}
 * or by EXPLAIN without executing.
 *
 * <p>The graph holds all {@link StageExecution} instances with their
 * state listeners already wired. No stage has been started yet — all
 * are in {@link StageExecution.State#CREATED}.
 *
 * @opensearch.internal
 */
public class ExecutionGraph {

    private final Map<Integer, StageExecution> executions;
    private final StageExecution rootExecution;
    private final List<StageExecution> leaves;
    private final String queryId;

    ExecutionGraph(
        String queryId,
        Map<Integer, StageExecution> executions,
        StageExecution rootExecution,
        List<StageExecution> leaves
    ) {
        this.queryId = queryId;
        this.executions = executions;
        this.rootExecution = rootExecution;
        this.leaves = leaves;
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

    /** Lookup a stage execution by stage id. */
    public StageExecution executionFor(int stageId) {
        return executions.get(stageId);
    }

    /** All stage executions in the graph. */
    public Collection<StageExecution> allExecutions() {
        return Collections.unmodifiableCollection(executions.values());
    }

    /** Number of stages in the graph. */
    public int stageCount() {
        return executions.size();
    }

    /**
     * Returns a human-readable summary of the execution graph for
     * EXPLAIN output. Lists each stage with its type, state, and
     * child dependencies.
     */
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
