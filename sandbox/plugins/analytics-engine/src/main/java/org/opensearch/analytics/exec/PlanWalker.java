/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.stage.DataProducer;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-query walker that owns the execution graph. Walks the DAG once,
 * constructs all {@link StageExecution} instances via
 * {@link StageExecutionBuilder#buildExecution} (including the root, wired through a
 * virtual sink-holder parent), wires per-parent listeners inline
 * during construction, and drives state transitions via local listener
 * closures (no global dispatcher).
 *
 * <p>The walker is pure topology: it does not know about scheduler types,
 * {@link DataProducer}, {@link org.opensearch.analytics.exec.stage.DataConsumer},
 * or how a child's sink is resolved from its parent. That logic lives
 * entirely in {@link StageExecutionBuilder}.
 *
 * <p>Lifecycle: constructed by {@link QueryScheduler#execute},
 * tracked in the scheduler's pool by query id, removed on terminal.
 *
 * @opensearch.internal
 */
public class PlanWalker {

    private static final Logger logger = LogManager.getLogger(PlanWalker.class);

    private final QueryContext config;
    private final StageExecutionBuilder stageExecutionBuilder;
    private final Map<Integer, StageExecution> executions = new ConcurrentHashMap<>();
    private final AtomicBoolean terminalFired = new AtomicBoolean(false);
    private final ActionListener<Iterable<Object[]>> completionListener;

    public PlanWalker(QueryContext config, StageExecutionBuilder stageExecutionBuilder, ActionListener<Iterable<Object[]>> listener) {
        this.config = config;
        this.stageExecutionBuilder = stageExecutionBuilder;
        this.completionListener = listener;
    }

    /**
     * Walks the DAG, builds all executions, wires per-parent listeners,
     * wires the root terminal listener, and starts leaves.
     *
     * <p>Four phases:
     * <ol>
     *   <li>Build root execution with a locally-owned {@link RowProducingSink}.</li>
     *   <li>Walk children recursively, wiring per-parent listeners inline.</li>
     *   <li>Wire the root's terminal listener.</li>
     *   <li>Start leaves.</li>
     * </ol>
     */
    public void walk() {
        // Walk the DAG and build StageExecutions - this sets up stage control flow
        Stage rootStage = config.dag().rootStage();
        final StageExecution rootExec = stageExecutionBuilder.buildRootExecution(rootStage, config);
        wireCompletionListener(rootExec);
        executions.put(rootStage.getStageId(), rootExec);

        buildChildrenRecursively(rootExec, rootStage);

        for (StageExecution leaf : findLeaves()) {
            leaf.start();
        }
    }

    /**
     * Top-down cancel: iterates all executions and cancels any in
     * {@code RUNNING} or {@code CREATED} state. Used by external
     * cancellation (task cancel, timeout) via
     * {@link QueryScheduler}.
     */
    public void cancelAll(String reason) {
        for (StageExecution exec : executions.values()) {
            StageExecution.State state = exec.getState();
            if (state == StageExecution.State.RUNNING || state == StageExecution.State.CREATED) {
                try {
                    exec.cancel(reason);
                } catch (Exception ignore) {}
            }
        }
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
        return executions.get(stageId);
    }

    public Collection<StageExecution> activeExecutions() {
        return executions.values().stream()
            .filter(e -> e.getState() == StageExecution.State.RUNNING)
            .toList();
    }

    public Collection<StageExecution> allExecutions() {
        return executions.values();
    }

    private void buildChildrenRecursively(StageExecution parentExec, Stage parentStage) {
        List<Stage> children = parentStage.getChildStages();
        if (children.isEmpty()) {
            return;
        }

        AtomicInteger pendingChildren = new AtomicInteger(children.size());

        for (Stage child : children) {
            StageExecution childExec = stageExecutionBuilder.buildExecution(child, parentExec, config);
            executions.put(child.getStageId(), childExec);

            // Per-parent listener: this child → this specific parent.
            // No global dispatch, no parentsByChild lookup, no re-deriving readiness.
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
                    default -> { }
                }
            });
            // Recurse into grandchildren
            buildChildrenRecursively(childExec, child);
        }
    }

    private void wireCompletionListener(StageExecution rootExec) {
        if ((rootExec instanceof DataProducer) == false) {
            throw new IllegalStateException(
                "Root execution " + rootExec.getClass().getSimpleName() + " does not implement DataProducer"
            );
        }
        final DataProducer producer = (DataProducer) rootExec;
        rootExec.addStateListener((from, to) -> {
            switch (to) {
                case SUCCEEDED -> fireTerminal(() -> completionListener.onResponse(producer.outputSource().readResult()));
                case FAILED, CANCELLED -> {
                    Exception failure = rootExec.getFailure();
                    if (config.parentTask() instanceof CancellableTask ct && ct.isCancelled()) {
                        fireTerminal(() -> completionListener.onFailure(new TaskCancelledException("query cancelled")));
                    } else if (failure != null) {
                        // The failure is already wrapped as "Stage N failed" at the point of origin
                        // (see ShardFragmentStageExecution.dispatchShardTask.onFailure). Forward as-is
                        // so the originating stage id is preserved through propagation.
                        fireTerminal(() -> completionListener.onFailure(failure));
                    } else {
                        fireTerminal(() -> completionListener.onFailure(
                            new RuntimeException("Stage " + rootExec.getStageId() + " " + to)));
                    }
                }
                default -> { }
            }
        });
    }

    private List<StageExecution> findLeaves() {
        final List<StageExecution> leaves = new ArrayList<>();
        collectLeaves(config.dag().rootStage(), leaves);
        return leaves;
    }

    private void collectLeaves(Stage stage, List<StageExecution> leaves) {
        if (stage.getChildStages().isEmpty()) {
            StageExecution exec = executions.get(stage.getStageId());
            if (exec != null) {
                leaves.add(exec);
            }
        } else {
            for (Stage child : stage.getChildStages()) {
                collectLeaves(child, leaves);
            }
        }
    }

    private void fireTerminal(Runnable action) {
        if (terminalFired.compareAndSet(false, true)) {
            action.run();
        }
    }
}
