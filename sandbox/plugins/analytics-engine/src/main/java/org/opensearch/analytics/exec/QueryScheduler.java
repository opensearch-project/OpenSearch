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
import org.opensearch.analytics.backend.AnalyticsOperationListener;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskState;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.exec.task.TaskRunner;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default {@link Scheduler} implementation. Builds a {@link QueryExecution} per
 * query, starts it, and tracks it in a pool for observability and cancellation.
 *
 * @opensearch.internal
 */
public class QueryScheduler implements Scheduler {

    private static final Logger logger = LogManager.getLogger(QueryScheduler.class);

    private final StageExecutionBuilder stageExecutionBuilder;
    private final Map<String, QueryExecution> executions = new ConcurrentHashMap<>();

    @Inject
    public QueryScheduler(StageExecutionBuilder stageExecutionBuilder) {
        this.stageExecutionBuilder = stageExecutionBuilder;
    }

    @Override
    public void execute(QueryContext context, ActionListener<Iterable<VectorSchemaRoot>> listener) {
        final String queryId = context.queryId();
        final AnalyticsOperationListener.CompositeListener opListener = new AnalyticsOperationListener.CompositeListener(
            context.operationListeners()
        );
        final long queryStartNanos = System.nanoTime();

        ExecutionGraph graph = ExecutionGraph.build(context, stageExecutionBuilder, this::scheduleStage);

        ActionListener<Iterable<VectorSchemaRoot>> wrapped = ActionListener.runBefore(ActionListener.wrap(result -> {
            opListener.onQuerySuccess(queryId, System.nanoTime() - queryStartNanos, 0);
            listener.onResponse(result);
        }, e -> {
            opListener.onQueryFailure(queryId, e);
            listener.onFailure(e);
        }), () -> executions.remove(queryId));

        QueryExecution execution = new QueryExecution(context, graph, this::scheduleStage, wrapped);
        executions.put(queryId, execution);

        setCancellationCallback(context, execution);

        opListener.onQueryStart(queryId, graph.stageCount());
        logger.debug("[QueryScheduler] ExecutionGraph built:\n{}", graph.explain());
        execution.start();
    }

    /**
     * Materialises {@code stage}'s tasks via {@link StageExecution#start()}, then dispatches each
     * through {@link StageExecution#taskRunner()} with a scheduler-owned {@link ActionListener}.
     * Skips dispatch when {@code start()} transitions straight to a terminal (empty target
     * resolution → SUCCEEDED; concurrent cancel → CANCELLED).
     *
     * <p>TODO: in-flight cap - we have pendingExecutions but that's per query per node
     */
    void scheduleStage(StageExecution stage) {
        stage.start();
        if (stage.getState() != StageExecution.State.RUNNING) return;
        // Stage owns both tasks() and taskRunner() — the variant types match by construction.
        @SuppressWarnings("unchecked")
        TaskRunner<StageTask> runner = (TaskRunner<StageTask>) stage.taskRunner();
        List<StageTask> tasks = stage.tasks();
        logger.debug("[QueryScheduler] dispatching stage {} ({} tasks)", stage.getStageId(), tasks.size());
        for (StageTask task : tasks) {
            assert task.state() == StageTaskState.CREATED : "stage "
                + stage.getStageId()
                + " task "
                + task.id()
                + " expected CREATED, got "
                + task.state();
            task.transitionTo(StageTaskState.RUNNING);
            runner.run(task, handleFor(stage, task));
        }
    }

    /**
     * On failure, retries via {@link StageExecution#retargetForRetry} if the stage hands
     * back an alternate; otherwise propagates via {@code onTaskTerminal} (fast-fails the stage).
     * Protected for retry/admission-aware subclasses.
     *
     * <p>Tracks the current attempt so each attempt's terminal state is recorded truthfully:
     * superseded attempts get FAILED, the final (succeeding or last-failed) attempt gets the
     * matching terminal. The {@code task} reference passed to {@code stage.onTaskTerminal}
     * stays the original — that's the slot identifier the stage's bookkeeping uses.
     */
    protected ActionListener<Void> handleFor(StageExecution stage, StageTask task) {
        return new ActionListener<>() {
            StageTask currentAttempt = task;

            @Override
            public void onResponse(Void unused) {
                currentAttempt.transitionTo(StageTaskState.FINISHED);
                stage.onTaskTerminal(task, null);
            }

            @Override
            public void onFailure(Exception cause) {
                Optional<StageTask> retry = stage.retargetForRetry(task, cause);
                if (retry.isPresent()) {
                    currentAttempt.transitionTo(StageTaskState.FAILED);  // previous attempt is now superseded
                    StageTask r = retry.get();
                    currentAttempt = r;
                    r.transitionTo(StageTaskState.RUNNING);
                    @SuppressWarnings("unchecked")
                    TaskRunner<StageTask> runner = (TaskRunner<StageTask>) stage.taskRunner();
                    runner.run(r, this);  // reuse this listener — retry loop until stage gives up
                    return;
                }
                currentAttempt.transitionTo(StageTaskState.FAILED);
                stage.onTaskTerminal(task, cause);
            }
        };
    }

    private static void setCancellationCallback(QueryContext config, QueryExecution execution) {
        final AnalyticsQueryTask queryTask = config.parentTask();
        queryTask.setOnCancelCallback(() -> {
            String reason = "task cancelled: " + (queryTask.getReasonCancelled() != null ? queryTask.getReasonCancelled() : "unknown");
            execution.cancelAll(reason);
        });
    }

    /**
     * Returns the underlying {@link StageExecutionBuilder} so callers can register a
     * custom {@link org.opensearch.analytics.exec.stage.StageExecutionFactory} for a stage
     * type (e.g. fault-injecting scheduler in resilience tests). Resolving via the
     * singleton scheduler avoids a Guice JIT lookup that would re-instantiate
     * {@link AnalyticsSearchTransportService} (whose ctor registers transport
     * handlers, only legal once per node).
     */
    public StageExecutionBuilder getStageExecutionBuilder() {
        return stageExecutionBuilder;
    }
}
