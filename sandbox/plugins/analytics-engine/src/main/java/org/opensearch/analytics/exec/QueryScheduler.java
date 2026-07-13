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
import org.opensearch.analytics.backend.AnalyticsOperationListener;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskState;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.exec.task.TaskRunner;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.TaskManager;
import org.opensearch.transport.TransportService;

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
    private final TaskManager taskManager;
    private final Map<String, QueryExecution> executions = new ConcurrentHashMap<>();

    @Inject
    public QueryScheduler(StageExecutionBuilder stageExecutionBuilder, TransportService transportService) {
        this.stageExecutionBuilder = stageExecutionBuilder;
        this.taskManager = transportService.getTaskManager();
    }

    @Override
    public QueryExecution execute(QueryContext context, ActionListener<Iterable<VectorSchemaRoot>> listener) {
        final String queryId = context.queryId();
        final AnalyticsOperationListener.CompositeListener opListener = new AnalyticsOperationListener.CompositeListener(
            context.operationListeners()
        );
        final long queryStartNanos = System.nanoTime();
        final AnalyticsQueryTask queryTask = context.parentTask();

        ExecutionGraph graph = ExecutionGraph.build(context, stageExecutionBuilder, this::scheduleStage);

        ActionListener<Iterable<VectorSchemaRoot>> wrapped = ActionListener.runBefore(ActionListener.wrap(result -> {
            opListener.onQuerySuccess(queryId, System.nanoTime() - queryStartNanos, 0);
            listener.onResponse(result);
        }, e -> {
            opListener.onQueryFailure(queryId, e);
            listener.onFailure(e);
        }), () -> {
            executions.remove(queryId);
            // Cascade a cancel to dispatched data-node fragments before the framework unregisters
            // the parent task. Otherwise TaskManager.setBan, which matches children by parent
            // TaskId in the cancellable-tasks registry, cannot reach fragments after the parent
            // has been unregistered — they survive into orphan state and run to natural
            // completion. Idempotent against the already-cancelled path.
            try {
                taskManager.cancelTaskAndDescendants(
                    queryTask,
                    "analytics query terminal — cleaning up dispatched fragments",
                    false,
                    ActionListener.wrap(
                        v -> {},
                        ex -> logger.debug(
                            new ParameterizedMessage("[QueryScheduler] orphan-cleanup cancel failed for queryId={}", queryId),
                            ex
                        )
                    )
                );
            } catch (Exception ex) {
                logger.debug(new ParameterizedMessage("[QueryScheduler] orphan-cleanup invocation failed for queryId={}", queryId), ex);
            }
        });

        QueryExecution execution = new QueryExecution(context, graph, this::scheduleStage, wrapped);
        executions.put(queryId, execution);

        setCancellationCallback(context, execution);

        opListener.onQueryStart(queryId, graph.stageCount());
        logger.debug("[QueryScheduler] ExecutionGraph built:\n{}", graph.explain());
        execution.start();
        return execution;
    }

    /**
     * Materialises {@code stage}'s tasks via {@link StageExecution#start()}, then hands the
     * scheduler-owned per-task listener factory to {@link StageExecution#dispatchTasks} —
     * the stage decides whether to dispatch eagerly (default for-loop) or incrementally
     * (shard fan-outs that want to bound the outbound-throttle queue depth). Skips dispatch
     * when {@code start()} transitions straight to a terminal (empty target resolution →
     * SUCCEEDED; concurrent cancel → CANCELLED).
     */
    void scheduleStage(StageExecution stage) {
        stage.start();
        if (stage.getState() != StageExecution.State.RUNNING) return;
        logger.debug("[QueryScheduler] dispatching stage {} ({} tasks)", stage.getStageId(), stage.tasks().size());
        stage.dispatchTasks(this::handleFor);
    }

    /**
     * On failure, retries via {@link StageExecution#retargetForRetry} if the stage hands
     * back an alternate; otherwise propagates via {@code onTaskTerminal} (fast-fails the stage).
     * Protected for retry/admission-aware subclasses.
     *
     * <p>Terminal-stage short-circuit lives here, not in each stage's {@code retargetForRetry}:
     * if the stage has already entered any terminal state ({@link StageExecution.State#SUCCEEDED SUCCEEDED},
     * {@link StageExecution.State#FAILED FAILED}, or {@link StageExecution.State#CANCELLED CANCELLED}),
     * retry is skipped uniformly across stage types. Spawning new dispatch work for a stage
     * that's done — whether it succeeded, already gave up on a different task, or was
     * cancelled — is always wrong. Catches the race where an in-flight task's onFailure
     * fires after the stage has transitioned terminally for some other reason.
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
                Optional<StageTask> retry = stage.getState().isTerminal() ? Optional.empty() : stage.retargetForRetry(task, cause);
                if (retry.isPresent()) {
                    logger.debug(
                        () -> new ParameterizedMessage(
                            "[QueryScheduler] task {} on stage {} failed, retrying on {}",
                            task,
                            stage.getStageId(),
                            retry.get()
                        )
                    );
                    currentAttempt.transitionTo(StageTaskState.FAILED);  // previous attempt is now superseded
                    StageTask r = retry.get();
                    currentAttempt = r;
                    r.transitionTo(StageTaskState.RUNNING);
                    @SuppressWarnings("unchecked")
                    TaskRunner<StageTask> runner = (TaskRunner<StageTask>) stage.taskRunner();
                    runner.run(r, this);  // reuse this listener — retry loop until stage gives up
                    return;
                }
                logger.debug(
                    () -> new ParameterizedMessage(
                        "[QueryScheduler] task {} on stage {} failed, no retry available (stageTerminal={}, cause={})",
                        task,
                        stage.getStageId(),
                        stage.getState().isTerminal(),
                        cause.getMessage()
                    )
                );
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
