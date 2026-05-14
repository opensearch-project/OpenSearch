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
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;

import java.util.Map;
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
    public void execute(QueryContext config, ActionListener<Iterable<VectorSchemaRoot>> listener) {
        final String queryId = config.queryId();
        final AnalyticsOperationListener.CompositeListener opListener = new AnalyticsOperationListener.CompositeListener(
            config.operationListeners()
        );

        ExecutionGraph graph = ExecutionGraph.build(config, stageExecutionBuilder);

        QueryExecution execution = new QueryExecution(config, graph, wrapWithLifecycle(config, opListener, listener));
        executions.put(queryId, execution);

        setCancellationCallback(config, execution);

        opListener.onQueryStart(queryId, graph.stageCount());
        logger.debug("[QueryScheduler] ExecutionGraph built:\n{}", graph.explain());
        execution.start();
    }

    private static void setCancellationCallback(QueryContext config, QueryExecution execution) {
        final AnalyticsQueryTask queryTask = config.parentTask();
        queryTask.setOnCancelCallback(() -> {
            String reason = "task cancelled: " + (queryTask.getReasonCancelled() != null ? queryTask.getReasonCancelled() : "unknown");
            execution.cancelAll(reason);
        });
    }

    /**
     * Wraps the caller's listener with: (1) operation-listener notification on
     * success/failure, and (2) pool removal before the listener is fired.
     */
    private ActionListener<Iterable<VectorSchemaRoot>> wrapWithLifecycle(
        QueryContext config,
        AnalyticsOperationListener opListener,
        ActionListener<Iterable<VectorSchemaRoot>> listener
    ) {
        final String queryId = config.queryId();
        final long queryStartNanos = System.nanoTime();
        return ActionListener.runBefore(ActionListener.wrap(result -> {
            opListener.onQuerySuccess(queryId, System.nanoTime() - queryStartNanos, 0);
            listener.onResponse(result);
        }, e -> {
            opListener.onQueryFailure(queryId, e);
            listener.onFailure(e);
        }), () -> executions.remove(queryId));
    }

    /**
     * Returns the underlying {@link StageExecutionBuilder} so callers can register a
     * custom {@link org.opensearch.analytics.exec.stage.StageScheduler} for a stage
     * type (e.g. fault-injecting scheduler in resilience tests). Resolving via the
     * singleton scheduler avoids a Guice JIT lookup that would re-instantiate
     * {@link AnalyticsSearchTransportService} (whose ctor registers transport
     * handlers, only legal once per node).
     */
    public StageExecutionBuilder getStageExecutionBuilder() {
        return stageExecutionBuilder;
    }
}
