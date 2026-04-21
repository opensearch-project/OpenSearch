/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.TimeoutTaskCancellationUtility;
import org.opensearch.analytics.EngineContext;
import org.opensearch.analytics.exec.action.AnalyticsQueryAction;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.SearchService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskAwareRequest;
import org.opensearch.tasks.TaskManager;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Map;
import java.util.concurrent.Executor;

import static org.opensearch.action.search.TransportSearchAction.SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING;

/**
 * Coordinator-level plan executor. Registered as a {@link HandledTransportAction}
 * so that Guice injects all dependencies ({@link TransportService},
 * {@link ClusterService}, {@link ThreadPool}, etc.) automatically.
 *
 * <p>The SQL plugin resolves this class from the Node's Guice injector and invokes
 * {@link #execute(RelNode, Object)} directly. The transport path ({@code doExecute})
 * is reserved for future remote query invocation.
 *
 * @opensearch.internal
 */
public class DefaultPlanExecutor extends HandledTransportAction<ActionRequest, ActionResponse>
    implements
        QueryPlanExecutor<RelNode, Iterable<Object[]>> {

    private static final Logger logger = LogManager.getLogger(DefaultPlanExecutor.class);

    private final CapabilityRegistry capabilityRegistry;
    private final ClusterService clusterService;
    private final Scheduler scheduler;
    private final Executor searchExecutor;
    private final TaskManager taskManager;
    private final NodeClient client;

    @Inject
    public DefaultPlanExecutor(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ThreadPool threadPool,
        CapabilityRegistry capabilityRegistry,
        EngineContext engineContext,
        NodeClient client,
        Scheduler scheduler
    ) {
        super(AnalyticsQueryAction.NAME, transportService, actionFilters, in -> {
            throw new UnsupportedOperationException("Transport path not implemented yet");
        });
        this.capabilityRegistry = capabilityRegistry;
        this.clusterService = clusterService;
        this.searchExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
        this.taskManager = transportService.getTaskManager();
        this.client = client;
        this.scheduler = scheduler;
    }

    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        RelNode plan = PlannerImpl.createPlan(logicalFragment, new PlannerContext(capabilityRegistry, clusterService.state()));
        QueryDAG dag = DAGBuilder.build(plan, capabilityRegistry, clusterService);
        logger.info("[DefaultPlanExecutor] QueryDAG:\n{}", dag);

        // Register coordinator-level query task with TaskManager (like SearchTask).
        // This gives us a proper unique ID, visibility in _tasks API, and cancellation support.
        // TODO: accept a request type from FrontEnd including cancelAfterTimeInterval - its set from cluster settings below, null in req.
        final AnalyticsQueryTask queryTask = (AnalyticsQueryTask) taskManager.register(
            "transport",
            "analytics_query",
            new AnalyticsQueryTaskRequest(dag.queryId(), null)
        );

        // Create per-query context
        QueryContext config = new QueryContext(dag, searchExecutor, queryTask);

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();

        // Per-query cleanup on terminal. Stage-execution cancellation on external
        // task-cancel/timeout is wired inside the Scheduler — on this path the
        // walker has already cascaded cancellations by the time we see the failure.
        ActionListener<Iterable<Object[]>> listener = ActionListener.wrap(result -> {
            config.closeBufferAllocator();
            taskManager.unregister(queryTask);
            future.onResponse(result);
        }, e -> {
            config.closeBufferAllocator();
            taskManager.unregister(queryTask);
            future.onFailure(e);
        });

        TimeValue taskTimeout = queryTask.getCancelAfterTimeInterval();
        TimeValue clusterTimeout = clusterService.getClusterSettings().get(SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING);
        if (taskTimeout != null || SearchService.NO_TIMEOUT.equals(clusterTimeout) == false) {
            listener = TimeoutTaskCancellationUtility.wrapWithCancellationListener(client, queryTask, clusterTimeout, listener, e -> {});
        }

        scheduler.execute(config, listener);
        return future.actionGet();  // TODO: single blocking point — Should be async with Front-End passing listener.
    }

    @Override
    protected void doExecute(Task task, ActionRequest request, ActionListener<ActionResponse> listener) {
        // Transport path — reserved for future remote query invocation.
        // Currently, the SQL plugin invokes execute(RelNode, Object) directly.
        listener.onFailure(new UnsupportedOperationException("Direct invocation only — use execute(RelNode, Object)"));
    }

    /**
     * Lightweight {@link TaskAwareRequest} for registering an {@link AnalyticsQueryTask}
     * with {@link TaskManager}. Mirrors how {@code SearchRequest.createTask()} returns
     * a {@code SearchTask}.
     */
    static class AnalyticsQueryTaskRequest implements TaskAwareRequest {
        private final String queryId;
        private final TimeValue cancelAfterTimeInterval;
        private TaskId parentTaskId = TaskId.EMPTY_TASK_ID;

        AnalyticsQueryTaskRequest(String queryId, @Nullable TimeValue cancelAfterTimeInterval) {
            this.queryId = queryId;
            this.cancelAfterTimeInterval = cancelAfterTimeInterval;
        }

        @Override
        public void setParentTask(TaskId taskId) {
            this.parentTaskId = taskId;
        }

        @Override
        public TaskId getParentTask() {
            return parentTaskId;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new AnalyticsQueryTask(id, type, action, queryId, parentTaskId, headers, cancelAfterTimeInterval);
        }
    }
}
