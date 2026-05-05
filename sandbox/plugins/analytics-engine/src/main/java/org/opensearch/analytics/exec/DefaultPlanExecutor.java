/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.TimeoutTaskCancellationUtility;
import org.opensearch.analytics.EngineContext;
import org.opensearch.analytics.exec.action.AnalyticsQueryAction;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.dag.BackendPlanAdapter;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.FragmentConversionDriver;
import org.opensearch.analytics.planner.dag.PlanForker;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.opensearch.action.search.TransportSearchAction.SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING;

/**
 * Coordinator-level plan executor. Registered as a {@link HandledTransportAction}
 * so that Guice injects all dependencies ({@link TransportService},
 * {@link ClusterService}, {@link ThreadPool}, etc.) automatically.
 *
 * <p>Front-end plugins resolve this class from the Node's Guice injector and invoke
 * {@link #execute(RelNode, Object, ActionListener)} directly. Execution is asynchronous —
 * the listener is fired by the scheduler once the query completes (or fails). The transport
 * path ({@code doExecute}) is reserved for future remote query invocation.
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
    public void execute(RelNode logicalFragment, Object context, ActionListener<Iterable<Object[]>> listener) {
        // Fork the entire query lifecycle (planning, scheduling, cleanup) onto the SEARCH
        // executor so the calling thread — which may be a transport thread — is freed
        // immediately. The scheduler then drives execution asynchronously and fires
        // {@code listener} once the query terminates; nothing on this path blocks.
        searchExecutor.execute(() -> {
            try {
                executeInternal(logicalFragment, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /**
     * Plans, registers the query task, and dispatches to the {@link Scheduler}. Runs on
     * the SEARCH thread pool — never on a transport thread. The result (or failure) is
     * delivered to {@code listener} by the scheduler; this method returns as soon as the
     * scheduler has accepted the query.
     */
    private void executeInternal(RelNode logicalFragment, ActionListener<Iterable<Object[]>> listener) {
        // Calcite's RelMetadataQuery reads its handler provider from a ThreadLocal
        // (RelMetadataQueryBase.THREAD_PROVIDERS). The frontend seeds it on its own
        // thread, but execute() hops to the SEARCH executor where the ThreadLocal is
        // unset — RelOptUtil.toString / RelNode.explain inside PlannerImpl would then
        // NPE on a null metadataHandlerProvider. Re-seed from the inbound cluster.
        RelMetadataQueryBase.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(logicalFragment.getCluster().getMetadataProvider()));
        logicalFragment.getCluster().invalidateMetadataQuery();

        RelNode plan = PlannerImpl.createPlan(logicalFragment, new PlannerContext(capabilityRegistry, clusterService.state()));
        QueryDAG dag = DAGBuilder.build(plan, capabilityRegistry, clusterService);
        PlanForker.forkAll(dag, capabilityRegistry);
        BackendPlanAdapter.adaptAll(dag, capabilityRegistry);
        FragmentConversionDriver.convertAll(dag, capabilityRegistry);
        logger.debug("[DefaultPlanExecutor] QueryDAG:\n{}", dag);

        // Register coordinator-level query task with TaskManager (like SearchTask).
        // This gives us a proper unique ID, visibility in _tasks API, and cancellation support.
        // TODO: accept a request type from FrontEnd including cancelAfterTimeInterval — set from cluster settings below, null in req.
        final AnalyticsQueryTask queryTask = (AnalyticsQueryTask) taskManager.register(
            "transport",
            "analytics_query",
            new AnalyticsQueryTaskRequest(dag.queryId(), null)
        );
        final QueryContext config = new QueryContext(dag, searchExecutor, queryTask);

        // Per-query cleanup on terminal. Stage-execution cancellation on external
        // task-cancel/timeout is wired inside the Scheduler — on this path the
        // walker has already cascaded cancellations by the time we see the failure.
        // Scheduler yields batches; we materialize rows at the API edge for callers
        // that still consume Iterable<Object[]>.
        ActionListener<Iterable<VectorSchemaRoot>> batchesListener = buildBatchesListener(listener, () -> {
            try {
                config.closeBufferAllocator();
            } finally {
                taskManager.unregister(queryTask);
            }
        });

        TimeValue taskTimeout = queryTask.getCancelAfterTimeInterval();
        TimeValue clusterTimeout = clusterService.getClusterSettings().get(SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING);
        if (taskTimeout != null || SearchService.NO_TIMEOUT.equals(clusterTimeout) == false) {
            batchesListener = TimeoutTaskCancellationUtility.wrapWithCancellationListener(
                client,
                queryTask,
                clusterTimeout,
                batchesListener,
                e -> {}
            );
        }

        scheduler.execute(config, batchesListener);
    }

    @Override
    protected void doExecute(Task task, ActionRequest request, ActionListener<ActionResponse> listener) {
        // Transport path — reserved for future remote query invocation.
        // Currently, front-ends invoke execute(RelNode, Object, ActionListener) directly.
        listener.onFailure(new UnsupportedOperationException("Direct invocation only — use execute(RelNode, Object, ActionListener)"));
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

    /**
     * Builds the batches→rows {@link ActionListener} used by {@link #executeInternal}. {@code cleanup}
     * runs exactly once before {@code downstream} is notified — on either response or failure paths.
     * A cleanup failure on the response path is routed to {@code downstream.onFailure}; on the failure
     * path it is attached as a suppressed exception. This eliminates the double-cleanup that the prior
     * try/finally pattern produced when an exception in the success path was caught by
     * {@link ActionListener#wrap} and re-routed to the failure callback.
     *
     * <p>Package-private for unit testing.
     */
    static ActionListener<Iterable<VectorSchemaRoot>> buildBatchesListener(
        ActionListener<Iterable<Object[]>> downstream,
        Runnable cleanup
    ) {
        ActionListener<Iterable<Object[]>> wrapped = ActionListener.runBefore(downstream, cleanup::run);
        return ActionListener.wrap(batches -> wrapped.onResponse(batchesToRows(batches)), wrapped::onFailure);
    }

    /**
     * Materializes Arrow batches into row-oriented {@code Object[]}s for the
     * external query API. The scheduler yields batches (the native wire format);
     * the row materialization happens here, once, at the API edge.
     *
     * <p>Package-private for unit testing.
     */
    static Iterable<Object[]> batchesToRows(Iterable<VectorSchemaRoot> batches) {
        List<Object[]> rows = new ArrayList<>();
        for (VectorSchemaRoot batch : batches) {
            try {
                int colCount = batch.getFieldVectors().size();
                int rowCount = batch.getRowCount();
                for (int r = 0; r < rowCount; r++) {
                    Object[] row = new Object[colCount];
                    for (int c = 0; c < colCount; c++) {
                        row[c] = ArrowValues.toJavaValue(batch.getVector(c), r);
                    }
                    rows.add(row);
                }
            } finally {
                // Release the Arrow buffers back to the query allocator. Without this the
                // query teardown's allocator.close() detects a leak and fails the query.
                batch.close();
            }
        }
        return rows;
    }
}
