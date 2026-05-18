/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
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
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.EngineContext;
import org.opensearch.analytics.exec.action.AnalyticsQueryAction;
import org.opensearch.analytics.exec.profile.ProfiledResult;
import org.opensearch.analytics.exec.profile.QueryProfile;
import org.opensearch.analytics.exec.profile.QueryProfileBuilder;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.exec.task.AnalyticsQueryTaskRequest;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.dag.BackendPlanAdapter;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.FragmentConversionDriver;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.arrow.memory.ArrowAllocatorService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.search.SearchService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

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
    private final ThreadPool threadPool;
    private final TaskManager taskManager;
    private final NodeClient client;
    private final EngineContext engineContext;
    // TODO: close on shutdown — currently arrow-base's root.close() will warn about this
    // outstanding child. Consider wrapping in a Guice-bound type owned by AnalyticsPlugin.
    private final BufferAllocator coordinatorAllocator;
    private volatile long perQueryBufferLimit;

    @Inject
    public DefaultPlanExecutor(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ThreadPool threadPool,
        CapabilityRegistry capabilityRegistry,
        EngineContext engineContext,
        NodeClient client,
        Scheduler scheduler,
        ArrowAllocatorService allocatorService
    ) {
        super(AnalyticsQueryAction.NAME, transportService, actionFilters, in -> {
            throw new UnsupportedOperationException("Transport path not implemented yet");
        });
        this.capabilityRegistry = capabilityRegistry;
        this.clusterService = clusterService;
        this.searchExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
        this.threadPool = threadPool;
        this.taskManager = transportService.getTaskManager();
        this.client = client;
        this.scheduler = scheduler;
        this.engineContext = engineContext;
        this.coordinatorAllocator = allocatorService.newChildAllocator("coordinator", Long.MAX_VALUE);
        this.perQueryBufferLimit = AnalyticsPlugin.COORDINATOR_BUFFER_LIMIT.get(clusterService.getSettings());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(AnalyticsPlugin.COORDINATOR_BUFFER_LIMIT, v -> perQueryBufferLimit = v);
    }

    @Override
    public void execute(RelNode logicalFragment, Object context, ActionListener<Iterable<Object[]>> listener) {
        // Wrap listener to convert backend-specific exceptions (e.g., native memory errors
        // arriving as StreamException from gRPC) into proper OpenSearch exception types.
        ActionListener<Iterable<Object[]>> convertingListener = ActionListener.wrap(
            listener::onResponse,
            e -> listener.onFailure(e instanceof Exception ex ? engineContext.convertException(ex) : e)
        );
        searchExecutor.execute(() -> {
            try {
                // Non-profile path: unwrap rows from ProfiledResult (profile is null)
                executeInternal(logicalFragment, false, ActionListener.wrap(
                    result -> convertingListener.onResponse(result.rows()),
                    convertingListener::onFailure
                ));
            } catch (Exception e) {
                convertingListener.onFailure(e);
            }
        });
    }

    @Override
    public void executeWithProfile(RelNode logicalFragment, Object context, ActionListener<ProfiledResult> listener) {
        searchExecutor.execute(() -> {
            try {
                executeInternal(logicalFragment, true, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            } catch (AssertionError e) {
                listener.onFailure(
                    new IllegalStateException("Analytics-engine executor rejected the plan: " + e.getMessage(), e)
                );
            }
        });
    }

    /**
     * Unified planning + execution path. When {@code profile} is true, captures the CBO
     * plan text and snapshots per-stage timing into the {@link ProfiledResult}; when false,
     * wraps rows into a ProfiledResult with null profile for uniform listener handling.
     */
    private void executeInternal(RelNode logicalFragment, boolean profile, ActionListener<ProfiledResult> listener) {
        RelMetadataQueryBase.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(logicalFragment.getCluster().getMetadataProvider()));
        logicalFragment.getCluster().invalidateMetadataQuery();

        final long planStartNanos = profile ? System.nanoTime() : 0;
        RelNode plan = PlannerImpl.createPlan(logicalFragment, new PlannerContext(capabilityRegistry, clusterService.state()));
        final String fullPlan = profile ? org.apache.calcite.plan.RelOptUtil.toString(plan) : null;
        QueryDAG dag = DAGBuilder.build(plan, capabilityRegistry, clusterService);
        PlanForker.forkAll(dag, capabilityRegistry);
        BackendPlanAdapter.adaptAll(dag, capabilityRegistry);
        FragmentConversionDriver.convertAll(dag, capabilityRegistry);
        final long planningTimeMs = profile ? java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - planStartNanos) : 0;
        logger.debug("[DefaultPlanExecutor] QueryDAG:\n{}", dag);

        final AnalyticsQueryTask queryTask = (AnalyticsQueryTask) taskManager.register(
            "transport",
            "analytics_query",
            new AnalyticsQueryTaskRequest(dag.queryId(), null)
        );
        final BufferAllocator queryAllocator;
        final boolean ownsAllocator;
        if (perQueryBufferLimit <= 0) {
            queryAllocator = coordinatorAllocator;
            ownsAllocator = false;
        } else {
            queryAllocator = coordinatorAllocator.newChildAllocator("query-" + dag.queryId(), 0, perQueryBufferLimit);
            ownsAllocator = true;
        }
        logger.debug("[query-{}] Arrow allocator created, limit={}B", dag.queryId(), perQueryBufferLimit);
        final QueryContext context;
        try {
            context = new QueryContext(dag, threadPool, queryTask, queryAllocator, ownsAllocator);
        } catch (Exception e) {
            if (ownsAllocator) queryAllocator.close();
            throw e;
        }

        final AtomicReference<QueryExecution> execRef = new AtomicReference<>();

        ActionListener<Iterable<Object[]>> rowsListener = profile
            ? buildProfilingRowsListener(execRef, context, fullPlan, planningTimeMs, listener)
            : ActionListener.wrap(rows -> listener.onResponse(new ProfiledResult(rows, null, null)), listener::onFailure);

        ActionListener<Iterable<VectorSchemaRoot>> batchesListener = ActionListener.runAfter(
            ActionListener.wrap(batches -> rowsListener.onResponse(batchesToRows(batches)), rowsListener::onFailure),
            () -> taskManager.unregister(queryTask)
        );

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

        execRef.set(scheduler.execute(context, batchesListener)); // execRef read by profile listener after execution completes
    }

    /**
     * Builds a rows listener that snapshots the {@link ExecutionGraph} into a {@link QueryProfile}
     * at terminal, delivering a {@link ProfiledResult} on both success and failure paths.
     */
    private static ActionListener<Iterable<Object[]>> buildProfilingRowsListener(
        AtomicReference<QueryExecution> execRef,
        QueryContext context,
        String fullPlan,
        long planningTimeMs,
        ActionListener<ProfiledResult> listener
    ) {
        return ActionListener.wrap(rows -> {
            QueryProfile qp = QueryProfileBuilder.snapshot(execRef.get().getGraph(), context, fullPlan, planningTimeMs);
            listener.onResponse(new ProfiledResult(rows, null, qp));
        }, e -> {
            QueryProfile qp = execRef.get() != null && execRef.get().getGraph() != null
                ? QueryProfileBuilder.snapshot(execRef.get().getGraph(), context, fullPlan, planningTimeMs)
                : new QueryProfile(context.queryId(), java.util.List.of(), planningTimeMs, 0L, java.util.List.of());
            listener.onResponse(new ProfiledResult(null, e, qp));
        });
    }

    @Override
    protected void doExecute(Task task, ActionRequest request, ActionListener<ActionResponse> listener) {
        // Transport path — reserved for future remote query invocation.
        // Currently, front-ends invoke execute(RelNode, Object, ActionListener) directly.
        listener.onFailure(new UnsupportedOperationException("Direct invocation only — use execute(RelNode, Object, ActionListener)"));
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
