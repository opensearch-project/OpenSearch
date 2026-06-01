/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.TimeoutTaskCancellationUtility;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.analytics.exec.action.AnalyticsQueryAction;
import org.opensearch.analytics.exec.action.AnalyticsQueryRequest;
import org.opensearch.analytics.exec.action.AnalyticsQueryResponse;
import org.opensearch.analytics.exec.profile.ProfiledResult;
import org.opensearch.analytics.exec.profile.QueryProfile;
import org.opensearch.analytics.exec.profile.QueryProfileBuilder;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.BackendPlanAdapter;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.FragmentConversionDriver;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.arrow.allocator.AllocationRejection;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchService;
import org.opensearch.tasks.Task;
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
 * {@link #execute(RelNode, QueryRequestContext, ActionListener)} directly. Execution is asynchronous —
 * the listener is fired by the scheduler once the query completes (or fails). The transport
 * path ({@code doExecute}) is reserved for future remote query invocation.
 *
 * @opensearch.internal
 */
public class DefaultPlanExecutor extends HandledTransportAction<AnalyticsQueryRequest, AnalyticsQueryResponse>
    implements
        QueryPlanExecutor<RelNode, Iterable<Object[]>> {

    private static final Logger logger = LogManager.getLogger(DefaultPlanExecutor.class);

    private final CapabilityRegistry capabilityRegistry;
    private final ClusterService clusterService;
    private final Scheduler scheduler;
    private final Executor searchExecutor;
    private final ThreadPool threadPool;
    private final NodeClient client;
    private final EngineContextProvider contextProvider;
    // Owned and closed by AnalyticsPlugin via the injected CoordinatorAllocatorHandle so that
    // shutdown closes this child of POOL_QUERY before arrow-base closes the root allocator.
    private final BufferAllocator coordinatorAllocator;
    private volatile long perQueryBufferLimit;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public DefaultPlanExecutor(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ThreadPool threadPool,
        CapabilityRegistry capabilityRegistry,
        EngineContextProvider contextProvider,
        NodeClient client,
        Scheduler scheduler,
        CoordinatorAllocatorHandle coordinatorAllocatorHandle,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(AnalyticsQueryAction.NAME, transportService, actionFilters, AnalyticsQueryRequest::new);
        this.capabilityRegistry = capabilityRegistry;
        this.clusterService = clusterService;
        this.searchExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
        this.threadPool = threadPool;
        this.client = client;
        this.scheduler = scheduler;
        this.contextProvider = contextProvider;
        // Use the plugin-owned coordinator allocator (a child of POOL_QUERY). The plugin
        // closes the underlying allocator on Plugin.close() via the handle, so coordinator-
        // side allocations are released deterministically before arrow-base tears down the
        // root allocator. Long.MAX_VALUE on the child means dynamic resizes of
        // parquet.native.pool.query.max take effect immediately via Arrow's parent-cap check
        // at allocateBytes — no listener needed.
        this.coordinatorAllocator = coordinatorAllocatorHandle.getAllocator();
        this.perQueryBufferLimit = AnalyticsPlugin.COORDINATOR_BUFFER_LIMIT.get(clusterService.getSettings());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(AnalyticsPlugin.COORDINATOR_BUFFER_LIMIT, v -> perQueryBufferLimit = v);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    public void execute(RelNode logicalFragment, QueryRequestContext queryCtx, ActionListener<Iterable<Object[]>> listener) {
        // Dispatch through ActionModule so the SecurityFilter evaluates index-level
        // permissions before any planning work begins. The AnalyticsQueryRequest
        // implements IndicesRequest.Replaceable, exposing target indices extracted
        // from the RelNode's TableScan nodes.
        String[] indices = RelNodeUtils.extractIndices(logicalFragment);
        AnalyticsQueryRequest request = new AnalyticsQueryRequest(logicalFragment, queryCtx, indices);
        client.execute(
            AnalyticsQueryAction.INSTANCE,
            request,
            ActionListener.wrap(resp -> listener.onResponse(resp.getRows()), listener::onFailure)
        );
    }

    @Override
    public void executeWithProfile(RelNode logicalFragment, QueryRequestContext queryCtx, ActionListener<ProfiledResult> listener) {
        // Route through the framework action (profile=true) just like execute(), so a profiling
        // query runs under the framework-provided, cancellable task rather than detached on
        // searchExecutor. The SecurityFilter also evaluates index permissions on this path.
        String[] indices = RelNodeUtils.extractIndices(logicalFragment);
        AnalyticsQueryRequest request = new AnalyticsQueryRequest(logicalFragment, queryCtx, indices, true);
        client.execute(
            AnalyticsQueryAction.INSTANCE,
            request,
            ActionListener.wrap(resp -> listener.onResponse(resp.getProfiledResult()), listener::onFailure)
        );
    }

    /**
     * Unified planning + execution path. When {@code profile} is true, captures the CBO
     * plan text and snapshots per-stage timing into the {@link ProfiledResult}; when false,
     * wraps rows into a ProfiledResult with null profile for uniform listener handling.
     *
     * <p>If {@code queryCtx} is non-null, its {@link QueryRequestContext#clusterState()} is
     * used for Calcite planning so the planner sees the same snapshot the front-end built
     * the schema from. Otherwise a fresh {@code clusterService.state()} is read.
     */
    private void executeInternal(
        AnalyticsQueryTask queryTask,
        RelNode logicalFragment,
        QueryRequestContext queryCtx,
        boolean profile,
        ActionListener<ProfiledResult> listener
    ) {
        RelMetadataQueryBase.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(logicalFragment.getCluster().getMetadataProvider()));
        logicalFragment.getCluster().invalidateMetadataQuery();

        final long planStartNanos = profile ? System.nanoTime() : 0;
        // Reuse the snapshot captured at REST entry when present; this is the same ClusterState
        // OpenSearchSchemaBuilder used to build the SchemaPlus, so planner and schema agree.
        // TODO: remove the null fallback once every front-end (test-ppl-frontend,
        // dsl-query-executor) threads an EngineContextProvider.getContext() snapshot through.
        ClusterState planningState = queryCtx != null ? queryCtx.clusterState() : clusterService.state();
        RelNode plan = PlannerImpl.createPlan(
            logicalFragment,
            new PlannerContext(capabilityRegistry, planningState, indexNameExpressionResolver, false)
        );
        final String fullPlan = profile ? org.apache.calcite.plan.RelOptUtil.toString(plan) : null;
        QueryDAG dag = DAGBuilder.build(plan, capabilityRegistry, clusterService, indexNameExpressionResolver);
        PlanForker.forkAll(dag, capabilityRegistry);
        BackendPlanAdapter.adaptAll(dag, capabilityRegistry);
        FragmentConversionDriver.convertAll(dag, capabilityRegistry);
        final long planningTimeMs = profile ? java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - planStartNanos) : 0;
        logger.debug("[DefaultPlanExecutor] QueryDAG:\n{}", dag);

        // The task is the framework-provided task from doExecute (registered by
        // HandledTransportAction before doExecute, unregistered when the listener completes).
        // Using it — rather than self-registering a detached task — is what lets a client
        // disconnect / explicit task cancel propagate into the running query.
        final BufferAllocator queryAllocator;
        final boolean ownsAllocator;
        if (perQueryBufferLimit <= 0) {
            queryAllocator = coordinatorAllocator;
            ownsAllocator = false;
        } else {
            // Per-request allocation: a failure here means the QUERY pool is exhausted, not
            // a framework misconfiguration. Translate Arrow's OutOfMemoryException into
            // OpenSearchRejectedExecutionException so the REST layer maps it to HTTP 429
            // and the client sees a proper backpressure signal rather than a generic 500.
            queryAllocator = AllocationRejection.wrap(
                "query-" + dag.queryId(),
                () -> coordinatorAllocator.newChildAllocator("query-" + dag.queryId(), 0, perQueryBufferLimit)
            );
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

        /*
        Profile and explain are captured within the QueryExecution, however QueryExecution requires the complete
        batchesListener to construct the ExecutionGraph. To get around this circular dependency we build a profiling
        listener with an empty QueryExecution reference, and then populate it once constructed.
         */
        final AtomicReference<QueryExecution> execRef = new AtomicReference<>();

        ActionListener<Iterable<Object[]>> rowsListener = profile
            ? buildProfilingRowsListener(execRef, context, fullPlan, planningTimeMs, listener)
            : ActionListener.wrap(rows -> listener.onResponse(new ProfiledResult(rows, null, null)), listener::onFailure);

        final List<String> outputColumnOrder = logicalFragment.getRowType().getFieldNames();
        // No taskManager.unregister here: the framework (HandledTransportAction) unregisters the
        // task it created for doExecute once this listener settles. Unregistering it ourselves
        // would double-free a task we no longer own.
        ActionListener<Iterable<VectorSchemaRoot>> batchesListener = ActionListener.wrap(
            batches -> rowsListener.onResponse(batchesToRows(batches, outputColumnOrder)),
            rowsListener::onFailure
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
    protected void doExecute(Task task, AnalyticsQueryRequest request, ActionListener<AnalyticsQueryResponse> listener) {
        // Runs after SecurityFilter has authorized the request.
        // Fork the entire query lifecycle (planning, scheduling, cleanup) onto the SEARCH
        // executor so the calling thread — which may be a transport thread — is freed
        // immediately. The listener is wrapped to convert backend-specific exceptions.
        ActionListener<AnalyticsQueryResponse> convertingListener = ActionListener.wrap(
            listener::onResponse,
            e -> listener.onFailure(e instanceof Exception ex ? contextProvider.convertException(ex) : e)
        );
        ContextAwareExecutor.wrap(searchExecutor, threadPool).execute(() -> {
            try {
                executeInternal(
                    (AnalyticsQueryTask) task,
                    request.getPlan(),
                    request.getQueryCtx(),
                    request.isProfile(),
                    ActionListener.wrap(
                        result -> convertingListener.onResponse(
                            request.isProfile() ? new AnalyticsQueryResponse(result) : new AnalyticsQueryResponse(result.rows())
                        ),
                        convertingListener::onFailure
                    )
                );
            } catch (Exception e) {
                convertingListener.onFailure(e);
            } catch (AssertionError e) {
                convertingListener.onFailure(
                    new IllegalStateException("Analytics-engine executor rejected the plan: " + e.getMessage(), e)
                );
            }
        });
    }

    /**
     * Materializes Arrow batches into row-oriented {@code Object[]}s for the
     * external query API. The scheduler yields batches (the native wire format);
     * the row materialization happens here, once, at the API edge.
     *
     * <p>Package-private for unit testing.
     */
    static Iterable<Object[]> batchesToRows(Iterable<VectorSchemaRoot> batches) {
        return batchesToRows(batches, null);
    }

    static Iterable<Object[]> batchesToRows(Iterable<VectorSchemaRoot> batches, List<String> targetColumnOrder) {
        List<Object[]> rows = new ArrayList<>();
        for (VectorSchemaRoot batch : batches) {
            try {
                List<FieldVector> ordered = orderedColumns(batch, targetColumnOrder);
                int colCount = ordered.size();
                int rowCount = batch.getRowCount();
                for (int r = 0; r < rowCount; r++) {
                    Object[] row = new Object[colCount];
                    for (int c = 0; c < colCount; c++) {
                        row[c] = ArrowValues.toJavaValue(ordered.get(c), r);
                    }
                    rows.add(row);
                }
            } finally {
                batch.close();
            }
        }
        return rows;
    }

    private static List<FieldVector> orderedColumns(VectorSchemaRoot batch, List<String> targetColumnOrder) {
        if (targetColumnOrder == null || targetColumnOrder.isEmpty()) {
            return batch.getFieldVectors();
        }
        List<FieldVector> ordered = new ArrayList<>(targetColumnOrder.size());
        for (String name : targetColumnOrder) {
            FieldVector vector = batch.getVector(name);
            if (vector == null) {
                throw new IllegalStateException(
                    "Column [" + name + "] expected by plan row type not found in batch schema: " + batch.getSchema().getFields()
                );
            }
            ordered.add(vector);
        }
        return ordered;
    }
}
