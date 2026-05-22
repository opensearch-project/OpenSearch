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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.TimeoutTaskCancellationUtility;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.AnalyticsSettings;
import org.opensearch.analytics.EngineContext;
import org.opensearch.analytics.exec.action.AnalyticsQueryAction;
import org.opensearch.analytics.exec.join.BroadcastDAGRewriter;
import org.opensearch.analytics.exec.join.BroadcastDispatch;
import org.opensearch.analytics.exec.join.JoinStrategy;
import org.opensearch.analytics.exec.join.JoinStrategyAdvisor;
import org.opensearch.analytics.exec.join.JoinStrategyMetrics;
import org.opensearch.analytics.exec.profile.ProfiledResult;
import org.opensearch.analytics.exec.profile.QueryProfile;
import org.opensearch.analytics.exec.profile.QueryProfileBuilder;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.exec.task.AnalyticsQueryTaskRequest;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.CapabilityResolutionUtils;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.dag.BackendPlanAdapter;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.FragmentConversionDriver;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
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

    /**
     * Default broadcast eligibility threshold — if the smaller side's shard count is at or below
     * this value, BROADCAST is selected. Picked conservatively so that single-shard indices are
     * always broadcast-eligible. A future cluster-setting wire-up will make this dynamic.
     */
    private static final int DEFAULT_BROADCAST_MAX_SHARDS = 2;

    private final CapabilityRegistry capabilityRegistry;
    private final ClusterService clusterService;
    private final Scheduler scheduler;
    private final Executor searchExecutor;
    private final ThreadPool threadPool;
    private final TaskManager taskManager;
    private final NodeClient client;
    private final JoinStrategyMetrics joinStrategyMetrics;
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
        JoinStrategyMetrics joinStrategyMetrics,
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
        this.joinStrategyMetrics = joinStrategyMetrics;
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
                executeInternal(
                    logicalFragment,
                    false,
                    ActionListener.wrap(result -> convertingListener.onResponse(result.rows()), convertingListener::onFailure)
                );
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
                listener.onFailure(new IllegalStateException("Analytics-engine executor rejected the plan: " + e.getMessage(), e));
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

        // Join strategy selection: inspect the DAG for a binary join and tag stage roles.
        //
        // For COORDINATOR_CENTRIC (no join, or a join that can't be MPP'd) the existing
        // single-pass dispatch works unchanged.
        //
        // For BROADCAST and HASH_SHUFFLE we currently degrade to the same coordinator-centric
        // dispatch: the build/probe or left/right scan stages reduce to SINGLETON and the join
        // runs on the coordinator (M0 shape). The phased scheduler, broadcast injection, and
        // shuffle transport paths are all SPI-complete but not yet wired end-to-end (doc 65 M1
        // probe-push-down / M2 shuffle-worker dispatch). Falling back — rather than rejecting
        // the query — preserves correctness: every query that works today keeps working while
        // the faster MPP paths are landed.
        //
        // HASH_SHUFFLE intentionally falls through to coordinator-centric rather than throwing
        // — large-index equi-joins must keep working on the M0 path until M2 lands. The chosen
        // strategy is logged below for observability.
        // Master kill switch — operators can disable MPP (broadcast / hash-shuffle) cluster-wide
        // and force every join through the coordinator-centric path. Useful as an incident
        // response control or for A/B comparison. Read first and short-circuit so the advisor's
        // synchronous IndicesStats fan-out is skipped entirely when MPP is off — otherwise the
        // kill switch leaves per-query stats load in place even though every join is forced
        // back to coord-centric, defeating the purpose of the setting.
        final boolean mppEnabled = clusterService.getClusterSettings().get(AnalyticsSettings.MPP_ENABLED);
        JoinStrategy joinStrategy;
        if (mppEnabled) {
            long broadcastMaxRows = clusterService.getClusterSettings().get(AnalyticsSettings.BROADCAST_MAX_ROWS);
            JoinStrategyAdvisor advisor = new JoinStrategyAdvisor(DEFAULT_BROADCAST_MAX_SHARDS, broadcastMaxRows, client);
            joinStrategy = advisor.adviseAndTag(dag, clusterService.state());
        } else {
            joinStrategy = JoinStrategy.COORDINATOR_CENTRIC;
        }

        final boolean dispatchBroadcast = mppEnabled && joinStrategy == JoinStrategy.BROADCAST && scheduler instanceof QueryScheduler;
        if (mppEnabled && joinStrategy == JoinStrategy.HASH_SHUFFLE) {
            logger.info("[DefaultPlanExecutor] HASH_SHUFFLE not yet wired end-to-end; falling back to coordinator-centric.");
        } else if (mppEnabled && joinStrategy == JoinStrategy.BROADCAST && !dispatchBroadcast) {
            logger.info(
                "[DefaultPlanExecutor] BROADCAST selected but scheduler is {}, not QueryScheduler; falling back to coordinator-centric.",
                scheduler.getClass().getSimpleName()
            );
        }

        // Record the routed strategy — what we actually run, not just what the advisor picked.
        // Kill-switch downgrades and HASH_SHUFFLE → coord-centric fallthrough both collapse to
        // COORDINATOR_CENTRIC here. Tests use the counter delta around a query to assert the
        // BROADCAST path actually fired (vs. silently degrading to M0).
        //
        // Gated on the DAG actually containing a join: scans, aggregations, and other non-join
        // queries route through the same code path, but counting them as COORDINATOR_CENTRIC
        // would swamp the metric (in any mixed workload non-joins dominate) and make the
        // /_analytics/_strategies API misleading for its documented purpose.
        if (JoinStrategyAdvisor.containsJoin(dag)) {
            joinStrategyMetrics.recordDispatch(dispatchBroadcast ? JoinStrategy.BROADCAST : JoinStrategy.COORDINATOR_CENTRIC);
        }

        // Register coordinator-level query task with TaskManager (like SearchTask).
        // This gives us a proper unique ID, visibility in _tasks API, and cancellation support.
        // TODO: accept a request type from FrontEnd including cancelAfterTimeInterval — set from cluster settings below, null in req.
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

        /*
        Profile and explain are captured within the QueryExecution, however QueryExecution requires the complete
        batchesListener to construct the ExecutionGraph. To get around this circular dependency we build a profiling
        listener with an empty QueryExecution reference, and then populate it once constructed.
         */
        final AtomicReference<QueryExecution> execRef = new AtomicReference<>();

        ActionListener<Iterable<Object[]>> rowsListener = profile
            ? buildProfilingRowsListener(execRef, context, fullPlan, planningTimeMs, listener)
            : ActionListener.wrap(rows -> listener.onResponse(new ProfiledResult(rows, null, null)), listener::onFailure);

        // Close the *original* QueryContext on every terminal — success or failure. For coord-
        // centric queries, QueryExecution.close() also calls config::close on the same context;
        // QueryContext.close() is idempotent so the duplicate is a no-op. For broadcast queries,
        // QueryExecution operates on the pass-2 derived (non-owning) context, so it never frees
        // the owning allocator — this runAfter is the only path that does. Without it, every
        // broadcast query leaks its per-query Arrow allocator (and any failed/cancelled query
        // that bails before QueryExecution is even constructed leaks unconditionally).
        ActionListener<Iterable<VectorSchemaRoot>> batchesListener = ActionListener.runAfter(
            ActionListener.wrap(batches -> rowsListener.onResponse(batchesToRows(batches)), rowsListener::onFailure),
            () -> {
                try {
                    context.close();
                } catch (Exception e) {
                    logger.warn(new ParameterizedMessage("[query-{}] QueryContext.close() threw on teardown", dag.queryId()), e);
                } finally {
                    taskManager.unregister(queryTask);
                }
            }
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

        // Route synchronous setup failures (DAG rewrite, fragment conversion, capture-sink
        // construction, scheduler dispatch wiring) through batchesListener so the per-query
        // allocator is closed and the AnalyticsQueryTask is unregistered. Without this guard,
        // a synchronous throw would escape to execute()'s outer catch which calls
        // listener.onFailure directly — leaving the task registered and the allocator open.
        try {
            if (dispatchBroadcast) {
                dispatchBroadcast(dag, context, batchesListener);
            } else {
                // execRef read by profile listener after execution completes
                execRef.set(scheduler.execute(context, batchesListener));
            }
        } catch (Exception e) {
            batchesListener.onFailure(e);
        } catch (Throwable t) {
            batchesListener.onFailure(new RuntimeException("dispatch failed", t));
        }
    }

    /**
     * Runs the M1 broadcast-join path: rewrites the DAG so the join lives on a new probe-side
     * stage, runs pass 1 (build only) with a backend-supplied capture sink, then pass 2 (probe
     * + root) with the broadcast instruction appended to the probe plan alternatives.
     *
     * <p>Any failure inside the dispatcher fires {@code terminal.onFailure(...)}; we do not fall
     * back to coordinator-centric at this point because the rewriter has already mutated the
     * DAG shape and re-run plan forking / conversion against it. Structural rewrite errors
     * surface as exceptions from {@link BroadcastDAGRewriter#rewrite} and are caught by
     * {@link #execute}'s outer {@code try/catch}, which invokes {@code listener.onFailure}.
     */
    private void dispatchBroadcast(QueryDAG dag, QueryContext context, ActionListener<Iterable<VectorSchemaRoot>> terminal) {
        QueryDAG rewritten = BroadcastDAGRewriter.rewrite(dag, capabilityRegistry, clusterService);
        // Backend-side fragment conversion is deliberately NOT done inside the rewriter so that
        // mock-backed planner tests can drive the rewrite. Run it here against the real backend.
        FragmentConversionDriver.convertAll(rewritten, capabilityRegistry);
        Stage rewrittenRoot = rewritten.rootStage();
        if (rewrittenRoot.getChildStages().size() != 1) {
            throw new IllegalStateException(
                "Rewritten broadcast DAG must have exactly one root child (the probe stage); got " + rewrittenRoot.getChildStages().size()
            );
        }
        Stage probe = rewrittenRoot.getChildStages().get(0);
        if (probe.getChildStages().size() != 1) {
            throw new IllegalStateException(
                "Rewritten broadcast DAG's probe stage must have exactly one child (the build stage); got " + probe.getChildStages().size()
            );
        }
        Stage build = probe.getChildStages().get(0);

        // Pick a capture sink from the first reduce-capable backend. For a single-backend
        // deployment (DataFusion) this is the only candidate and mirrors DAGBuilder's reduce
        // sink provider lookup.
        List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(
            capabilityRegistry,
            ((org.opensearch.analytics.planner.rel.OpenSearchRelNode) dag.rootStage().getFragment()).getViableBackends()
        );
        if (reduceViable.isEmpty()) {
            throw new IllegalStateException("No reduce-capable backend for broadcast capture sink");
        }
        final String captureBackendId = reduceViable.get(0);
        QueryScheduler qscheduler = (QueryScheduler) scheduler;
        BroadcastDispatch dispatch = new BroadcastDispatch(qscheduler.getStageExecutionBuilder(), qscheduler);
        QueryContext rewrittenCtx = context.withDag(rewritten);

        // Pass the build's RelDataType so the backend can build a fallback Arrow schema for
        // the IPC header. An all-empty build payload then registers a memtable with the real
        // row type instead of a zero-column one, so the probe-side join's NamedScan still
        // binds correctly and the join produces zero matches for INNER joins.
        final org.apache.calcite.rel.type.RelDataType buildRowType = build.getFragment().getRowType();
        // Runtime byte cap from settings — the sink fails the dispatcher's terminal listener
        // when accumulated buffer size exceeds this, preventing runaway broadcast payloads
        // from blowing up coordinator memory.
        final long broadcastMaxBytes = clusterService.getClusterSettings().get(AnalyticsSettings.BROADCAST_MAX_BYTES).getBytes();
        dispatch.run(
            rewrittenCtx,
            rewritten,
            build,
            probe,
            rewrittenRoot,
            () -> capabilityRegistry.getBackend(captureBackendId)
                .getExchangeSinkProvider()
                .createBroadcastCaptureSink(rewrittenCtx.bufferAllocator(), buildRowType, broadcastMaxBytes),
            terminal
        );
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
