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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.TimeoutTaskCancellationUtility;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.AnalyticsSettings;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.analytics.exec.action.AnalyticsQueryAction;
import org.opensearch.analytics.exec.action.AnalyticsQueryRequest;
import org.opensearch.analytics.exec.action.AnalyticsQueryResponse;
import org.opensearch.analytics.exec.agg.AggregateStrategyAdvisor;
import org.opensearch.analytics.exec.agg.HashShuffleAggregateDispatch;
import org.opensearch.analytics.exec.join.BroadcastDispatch;
import org.opensearch.analytics.exec.join.HashShuffleDispatch;
import org.opensearch.analytics.exec.join.JoinStrategyAdvisor;
import org.opensearch.analytics.exec.join.MppStrategy;
import org.opensearch.analytics.exec.join.MppStrategyMetrics;
import org.opensearch.analytics.exec.profile.ProfiledResult;
import org.opensearch.analytics.exec.profile.QueryProfile;
import org.opensearch.analytics.exec.profile.QueryProfileBuilder;
import org.opensearch.analytics.exec.shuffle.ShuffleBufferManager;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.CapabilityResolutionUtils;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.BackendPlanAdapter;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.FragmentConversionDriver;
import org.opensearch.analytics.planner.dag.PlanAlternativeSelector;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.settings.AnalyticsQuerySettings;
import org.opensearch.analytics.settings.PlannerSettings;
import org.opensearch.analytics.stats.AnalyticsStatsCollector;
import org.opensearch.arrow.allocator.AllocationRejection;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToLongFunction;

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
    private final MppStrategyMetrics mppStrategyMetrics;
    private final EngineContextProvider contextProvider;
    private final ShuffleBufferManager shuffleBufferManager;
    private final AnalyticsStatsCollector statsCollector;
    // Owned and closed by AnalyticsPlugin via the injected CoordinatorAllocatorHandle so that
    // shutdown closes this child of POOL_QUERY before arrow-base closes the root allocator.
    private final BufferAllocator coordinatorAllocator;
    private volatile long perQueryBufferLimit;
    private volatile int maxShardsPerQuery;
    private volatile int maxConcurrentShardRequestsPerNode;
    private volatile boolean preferMetadataDriver;
    private final PlannerSettings plannerSettings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final AnalyticsSearchSlowLog analyticsSearchSlowLog;

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
        IndexNameExpressionResolver indexNameExpressionResolver,
        AnalyticsSearchSlowLog analyticsSearchSlowLog,
        AnalyticsStatsCollector statsCollector,
        // Feature-branch (MPP) additions — appended last so upstream constructor extensions don't collide.
        MppStrategyMetrics mppStrategyMetrics,
        ShuffleBufferManager shuffleBufferManager
    ) {
        super(AnalyticsQueryAction.NAME, transportService, actionFilters, AnalyticsQueryRequest::new);
        this.capabilityRegistry = capabilityRegistry;
        this.clusterService = clusterService;
        this.searchExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
        this.threadPool = threadPool;
        this.client = client;
        this.scheduler = scheduler;
        this.mppStrategyMetrics = mppStrategyMetrics;
        this.contextProvider = contextProvider;
        this.shuffleBufferManager = shuffleBufferManager;
        this.statsCollector = statsCollector;
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

        // TODO: These should be honored as query params, but requires front-end changes to pass request options.
        this.maxShardsPerQuery = AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY.get(clusterService.getSettings());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY, v -> maxShardsPerQuery = v);
        this.maxConcurrentShardRequestsPerNode = AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE.get(
            clusterService.getSettings()
        );
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE,
                v -> maxConcurrentShardRequestsPerNode = v
            );
        this.preferMetadataDriver = AnalyticsPlugin.PREFER_METADATA_DRIVER.get(clusterService.getSettings());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(AnalyticsPlugin.PREFER_METADATA_DRIVER, v -> preferMetadataDriver = v);
        // Planner settings (oversampling factor + delegation block-list); self-registers update
        // consumers for live changes.
        this.plannerSettings = PlannerSettings.create(
            clusterService.getClusterSettings(),
            clusterService.getSettings(),
            capabilityRegistry
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.analyticsSearchSlowLog = analyticsSearchSlowLog;
    }

    /** Visible for testing: the live per-node concurrent-shard-request limit (reflects dynamic updates). */
    public int maxConcurrentShardRequestsPerNode() {
        return maxConcurrentShardRequestsPerNode;
    }

    /** Visible for testing: the live max-shards-per-query limit (reflects dynamic updates). */
    public int maxShardsPerQuery() {
        return maxShardsPerQuery;
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

        // Create the slow log wrapper at the start so it observes the full query lifecycle.
        final String querySource = queryCtx != null ? queryCtx.querySource() : null;
        final AnalyticsSearchSlowLog.QuerySlowLogListener queryListener = analyticsSearchSlowLog.createQueryListener(querySource);
        final long queryStartNanos = System.nanoTime();

        // Always time planning and capture the full plan: every query produces a QueryProfile
        // that's fed into AnalyticsStatsCollector for the _plugins/_analytics/stats endpoint.
        // The non-explain path drops the profile from the response after recording it; the
        // explain path returns it inline.
        final long planStartNanos = System.nanoTime();
        // Build a per-query Settings snapshot that overlays the live cluster-state values for
        // analytics-* settings on top of the node-startup settings. Without this overlay, dynamic
        // updates via PUT /_cluster/settings (e.g. analytics.mpp.enabled) are invisible to the
        // planner rules — they'd see the node-bootstrap default forever, defeating both the
        // operator kill switch and the IT framework's per-test setSetting calls.
        final Settings perQuerySettings = Settings.builder()
            .put(clusterService.getSettings())
            .put(AnalyticsSettings.MPP_ENABLED.getKey(), clusterService.getClusterSettings().get(AnalyticsSettings.MPP_ENABLED))
            .put(
                AnalyticsSettings.MPP_SHUFFLE_AGGREGATE_ENABLED.getKey(),
                clusterService.getClusterSettings().get(AnalyticsSettings.MPP_SHUFFLE_AGGREGATE_ENABLED)
            )
            .put(
                AnalyticsSettings.MPP_SHUFFLE_PARTITIONS.getKey(),
                clusterService.getClusterSettings().get(AnalyticsSettings.MPP_SHUFFLE_PARTITIONS)
            )
            .put(
                AnalyticsSettings.MPP_BROADCAST_PROBE_ESTIMATE.getKey(),
                clusterService.getClusterSettings().get(AnalyticsSettings.MPP_BROADCAST_PROBE_ESTIMATE)
            )
            .build();
        // Reuse the snapshot captured at REST entry when present; this is the same ClusterState
        // OpenSearchSchemaBuilder used to build the SchemaPlus, so planner and schema agree.
        // TODO: remove the null fallback once every front-end (test-ppl-frontend,
        // dsl-query-executor) threads an EngineContextProvider.getContext() snapshot through.
        ClusterState planningState = queryCtx != null ? queryCtx.clusterState() : clusterService.state();
        // Fetch primary-shard doc counts for every index this query touches. The CBO cost
        // model needs them to discriminate broadcast (small build × N probes) from
        // coord-centric (gather both sides) — without real counts every scan is Calcite's
        // default 100 rows and broadcast loses the cost race against SINGLETON gather even
        // for tiny dimensions. See IndexRowCountFetcher's class javadoc for the full story.
        ToLongFunction<String> tableRowCounts = IndexRowCountFetcher.fetchFor(logicalFragment, client);
        PlannerContext plannerContext = new PlannerContext(
            capabilityRegistry,
            planningState,
            indexNameExpressionResolver,
            false,
            preferMetadataDriver,
            perQuerySettings,
            tableRowCounts
        );
        plannerContext.setPlannerSettings(plannerSettings);
        RelNode plan = PlannerImpl.createPlan(logicalFragment, plannerContext);
        final String fullPlan = profile ? RelOptUtil.toString(plan) : null;
        QueryDAG dag = DAGBuilder.build(plan, capabilityRegistry, clusterService, indexNameExpressionResolver);

        // Join strategy resolution under the CBO-driven model:
        // - The Volcano CBO already chose between coord-centric, broadcast, and hash-shuffle
        // by ranking alternatives produced by the three split rules under the cost model
        // (see OpenSearchJoin{,Hash,Broadcast}JoinSplitRule + OpenSearchBroadcastExchange /
        // OpenSearchShuffleExchange cost functions). DAGBuilder cut at the chosen exchange
        // RelNodes and tagged stages BROADCAST_BUILD / BROADCAST_PROBE / SHUFFLE_*.
        // - The advisor here is read-only: it inspects role tags so we know which dispatch
        // path to invoke and which counter to increment. No decisions, no mutations.
        //
        // Counter recording sits BEFORE the plan-side pipeline (PlanForker / BackendPlanAdapter /
        // FragmentConversionDriver) so the strategy that CBO picked is observable even when
        // backend conversion fails for a not-yet-wired shape (e.g. HASH_SHUFFLE before the
        // M2 producer is filled). The "routed" claim is honest: counter reflects what the
        // dispatcher would route IF execution proceeds — partial wiring failures don't hide
        // the planner's decision from /_analytics/_strategies observers.
        final MppStrategy joinStrategy = JoinStrategyAdvisor.observe(dag);
        final Stage broadcastBuild = JoinStrategyAdvisor.findBroadcastBuild(dag);
        final Stage broadcastProbe = JoinStrategyAdvisor.findBroadcastProbe(dag);
        final Stage shuffleLeft = JoinStrategyAdvisor.findShuffleScanLeft(dag);
        final Stage shuffleRight = JoinStrategyAdvisor.findShuffleScanRight(dag);
        final Stage shuffleAggProducer = AggregateStrategyAdvisor.findAggregateShuffleProducer(dag);
        final boolean isQueryScheduler = scheduler instanceof QueryScheduler;
        final boolean dispatchBroadcast = joinStrategy == MppStrategy.BROADCAST
            && broadcastBuild != null
            && broadcastProbe != null
            && isQueryScheduler;
        final boolean dispatchHashShuffle = joinStrategy == MppStrategy.HASH_SHUFFLE
            && shuffleLeft != null
            && shuffleRight != null
            && isQueryScheduler;
        final boolean dispatchHashShuffleAggregate = shuffleAggProducer != null && isQueryScheduler;

        if (JoinStrategyAdvisor.containsJoin(dag)) {
            MppStrategy routedStrategy;
            if (dispatchBroadcast) {
                routedStrategy = MppStrategy.BROADCAST;
            } else if (dispatchHashShuffle) {
                routedStrategy = MppStrategy.HASH_SHUFFLE;
            } else {
                routedStrategy = MppStrategy.COORDINATOR_CENTRIC;
            }
            mppStrategyMetrics.recordDispatch(routedStrategy);
        } else if (AggregateStrategyAdvisor.containsFinalAggregate(dag)) {
            // Agg-shaped query (FINAL aggregate present, no join): record the dispatch shape so
            // /_analytics/_strategies surfaces whether the M3 worker tier actually fired vs.
            // fell back to coord-centric (e.g. when the SHUFFLE_SCAN_AGG producer is absent
            // because CBO picked the coord-centric alternative).
            MppStrategy routedStrategy = dispatchHashShuffleAggregate ? MppStrategy.HASH_SHUFFLE_AGG : MppStrategy.COORDINATOR_CENTRIC;
            mppStrategyMetrics.recordDispatch(routedStrategy);
        }

        PlanForker.forkAll(dag, capabilityRegistry);
        BackendPlanAdapter.adaptAll(dag, capabilityRegistry);
        // Collapse multi-backend stages to a single chosen alternative before conversion
        // so the convertor runs once per stage and the wire request carries one PlanAlternative.
        PlanAlternativeSelector.selectAll(dag, capabilityRegistry, preferMetadataDriver);
        FragmentConversionDriver.convertAll(dag, capabilityRegistry);
        final long planningTimeNanos = System.nanoTime() - planStartNanos;
        final long planningTimeMs = profile ? TimeUnit.NANOSECONDS.toMillis(planningTimeNanos) : 0;
        logger.debug("[DefaultPlanExecutor] QueryDAG:\n{}", dag);

        if (joinStrategy == MppStrategy.HASH_SHUFFLE && !dispatchHashShuffle) {
            logger.info(
                "[DefaultPlanExecutor] HASH_SHUFFLE plan-shape produced but dispatch ineligible (left={}, right={}, scheduler={}); falling back to single-pass execution.",
                shuffleLeft != null,
                shuffleRight != null,
                scheduler.getClass().getSimpleName()
            );
        } else if (joinStrategy == MppStrategy.BROADCAST && !dispatchBroadcast) {
            logger.info(
                "[DefaultPlanExecutor] BROADCAST plan-shape produced but scheduler is {}, not QueryScheduler; falling back to single-pass execution.",
                scheduler.getClass().getSimpleName()
            );
        }

        queryListener.onPlanningComplete(dag.queryId(), planningTimeNanos);

        // The task is the framework-provided task from doExecute (registered by
        // HandledTransportAction before doExecute, unregistered when the listener completes).
        // Using it — rather than self-registering a detached task — is what lets a client
        // disconnect / explicit task cancel propagate into the running query.
        queryListener.setHeaders(queryTask.getHeader(Task.X_OPAQUE_ID), queryTask.getHeader(Task.X_REQUEST_ID));
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

        // ─── Build query context ──────────────────────────────────────────
        final QueryContext context;
        try {
            context = new QueryContext(
                dag,
                threadPool,
                queryTask,
                maxConcurrentShardRequestsPerNode,
                maxShardsPerQuery,
                List.of(queryListener),
                queryAllocator,
                ownsAllocator,
                profile
            );
        } catch (Exception e) {
            if (ownsAllocator) queryAllocator.close();
            throw e;
        }

        // ─── Execution + materialization ──────────────────────────────────
        /*
        Profile and explain are captured within the QueryExecution, however QueryExecution requires the complete
        batchesListener to construct the ExecutionGraph. To get around this circular dependency we build a profiling
        listener with an empty QueryExecution reference, and then populate it once constructed.
         */
        final AtomicReference<QueryExecution> execRef = new AtomicReference<>();

        // Build the profile on every terminal — both for the _explain payload (when profile=true)
        // and for the stats collector (always). When profile=false the resulting profile is
        // recorded into the collector and dropped from the response.
        ActionListener<Iterable<Object[]>> rowsListener = buildProfilingRowsListener(
            execRef,
            context,
            fullPlan,
            planningTimeMs,
            statsCollector,
            profile,
            listener
        );

        // Close the *original* QueryContext on every terminal — success or failure. For coord-
        // centric queries, QueryExecution.close() also calls config::close on the same context;
        // QueryContext.close() is idempotent so the duplicate is a no-op. For broadcast queries,
        // QueryExecution operates on the pass-2 derived (non-owning) context, so it never frees
        // the owning allocator — this runAfter is the only path that does. Without it, every
        // broadcast query leaks its per-query Arrow allocator (and any failed/cancelled query
        // that bails before QueryExecution is even constructed leaks unconditionally).
        final List<String> outputColumnOrder = logicalFragment.getRowType().getFieldNames();
        // No taskManager.unregister here: the framework (HandledTransportAction) unregisters the
        // task it created for doExecute once this listener settles. Unregistering it ourselves
        // would double-free a task we no longer own.
        ActionListener<Iterable<VectorSchemaRoot>> batchesListener = ActionListener.runAfter(ActionListener.wrap(batches -> {
            Iterable<Object[]> rows = batchesToRows(batches, outputColumnOrder);
            long totalRows = rows instanceof List ? ((List<?>) rows).size() : 0;
            queryListener.onQueryComplete(dag.queryId(), System.nanoTime() - queryStartNanos, totalRows);
            rowsListener.onResponse(rows);
        }, rowsListener::onFailure), () -> {
            try {
                context.close();
            } catch (Exception e) {
                logger.warn(new ParameterizedMessage("[query-{}] QueryContext.close() threw on teardown", dag.queryId()), e);
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

        // Route synchronous setup failures (capture-sink construction, scheduler dispatch
        // wiring) through batchesListener so the per-query allocator is closed and the
        // AnalyticsQueryTask is unregistered. Without this guard, a synchronous throw would
        // escape to execute()'s outer catch which calls listener.onFailure directly — leaving
        // the task registered and the allocator open.
        try {
            if (dispatchBroadcast) {
                dispatchBroadcast(dag, broadcastBuild, broadcastProbe, context, execRef, batchesListener);
            } else if (dispatchHashShuffle) {
                dispatchHashShuffle(dag, shuffleLeft, shuffleRight, context, execRef, batchesListener);
            } else if (dispatchHashShuffleAggregate) {
                dispatchHashShuffleAggregate(dag, shuffleAggProducer, context, execRef, batchesListener);
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
     * Runs the broadcast-join path against a CBO-produced DAG. The DAG already has the
     * broadcast shape — root → probe stage → build stage — because the cost model picked the
     * broadcast alternative emitted by {@link
     * org.opensearch.analytics.planner.rules.OpenSearchBroadcastJoinSplitRule} and DAGBuilder
     * cut at the resulting {@link org.opensearch.analytics.planner.rel.OpenSearchBroadcastExchange}.
     * No DAG rewrite is needed.
     *
     * <p>Pass 1 runs the build stage with a backend-supplied capture sink; pass 2 runs probe +
     * root after enriching the probe stage's plan alternatives with a {@link
     * org.opensearch.analytics.spi.BroadcastInjectionInstructionNode} carrying the captured IPC
     * bytes. Failures surface via {@code terminal.onFailure(...)}.
     */
    private void dispatchBroadcast(
        QueryDAG dag,
        Stage build,
        Stage probe,
        QueryContext context,
        AtomicReference<QueryExecution> execRef,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        Stage root = dag.rootStage();
        // Pick a capture sink from the first reduce-capable backend. For a single-backend
        // deployment (DataFusion) this is the only candidate and mirrors DAGBuilder's reduce
        // sink provider lookup.
        List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(
            capabilityRegistry,
            ((OpenSearchRelNode) root.getFragment()).getViableBackends()
        );
        if (reduceViable.isEmpty()) {
            throw new IllegalStateException("No reduce-capable backend for broadcast capture sink");
        }
        final String captureBackendId = reduceViable.get(0);
        QueryScheduler qscheduler = (QueryScheduler) scheduler;
        BroadcastDispatch dispatch = new BroadcastDispatch(qscheduler.getStageExecutionBuilder(), qscheduler);

        // Runtime byte cap from settings — the sink fails the dispatcher's terminal listener
        // when accumulated buffer size exceeds this, preventing runaway broadcast payloads
        // from blowing up coordinator memory.
        final long broadcastMaxBytes = clusterService.getClusterSettings().get(AnalyticsSettings.BROADCAST_MAX_BYTES).getBytes();
        // Per-build capture-sink factory: each broadcast build gets a sink built for ITS output
        // rowType (multi-broadcast queries resolve several builds). The build's RelDataType lets
        // the backend build a fallback Arrow schema for the IPC header so an all-empty build payload
        // still registers a memtable with the real row type — the probe-side join's NamedScan binds
        // correctly and INNER joins produce zero matches rather than failing.
        dispatch.run(
            context,
            dag,
            build,
            probe,
            root,
            buildStage -> capabilityRegistry.getBackend(captureBackendId)
                .getExchangeSinkProvider()
                .createBroadcastCaptureSink(context.bufferAllocator(), buildStage.getFragment().getRowType(), broadcastMaxBytes),
            execRef::set,
            terminal
        );
    }

    /**
     * Runs the hash-shuffle path against a CBO-produced DAG. The DAG already has the shuffle
     * shape — root with two SHUFFLE_SCAN_* producer children — because the cost model picked
     * the hash alternative emitted by {@link
     * org.opensearch.analytics.planner.rules.OpenSearchHashJoinSplitRule} and DAGBuilder cut at
     * the resulting {@link org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange}s and
     * tagged the children. Pre-allocates per-partition shuffle buffers, attaches per-side
     * ShuffleProducerInstructionNodes to the producers and ShuffleScanInstructionNodes to the
     * consumer, then hands off to the standard scheduler for concurrent dispatch.
     */
    private void dispatchHashShuffle(
        QueryDAG dag,
        Stage leftProducer,
        Stage rightProducer,
        QueryContext context,
        AtomicReference<QueryExecution> execRef,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        // Both producers share the same parent (the consumer / join stage). We find it via the
        // root walk: the consumer is the parent of both producers; for a CBO-produced
        // hash-shuffle DAG with no extra wrappers the consumer IS the root, but for
        // wrappers-above-join shapes (Sort, Project) the consumer is whichever ancestor
        // contains both producers as its direct child stages. Walk from the root.
        Stage consumer = findConsumerStage(dag.rootStage(), leftProducer, rightProducer);
        if (consumer == null) {
            throw new IllegalStateException(
                "HashShuffleDispatch: could not locate consumer stage that holds both shuffle producers as children"
            );
        }
        QueryScheduler qscheduler = (QueryScheduler) scheduler;
        new HashShuffleDispatch(qscheduler, clusterService, shuffleBufferManager, capabilityRegistry).run(
            context,
            dag,
            leftProducer,
            rightProducer,
            consumer,
            execRef::set,
            terminal
        );
    }

    /**
     * Runs the hash-shuffle aggregate path against a CBO-produced DAG. The DAG's producer child
     * stage is tagged {@link Stage.StageRole#SHUFFLE_SCAN_AGG} by {@code DAGBuilder.cutShuffle}
     * when the shuffle's parent is an {@link org.opensearch.analytics.planner.rel.OpenSearchAggregate}.
     * The dispatcher lifts the FINAL aggregate into a worker stage via
     * {@link org.opensearch.analytics.exec.agg.HashShuffleAggregateDAGRewriter}, attaches the
     * single-side shuffle instructions, and hands off to the scheduler.
     */
    private void dispatchHashShuffleAggregate(
        QueryDAG dag,
        Stage producer,
        QueryContext context,
        AtomicReference<QueryExecution> execRef,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        Stage consumer = findAggregateConsumerStage(dag.rootStage(), producer);
        if (consumer == null) {
            throw new IllegalStateException(
                "HashShuffleAggregateDispatch: could not locate consumer stage that holds the agg-shuffle producer as a child"
            );
        }
        QueryScheduler qscheduler = (QueryScheduler) scheduler;
        new HashShuffleAggregateDispatch(qscheduler, clusterService, shuffleBufferManager, capabilityRegistry).run(
            context,
            dag,
            producer,
            consumer,
            execRef::set,
            terminal,
            preferMetadataDriver
        );
    }

    /** Walks {@code stage}'s subtree looking for the parent of {@code producer}. */
    private static Stage findAggregateConsumerStage(Stage stage, Stage producer) {
        if (stage == null) return null;
        if (stage.getChildStages().contains(producer)) return stage;
        for (Stage child : stage.getChildStages()) {
            Stage found = findAggregateConsumerStage(child, producer);
            if (found != null) return found;
        }
        return null;
    }

    /** Walks {@code stage}'s subtree looking for the stage whose direct {@code childStages} list
     *  contains both producers. */
    private static Stage findConsumerStage(Stage stage, Stage left, Stage right) {
        if (stage == null) return null;
        List<Stage> children = stage.getChildStages();
        if (children.contains(left) && children.contains(right)) {
            return stage;
        }
        for (Stage child : children) {
            Stage found = findConsumerStage(child, left, right);
            if (found != null) return found;
        }
        return null;
    }

    /**
     * Builds a rows listener that, on every terminal, feeds the {@link ExecutionGraph} into the
     * stats collector via {@link AnalyticsStatsCollector#recordExecution} — no allocation, no
     * plan stringification. The full {@link QueryProfile} is only built when
     * {@code includeProfileInResponse} is true (i.e. for the {@code _explain} response).
     */
    private static ActionListener<Iterable<Object[]>> buildProfilingRowsListener(
        AtomicReference<QueryExecution> execRef,
        QueryContext context,
        String fullPlan,
        long planningTimeMs,
        AnalyticsStatsCollector statsCollector,
        boolean includeProfileInResponse,
        ActionListener<ProfiledResult> listener
    ) {
        return ActionListener.wrap(rows -> {
            // execRef is populated by scheduler.execute (single-pass) or by the MPP dispatchers
            // (BROADCAST / HASH_SHUFFLE / HASH_SHUFFLE_AGG). The dispatcher path threads its
            // inner scheduler.execute result through a Consumer back to execRef. If something
            // in that wiring drifts and execRef (or its graph) stays null, fall back to a
            // planning-only profile rather than NPE-ing — the failure path below uses the same
            // fallback for consistency.
            QueryExecution successExec = execRef.get();
            ExecutionGraph graph = successExec != null ? successExec.getGraph() : null;
            statsCollector.recordExecution(graph, context.dag(), planningTimeMs);
            QueryProfile qp = includeProfileInResponse
                ? (graph != null
                    ? QueryProfileBuilder.snapshot(graph, context, fullPlan, planningTimeMs)
                    : new QueryProfile(context.queryId(), List.of(), planningTimeMs, 0L, List.of()))
                : null;
            listener.onResponse(new ProfiledResult(rows, null, qp));
        }, e -> {
            QueryExecution exec = execRef.get();
            ExecutionGraph graph = exec != null ? exec.getGraph() : null;
            statsCollector.recordExecution(graph, context.dag(), planningTimeMs);
            QueryProfile qp = includeProfileInResponse
                ? (graph != null
                    ? QueryProfileBuilder.snapshot(graph, context, fullPlan, planningTimeMs)
                    : new QueryProfile(context.queryId(), List.of(), planningTimeMs, 0L, List.of()))
                : null;
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
                    ActionListener.wrap(result -> {
                        if (result.isSuccess()) {
                            convertingListener.onResponse(
                                request.isProfile() ? new AnalyticsQueryResponse(result) : new AnalyticsQueryResponse(result.rows())
                            );
                        } else {
                            // executeWithProfile delivers failures via onResponse with a populated
                            // ProfiledResult.failure so the profile is always recorded; surface it
                            // here as a true onFailure so the caller doesn't see a null-rows response.
                            convertingListener.onFailure(
                                result.failure() instanceof Exception ex ? ex : new RuntimeException(result.failure())
                            );
                        }
                    }, convertingListener::onFailure)
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
