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
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.TimeoutTaskCancellationUtility;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.AnalyticsSettings;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.analytics.exec.action.AnalyticsClearShuffleAction;
import org.opensearch.analytics.exec.action.AnalyticsClearShuffleRequest;
import org.opensearch.analytics.exec.action.AnalyticsClearShuffleResponse;
import org.opensearch.analytics.exec.action.AnalyticsQueryAction;
import org.opensearch.analytics.exec.action.AnalyticsQueryRequest;
import org.opensearch.analytics.exec.action.AnalyticsQueryResponse;
import org.opensearch.analytics.exec.agg.AggregateStrategyAdvisor;
import org.opensearch.analytics.exec.agg.HashShuffleAggregateDispatch;
import org.opensearch.analytics.exec.join.BroadcastDispatch;
import org.opensearch.analytics.exec.join.CascadeShuffleDAGRewriter;
import org.opensearch.analytics.exec.join.CascadeShuffleDispatch;
import org.opensearch.analytics.exec.join.CascadeShufflePlanRewriter;
import org.opensearch.analytics.exec.join.DistributedAggOverJoinDispatch;
import org.opensearch.analytics.exec.join.DistributedAggOverJoinRewriter;
import org.opensearch.analytics.exec.join.HashShuffleDispatch;
import org.opensearch.analytics.exec.join.JoinStrategyAdvisor;
import org.opensearch.analytics.exec.join.MppShufflePartitions;
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
import org.opensearch.analytics.spi.BroadcastSizeExceededException;
import org.opensearch.analytics.stats.AnalyticsStatsCollector;
import org.opensearch.arrow.allocator.AllocationRejection;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.SearchService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    // Per-row byte estimate for converting the coordinator buffer limit into a bottom-join row floor for
    // the agg-over-join shuffle seed. Deliberately a HIGH estimate of the gathered row width: a higher
    // bytes/row yields a LOWER row threshold, so we seed earlier and never miss a join whose gather would
    // overflow (e.g. TPC-H partsupp's 8M rows gather to ~270MB ≈ 34 B/row — a 32 B estimate put the
    // threshold just ABOVE 8M and missed it). 64 B/row → ~4.2M-row floor at the 256 MiB default, catching
    // q2/q11 with margin while leaving genuinely small joins coordinator-centric. The seed is
    // correctness-safe at any size; the floor only avoids needless shuffles of joins that gather safely.
    private static final long ASSUMED_BYTES_PER_GATHERED_ROW = 64L;

    private final CapabilityRegistry capabilityRegistry;
    private final ClusterService clusterService;
    private final TransportService transportService;
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
        this.transportService = transportService;
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
        applyParentTask(request, queryCtx);
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
        applyParentTask(request, queryCtx);
        client.execute(
            AnalyticsQueryAction.INSTANCE,
            request,
            ActionListener.wrap(resp -> listener.onResponse(resp.getProfiledResult()), listener::onFailure)
        );
    }

    /**
     * Registers the analytics query task as a child of the front-end task so a front-end cancel
     * (disconnect / timeout / {@code _tasks/_cancel}) cascades into the query and its fragments.
     * Set here, not in {@code createTask}, because the parent {@link TaskId} needs the local node id.
     */
    private void applyParentTask(AnalyticsQueryRequest request, QueryRequestContext queryCtx) {
        if (queryCtx == null || queryCtx.parentTask() == null) {
            return;
        }
        request.setParentTask(new TaskId(clusterService.localNode().getId(), queryCtx.parentTask().getId()));
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
        executeInternal(queryTask, logicalFragment, queryCtx, profile, listener, /* broadcastDisabled */ false);
    }

    /**
     * @param broadcastDisabled when true, the planner is told to suppress every BROADCAST join
     *   alternative (via {@code PlannerContext.setBroadcastEligible(false)}, which makes
     *   {@code OpenSearchBroadcastJoinSplitRule.matches()} return false). Set on the automatic retry
     *   after a broadcast build overflowed the runtime cap at execution time — pre-flight row
     *   estimates can under-count filter/semijoin selectivity, so a build CBO judged small enough can
     *   still blow the cap; rather than fail, we re-plan without broadcast so CBO falls back to
     *   hash-shuffle / coordinator-centric. The
     *   flag also guards against infinite retry (the second attempt never picks broadcast).
     */
    private void executeInternal(
        AnalyticsQueryTask queryTask,
        RelNode logicalFragment,
        QueryRequestContext queryCtx,
        boolean profile,
        ActionListener<ProfiledResult> outerListener,
        boolean broadcastDisabled
    ) {
        // Broadcast→shuffle retry: on the FIRST attempt, intercept a terminal failure whose cause
        // chain holds a BroadcastSizeExceededException (the build overflowed the runtime cap — a
        // pre-flight under-estimate of filter/semijoin selectivity) and re-plan once with broadcast
        // made ineligible. The broadcastDisabled flag bounds it to a single retry. Mirrors
        // Presto-on-Spark's DISABLE_BROADCAST_JOIN retry.
        //
        // Broadcast→shuffle retry, in two halves so it is both ORDERING-safe and EXCEPTION-safe:
        // 1) The wrapper below only DETECTS the overflow and stashes it in retryOverflow; it does
        // NOT submit the retry. This keeps the outer listener un-fired on the overflow path.
        // 2) The actual retry is submitted from a runAfter installed on the terminal batches
        // listener (see below), which runs strictly AFTER attempt-1's QueryContext/allocator
        // teardown completes — so attempt 2 never overlaps attempt 1's allocator under the
        // shared pool. It is dispatched on the SEARCH executor (off the callback thread, with a
        // fresh THREAD_PROVIDERS) and guarded by try/catch so a synchronous planning/conversion
        // throw in attempt 2 still reaches outerListener instead of being lost on a worker.
        // retryOverflow is non-null only on the first attempt's overflow; broadcastDisabled bounds
        // it to a single retry.
        final AtomicReference<BroadcastSizeExceededException> retryOverflow = new AtomicReference<>();
        final ActionListener<ProfiledResult> listener;
        if (broadcastDisabled) {
            listener = outerListener;
        } else {
            listener = ActionListener.wrap(result -> {
                BroadcastSizeExceededException overflow = result.isSuccess()
                    ? null
                    : (BroadcastSizeExceededException) ExceptionsHelper.unwrap(result.failure(), BroadcastSizeExceededException.class);
                if (overflow != null) {
                    retryOverflow.set(overflow); // retry submitted post-teardown by the runAfter below
                } else {
                    outerListener.onResponse(result);
                }
            }, outerListener::onFailure);
        }
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
            // Cascade kill switch must follow dynamic updates too — the rewrite is gated on this in
            // executeInternal below; without the overlay a PUT /_cluster/settings to disable cascade
            // would be ignored and the rewrite would keep firing on the node-bootstrap default (true).
            .put(
                AnalyticsSettings.MPP_SHUFFLE_CASCADE_ENABLED.getKey(),
                clusterService.getClusterSettings().get(AnalyticsSettings.MPP_SHUFFLE_CASCADE_ENABLED)
            )
            // Distributed-agg-over-cascade kill switch must follow dynamic updates too — the dispatch
            // branch below gates on it; without the overlay a PUT /_cluster/settings disable would be
            // ignored and the rewrite would keep firing on the node-bootstrap default (true).
            .put(
                AnalyticsSettings.MPP_SHUFFLE_AGGREGATE_OVER_JOIN_ENABLED.getKey(),
                clusterService.getClusterSettings().get(AnalyticsSettings.MPP_SHUFFLE_AGGREGATE_OVER_JOIN_ENABLED)
            )
            .put(
                AnalyticsSettings.MPP_BROADCAST_PROBE_ESTIMATE.getKey(),
                clusterService.getClusterSettings().get(AnalyticsSettings.MPP_BROADCAST_PROBE_ESTIMATE)
            )
            // Overlay the broadcast byte cap too: OpenSearchBroadcastJoinSplitRule's pre-flight size
            // gate reads it from PlannerContext settings, and it must agree with the runtime cap
            // BroadcastCaptureSink enforces (DefaultPlanExecutor reads it live from cluster settings).
            // Without this, a dynamic PUT /_cluster/settings update would shift the runtime cap while
            // the planner gate kept the node-bootstrap value — gate and runtime would disagree mid-query.
            .put(
                AnalyticsSettings.BROADCAST_MAX_BYTES.getKey(),
                clusterService.getClusterSettings().get(AnalyticsSettings.BROADCAST_MAX_BYTES)
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
        // On the broadcast→shuffle retry, make BROADCAST ineligible so CBO falls back to
        // hash-shuffle / coordinator-centric (the build overflowed the runtime cap on attempt 1).
        plannerContext.setBroadcastEligible(!broadcastDisabled);
        RelNode plan = PlannerImpl.createPlan(logicalFragment, plannerContext);
        // Cascaded hash-shuffle: post-CBO, turn a multi-way join's coordinator-gathered inputs
        // into hash-shuffle inputs so DAGBuilder cuts a cascaded shuffle DAG (each join level its
        // own worker tier) instead of leaving the outer join COORDINATOR_CENTRIC → ReduceSize
        // overflow at scale. CBO is not shuffle-cascade-native (it only shuffles a join over two
        // pure shard scans); the rewrite extends an existing lower-level shuffle upward. Gated on
        // MPP + the cascade sub-toggle; the rewriter itself only fires on a join whose subtree
        // already shuffles, so small-probe joins CBO kept coord-centric are untouched.
        if (AnalyticsSettings.MPP_ENABLED.get(perQuerySettings) && AnalyticsSettings.MPP_SHUFFLE_CASCADE_ENABLED.get(perQuerySettings)) {
            int cascadePartitions = MppShufflePartitions.resolve(
                perQuerySettings,
                planningState,
                capabilityRegistry,
                ((OpenSearchRelNode) plan).getViableBackends()
            );
            // Seed the agg-over-join shape (q2/q11) FIRST: when a decomposable aggregate sits above a
            // multi-way INNER join whose large×small bottom join CBO kept coordinator-centric, shuffle
            // that bottom join so the cascade extend + DistributedAggOverJoinRewriter can push a PARTIAL
            // below the coordinator gather (the proven q5/q10 path). CBO won't shuffle a large×small
            // bottom join itself — broadcasting the small side still gathers the large join output to
            // SINGLETON for the parent dimension join — so the PARTIAL push is the only thing that caps
            // the gather. Gated additionally on the aggregate-over-join toggle (the seed is pointless
            // without that rewriter) and a row floor tied to the coordinator buffer (don't shuffle a
            // bottom join small enough to gather safely).
            //
            // SELF-VALIDATING: the seed shuffles a bottom join the RelNode-level shape check accepts, but
            // canPushPartial also applies DAG-level gates (each dimension must be a reducer-fed
            // StageInputScan) the seed can't fully replicate pre-DAG. Keep the seeded plan ONLY if it
            // yields a dispatchable distributed-agg-over-join DAG; otherwise revert to the un-seeded plan
            // so an un-acceptable seed can't leave an orphaned shuffle that the generic HashShuffleDispatch
            // would mis-lift (the "No table named input-N" failure). Then the existing extend runs on the
            // chosen plan.
            if (AnalyticsSettings.MPP_SHUFFLE_AGGREGATE_OVER_JOIN_ENABLED.get(perQuerySettings)) {
                long minBottomJoinRows = perQueryBufferLimit / ASSUMED_BYTES_PER_GATHERED_ROW;
                RelNode seeded = CascadeShufflePlanRewriter.seedAggOverJoinBottomShuffle(plan, cascadePartitions, minBottomJoinRows);
                if (seeded != plan) {
                    RelNode seededExtended = CascadeShufflePlanRewriter.rewrite(seeded, cascadePartitions);
                    QueryDAG trialDag = DAGBuilder.build(seededExtended, capabilityRegistry, clusterService, indexNameExpressionResolver);
                    if (DistributedAggOverJoinRewriter.canPushPartial(trialDag)) {
                        plan = seeded;
                    } else {
                        logger.debug("agg-over-join seed produced a non-distributable DAG; reverting to un-seeded plan");
                    }
                }
            }
            plan = CascadeShufflePlanRewriter.rewrite(plan, cascadePartitions);
        }
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
        // Cascade: a multi-way join where every level shuffles (>1 join-over-two-shuffles stage).
        // The single-level HashShuffleDispatch only lifts one join; the cascade dispatcher lifts
        // every level into its own worker tier. Detected structurally so a plain 2-way shuffle join
        // keeps the simpler path.
        final boolean dispatchCascadeShuffle = dispatchHashShuffle && CascadeShuffleDAGRewriter.isCascade(dag);
        // Distributed aggregation over a join (q5/q10 multi-way cascade; q2/q11 single bottom join with
        // dimension joins above): an Aggregate(SINGLE) over dimension joins over a hash-shuffle bottom
        // join. Push a PARTIAL onto the top worker (dimensions broadcast in) and keep FINAL on the
        // coordinator. canPushPartial validates the FULL shape itself (decomposable agg, partition-
        // preserving path, exactly one liftable join-over-two-shuffles, broadcastable dims) — it needs a
        // single liftable join, NOT a multi-LEVEL cascade — so it is gated on dispatchHashShuffle (a
        // shuffle join exists), NOT dispatchCascadeShuffle (>1 shuffle level). A q5/q10 multi-way cascade
        // and a q2/q11 single bottom join both satisfy it; takes precedence over both the plain cascade
        // and the single-level HashShuffleDispatch (which can't lift the dimension joins). See
        // DistributedAggOverJoinRewriter.
        final boolean dispatchDistributedAggOverJoin = dispatchHashShuffle
            && AnalyticsSettings.MPP_SHUFFLE_AGGREGATE_OVER_JOIN_ENABLED.get(perQuerySettings)
            && DistributedAggOverJoinRewriter.canPushPartial(dag);
        final boolean dispatchHashShuffleAggregate = shuffleAggProducer != null && isQueryScheduler;
        // Whether the plan contains any HASH exchange — the physical signal that ShuffleBufferManager
        // buffers may be populated on data nodes (join shuffle, cascade, or shuffle-aggregate all cut
        // a HASH_DISTRIBUTED exchange in the DAG). Drives the terminal cleanup broadcast: skip it for
        // the common non-shuffle query so we don't fan O(data-nodes) no-op RPCs on every analytics
        // query. Computed from the DAG (not the dispatch flags) so a shuffle plan that falls back to
        // coord-centric at dispatch still cleans up any buffers a partially-run producer created.
        final boolean planUsesShuffle = dagHasHashExchange(dag);

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
            // Release shuffle buffers on query terminal (success / failure / cancel). This runAfter
            // fires unconditionally, so it covers the FAILURE path that does NOT cancel data-node
            // tasks (e.g. a shuffle byte-budget breach) and so never triggers the per-task
            // cancellation-listener cleanup — without this, a failed shuffle query leaks its buffered
            // byte[] on-heap until the next query OOMs. Gated on the plan actually containing a HASH
            // exchange: only hash-shuffle producers populate ShuffleBufferManager, so a non-shuffle
            // query (the vast majority) needn't pay O(data-nodes) cleanup RPCs. The gate
            // over-approximates safely — it fires even if dispatch later falls back to coord-centric
            // (a harmless no-op broadcast), and never misses a real shuffle (a miss re-opens the leak).
            if (planUsesShuffle) {
                broadcastClearShuffle(dag.queryId());
            }
            try {
                context.close();
            } catch (Exception e) {
                logger.warn(new ParameterizedMessage("[query-{}] QueryContext.close() threw on teardown", dag.queryId()), e);
            }
            // Submit the broadcast→shuffle retry ONLY after this attempt's context/allocator is
            // closed above, so attempt 2 never overlaps attempt 1's per-query allocator. Dispatched
            // on the SEARCH executor (off this terminal/callback thread, fresh THREAD_PROVIDERS) and
            // guarded so a synchronous planning/conversion throw in attempt 2 still reaches
            // outerListener rather than being lost on a worker thread. retryOverflow is non-null only
            // on the first attempt's overflow; the retry runs with broadcastDisabled=true (no re-retry).
            BroadcastSizeExceededException overflow = retryOverflow.get();
            if (overflow != null) {
                logger.warn(
                    "[task-{}] broadcast build overflowed the runtime cap (observed={} bytes, limit={} bytes); "
                        + "re-planning without broadcast (falling back to hash-shuffle / coordinator-centric). "
                        + "Raise analytics.mpp.broadcast.max_bytes to keep broadcasting larger builds.",
                    queryTask.getId(),
                    overflow.observedBytes(),
                    overflow.limitBytes()
                );
                ContextAwareExecutor.wrap(searchExecutor, threadPool).execute(() -> {
                    try {
                        executeInternal(queryTask, logicalFragment, queryCtx, profile, outerListener, /* broadcastDisabled */ true);
                    } catch (Exception e) {
                        outerListener.onFailure(e);
                    } catch (Throwable t) {
                        outerListener.onFailure(new RuntimeException("broadcast→shuffle retry failed", t));
                    }
                });
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
            } else if (dispatchDistributedAggOverJoin) {
                dispatchDistributedAggOverJoin(dag, context, execRef, batchesListener);
            } else if (dispatchCascadeShuffle) {
                dispatchCascadeShuffle(dag, context, execRef, batchesListener);
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
            terminal,
            preferMetadataDriver
        );
    }

    /**
     * Runs the CASCADED hash-shuffle path: a multi-way join where every level shuffles. Delegates to
     * {@link CascadeShuffleDispatch}, which rewrites the DAG into a chain of worker tiers (each join
     * level → one worker; intermediate workers consume the level below and produce to the level
     * above) and enriches every level's shuffle instructions. Unlike {@link #dispatchHashShuffle},
     * there is no single (left, right, consumer) triple — the dispatcher walks all levels.
     */
    private void dispatchCascadeShuffle(
        QueryDAG dag,
        QueryContext context,
        AtomicReference<QueryExecution> execRef,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        QueryScheduler qscheduler = (QueryScheduler) scheduler;
        new CascadeShuffleDispatch(qscheduler, clusterService, shuffleBufferManager, capabilityRegistry, preferMetadataDriver).run(
            context,
            dag,
            execRef::set,
            terminal
        );
    }

    /**
     * Runs the distributed-aggregation-over-cascade-join path (q5/q10). Delegates to
     * {@link DistributedAggOverJoinDispatch}, which rewrites the DAG so the cascade top worker carries
     * the pre-aggregate pipeline + {@code Aggregate(PARTIAL)} (dimension tables broadcast into the
     * worker), leaving {@code Aggregate(FINAL) → Sort} on the coordinator. Phase 1 builds + captures
     * each dimension's Arrow IPC; phase 2 injects it into the worker and runs the cascade. Uses the
     * same backend capture-sink factory as {@link #dispatchBroadcast}.
     */
    private void dispatchDistributedAggOverJoin(
        QueryDAG dag,
        QueryContext context,
        AtomicReference<QueryExecution> execRef,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        Stage root = dag.rootStage();
        List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(
            capabilityRegistry,
            ((OpenSearchRelNode) root.getFragment()).getViableBackends()
        );
        if (reduceViable.isEmpty()) {
            throw new IllegalStateException("No reduce-capable backend for distributed-agg-over-join dimension capture sink");
        }
        final String captureBackendId = reduceViable.get(0);
        final long broadcastMaxBytes = clusterService.getClusterSettings().get(AnalyticsSettings.BROADCAST_MAX_BYTES).getBytes();
        QueryScheduler qscheduler = (QueryScheduler) scheduler;
        new DistributedAggOverJoinDispatch(
            qscheduler.getStageExecutionBuilder(),
            qscheduler,
            clusterService,
            shuffleBufferManager,
            capabilityRegistry,
            preferMetadataDriver
        ).run(
            context,
            dag,
            buildStage -> capabilityRegistry.getBackend(captureBackendId)
                .getExchangeSinkProvider()
                .createBroadcastCaptureSink(context.bufferAllocator(), buildStage.getFragment().getRowType(), broadcastMaxBytes),
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

    /** True if any stage in the DAG carries a HASH-distributed exchange — i.e. the plan can populate
     *  {@link org.opensearch.analytics.exec.shuffle.ShuffleBufferManager} buffers on data nodes
     *  (join shuffle, cascade, or shuffle-aggregate). Used to gate the terminal cleanup broadcast so
     *  non-shuffle queries don't fan no-op cleanup RPCs to every data node. Over-approximates safely. */
    private static boolean dagHasHashExchange(QueryDAG dag) {
        return dag != null && stageHasHashExchange(dag.rootStage());
    }

    private static boolean stageHasHashExchange(Stage stage) {
        if (stage == null) {
            return false;
        }
        if (stage.getExchangeInfo() != null && stage.getExchangeInfo().distributionType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            return true;
        }
        for (Stage child : stage.getChildStages()) {
            if (stageHasHashExchange(child)) {
                return true;
            }
        }
        return false;
    }

    /** Max attempts to deliver a shuffle-cleanup RPC to one node before giving up. A still-alive node
     *  whose cleanup never lands holds those buffers until it restarts (no later terminal clears THIS
     *  queryId), so we retry across a generous window (≈26s with the cap below) to ride out transient
     *  transport blips before accepting the rare leak. */
    private static final int CLEAR_SHUFFLE_MAX_ATTEMPTS = 10;
    /** Base backoff between cleanup retries; doubles each attempt, capped at {@link #CLEAR_SHUFFLE_MAX_BACKOFF_MS}. */
    private static final long CLEAR_SHUFFLE_RETRY_BASE_MS = 200L;
    /** Backoff ceiling (mirrors the sender retry cap) so late attempts don't grow unbounded. */
    private static final long CLEAR_SHUFFLE_MAX_BACKOFF_MS = 5_000L;

    /**
     * Broadcasts a shuffle-buffer release for {@code queryId} to every data node, on query terminal
     * (success / failure / cancel). This is the cleanup the FAILURE path needs — a failed query does
     * not cancel data-node tasks, so the per-task cancellation-listener cleanup never fires; an
     * undelivered release permanently shrinks that node's per-node shuffle budget (the buffer is
     * never reclaimed), so a transient send failure is retried with bounded backoff rather than just
     * logged. Each node's {@code clearForQuery} is idempotent and a no-op when it holds no buffers,
     * so re-sends are harmless and we don't track the exact participating set. A retry is abandoned
     * once the node leaves the cluster (its buffers died with the process) or the attempt cap is hit.
     */
    private void broadcastClearShuffle(String queryId) {
        if (queryId == null) {
            return;
        }
        Map<String, DiscoveryNode> dataNodes;
        try {
            dataNodes = clusterService.state().nodes().getDataNodes();
        } catch (Exception e) {
            logger.warn(new ParameterizedMessage("[query-{}] could not resolve data nodes for shuffle cleanup", queryId), e);
            return;
        }
        if (dataNodes == null || dataNodes.isEmpty()) {
            return;
        }
        AnalyticsClearShuffleRequest request = new AnalyticsClearShuffleRequest(queryId);
        for (DiscoveryNode node : dataNodes.values()) {
            sendClearShuffle(queryId, node, request, 1);
        }
    }

    /** Sends one shuffle-cleanup RPC to {@code node}; on failure reschedules up to
     *  {@link #CLEAR_SHUFFLE_MAX_ATTEMPTS} with exponential backoff, abandoning once the node has left
     *  the cluster (no buffers to leak) or the cap is reached. */
    private void sendClearShuffle(String queryId, DiscoveryNode node, AnalyticsClearShuffleRequest request, int attempt) {
        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.REG).build();
        try {
            transportService.sendRequest(
                node,
                AnalyticsClearShuffleAction.NAME,
                request,
                options,
                new TransportResponseHandler<AnalyticsClearShuffleResponse>() {
                    @Override
                    public AnalyticsClearShuffleResponse read(StreamInput in) throws IOException {
                        return new AnalyticsClearShuffleResponse(in);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public void handleResponse(AnalyticsClearShuffleResponse response) {
                        // delivered — buffers (if any) released on the node
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        retryClearShuffle(queryId, node, request, attempt, exp);
                    }
                }
            );
        } catch (Exception e) {
            retryClearShuffle(queryId, node, request, attempt, e);
        }
    }

    /** Reschedules a failed cleanup send if the node is still in the cluster and attempts remain. */
    private void retryClearShuffle(String queryId, DiscoveryNode node, AnalyticsClearShuffleRequest request, int attempt, Exception cause) {
        // A node no longer in the cluster has dropped its on-heap buffers with the process — nothing
        // to reclaim, so stop retrying.
        if (!clusterService.state().nodes().nodeExists(node)) {
            logger.debug(
                new ParameterizedMessage("[query-{}] shuffle cleanup to {} abandoned — node left cluster", queryId, node.getId()),
                cause
            );
            return;
        }
        if (attempt >= CLEAR_SHUFFLE_MAX_ATTEMPTS) {
            logger.warn(
                new ParameterizedMessage(
                    "[query-{}] shuffle cleanup to {} failed after {} attempts while the node is still in the cluster; "
                        + "it may hold this query's shuffle buffers until it restarts (no later query terminal clears this queryId)",
                    queryId,
                    node.getId(),
                    attempt
                ),
                cause
            );
            return;
        }
        // Exponential backoff capped at the ceiling; guard the shift against overflow at high attempts.
        int shift = Math.min(attempt - 1, 32);
        long delayMs = Math.min(CLEAR_SHUFFLE_MAX_BACKOFF_MS, CLEAR_SHUFFLE_RETRY_BASE_MS << shift);
        try {
            threadPool.schedule(
                () -> sendClearShuffle(queryId, node, request, attempt + 1),
                TimeValue.timeValueMillis(delayMs),
                ThreadPool.Names.SAME
            );
        } catch (Exception e) {
            logger.debug(new ParameterizedMessage("[query-{}] could not schedule shuffle cleanup retry to {}", queryId, node.getId()), e);
        }
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
            // inner scheduler.execute result through a Consumer back to execRef. A fast query can
            // also fire this terminal inline (on the calling thread, inside scheduler.execute)
            // BEFORE execRef.set runs. Either way, if execRef (or its graph) is null, fall back to a
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
        ActionListener<AnalyticsQueryResponse> convertingListener = ActionListener.wrap(listener::onResponse, e -> {
            Exception converted = e instanceof Exception ex ? contextProvider.convertException(ex) : new RuntimeException(e);
            listener.onFailure(converted);
        });
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
