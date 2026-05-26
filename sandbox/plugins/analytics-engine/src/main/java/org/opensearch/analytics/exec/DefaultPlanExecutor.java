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
import java.util.function.ToLongFunction;

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
    private final JoinStrategyMetrics joinStrategyMetrics;
    private final EngineContext engineContext;
    private final org.opensearch.analytics.exec.shuffle.ShuffleBufferManager shuffleBufferManager;
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
        ArrowAllocatorService allocatorService,
        org.opensearch.analytics.exec.shuffle.ShuffleBufferManager shuffleBufferManager
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
        this.shuffleBufferManager = shuffleBufferManager;
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
            } catch (AssertionError e) {
                // Calcite's Litmus.THROW (used by RelOptUtil.eq, RexUtil.isFlat, Project.isValid,
                // RexChecker) throws AssertionError directly via Java code rather than via the
                // `assert` keyword, so JVM -da doesn't gate them. If one fires inside this
                // executor, OpenSearchUncaughtExceptionHandler exits the cluster JVM. Convert to
                // an IllegalStateException so the query path treats it as a per-query failure
                // (HTTP 500 with a bucketable message) instead of cluster-fatal.
                convertingListener.onFailure(
                    new IllegalStateException("Analytics-engine executor rejected the plan: " + e.getMessage(), e)
                );
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
        // Build a per-query Settings snapshot that overlays the live cluster-state values for
        // analytics-* settings on top of the node-startup settings. Without this overlay, dynamic
        // updates via PUT /_cluster/settings (e.g. analytics.mpp.enabled) are invisible to the
        // planner rules — they'd see the node-bootstrap default forever, defeating both the
        // operator kill switch and the IT framework's per-test setSetting calls.
        final org.opensearch.common.settings.Settings perQuerySettings = org.opensearch.common.settings.Settings.builder()
            .put(clusterService.getSettings())
            .put(AnalyticsSettings.MPP_ENABLED.getKey(), clusterService.getClusterSettings().get(AnalyticsSettings.MPP_ENABLED))
            .put(
                AnalyticsSettings.MPP_SHUFFLE_PARTITIONS.getKey(),
                clusterService.getClusterSettings().get(AnalyticsSettings.MPP_SHUFFLE_PARTITIONS)
            )
            .put(
                AnalyticsSettings.MPP_BROADCAST_PROBE_ESTIMATE.getKey(),
                clusterService.getClusterSettings().get(AnalyticsSettings.MPP_BROADCAST_PROBE_ESTIMATE)
            )
            .build();
        // Fetch primary-shard doc counts for every index this query touches. The CBO cost
        // model needs them to discriminate broadcast (small build × N probes) from
        // coord-centric (gather both sides) — without real counts every scan is Calcite's
        // default 100 rows and broadcast loses the cost race against SINGLETON gather even
        // for tiny dimensions. See IndexRowCountFetcher's class javadoc for the full story.
        ToLongFunction<String> tableRowCounts = IndexRowCountFetcher.fetchFor(logicalFragment, client);
        RelNode plan = PlannerImpl.createPlan(
            logicalFragment,
            new PlannerContext(capabilityRegistry, clusterService.state(), perQuerySettings, tableRowCounts, profile)
        );
        final String fullPlan = profile ? org.apache.calcite.plan.RelOptUtil.toString(plan) : null;
        QueryDAG dag = DAGBuilder.build(plan, capabilityRegistry, clusterService);

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
        final JoinStrategy joinStrategy = JoinStrategyAdvisor.observe(dag);
        final Stage broadcastBuild = JoinStrategyAdvisor.findBroadcastBuild(dag);
        final Stage broadcastProbe = JoinStrategyAdvisor.findBroadcastProbe(dag);
        final Stage shuffleLeft = JoinStrategyAdvisor.findShuffleScanLeft(dag);
        final Stage shuffleRight = JoinStrategyAdvisor.findShuffleScanRight(dag);
        final boolean isQueryScheduler = scheduler instanceof QueryScheduler;
        final boolean dispatchBroadcast = joinStrategy == JoinStrategy.BROADCAST
            && broadcastBuild != null
            && broadcastProbe != null
            && isQueryScheduler;
        final boolean dispatchHashShuffle = joinStrategy == JoinStrategy.HASH_SHUFFLE
            && shuffleLeft != null
            && shuffleRight != null
            && isQueryScheduler;

        if (JoinStrategyAdvisor.containsJoin(dag)) {
            JoinStrategy routedStrategy;
            if (dispatchBroadcast) {
                routedStrategy = JoinStrategy.BROADCAST;
            } else if (dispatchHashShuffle) {
                routedStrategy = JoinStrategy.HASH_SHUFFLE;
            } else {
                routedStrategy = JoinStrategy.COORDINATOR_CENTRIC;
            }
            joinStrategyMetrics.recordDispatch(routedStrategy);
        }

        PlanForker.forkAll(dag, capabilityRegistry);
        BackendPlanAdapter.adaptAll(dag, capabilityRegistry);
        FragmentConversionDriver.convertAll(dag, capabilityRegistry);
        final long planningTimeMs = profile ? java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - planStartNanos) : 0;
        logger.debug("[DefaultPlanExecutor] QueryDAG:\n{}", dag);

        if (joinStrategy == JoinStrategy.HASH_SHUFFLE && !dispatchHashShuffle) {
            logger.info(
                "[DefaultPlanExecutor] HASH_SHUFFLE plan-shape produced but dispatch ineligible (left={}, right={}, scheduler={}); falling back to single-pass execution.",
                shuffleLeft != null,
                shuffleRight != null,
                scheduler.getClass().getSimpleName()
            );
        } else if (joinStrategy == JoinStrategy.BROADCAST && !dispatchBroadcast) {
            logger.info(
                "[DefaultPlanExecutor] BROADCAST plan-shape produced but scheduler is {}, not QueryScheduler; falling back to single-pass execution.",
                scheduler.getClass().getSimpleName()
            );
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
            ((org.opensearch.analytics.planner.rel.OpenSearchRelNode) root.getFragment()).getViableBackends()
        );
        if (reduceViable.isEmpty()) {
            throw new IllegalStateException("No reduce-capable backend for broadcast capture sink");
        }
        final String captureBackendId = reduceViable.get(0);
        QueryScheduler qscheduler = (QueryScheduler) scheduler;
        BroadcastDispatch dispatch = new BroadcastDispatch(qscheduler.getStageExecutionBuilder(), qscheduler);

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
            context,
            dag,
            build,
            probe,
            root,
            () -> capabilityRegistry.getBackend(captureBackendId)
                .getExchangeSinkProvider()
                .createBroadcastCaptureSink(context.bufferAllocator(), buildRowType, broadcastMaxBytes),
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
        new org.opensearch.analytics.exec.join.HashShuffleDispatch(qscheduler, clusterService, shuffleBufferManager, capabilityRegistry)
            .run(context, dag, leftProducer, rightProducer, consumer, execRef::set, terminal);
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
            // execRef is populated by scheduler.execute (single-pass) or by the MPP dispatchers
            // (BROADCAST / HASH_SHUFFLE). The dispatcher path threads its inner scheduler.execute
            // result through a Consumer back to execRef. If something in that wiring drifts and
            // execRef stays null, fall back to a planning-only profile rather than NPE-ing —
            // the failure path below uses the same fallback for consistency.
            QueryProfile qp = execRef.get() != null && execRef.get().getGraph() != null
                ? QueryProfileBuilder.snapshot(execRef.get().getGraph(), context, fullPlan, planningTimeMs)
                : new QueryProfile(context.queryId(), java.util.List.of(), planningTimeMs, 0L, java.util.List.of());
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
