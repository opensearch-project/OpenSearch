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
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.IndexFilterTree;
import org.opensearch.index.engine.exec.FilterTreeCallbackBridge;
import org.opensearch.index.engine.exec.IndexFilterTreeContext;
import org.opensearch.index.engine.exec.IndexFilterTreeProvider;
import org.opensearch.index.engine.exec.SearchBackendFactory;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.io.IOException;
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
    private final Map<String, AnalyticsSearchBackendPlugin> backEnds;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final SearchBackendFactory searchBackendFactory;

    /**
     * Constructs a DefaultPlanExecutor.
     *
     * @param providers list of search execution engine providers
     * @param indicesService service for accessing index shards
     * @param clusterService service for accessing cluster state
     */
    public DefaultPlanExecutor(List<AnalyticsSearchBackendPlugin> providers, IndicesService indicesService, ClusterService clusterService) {
        this(providers, indicesService, clusterService, new SearchBackendFactory());
    }

    /**
     * Constructs a DefaultPlanExecutor with an explicit SearchBackendFactory.
     *
     * @param providers list of search execution engine providers
     * @param indicesService service for accessing index shards
     * @param clusterService service for accessing cluster state
     * @param searchBackendFactory registry for tree query providers
     */
    public DefaultPlanExecutor(
        List<AnalyticsSearchBackendPlugin> providers,
        IndicesService indicesService,
        ClusterService clusterService,
        SearchBackendFactory searchBackendFactory
    ) {
        this.backEnds = new LinkedHashMap<>();
        for (AnalyticsSearchBackendPlugin provider : providers) {
            this.backEnds.put(provider.name(), provider);
        }
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.searchBackendFactory = searchBackendFactory;
    }

    // TODO: Extract plan → optimize → fork → convert → DAG into a dedicated component (e.g. QueryDAGBuilder)
    // that takes the logical fragment and returns a fully-built DAG ready for scheduling.
    // Also add per-step timing (plan, fork, convert, schedule, execute) for observability.
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
        RelNode plan = PlannerImpl.createPlan(logicalFragment, new PlannerContext(capabilityRegistry, clusterService.state()));
        QueryDAG dag = DAGBuilder.build(plan, capabilityRegistry, clusterService);
        PlanForker.forkAll(dag, capabilityRegistry);
        FragmentConversionDriver.convertAll(dag, capabilityRegistry);
        logger.info("[DefaultPlanExecutor] QueryDAG:\n{}", dag);

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
            int colCount = batch.getFieldVectors().size();
            int rowCount = batch.getRowCount();
            for (int r = 0; r < rowCount; r++) {
                Object[] row = new Object[colCount];
                for (int c = 0; c < colCount; c++) {
                    row[c] = ArrowValues.toJavaValue(batch.getVector(c), r);
                }
                rows.add(row);
            }
            batch.close();
        }
        return rows;
    }

    static String extractTableName(RelNode node) {
        if (node instanceof TableScan) {
            List<String> qn = node.getTable().getQualifiedName();
            return qn.get(qn.size() - 1);
        }
        for (RelNode input : node.getInputs()) {
            String name = extractTableName(input);
            if (name != null) return name;
        }
        throw new IllegalArgumentException("No TableScan found in plan fragment");
    }

    private IndexShard resolveShard(String indexName) {
        IndexService indexService = indicesService.indexService(clusterService.state().metadata().index(indexName).getIndex());
        if (indexService == null) throw new IllegalStateException("Index [" + indexName + "] not on this node");
        Set<Integer> shardIds = indexService.shardIds();
        if (shardIds.isEmpty()) throw new IllegalStateException("No shards for [" + indexName + "]");
        return indexService.getShardOrNull(shardIds.iterator().next());
    }

    private AnalyticsSearchBackendPlugin selectBackEnd() {
        if (backEnds.isEmpty()) {
            logger.warn("No back-end plugins registered — queries will return empty results");
            return null;
        }
        // TODO: select based on data format available in the catalog snapshot
        return backEnds.values().iterator().next();
    }

    /**
     * Returns the SearchBackendFactory for registering tree query providers.
     *
     * @return the search backend factory
     */
    public SearchBackendFactory getSearchBackendFactory() {
        return searchBackendFactory;
    }

    // ── Tree Query Execution ────────────────────────────────────────────

    /**
     * Executes a query using the boolean filter tree path.
     * <p>
     * Orchestrates the full lifecycle: normalize tree → group CollectorLeaf by providerId →
     * look up providers from SearchBackendFactory → create tree contexts → register with
     * FilterTreeCallbackBridge → serialize tree → delegate to primary engine → cleanup.
     *
     * @param tableName      the target table name
     * @param substraitBytes serialized substrait plan bytes
     * @param tree           the boolean filter tree
     * @param collectorQueries per-provider queries indexed by providerId → query array
     * @param engine         the primary search execution engine to delegate to
     * @param ctx            the execution context
     * @return list of result rows
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    List<Object[]> executeViaTreeQuery(
        String tableName,
        byte[] substraitBytes,
        IndexFilterTree tree,
        Map<Integer, Object[]> collectorQueries,
        SearchExecEngine<ExecutionContext, EngineResultStream> engine,
        ExecutionContext ctx
    ) {
        long contextId = 0;
        List<IndexFilterTreeContext<?>> createdContexts = new ArrayList<>();

        try {
            // Step 1: Normalize the tree (De Morgan's NOT push-down)
            IndexFilterTree normalizedTree = tree.normalize();

            // Step 2: Group CollectorLeaf by providerId and create tree contexts
            // The collectorQueries map is keyed by providerId, each value is the query array for that provider
            contextId = FilterTreeCallbackBridge.createContext();

            for (Map.Entry<Integer, Object[]> entry : collectorQueries.entrySet()) {
                int providerId = entry.getKey();
                Object[] queries = entry.getValue();

                IndexFilterTreeProvider provider = searchBackendFactory.getProvider(providerId);

                // Create tree context — the provider knows its own query/reader types
                IndexFilterTreeContext<?> treeContext = provider.createTreeContext(
                    queries,
                    ctx.getReader(),
                    normalizedTree
                );
                createdContexts.add(treeContext);

                FilterTreeCallbackBridge.registerProvider(contextId, providerId, provider, treeContext);
            }

            // Step 3: Serialize tree and delegate to primary engine
            byte[] treeBytes = normalizedTree.serialize();

            logger.info(
                "[DefaultPlanExecutor] Tree query: contextId={}, providers={}, collectorLeaves={}, treeBytes={}",
                contextId, collectorQueries.size(), normalizedTree.collectorLeafCount(), treeBytes.length
            );

            // Step 4: Consume the result stream
            List<Object[]> rows = new ArrayList<>();
            try (EngineResultStream resultStream = engine.execute(ctx)) {
                Iterator<EngineResultBatch> batchIterator = resultStream.iterator();
                while (batchIterator.hasNext()) {
                    EngineResultBatch batch = batchIterator.next();
                    List<String> fieldNames = batch.getFieldNames();
                    for (int row = 0; row < batch.getRowCount(); row++) {
                        Object[] rowValues = new Object[fieldNames.size()];
                        for (int col = 0; col < fieldNames.size(); col++) {
                            rowValues[col] = batch.getFieldValue(fieldNames.get(col), row);
                        }
                        rows.add(rowValues);
                    }
                }
            }

            logger.info("[DefaultPlanExecutor] Tree query completed, {} rows", rows.size());
            return rows;

        } catch (Exception e) {
            throw new RuntimeException("Tree query execution failed: " + e.getMessage(), e);
        } finally {
            // Cleanup: unregister + close all contexts (always runs)
            try {
                if (contextId > 0) {
                    FilterTreeCallbackBridge.unregister(contextId);
                }
            } finally {
                for (IndexFilterTreeContext<?> treeCtx : createdContexts) {
                    try {
                        treeCtx.close();
                    } catch (IOException e) {
                        logger.warn("Failed to close tree context", e);
                    }
                }
            }
        }
    }
}
