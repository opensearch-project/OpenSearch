/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.backend.AnalyticsOperationListener;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.settings.AnalyticsQuerySettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Per-query context — immutable config (DAG, executor, parent task) + lazy per-query
 * resources (Arrow buffer allocator, virtual-thread executor for LOCAL tasks).
 *
 * <p>The phased MPP dispatcher ({@code UnifiedDispatch}) needs a derived context pointing at
 * a different DAG (e.g. the broadcast-free residual after build capture) that still shares this
 * context's allocator + lazy executor. Use
 * {@link #withDag(QueryDAG)} for that. The derived context is non-owning: only the original
 * context's {@link #close()} releases the allocator + shuts down the executor.
 *
 * @opensearch.internal
 */
public class QueryContext {

    /** Setting defaults for {@code analytics.query.*}; used by test contexts and as the baseline. */
    private static final int DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE = AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE
        .get(Settings.EMPTY);
    private static final int DEFAULT_MAX_SHARDS_PER_QUERY = AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY.get(Settings.EMPTY);

    private final QueryDAG dag;
    private final ThreadPool threadPool;
    private final AnalyticsQueryTask parentTask;
    private final int maxConcurrentShardRequestsPerNode;
    private final int maxShardsPerQuery;
    private final List<AnalyticsOperationListener> operationListeners;
    private final BufferAllocator allocator;
    private final boolean ownsAllocator;
    /** Whether profiling is enabled for this query (data nodes should collect and return metrics). */
    private final boolean profile;
    /**
     * Per-instance flag: has THIS context's {@link #close()} already disposed of its instance-
     * scoped resources (the owning allocator)? Independent of
     * {@link SharedState#executorClosed}, which tracks the cross-instance executor shutdown.
     */
    private boolean closed;  // guarded by synchronized(sharedState)
    /**
     * Holder for the lazy local-task executor + executor-close flag, shared across phased
     * contexts so pass 1 and pass 2 of multi-phase dispatch (e.g. M1 broadcast) reuse a single
     * executor and shut it down exactly once. Non-shared queries get a holder of their own.
     */
    private final SharedState sharedState;
    /**
     * HACK: side-table for cross-stage routing of resolved {@link ShardExecutionTarget}s.
     * Today's only consumer is the QTF (late-materialization) Phase C, which needs to map
     * an incoming row's {@code ___ugsi} ordinal back to the {@code (DiscoveryNode, ShardId)}
     * to dispatch a fetch. Stage 1 (SHARD_FRAGMENT) populates this once after resolve;
     * Stage 3 (LM) reads it.
     *
     * <p>TODO: this is a placeholder seam. {@code QueryContext} should not be a generic
     * "things stages leave for other stages to find" map. Cleaner shapes: cache on
     * {@code Stage} alongside {@code targetResolver}, or reify a typed cross-stage routing
     * table. Revisit when a second consumer appears or when extending QTF to UNION/JOIN.
     *
     * <p>Single-threaded write inside one stage's {@code materializeTasks}; reads happen
     * only after that stage SUCCEEDED → plain {@link HashMap} suffices.
     */
    private final Map<Integer, Map<Integer, ShardExecutionTarget>> resolvedTargetsByStage = new HashMap<>();

    private static final class SharedState {
        volatile ExecutorService localTaskExecutor;
        boolean executorClosed;  // guarded by synchronized(this)
    }

    public QueryContext(
        QueryDAG dag,
        ThreadPool threadPool,
        AnalyticsQueryTask parentTask,
        BufferAllocator allocator,
        boolean ownsAllocator,
        int maxConcurrentShardRequestsPerNode,
        int maxShardsPerQuery
    ) {
        this(
            dag,
            threadPool,
            parentTask,
            maxConcurrentShardRequestsPerNode,
            maxShardsPerQuery,
            List.of(),
            allocator,
            ownsAllocator,
            /* profile */ false,
            new SharedState()
        );
    }

    public QueryContext(
        QueryDAG dag,
        ThreadPool threadPool,
        AnalyticsQueryTask parentTask,
        BufferAllocator allocator,
        boolean ownsAllocator,
        int maxConcurrentShardRequestsPerNode,
        int maxShardsPerQuery,
        List<AnalyticsOperationListener> operationListeners
    ) {
        this(
            dag,
            threadPool,
            parentTask,
            maxConcurrentShardRequestsPerNode,
            maxShardsPerQuery,
            operationListeners,
            allocator,
            ownsAllocator,
            /* profile */ false,
            new SharedState()
        );
    }

    /**
     * Public constructor used by {@link DefaultPlanExecutor} — carries the {@code profile} flag
     * and fresh {@link SharedState}. Param order matches the private full-ctor (minus the
     * SharedState seam).
     */
    public QueryContext(
        QueryDAG dag,
        ThreadPool threadPool,
        AnalyticsQueryTask parentTask,
        int maxConcurrentShardRequestsPerNode,
        int maxShardsPerQuery,
        List<AnalyticsOperationListener> operationListeners,
        BufferAllocator allocator,
        boolean ownsAllocator,
        boolean profile
    ) {
        this(
            dag,
            threadPool,
            parentTask,
            maxConcurrentShardRequestsPerNode,
            maxShardsPerQuery,
            operationListeners,
            allocator,
            ownsAllocator,
            profile,
            new SharedState()
        );
    }

    /**
     * Full-parameter constructor. Private; tests use {@link #forTest} factories.
     *
     * <p>Param order: upstream-owned params first, then our (feature-branch) {@code sharedState}
     * LAST — per the "append our new params to the end of upstream-owned signatures" policy, so
     * future upstream re-syncs don't collide on it.
     */
    private QueryContext(
        QueryDAG dag,
        ThreadPool threadPool,
        AnalyticsQueryTask parentTask,
        int maxConcurrentShardRequestsPerNode,
        int maxShardsPerQuery,
        List<AnalyticsOperationListener> operationListeners,
        BufferAllocator allocator,
        boolean ownsAllocator,
        boolean profile,
        SharedState sharedState
    ) {
        this.dag = dag;
        this.threadPool = threadPool;
        this.parentTask = parentTask;
        this.maxConcurrentShardRequestsPerNode = maxConcurrentShardRequestsPerNode;
        this.maxShardsPerQuery = maxShardsPerQuery;
        this.operationListeners = operationListeners;
        this.allocator = allocator;
        this.ownsAllocator = ownsAllocator;
        this.sharedState = sharedState;
        this.profile = profile;
    }

    /**
     * Returns a derived context pointing at a different {@link QueryDAG} but sharing this
     * context's buffer allocator, parent task, executor, listener list, and lazy local-task
     * executor. Used by multi-phase join dispatch (e.g. M1 broadcast) where pass 1 drives only
     * the build stage and pass 2 drives the probe + root; both phases belong to the same query
     * and must share a single per-query allocator.
     *
     * <p>The derived context is non-owning: closing it is a no-op for the allocator. Only the
     * original context's {@link #close()} releases the allocator (and shuts down the shared
     * lazy executor) — the caller is responsible for closing the original exactly once.
     */
    public QueryContext withDag(QueryDAG newDag) {
        return new QueryContext(
            newDag,
            threadPool,
            parentTask,
            maxConcurrentShardRequestsPerNode,
            maxShardsPerQuery,
            operationListeners,
            allocator,
            /* ownsAllocator */ false,
            profile,
            sharedState
        );
    }

    public QueryDAG dag() {
        return dag;
    }

    /** Whether profiling is enabled for this query (data nodes should collect and return metrics). */
    public boolean profile() {
        return profile;
    }

    public Executor searchExecutor() {
        return threadPool != null ? threadPool.executor(ThreadPool.Names.SEARCH) : Runnable::run;
    }

    public Executor schedulerExecutor() {
        return threadPool != null ? threadPool.executor(AnalyticsPlugin.SCHEDULER_THREAD_POOL_NAME) : Runnable::run;
    }

    public Executor reduceExecutor() {
        return threadPool != null ? threadPool.executor(AnalyticsPlugin.REDUCE_THREAD_POOL_NAME) : Runnable::run;
    }

    public AnalyticsQueryTask parentTask() {
        return parentTask;
    }

    public String queryId() {
        return dag.queryId();
    }

    /** Max in-flight shard fragment requests the coordinator dispatches to any single data node. */
    public int maxConcurrentShardRequestsPerNode() {
        return maxConcurrentShardRequestsPerNode;
    }

    /**
     * Max shards a multi-index (alias / pattern / comma-list) query may fan out to before it is
     * rejected. Snapshotted from {@code analytics.query.max_shards_per_query} at query start by
     * {@link DefaultPlanExecutor} (which owns the settings-update consumer), so the value is
     * stable for this query and readable from any stage via the context.
     */
    public int maxShardsPerQuery() {
        return maxShardsPerQuery;
    }

    /** Returns the operation listeners for this query. */
    public List<AnalyticsOperationListener> operationListeners() {
        return operationListeners;
    }

    /**
     * Records the {@link ShardExecutionTarget}s resolved for a stage. Called once by the
     * stage execution after {@code TargetResolver.resolve(...)} runs. See the field-level
     * Javadoc on {@code resolvedTargetsByStage} for context on why this lives on
     * {@code QueryContext}.
     */
    public void recordResolvedTargets(int stageId, List<ShardExecutionTarget> targets) {
        Map<Integer, ShardExecutionTarget> byOrdinal = new HashMap<>(targets.size());
        for (ShardExecutionTarget t : targets) {
            byOrdinal.put(t.ordinal(), t);
        }
        resolvedTargetsByStage.put(stageId, byOrdinal);
    }

    /**
     * Returns the resolved targets for a stage keyed by per-shard ordinal (UGSI), or
     * {@code null} if that stage hasn't resolved yet (or doesn't have a resolver). The
     * Map is built once at record time so callers can do O(1) ordinal-to-target lookup.
     */
    public Map<Integer, ShardExecutionTarget> getResolvedTargets(int stageId) {
        return resolvedTargetsByStage.get(stageId);
    }

    public BufferAllocator bufferAllocator() {
        return allocator;
    }

    /** Lazy per-query virtual-thread executor for LOCAL tasks. Shared across phased contexts. */
    public ExecutorService localTaskExecutor() {
        ExecutorService exec = sharedState.localTaskExecutor;
        if (exec == null) {
            synchronized (sharedState) {
                exec = sharedState.localTaskExecutor;
                if (exec == null) {
                    if (sharedState.executorClosed) {
                        throw new IllegalStateException("QueryContext closed for query " + dag.queryId());
                    }
                    exec = Executors.newThreadPerTaskExecutor(
                        Thread.ofVirtual().name("analytics-local-task-" + dag.queryId() + "-", 0).factory()
                    );
                    sharedState.localTaskExecutor = exec;
                }
            }
        }
        return exec;
    }

    boolean ownsAllocator() {
        return ownsAllocator;
    }

    /**
     * Idempotent. Serialised with lazy-init accessors; post-close executor accessors throw.
     *
     * <p>Two close paths run independently:
     * <ul>
     *   <li><b>Per-instance:</b> if this context owns the allocator, close it exactly once
     *       <i>per instance</i>. Each instance has its own {@code closed} flag so calling
     *       {@code close()} twice on the same instance is safe even though Arrow's
     *       {@code BufferAllocator.close()} is not idempotent. (Coord-centric queries hit this
     *       path twice — once from {@code QueryExecution.close()} and once from
     *       {@code DefaultPlanExecutor.batchesListener.runAfter}.)</li>
     *   <li><b>Cross-instance:</b> shut down the lazy local-task executor exactly once across
     *       all phased sharers via {@code sharedState.executorClosed}. The original and any
     *       {@link #withDag(QueryDAG)}-derived contexts share the same executor; the first
     *       {@code close()} to reach this point shuts it down.</li>
     * </ul>
     *
     * <p>Crucially: a derived (non-owning) context's {@code close()} that runs first must NOT
     * prevent the original (owning) context from running its allocator-close branch later.
     * That's why the two flags are separate — the old single-flag design caused every
     * broadcast query to leak its allocator (the derived pass-2 context closed first, set the
     * shared flag, and the original's later teardown short-circuited before reaching the
     * allocator).
     */
    public void close() {
        // Per-instance: close the owning allocator at most once. Independent of any shared state.
        boolean closeAllocator;
        synchronized (sharedState) {
            closeAllocator = !closed && ownsAllocator;
            closed = true;
        }
        if (closeAllocator) {
            allocator.close();
        }

        // Cross-instance: shut down the lazy executor at most once across all phased sharers.
        synchronized (sharedState) {
            if (sharedState.executorClosed) return;
            sharedState.executorClosed = true;
            if (sharedState.localTaskExecutor != null) {
                sharedState.localTaskExecutor.shutdown();
                sharedState.localTaskExecutor = null;
            }
        }
    }

    // ─── Test factories ────────────────────────────────────────────────

    private static final RootAllocator TEST_ROOT = new RootAllocator(Long.MAX_VALUE);

    /** Creates a test context with a synchronous executor. */
    public static QueryContext forTest(QueryDAG dag, AnalyticsQueryTask parentTask) {
        return forTest(dag, parentTask, List.of());
    }

    /** Creates a test context with synchronous executors and the supplied operation listeners. */
    public static QueryContext forTest(QueryDAG dag, AnalyticsQueryTask parentTask, List<AnalyticsOperationListener> operationListeners) {
        BufferAllocator testAllocator = TEST_ROOT.newChildAllocator("test-" + dag.queryId(), 0, Long.MAX_VALUE);
        return new QueryContext(
            dag,
            null,
            parentTask,
            DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE,
            DEFAULT_MAX_SHARDS_PER_QUERY,
            operationListeners,
            testAllocator,
            true,
            /* profile */ false,
            new SharedState()
        );
    }
}
