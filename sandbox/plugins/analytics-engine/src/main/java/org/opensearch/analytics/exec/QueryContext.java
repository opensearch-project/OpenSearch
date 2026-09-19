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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Per-query context — immutable config (DAG, executor, parent task) + lazy per-query
 * resources (Arrow buffer allocator, platform thread-per-task executor for LOCAL tasks).
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
    private static final int DEFAULT_PRE_FILTER_SHARD_SIZE = AnalyticsQuerySettings.PRE_FILTER_SHARD_SIZE.get(Settings.EMPTY);

    private final QueryDAG dag;
    private final ThreadPool threadPool;
    private final AnalyticsQueryTask parentTask;
    private final int maxConcurrentShardRequestsPerNode;
    private final int preFilterShardSize;
    private final List<AnalyticsOperationListener> operationListeners;
    private final BufferAllocator allocator;
    private final boolean ownsAllocator;
    /** Caller-owned; see {@link #importStagingAllocator()}. Never closed by this context. */
    private BufferAllocator importStagingAllocator;
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
     * <p>The inner map is a {@link java.util.concurrent.ConcurrentHashMap} because
     * {@code retargetForRetry} may update entries concurrently when multiple shards
     * fail and retry in parallel on the scheduler thread pool.
     */
    private final Map<Integer, Map<Integer, ShardExecutionTarget>> resolvedTargetsByStage = new ConcurrentHashMap<>();

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
        int preFilterShardSize
    ) {
        this(
            dag,
            threadPool,
            parentTask,
            maxConcurrentShardRequestsPerNode,
            preFilterShardSize,
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
        int preFilterShardSize,
        List<AnalyticsOperationListener> operationListeners
    ) {
        this(
            dag,
            threadPool,
            parentTask,
            maxConcurrentShardRequestsPerNode,
            preFilterShardSize,
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
        int preFilterShardSize,
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
            preFilterShardSize,
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
        int preFilterShardSize,
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
        this.preFilterShardSize = preFilterShardSize;
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
            preFilterShardSize,
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
     * Fan-out above which the can-match pre-filter phase runs. Snapshotted from
     * {@code analytics.query.pre_filter_shard_size} at query start by {@link DefaultPlanExecutor},
     * so a mid-query settings change cannot make one stage probe and another not.
     */
    public int preFilterShardSize() {
        return preFilterShardSize;
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
        Map<Integer, ShardExecutionTarget> byOrdinal = new ConcurrentHashMap<>(targets.size());
        for (ShardExecutionTarget t : targets) {
            byOrdinal.put(t.ordinal(), t);
        }
        resolvedTargetsByStage.put(stageId, byOrdinal);
    }

    /**
     * Updates a single resolved target after a successful shard retry on a different copy.
     * This ensures downstream stages (e.g. LM fetch) route to the node that actually
     * executed the query, not the original primary that failed.
     */
    public void updateResolvedTarget(int stageId, int ordinal, ShardExecutionTarget target) {
        Map<Integer, ShardExecutionTarget> byOrdinal = resolvedTargetsByStage.get(stageId);
        if (byOrdinal != null) {
            byOrdinal.put(ordinal, target);
        }
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

    /**
     * Returns the node-scoped allocator coordinator-side Arrow C Data imports are staged on — unbounded
     * and parented at the root so an import cannot fail part-way through an array (which strands the whole
     * native batch, see {@code ShardScanExecutionContext#getImportStagingAllocator}), and long-lived
     * because the Flight transport keeps charging it after the importing stream closes.
     *
     * <p>Falls back to {@link #bufferAllocator()} when unset — that is the pre-staging behaviour, correct
     * but without the mid-import-OOM mitigation, and it keeps test contexts (whose allocators are unbounded
     * roots anyway) working without wiring one.
     */
    public BufferAllocator importStagingAllocator() {
        return importStagingAllocator != null ? importStagingAllocator : allocator;
    }

    /** Set once by {@code DefaultPlanExecutor} after construction. The caller owns the allocator. */
    public void setImportStagingAllocator(BufferAllocator importStagingAllocator) {
        this.importStagingAllocator = importStagingAllocator;
    }

    /** Lazy per-query thread-per-task executor for LOCAL tasks. Shared across phased contexts. */
    public ExecutorService localTaskExecutor() {
        ExecutorService exec = sharedState.localTaskExecutor;
        if (exec == null) {
            synchronized (sharedState) {
                exec = sharedState.localTaskExecutor;
                if (exec == null) {
                    if (sharedState.executorClosed) {
                        throw new IllegalStateException("QueryContext closed for query " + dag.queryId());
                    }
                    exec = Executors.newThreadPerTaskExecutor(localTaskThreadFactory(dag.queryId()));
                    sharedState.localTaskExecutor = exec;
                }
            }
        }
        return exec;
    }

    /**
     * Platform — deliberately NOT virtual — threads for local tasks.
     *
     * <p>A local task executes a fragment through the native backend, and the Arrow C Data Interface
     * runs its release callbacks synchronously on whichever thread drops an exported array. That
     * leaves a native frame on the stack, which pins a virtual thread to its carrier; if it then
     * blocks on a Netty allocator lock it never yields the carrier, and once every carrier is pinned
     * the thread holding the lock can never be scheduled — the node deadlocks at 0% CPU.
     *
     * <p>Thread-per-task is still right: local tasks are per-stage, not per-batch, so the count is
     * small and bounded by the query's plan rather than by its data volume.
     *
     * <p>{@code daemon(true)} is not optional. Virtual threads are always daemon, so switching to
     * platform threads silently made the daemon flag inherited from whichever thread submits the
     * first task ({@code newThreadPerTaskExecutor} calls {@code newThread} on the submitter). A
     * non-daemon local task stuck in a native call would then keep the JVM from exiting. Hand-rolled
     * rather than {@code OpenSearchExecutors.daemonThreadFactory} so the name can carry the query id,
     * which is what makes these threads attributable in a jstack of a wedged node.
     */
    static ThreadFactory localTaskThreadFactory(String queryId) {
        return Thread.ofPlatform().daemon(true).name("analytics-local-task-" + queryId + "-", 0).factory();
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
                // shutdownNow, not shutdown: these are platform threads now, so a straggler still
                // running here costs an OS thread and its 1MB stack for the node's lifetime — and we
                // are about to drop the last reference to the executor, so nothing could ever reach
                // it again. Interrupting surfaces the straggler instead of silently stranding it.
                // Nothing is dropped by cancelling queued work: a thread-per-task executor has no
                // queue, so every submitted task is already running.
                sharedState.localTaskExecutor.shutdownNow();
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
            DEFAULT_PRE_FILTER_SHARD_SIZE,
            operationListeners,
            testAllocator,
            true,
            /* profile */ false,
            new SharedState()
        );
    }
}
