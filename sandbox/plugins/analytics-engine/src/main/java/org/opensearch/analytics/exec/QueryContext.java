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
import org.opensearch.analytics.backend.AnalyticsOperationListener;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.QueryDAG;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Per-query context. Created once in {@link DefaultPlanExecutor#execute}
 * and threaded through execution components. Holds immutable config (DAG,
 * executor, parent task) and the lazy per-query {@link BufferAllocator}.
 *
 * <p>The execution registry has moved to {@link PlanWalker}, which owns
 * the per-query execution map internally. This context is now purely
 * configuration plus one lazy allocator.
 *
 * @opensearch.internal
 */
public class QueryContext {

    // TODO: make configurable via cluster setting (like search.max_concurrent_shard_requests)
    private static final int DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS = 5;

    /** Default per-query memory limit for Arrow allocations (256 MB). */
    private static final long DEFAULT_PER_QUERY_MEMORY_LIMIT = 256L * 1024 * 1024;

    /**
     * Shared root allocator across all queries. Per-query child allocators
     * are created from this root with individual limits.
     */
    private static final BufferAllocator SHARED_ROOT = new RootAllocator(Long.MAX_VALUE);

    private final QueryDAG dag;
    private final Executor searchExecutor;
    private final AnalyticsQueryTask parentTask;
    private final int maxConcurrentShardRequests;
    private final long perQueryMemoryLimit;
    private final List<AnalyticsOperationListener> operationListeners;
    private volatile BufferAllocator bufferAllocator;
    private boolean closed;  // guarded by `this`

    public QueryContext(QueryDAG dag, Executor searchExecutor, AnalyticsQueryTask parentTask) {
        this(dag, searchExecutor, parentTask, DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS, DEFAULT_PER_QUERY_MEMORY_LIMIT, List.of());
    }

    public QueryContext(QueryDAG dag, Executor searchExecutor, AnalyticsQueryTask parentTask, int maxConcurrentShardRequests) {
        this(dag, searchExecutor, parentTask, maxConcurrentShardRequests, DEFAULT_PER_QUERY_MEMORY_LIMIT, List.of());
    }

    public QueryContext(
        QueryDAG dag,
        Executor searchExecutor,
        AnalyticsQueryTask parentTask,
        int maxConcurrentShardRequests,
        long perQueryMemoryLimit
    ) {
        this(dag, searchExecutor, parentTask, maxConcurrentShardRequests, perQueryMemoryLimit, List.of());
    }

    public QueryContext(
        QueryDAG dag,
        Executor searchExecutor,
        AnalyticsQueryTask parentTask,
        int maxConcurrentShardRequests,
        long perQueryMemoryLimit,
        List<AnalyticsOperationListener> operationListeners
    ) {
        this.dag = dag;
        this.searchExecutor = searchExecutor;
        this.parentTask = parentTask;
        this.maxConcurrentShardRequests = maxConcurrentShardRequests;
        this.perQueryMemoryLimit = perQueryMemoryLimit;
        this.operationListeners = operationListeners;
    }

    public QueryDAG dag() {
        return dag;
    }

    public Executor searchExecutor() {
        return searchExecutor;
    }

    public AnalyticsQueryTask parentTask() {
        return parentTask;
    }

    public String queryId() {
        return dag.queryId();
    }

    public int maxConcurrentShardRequests() {
        return maxConcurrentShardRequests;
    }

    /** Returns the operation listeners for this query. */
    public List<AnalyticsOperationListener> operationListeners() {
        return operationListeners;
    }

    /**
     * Returns the per-query Arrow buffer allocator, creating it lazily on first access.
     * The allocator is a child of the shared root with a per-query memory limit.
     * When the limit is exceeded, Arrow throws {@code OutOfMemoryException} which
     * the stage catches and transitions to FAILED.
     */
    public BufferAllocator bufferAllocator() {
        BufferAllocator alloc = bufferAllocator;
        if (alloc == null) {
            synchronized (this) {
                alloc = bufferAllocator;
                if (alloc == null) {
                    if (closed) {
                        throw new IllegalStateException("QueryContext closed for query " + dag.queryId());
                    }
                    alloc = SHARED_ROOT.newChildAllocator("query-" + dag.queryId(), 0, perQueryMemoryLimit);
                    bufferAllocator = alloc;
                }
            }
        }
        return alloc;
    }

    /**
     * Closes the per-query buffer allocator if it was created. Idempotent and
     * serialized with {@link #bufferAllocator()} so close can't race with lazy
     * creation. After close, subsequent {@link #bufferAllocator()} calls throw
     * rather than silently creating a second allocator.
     */
    public void closeBufferAllocator() {
        synchronized (this) {
            if (closed) return;
            closed = true;
            if (bufferAllocator != null) {
                bufferAllocator.close();
                bufferAllocator = null;
            }
        }
    }

    // ─── Test factories ────────────────────────────────────────────────

    /** Creates a test context with a synchronous executor. */
    public static QueryContext forTest(QueryDAG dag, AnalyticsQueryTask parentTask) {
        return new QueryContext(dag, Runnable::run, parentTask, DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS, Long.MAX_VALUE);
    }

    /** Creates a test context with a stub DAG. */
    public static QueryContext forTest(String queryId, AnalyticsQueryTask parentTask) {
        return new QueryContext(
            new QueryDAG(queryId, null),
            Runnable::run,
            parentTask,
            DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS,
            Long.MAX_VALUE
        );
    }
}
