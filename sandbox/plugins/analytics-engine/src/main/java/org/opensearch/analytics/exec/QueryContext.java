/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.analytics.backend.AnalyticsOperationListener;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.arrow.flight.transport.ArrowAllocatorProvider;

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

    private final QueryDAG dag;
    private final Executor searchExecutor;
    private final AnalyticsQueryTask parentTask;
    private final int maxConcurrentShardRequests;
    private final long perQueryMemoryLimit;
    private final List<AnalyticsOperationListener> operationListeners;
    /**
     * Allocator state shared across phased contexts. Instances produced by
     * {@link #withDag(QueryDAG)} share the same mutable holder so the lazy allocator
     * (and its {@link #closeBufferAllocator()} lifecycle) remain single-instance
     * across all phases of the same query.
     */
    private final AllocatorHolder allocatorHolder;

    private static final class AllocatorHolder {
        volatile BufferAllocator bufferAllocator;
        boolean closed;
    }

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
        this(dag, searchExecutor, parentTask, maxConcurrentShardRequests, perQueryMemoryLimit, operationListeners, new AllocatorHolder());
    }

    private QueryContext(
        QueryDAG dag,
        Executor searchExecutor,
        AnalyticsQueryTask parentTask,
        int maxConcurrentShardRequests,
        long perQueryMemoryLimit,
        List<AnalyticsOperationListener> operationListeners,
        AllocatorHolder allocatorHolder
    ) {
        this.dag = dag;
        this.searchExecutor = searchExecutor;
        this.parentTask = parentTask;
        this.maxConcurrentShardRequests = maxConcurrentShardRequests;
        this.perQueryMemoryLimit = perQueryMemoryLimit;
        this.operationListeners = operationListeners;
        this.allocatorHolder = allocatorHolder;
    }

    /**
     * Returns a derived context pointing at a different {@link QueryDAG} but sharing this
     * context's buffer allocator, parent task, executor, and listener list. Used by multi-phase
     * join dispatch (e.g. M1 broadcast) where pass 1 drives only the build stage and pass 2
     * drives the probe + root; both phases belong to the same query and must share a single
     * per-query allocator.
     */
    public QueryContext withDag(QueryDAG newDag) {
        return new QueryContext(
            newDag,
            searchExecutor,
            parentTask,
            maxConcurrentShardRequests,
            perQueryMemoryLimit,
            operationListeners,
            allocatorHolder
        );
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
        BufferAllocator alloc = allocatorHolder.bufferAllocator;
        if (alloc == null) {
            synchronized (allocatorHolder) {
                alloc = allocatorHolder.bufferAllocator;
                if (alloc == null) {
                    if (allocatorHolder.closed) {
                        throw new IllegalStateException("QueryContext closed for query " + dag.queryId());
                    }
                    alloc = ArrowAllocatorProvider.newChildAllocator("query-" + dag.queryId(), perQueryMemoryLimit);
                    allocatorHolder.bufferAllocator = alloc;
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
     *
     * <p>When multiple {@link QueryContext} instances share the same allocator holder (via
     * {@link #withDag(QueryDAG)}), closing any one of them closes the shared allocator — the
     * caller is responsible for calling this exactly once per query.
     */
    public void closeBufferAllocator() {
        synchronized (allocatorHolder) {
            if (allocatorHolder.closed) return;
            allocatorHolder.closed = true;
            if (allocatorHolder.bufferAllocator != null) {
                allocatorHolder.bufferAllocator.close();
                allocatorHolder.bufferAllocator = null;
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
