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
import org.opensearch.arrow.memory.ArrowAllocatorService;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Per-query context — immutable config (DAG, executor, parent task) + lazy per-query
 * resources (Arrow buffer allocator, virtual-thread executor for LOCAL tasks).
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
    private final ArrowAllocatorService allocatorService;
    /**
     * Per-query state shared across phased contexts. Instances produced by
     * {@link #withDag(QueryDAG)} share the same mutable holder so the lazy allocator and
     * lazy {@code localTaskExecutor} (and the {@link #close()} lifecycle) remain
     * single-instance across all phases of the same query (e.g. M1 broadcast pass-1 +
     * pass-2).
     */
    private final SharedState sharedState;

    private static final class SharedState {
        volatile BufferAllocator bufferAllocator;
        volatile ExecutorService localTaskExecutor;
        boolean closed;
    }

    public QueryContext(QueryDAG dag, Executor searchExecutor, AnalyticsQueryTask parentTask, ArrowAllocatorService allocatorService) {
        this(
            dag,
            searchExecutor,
            parentTask,
            DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS,
            DEFAULT_PER_QUERY_MEMORY_LIMIT,
            List.of(),
            allocatorService
        );
    }

    /** Full-parameter constructor. Private; tests use {@link #forTest} factories. */
    private QueryContext(
        QueryDAG dag,
        Executor searchExecutor,
        AnalyticsQueryTask parentTask,
        int maxConcurrentShardRequests,
        long perQueryMemoryLimit,
        List<AnalyticsOperationListener> operationListeners,
        ArrowAllocatorService allocatorService
    ) {
        this(
            dag,
            searchExecutor,
            parentTask,
            maxConcurrentShardRequests,
            perQueryMemoryLimit,
            operationListeners,
            allocatorService,
            new SharedState()
        );
    }

    private QueryContext(
        QueryDAG dag,
        Executor searchExecutor,
        AnalyticsQueryTask parentTask,
        int maxConcurrentShardRequests,
        long perQueryMemoryLimit,
        List<AnalyticsOperationListener> operationListeners,
        ArrowAllocatorService allocatorService,
        SharedState sharedState
    ) {
        this.dag = dag;
        this.searchExecutor = searchExecutor;
        this.parentTask = parentTask;
        this.maxConcurrentShardRequests = maxConcurrentShardRequests;
        this.perQueryMemoryLimit = perQueryMemoryLimit;
        this.operationListeners = operationListeners;
        this.allocatorService = allocatorService;
        this.sharedState = sharedState;
        // sharedState volatile fields (bufferAllocator, localTaskExecutor) are filled lazily on
        // first accessor call; constructor leaves them null intentionally.
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
            allocatorService,
            sharedState
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

    /** Lazy per-query allocator (child of shared root) with {@link #perQueryMemoryLimit}. */
    public BufferAllocator bufferAllocator() {
        BufferAllocator alloc = sharedState.bufferAllocator;
        if (alloc == null) {
            synchronized (sharedState) {
                alloc = sharedState.bufferAllocator;
                if (alloc == null) {
                    if (sharedState.closed) {
                        throw new IllegalStateException("QueryContext closed for query " + dag.queryId());
                    }
                    alloc = allocatorService.newChildAllocator("query-" + dag.queryId(), perQueryMemoryLimit);
                    sharedState.bufferAllocator = alloc;
                }
            }
        }
        return alloc;
    }

    /** Lazy per-query virtual-thread executor for LOCAL tasks. Shared across phased contexts. */
    public ExecutorService localTaskExecutor() {
        ExecutorService exec = sharedState.localTaskExecutor;
        if (exec == null) {
            synchronized (sharedState) {
                exec = sharedState.localTaskExecutor;
                if (exec == null) {
                    if (sharedState.closed) {
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

    /**
     * Closes the per-query buffer allocator and shuts down the lazy local-task executor if
     * either was created. Idempotent and serialized with the lazy-init accessors so close
     * can't race with creation. After close, subsequent accessors throw rather than silently
     * creating new resources.
     *
     * <p>When multiple {@link QueryContext} instances share the same {@link SharedState} (via
     * {@link #withDag(QueryDAG)}), closing any one of them closes the shared resources — the
     * caller is responsible for calling this exactly once per query.
     */
    public void close() {
        synchronized (sharedState) {
            if (sharedState.closed) return;
            sharedState.closed = true;
            if (sharedState.bufferAllocator != null) {
                sharedState.bufferAllocator.close();
                sharedState.bufferAllocator = null;
            }
            if (sharedState.localTaskExecutor != null) {
                sharedState.localTaskExecutor.shutdown();
                sharedState.localTaskExecutor = null;
            }
        }
    }

    // ─── Test factories ────────────────────────────────────────────────

    /** Test-only: wraps a fresh {@link RootAllocator} as an {@link ArrowAllocatorService}. */
    private static ArrowAllocatorService testAllocatorService() {
        return new ArrowAllocatorService() {
            private final RootAllocator root = new RootAllocator(Long.MAX_VALUE);

            @Override
            public BufferAllocator newChildAllocator(String name, long limit) {
                return root.newChildAllocator(name, 0, limit);
            }

            @Override
            public long getAllocatedMemory() {
                return root.getAllocatedMemory();
            }

            @Override
            public long getPeakMemoryAllocation() {
                return root.getPeakMemoryAllocation();
            }
        };
    }

    /** Creates a test context with a synchronous executor. */
    public static QueryContext forTest(QueryDAG dag, AnalyticsQueryTask parentTask) {
        return forTest(dag, parentTask, List.of());
    }

    /** Creates a test context with a synchronous executor and the supplied operation listeners. */
    public static QueryContext forTest(QueryDAG dag, AnalyticsQueryTask parentTask, List<AnalyticsOperationListener> operationListeners) {
        return new QueryContext(
            dag,
            Runnable::run,
            parentTask,
            DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS,
            Long.MAX_VALUE,
            operationListeners,
            testAllocatorService()
        );
    }
}
