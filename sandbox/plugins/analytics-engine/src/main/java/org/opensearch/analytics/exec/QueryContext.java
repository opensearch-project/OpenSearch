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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    private static final Logger logger = LogManager.getLogger(QueryContext.class);

    // TODO: make configurable via cluster setting (like search.max_concurrent_shard_requests)
    private static final int DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS = 5;

    private static final long DEFAULT_PER_QUERY_MEMORY_LIMIT = 256L * 1024 * 1024;

    private final QueryDAG dag;
    private final Executor searchExecutor;
    private final AnalyticsQueryTask parentTask;
    private final int maxConcurrentShardRequests;
    private final long perQueryMemoryLimit;
    private final List<AnalyticsOperationListener> operationListeners;
    private final ArrowAllocatorService allocatorService;
    private volatile BufferAllocator bufferAllocator;
    private volatile ExecutorService localTaskExecutor;
    private boolean closed;  // guarded by `this`

    public QueryContext(
        QueryDAG dag,
        Executor searchExecutor,
        AnalyticsQueryTask parentTask,
        long perQueryMemoryLimit,
        ArrowAllocatorService allocatorService
    ) {
        this(
            dag,
            searchExecutor,
            parentTask,
            DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS,
            perQueryMemoryLimit,
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
        this.dag = dag;
        this.searchExecutor = searchExecutor;
        this.parentTask = parentTask;
        this.maxConcurrentShardRequests = maxConcurrentShardRequests;
        this.perQueryMemoryLimit = perQueryMemoryLimit;
        this.operationListeners = operationListeners;
        this.allocatorService = allocatorService;
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
        BufferAllocator alloc = bufferAllocator;
        if (alloc == null) {
            synchronized (this) {
                alloc = bufferAllocator;
                if (alloc == null) {
                    if (closed) {
                        throw new IllegalStateException("QueryContext closed for query " + dag.queryId());
                    }
                    alloc = allocatorService.newChildAllocator("query-" + dag.queryId(), perQueryMemoryLimit);
                    bufferAllocator = alloc;
                    logger.debug("[query-{}] Arrow allocator created, limit={}B", dag.queryId(), perQueryMemoryLimit);
                }
            }
        }
        return alloc;
    }

    /** Lazy per-query virtual-thread executor for LOCAL tasks. */
    public ExecutorService localTaskExecutor() {
        ExecutorService exec = localTaskExecutor;
        if (exec == null) {
            synchronized (this) {
                exec = localTaskExecutor;
                if (exec == null) {
                    if (closed) {
                        throw new IllegalStateException("QueryContext closed for query " + dag.queryId());
                    }
                    exec = Executors.newThreadPerTaskExecutor(
                        Thread.ofVirtual().name("analytics-local-task-" + dag.queryId() + "-", 0).factory()
                    );
                    localTaskExecutor = exec;
                }
            }
        }
        return exec;
    }

    BufferAllocator getAllocatorIfCreated() {
        return bufferAllocator;
    }

    /** Idempotent. Serialised with lazy-init accessors; post-close accessors throw. */
    public void close() {
        synchronized (this) {
            if (closed) return;
            closed = true;
            if (bufferAllocator != null) {
                bufferAllocator.close();
                bufferAllocator = null;
            }
            if (localTaskExecutor != null) {
                localTaskExecutor.shutdown();
                localTaskExecutor = null;
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
