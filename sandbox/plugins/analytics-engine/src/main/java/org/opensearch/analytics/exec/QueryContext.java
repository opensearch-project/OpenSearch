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
import org.opensearch.threadpool.ThreadPool;

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

    private final QueryDAG dag;
    private final ThreadPool threadPool;
    private final AnalyticsQueryTask parentTask;
    private final int maxConcurrentShardRequests;
    private final List<AnalyticsOperationListener> operationListeners;
    private final BufferAllocator allocator;
    private final boolean ownsAllocator;
    private volatile ExecutorService localTaskExecutor;
    private boolean closed;  // guarded by `this`

    public QueryContext(
        QueryDAG dag,
        ThreadPool threadPool,
        AnalyticsQueryTask parentTask,
        BufferAllocator allocator,
        boolean ownsAllocator
    ) {
        this(dag, threadPool, parentTask, DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS, List.of(), allocator, ownsAllocator);
    }

    /** Full-parameter constructor. Private; tests use {@link #forTest} factories. */
    private QueryContext(
        QueryDAG dag,
        ThreadPool threadPool,
        AnalyticsQueryTask parentTask,
        int maxConcurrentShardRequests,
        List<AnalyticsOperationListener> operationListeners,
        BufferAllocator allocator,
        boolean ownsAllocator
    ) {
        this.dag = dag;
        this.threadPool = threadPool;
        this.parentTask = parentTask;
        this.maxConcurrentShardRequests = maxConcurrentShardRequests;
        this.operationListeners = operationListeners;
        this.allocator = allocator;
        this.ownsAllocator = ownsAllocator;
    }

    public QueryDAG dag() {
        return dag;
    }

    public Executor searchExecutor() {
        return threadPool != null ? threadPool.executor(ThreadPool.Names.SEARCH) : Runnable::run;
    }

    public Executor schedulerExecutor() {
        return threadPool != null ? threadPool.executor(AnalyticsPlugin.SCHEDULER_THREAD_POOL_NAME) : Runnable::run;
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

    public BufferAllocator bufferAllocator() {
        return allocator;
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

    boolean ownsAllocator() {
        return ownsAllocator;
    }

    /** Idempotent. Serialised with lazy-init accessors; post-close accessors throw. */
    public void close() {
        synchronized (this) {
            if (closed) return;
            closed = true;
            if (ownsAllocator) {
                allocator.close();
            }
            if (localTaskExecutor != null) {
                localTaskExecutor.shutdown();
                localTaskExecutor = null;
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
        return new QueryContext(dag, null, parentTask, DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS, operationListeners, testAllocator, true);
    }
}
