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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Per-query context — immutable config (DAG, executor, parent task) + lazy per-query
 * resources (Arrow buffer allocator, virtual-thread executor for LOCAL tasks).
 *
 * <p>Phased dispatchers (e.g. M1 {@code BroadcastDispatch}) need a derived context pointing at
 * a different DAG that still shares this context's allocator + lazy executor. Use
 * {@link #withDag(QueryDAG)} for that. The derived context is non-owning: only the original
 * context's {@link #close()} releases the allocator + shuts down the executor.
 *
 * @opensearch.internal
 */
public class QueryContext {

    // TODO: make configurable via cluster setting (like search.max_concurrent_shard_requests)
    private static final int DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS = 5;

    private final QueryDAG dag;
    private final Executor searchExecutor;
    private final AnalyticsQueryTask parentTask;
    private final int maxConcurrentShardRequests;
    private final List<AnalyticsOperationListener> operationListeners;
    private final BufferAllocator allocator;
    private final boolean ownsAllocator;
    /**
     * Holder for the lazy local-task executor + closed flag, shared across phased contexts so
     * pass 1 and pass 2 of multi-phase dispatch (e.g. M1 broadcast) reuse a single executor and
     * a single close-once semantics. Non-shared queries get a holder of their own.
     */
    private final SharedState sharedState;

    private static final class SharedState {
        volatile ExecutorService localTaskExecutor;
        boolean closed;  // guarded by synchronized(this)
    }

    public QueryContext(
        QueryDAG dag,
        Executor searchExecutor,
        AnalyticsQueryTask parentTask,
        BufferAllocator allocator,
        boolean ownsAllocator
    ) {
        this(
            dag,
            searchExecutor,
            parentTask,
            DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS,
            List.of(),
            allocator,
            ownsAllocator,
            new SharedState()
        );
    }

    /** Full-parameter constructor. Private; tests use {@link #forTest} factories. */
    private QueryContext(
        QueryDAG dag,
        Executor searchExecutor,
        AnalyticsQueryTask parentTask,
        int maxConcurrentShardRequests,
        List<AnalyticsOperationListener> operationListeners,
        BufferAllocator allocator,
        boolean ownsAllocator,
        SharedState sharedState
    ) {
        this.dag = dag;
        this.searchExecutor = searchExecutor;
        this.parentTask = parentTask;
        this.maxConcurrentShardRequests = maxConcurrentShardRequests;
        this.operationListeners = operationListeners;
        this.allocator = allocator;
        this.ownsAllocator = ownsAllocator;
        this.sharedState = sharedState;
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
            searchExecutor,
            parentTask,
            maxConcurrentShardRequests,
            operationListeners,
            allocator,
            /* ownsAllocator */ false,
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

    boolean ownsAllocator() {
        return ownsAllocator;
    }

    /**
     * Idempotent. Serialised with lazy-init accessors; post-close executor accessors throw.
     *
     * <p>For phased contexts produced by {@link #withDag(QueryDAG)}: only the original context
     * closes the allocator and shuts down the shared executor. Closing a derived context is a
     * no-op for the allocator (because {@code ownsAllocator=false}); the executor shutdown is
     * idempotent across all sharers via {@code sharedState.closed}.
     */
    public void close() {
        synchronized (sharedState) {
            if (sharedState.closed) return;
            sharedState.closed = true;
            if (sharedState.localTaskExecutor != null) {
                sharedState.localTaskExecutor.shutdown();
                sharedState.localTaskExecutor = null;
            }
        }
        // Allocator close is outside the sharedState lock — close() should not race with
        // bufferAllocator() (the field is final), and arrow's allocator close() can take a lock
        // that we don't want to interleave with the executor shutdown.
        if (ownsAllocator) {
            allocator.close();
        }
    }

    // ─── Test factories ────────────────────────────────────────────────

    private static final RootAllocator TEST_ROOT = new RootAllocator(Long.MAX_VALUE);

    /** Creates a test context with a synchronous executor. */
    public static QueryContext forTest(QueryDAG dag, AnalyticsQueryTask parentTask) {
        return forTest(dag, parentTask, List.of());
    }

    /** Creates a test context with a synchronous executor and the supplied operation listeners. */
    public static QueryContext forTest(QueryDAG dag, AnalyticsQueryTask parentTask, List<AnalyticsOperationListener> operationListeners) {
        BufferAllocator testAllocator = TEST_ROOT.newChildAllocator("test-" + dag.queryId(), 0, Long.MAX_VALUE);
        return new QueryContext(
            dag,
            Runnable::run,
            parentTask,
            DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS,
            operationListeners,
            testAllocator,
            true,
            new SharedState()
        );
    }
}
