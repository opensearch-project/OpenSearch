/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.TaskExecutor;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;

/**
 * {@link HybridQueryExecutor} provides necessary implementation and instances to execute
 * sub-queries from hybrid query in parallel as a Task by caller. This ensures that one thread pool
 * is used for hybrid query execution per node. The number of parallelization is also constrained
 * by twice allocated processor count since most of the operation from hybrid search is expected to be
 * short-lived thread. This will help us to achieve optimal parallelization and reasonable throughput.
 */
public final class HybridQueryExecutor {
    private static final String HYBRID_QUERY_EXEC_THREAD_POOL_NAME = "_plugin_neural_search_hybrid_query_executor";
    private static final Integer HYBRID_QUERY_EXEC_THREAD_POOL_QUEUE_SIZE = 1000;
    private static final Integer MAX_THREAD_SIZE = 1000;
    private static final Integer MIN_THREAD_SIZE = 2;
    private static final Integer PROCESSOR_COUNT_MULTIPLIER = 2;
    private static TaskExecutor taskExecutor;

    private HybridQueryExecutor() {}

    /**
     * Provide fixed executor builder to use for hybrid query executors
     * @param settings Node level settings
     * @return the executor builder for hybrid query's custom thread pool.
     */
    public static ExecutorBuilder getExecutorBuilder(final Settings settings) {

        int numberOfThreads = getFixedNumberOfThreadSize(settings);
        return new FixedExecutorBuilder(
            settings,
            HYBRID_QUERY_EXEC_THREAD_POOL_NAME,
            numberOfThreads,
            HYBRID_QUERY_EXEC_THREAD_POOL_QUEUE_SIZE,
            HYBRID_QUERY_EXEC_THREAD_POOL_NAME
        );
    }

    // /**
    // * Initialize @{@link TaskExecutor} to run tasks concurrently using {@link ThreadPool}
    // * @param threadPool OpenSearch's thread pool instance
    // */
    // public static void initialize(ThreadPool threadPool) {
    // if (threadPool == null) {
    // throw new IllegalArgumentException(
    // "Argument thread-pool to Hybrid Query Executor cannot be null. This is required to build executor to run actions in parallel"
    // );
    // }
    // taskExecutor = new TaskExecutor(threadPool.executor(HYBRID_QUERY_EXEC_THREAD_POOL_NAME));
    // }

    /**
     * Return TaskExecutor Wrapper that helps runs tasks concurrently
     * @return TaskExecutor instance to help run search tasks in parallel
     */
    public static TaskExecutor getExecutor() {
        return taskExecutor != null ? taskExecutor : new TaskExecutor(Runnable::run);
    }

    // static String getThreadPoolName() {
    // return HYBRID_QUERY_EXEC_THREAD_POOL_NAME;
    // }

    /**
     * Will use thread size as twice the default allocated processor. We selected twice allocated processor
     * since hybrid query action is expected to be short-lived . This will balance throughput and latency
     * To avoid out of range, we will return 2 as minimum processor count and 1000 as maximum thread size
     */
    private static int getFixedNumberOfThreadSize(final Settings settings) {
        final int allocatedProcessors = OpenSearchExecutors.allocatedProcessors(settings);
        int threadSize = Math.max(PROCESSOR_COUNT_MULTIPLIER * allocatedProcessors, MIN_THREAD_SIZE);
        return Math.min(threadSize, MAX_THREAD_SIZE);
    }
}
