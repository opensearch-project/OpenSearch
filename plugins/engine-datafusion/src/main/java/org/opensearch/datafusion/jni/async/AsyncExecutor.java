/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.jni.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executor for async operations bridging Java and Tokio runtime.
 * Manages thread pool for non-blocking JNI calls.
 */
public final class AsyncExecutor implements AutoCloseable {

    private final ExecutorService executor;
    private final long tokioRuntimePtr;

    /**
     * Creates async executor with default thread pool size.
     * @param tokioRuntimePtr pointer to Tokio runtime
     */
    public AsyncExecutor(long tokioRuntimePtr) {
        this(tokioRuntimePtr, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Creates async executor with specified thread pool size.
     * @param tokioRuntimePtr pointer to Tokio runtime
     * @param threadPoolSize number of threads
     */
    public AsyncExecutor(long tokioRuntimePtr, int threadPoolSize) {
        if (tokioRuntimePtr == 0) {
            throw new IllegalArgumentException("Invalid Tokio runtime pointer");
        }
        this.tokioRuntimePtr = tokioRuntimePtr;
        this.executor = Executors.newFixedThreadPool(threadPoolSize, new DataFusionThreadFactory());
    }

    /**
     * Submits async task for execution.
     * @param task the task to execute
     * @return CompletableFuture with result
     */
    public <T> CompletableFuture<T> submit(AsyncTask<T> task) {
        return CompletableFuture.supplyAsync(() -> task.execute(tokioRuntimePtr), executor);
    }

    /**
     * Gets the Tokio runtime pointer.
     * @return runtime pointer
     */
    public long getTokioRuntimePtr() {
        return tokioRuntimePtr;
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    /**
     * Task that executes with Tokio runtime.
     */
    @FunctionalInterface
    public interface AsyncTask<T> {
        T execute(long tokioRuntimePtr);
    }

    private static final class DataFusionThreadFactory implements ThreadFactory {
        private final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, "datafusion-async-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
