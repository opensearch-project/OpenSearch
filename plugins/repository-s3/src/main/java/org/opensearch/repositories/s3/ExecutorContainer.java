/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Composed of threadExecutor and shutdown signal provider of an executor
 */
public class ExecutorContainer {
    private final BiFunction<Callable<?>, String, Future<?>> threadExecutor;
    private final Function<String, Boolean> executorShutdownSignal;

    /**
     * @param threadExecutor Function reference of an executor to delegate thread creation
     * @param executorShutdownSignal Signalling function for shutdown of an executor.
     */
    public ExecutorContainer(BiFunction<Callable<?>, String, Future<?>> threadExecutor, Function<String, Boolean> executorShutdownSignal) {
        this.threadExecutor = threadExecutor;
        this.executorShutdownSignal = executorShutdownSignal;
    }

    /**
     *
     * @return function reference of executor
     */
    public BiFunction<Callable<?>, String, Future<?>> getThreadExecutor() {
        return threadExecutor;
    }

    /**
     *
     * @return function reference of signal provider of executor
     */
    public Function<String, Boolean> getExecutorShutdownSignal() {
        return executorShutdownSignal;
    }
}
