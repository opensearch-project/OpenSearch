/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.opensearch.common.CheckedFunction;

/**
 * A utility class which can be used to serialize concurrent invocations and to achieve "invoke simultaneously once at any time"
 * semantic. This class does not implement any concurrency itself. When there is no concurrent access the work will be performed
 * on the calling thread, though the result of that work will be shared with any concurrent requests for the same key.
 *
 * @param <METHOD_PARAM_TYPE> the method parameter type where this method invocation will be linearized
 * @param <RET_TYPE>          return type of the method
 * @opensearch.internal
 */
class ConcurrentInvocationLinearizer<METHOD_PARAM_TYPE, RET_TYPE> {
    private final ConcurrentMap<METHOD_PARAM_TYPE, CompletableFuture<RET_TYPE>> invokeOnceCache = new ConcurrentHashMap<>();

    /**
     * Invokes the given function. If another thread is concurrently invoking the same function, as
     * identified by the given input, then this call will block and return the result of that
     * computation. Otherwise it will synchronously invoke the given function and return the result.
     * @param input The input to uniquely identify this function
     * @param function The function to invoke
     * @return The result of the function
     * @throws InterruptedException thrown if interrupted while blocking
     * @throws IOException thrown from given function
     */
    RET_TYPE linearize(METHOD_PARAM_TYPE input, CheckedFunction<METHOD_PARAM_TYPE, RET_TYPE, IOException> function)
        throws InterruptedException, IOException {
        try {
            return linearizeInternal(input, function).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() instanceof Error) {
                throw (Error) e.getCause();
            }
            throw new RuntimeException("Unknown exception cause", e.getCause());
        }
    }

    // Visible for testing
    CompletableFuture<RET_TYPE> linearizeInternal(
        METHOD_PARAM_TYPE input,
        CheckedFunction<METHOD_PARAM_TYPE, RET_TYPE, IOException> function
    ) {
        final CompletableFuture<RET_TYPE> newFuture = new CompletableFuture<>();
        final CompletableFuture<RET_TYPE> existing = invokeOnceCache.putIfAbsent(input, newFuture);
        if (existing == null) {
            // No concurrent work is happening for this key, so need to do the
            // work and complete the future with the result.
            try {
                newFuture.complete(function.apply(input));
            } catch (Throwable e) {
                newFuture.completeExceptionally(e);
            } finally {
                invokeOnceCache.remove(input);
            }
            return newFuture;
        } else {
            // Another thread is doing the work, so return the future to its result
            return existing;
        }
    }

    // Visible for testing
    Map<METHOD_PARAM_TYPE, CompletableFuture<RET_TYPE>> getInvokeOnceCache() {
        return Collections.unmodifiableMap(invokeOnceCache);
    }
}
