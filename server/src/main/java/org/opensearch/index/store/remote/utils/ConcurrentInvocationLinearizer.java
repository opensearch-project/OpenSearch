/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * A utility class which can be used to serialize concurrent invocations and to achieve "invoke simultaneously once at any time" semantic
 *
 * @param <METHOD_PARAM_TYPE> the method parameter type where this method invocation will be linearized
 * @param <RET_TYPE>          return type of the method
 * @opensearch.internal
 */
public class ConcurrentInvocationLinearizer<METHOD_PARAM_TYPE, RET_TYPE> {
    private final ConcurrentMap<METHOD_PARAM_TYPE, CompletableFuture<RET_TYPE>> invokeOnceCache;
    private final ExecutorService executorService;

    /**
     * Constructs the object
     *
     * @param executorService which will be used to execute the concurrent invocations
     */
    public ConcurrentInvocationLinearizer(ExecutorService executorService) {
        this.invokeOnceCache = new ConcurrentHashMap<>();
        this.executorService = executorService;
    }

    /**
     * @param input    the argument to the method
     * @param function delegate to actual function/method
     * @return return value of the function
     */
    public CompletableFuture<RET_TYPE> linearize(METHOD_PARAM_TYPE input, Function<METHOD_PARAM_TYPE, RET_TYPE> function) {
        return invokeOnceCache.computeIfAbsent(input, in -> CompletableFuture.supplyAsync(() -> function.apply(in), executorService))
            .whenComplete((ret, throwable) -> {
                // whenComplete will always be executed (when run normally or when exception happen)
                invokeOnceCache.remove(input);
            });
    }

    Map<METHOD_PARAM_TYPE, CompletableFuture<RET_TYPE>> getInvokeOnceCache() {
        return Collections.unmodifiableMap(invokeOnceCache);
    }
}
