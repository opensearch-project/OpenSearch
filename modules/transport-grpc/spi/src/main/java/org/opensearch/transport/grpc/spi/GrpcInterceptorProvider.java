/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.spi;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;

import java.util.List;

import io.grpc.ServerInterceptor;

/**
 * SPI interface for providing gRPC interceptors.
 * Plugins can implement this interface to provide custom gRPC interceptors
 * that will be executed in the order specified by their OrderedGrpcInterceptor.
 */
public interface GrpcInterceptorProvider {

    /**
     * Provide visibility into node settings.
     * @param settings for use in interceptors.
     */
    default void initNodeSettings(Settings settings) {}

    /**
     * Returns a list of ordered gRPC interceptors with access to ThreadContext.
     * Each interceptor must have a unique order value.
     *
     * This follows the pattern established by REST handler wrappers where
     * the thread context is provided to allow interceptors to:
     * - Extract headers from gRPC metadata and store in ThreadContext
     * - Preserve context across async boundaries
     * @param threadContext The thread context for managing request context
     * @return List of ordered gRPC interceptors
     */
    List<OrderedGrpcInterceptor> getOrderedGrpcInterceptors(ThreadContext threadContext);

    /**
     * Provides a gRPC interceptor with an order value for execution priority.
     * Interceptors with lower order values are applied earlier.
     */
    interface OrderedGrpcInterceptor {
        /**
         * Defines the order in which the interceptor should be applied.
         * Lower values indicate higher priority.
         * Must be unique across all interceptors - no two interceptors should have the same order.
         *
         * @return the order value
         */
        int order();

        /**
         * Returns the actual gRPC ServerInterceptor instance.
         * The interceptor can use the ThreadContext provided to the parent
         * GrpcInterceptorProvider to manage request context.
         *
         * @return the server interceptor
         */
        ServerInterceptor getInterceptor();
    }
}
