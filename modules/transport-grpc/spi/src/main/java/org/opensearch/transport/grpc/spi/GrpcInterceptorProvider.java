/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.spi;

import java.util.List;

import io.grpc.ServerInterceptor;

/**
 * SPI interface for providing gRPC interceptors.
 * Plugins can implement this interface to provide custom gRPC interceptors
 * that will be executed in the order specified by their OrderedGrpcInterceptor.
 */
public interface GrpcInterceptorProvider {

    /**
     * Returns a list of ordered gRPC interceptors.
     * Each interceptor must have a unique order value.
     *
     * @return List of ordered gRPC interceptors
     */
    List<OrderedGrpcInterceptor> getOrderedGrpcInterceptors();

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
         *
         * @return the server interceptor
         */
        ServerInterceptor getInterceptor();
    }
}
