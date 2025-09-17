/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.interceptor;

import io.grpc.ServerInterceptor;

/**
 * Provides a list of ordered gRPC interceptors to be applied during
 * server initialization. Interceptors with lower order values are
 * applied earlier.
 */
public interface OrderedGrpcInterceptor {
    /**
     * Defines the order in which the interceptor should be applied.
     * Lower values indicate higher priority.
     * Must be implemented by all interceptors. No two interceptors should have same order
     */
    int getOrder();

    /**
     * Returns the actual gRPC ServerInterceptor instance.
     */
    ServerInterceptor getInterceptor();
}
