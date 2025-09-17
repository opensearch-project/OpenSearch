/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.interceptor;

import java.util.List;

/**
 * Interface for providing gRPC interceptors with defined execution order.
 * Implementations of this interface are expected to return a list of
 * {@link OrderedGrpcInterceptor} instances, which the gRPC server can register.
 *
 */
public interface GrpcInterceptorProvider {
    /**
     * Returns a list of gRPC interceptors along with their defined execution order.
     * Interceptors with lower order values will be applied first.
     * @return List of ordered gRPC interceptors
     */
    List<OrderedGrpcInterceptor> getOrderedGrpcInterceptors();
}
