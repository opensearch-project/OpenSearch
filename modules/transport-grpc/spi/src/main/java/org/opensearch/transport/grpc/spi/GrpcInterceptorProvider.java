/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.spi;

import java.util.List;

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
}
