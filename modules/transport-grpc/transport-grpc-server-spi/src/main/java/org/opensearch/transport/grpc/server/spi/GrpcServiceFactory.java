/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.server.spi;

import org.opensearch.transport.client.Client;

import io.grpc.BindableService;

/**
 * Extension point for plugins to add a BindableService to the grpc-transport.
 */
public interface GrpcServiceFactory {
    /**
     * Called previous to build.
     * @param client OpenSearch client for use in gRPC service definition.
     * @return GrpcServiceFactory for chaining.
     */
    GrpcServiceFactory initClient(Client client);

    /**
     * Build gRPC service.
     * @return BindableService.
     */
    BindableService build();
}
