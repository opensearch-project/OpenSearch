/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.transport.client.Client;

import io.grpc.BindableService;

/**
 * Extension point for plugins to add a BindableService to the grpc-transport.
 */
public interface GrpcServiceFactory {
    GrpcServiceFactory initClient(Client client);

    BindableService build();
}
