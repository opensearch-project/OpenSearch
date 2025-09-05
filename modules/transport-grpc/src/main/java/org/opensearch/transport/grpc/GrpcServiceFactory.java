/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import io.grpc.BindableService;
import org.opensearch.transport.client.Client;

/**
 * Extension point for plugins to add a BindableService to the grpc-transport.
 */
public interface GrpcServiceFactory {
    void initClient(Client client);
    BindableService create();
}
