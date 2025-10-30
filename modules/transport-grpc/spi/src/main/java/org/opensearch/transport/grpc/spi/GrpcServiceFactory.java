/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.spi;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.util.List;

import io.grpc.BindableService;

/**
 * Extension point for plugins to add a BindableService list to the grpc-transport.
 * Provides init methods to allow service definitions access to OpenSearch clients, settings, ect.
 */
public interface GrpcServiceFactory {

    /**
     * For logging.
     * @return owning plugin identifier for service validation.
     */
    String plugin();

    /**
     * Provide client for executing requests on the cluster.
     * @param client for use in services.
     * @return chaining.
     */
    default GrpcServiceFactory initClient(Client client) {
        return this;
    }

    /**
     * Provide visibility into node settings.
     * @param settings for use in services.
     * @return chaining.
     */
    default GrpcServiceFactory initSettings(Settings settings) {
        return this;
    }

    /**
     * Provide visibility into cluster settings.
     * @param clusterSettings for use in services.
     * @return chaining.
     */
    default GrpcServiceFactory initClusterSettings(ClusterSettings clusterSettings) {
        return this;
    }

    /**
     * Provide access to thread pool.
     * @param threadPool for use in services.
     * @return chaining.
     */
    default GrpcServiceFactory initThreadPool(ThreadPool threadPool) {
        return this;
    }

    /**
     * Build gRPC services.
     * @return BindableService.
     */
    List<BindableService> build();
}
