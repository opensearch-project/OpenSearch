/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc;

import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.GRPC_TRANSPORT_SETTING_KEY;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_BIND_HOST;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_HOST;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PORT;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PUBLISH_HOST;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PUBLISH_PORT;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_WORKER_COUNT;

/**
 * Main class for the gRPC plugin.
 */
public final class GrpcPlugin extends Plugin implements NetworkPlugin {

    /**
     * Creates a new GrpcPlugin instance.
     */
    public GrpcPlugin() {}

    @Override
    public Map<String, Supplier<AuxTransport>> getAuxTransports(
        Settings settings,
        ThreadPool threadPool,
        CircuitBreakerService circuitBreakerService,
        NetworkService networkService,
        ClusterSettings clusterSettings,
        Tracer tracer
    ) {
        return Collections.singletonMap(
            GRPC_TRANSPORT_SETTING_KEY,
            () -> new Netty4GrpcServerTransport(settings, Collections.emptyList(), networkService)
        );
    }

    @Override
    public Map<String, Supplier<AuxTransport>> getSecureAuxTransports(
        Settings settings,
        ThreadPool threadPool,
        CircuitBreakerService circuitBreakerService,
        NetworkService networkService,
        ClusterSettings clusterSettings,
        SecureAuxTransportSettingsProvider secureAuxTransportSettingsProvider,
        Tracer tracer
    ) {
        return Collections.singletonMap(
            GRPC_TRANSPORT_SETTING_KEY,
            () -> new SecureNetty4GrpcServerTransport(settings, Collections.emptyList(), networkService, secureAuxTransportSettingsProvider)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            SETTING_GRPC_PORT,
            SETTING_GRPC_HOST,
            SETTING_GRPC_PUBLISH_HOST,
            SETTING_GRPC_BIND_HOST,
            SETTING_GRPC_WORKER_COUNT,
            SETTING_GRPC_PUBLISH_PORT
        );
    }
}
