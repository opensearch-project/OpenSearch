/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugin.transport.grpc.services.DocumentServiceImpl;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import io.grpc.BindableService;

import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.GRPC_TRANSPORT_SETTING_KEY;
import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_BIND_HOST;
import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_HOST;
import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PORT;
import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PUBLISH_HOST;
import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PUBLISH_PORT;
import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_WORKER_COUNT;
import static org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport.GRPC_SECURE_TRANSPORT_SETTING_KEY;
import static org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport.SETTING_GRPC_SECURE_PORT;

/**
 * Main class for the gRPC plugin.
 */
public final class GrpcPlugin extends Plugin implements NetworkPlugin {

    private Client client;

    /**
     * Creates a new GrpcPlugin instance.
     */
    public GrpcPlugin() {}

    /**
     * Provides auxiliary transports for the plugin.
     * Creates and returns a map of transport names to transport suppliers.
     *
     * @param settings The node settings
     * @param threadPool The thread pool
     * @param circuitBreakerService The circuit breaker service
     * @param networkService The network service
     * @param clusterSettings The cluster settings
     * @param tracer The tracer
     * @return A map of transport names to transport suppliers
     */
    @Override
    public Map<String, Supplier<AuxTransport>> getAuxTransports(
        Settings settings,
        ThreadPool threadPool,
        CircuitBreakerService circuitBreakerService,
        NetworkService networkService,
        ClusterSettings clusterSettings,
        Tracer tracer
    ) {
        if (client == null) {
            throw new RuntimeException("client cannot be null");
        }
        List<BindableService> grpcServices = registerGRPCServices(new DocumentServiceImpl(client));
        return Collections.singletonMap(
            GRPC_TRANSPORT_SETTING_KEY,
            () -> new Netty4GrpcServerTransport(settings, grpcServices, networkService)
        );
    }

    /**
     * Provides secure auxiliary transports for the plugin.
     * Registered under a distinct key from gRPC transport.
     * Consumes pluggable security settings as provided by a SecureAuxTransportSettingsProvider.
     *
     * @param settings The node settings
     * @param threadPool The thread pool
     * @param circuitBreakerService The circuit breaker service
     * @param networkService The network service
     * @param clusterSettings The cluster settings
     * @param tracer The tracer
     * @param secureAuxTransportSettingsProvider provides ssl context params
     * @return A map of transport names to transport suppliers
     */
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
        if (client == null) {
            throw new RuntimeException("client cannot be null");
        }
        List<BindableService> grpcServices = registerGRPCServices(new DocumentServiceImpl(client));
        return Collections.singletonMap(
            GRPC_SECURE_TRANSPORT_SETTING_KEY,
            () -> new SecureNetty4GrpcServerTransport(settings, grpcServices, networkService, secureAuxTransportSettingsProvider)
        );
    }

    /**
     * Registers gRPC services to be exposed by the transport.
     *
     * @param services The gRPC services to register
     * @return A list of registered bindable services
     */
    private List<BindableService> registerGRPCServices(BindableService... services) {
        return List.of(services);
    }

    /**
     * Returns the settings defined by this plugin.
     *
     * @return A list of settings
     */
    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            SETTING_GRPC_PORT,
            SETTING_GRPC_SECURE_PORT,
            SETTING_GRPC_HOST,
            SETTING_GRPC_PUBLISH_HOST,
            SETTING_GRPC_BIND_HOST,
            SETTING_GRPC_WORKER_COUNT,
            SETTING_GRPC_PUBLISH_PORT
        );
    }

    /**
     * Creates components used by the plugin.
     * Stores the client for later use in creating gRPC services.
     *
     * @param client The client
     * @param clusterService The cluster service
     * @param threadPool The thread pool
     * @param resourceWatcherService The resource watcher service
     * @param scriptService The script service
     * @param xContentRegistry The named content registry
     * @param environment The environment
     * @param nodeEnvironment The node environment
     * @param namedWriteableRegistry The named writeable registry
     * @param indexNameExpressionResolver The index name expression resolver
     * @param repositoriesServiceSupplier The repositories service supplier
     * @return A collection of components
     */
    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.client = client;

        return super.createComponents(
            client,
            clusterService,
            threadPool,
            resourceWatcherService,
            scriptService,
            xContentRegistry,
            environment,
            nodeEnvironment,
            namedWriteableRegistry,
            indexNameExpressionResolver,
            repositoriesServiceSupplier
        );
    }
}
