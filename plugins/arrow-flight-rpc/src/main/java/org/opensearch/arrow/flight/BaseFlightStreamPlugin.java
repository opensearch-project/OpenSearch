/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ClusterPlugin;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.plugins.StreamManagerPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * BaseFlightStreamPlugin is a plugin that implements the StreamManagerPlugin interface.
 * It provides the necessary components for handling flight streams in the OpenSearch cluster.
 */
public abstract class BaseFlightStreamPlugin extends Plugin implements StreamManagerPlugin, NetworkPlugin, ActionPlugin, ClusterPlugin {

    /**
     * Constructor for BaseFlightStreamPlugin.
     */
    public BaseFlightStreamPlugin() {
        super();
    }

    /**
     * createComponents for BaseFlightStreamPlugin
     * @param client The client instance.
     * @param clusterService The cluster service instance.
     * @param threadPool The thread pool instance.
     * @param resourceWatcherService The resource watcher service instance.
     * @param scriptService The script service instance.
     * @param xContentRegistry The named XContent registry.
     * @param environment The environment instance.
     * @param nodeEnvironment The node environment instance.
     * @param namedWriteableRegistry The named writeable registry.
     * @param indexNameExpressionResolver The index name expression resolver instance.
     * @param repositoriesServiceSupplier The supplier for the repositories service.
     * @return A collection of components.
     */
    @Override
    public abstract Collection<Object> createComponents(
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
    );

    /**
     * Used to get the transports for SSL/TLS configuration of Flight server and clients
     * @param settings The settings for the plugin
     * @param threadPool The thread pool instance
     * @param pageCacheRecycler The page cache recycler instance
     * @param circuitBreakerService The circuit breaker service instance
     * @param namedWriteableRegistry The named writeable registry
     * @param networkService The network service instance
     * @param secureTransportSettingsProvider The secure transport settings provider
     * @param tracer The tracer instance
     * @return A map of secure transports
     */
    @Override
    public abstract Map<String, Supplier<Transport>> getSecureTransports(
        Settings settings,
        ThreadPool threadPool,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService,
        SecureTransportSettingsProvider secureTransportSettingsProvider,
        Tracer tracer
    );

    /**
     * Used to get the auxiliary transports for Flight server and clients
     * @param settings The settings for the plugin
     * @param threadPool The thread pool instance
     * @param circuitBreakerService The circuit breaker service instance
     * @param networkService The network service instance
     * @param clusterSettings The cluster settings instance
     * @param tracer The tracer instance
     * @return A map of auxiliary transports
     */
    @Override
    public abstract Map<String, Supplier<AuxTransport>> getAuxTransports(
        Settings settings,
        ThreadPool threadPool,
        CircuitBreakerService circuitBreakerService,
        NetworkService networkService,
        ClusterSettings clusterSettings,
        Tracer tracer
    );

    /**
     * Returns the StreamManager instance for managing flight streams.
     */
    @Override
    public abstract Supplier<StreamManager> getStreamManager();

    /**
     * Returns a list of ExecutorBuilder instances for building thread pools used for FlightServer
     * @param settings The settings for the plugin
     */
    @Override
    public abstract List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings);

    /**
     * Returns a list of settings for the Flight plugin.
     */
    @Override
    public abstract List<Setting<?>> getSettings();

    /**
     * Returns the REST handlers for the Flight plugin.
     * @param settings The settings for the plugin.
     * @param restController The REST controller instance.
     * @param clusterSettings The cluster settings instance.
     * @param indexScopedSettings The index scoped settings instance.
     * @param settingsFilter The settings filter instance.
     * @param indexNameExpressionResolver The index name expression resolver instance.
     * @param nodesInCluster The supplier for the discovery nodes.
     * @return A list of REST handlers.
     */
    @Override
    public abstract List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    );

    /**
     * Returns the list of action handlers for the Flight plugin.
     */
    @Override
    public abstract List<ActionHandler<?, ?>> getActions();

    /**
     * Called when node is started. DiscoveryNode argument is passed to allow referring localNode value inside plugin
     *
     * @param localNode local Node info
     */
    public abstract void onNodeStarted(DiscoveryNode localNode);
}
