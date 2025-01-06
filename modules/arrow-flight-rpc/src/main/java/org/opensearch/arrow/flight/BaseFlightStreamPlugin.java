/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ClusterPlugin;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.plugins.StreamManagerPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * BaseFlightStreamPlugin is a plugin that implements the StreamManagerPlugin interface.
 * It provides the necessary components for handling flight streams in the OpenSearch cluster.
 */
public abstract class BaseFlightStreamPlugin extends Plugin implements StreamManagerPlugin, NetworkPlugin, ClusterPlugin {

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
     * Called when a node is started. ClusterService is started by this time
     * @param localNode local Node info
     */
    @Override
    public abstract void onNodeStarted(DiscoveryNode localNode);
}
