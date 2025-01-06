/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import org.opensearch.arrow.flight.bootstrap.FlightStreamPluginImpl;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.SecureTransportSettingsProvider;
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

import static org.opensearch.common.util.FeatureFlags.ARROW_STREAMS_SETTING;

/**
 * Delegates the plugin implementation to the {@link FlightStreamPluginImpl} if the Arrow Streams feature flag is enabled.
 * Otherwise, it creates a no-op implementation.
 */
@ExperimentalApi
public class FlightStreamPlugin extends BaseFlightStreamPlugin {

    private final BaseFlightStreamPlugin delegate;

    /**
     * Constructor for FlightStreamPlugin.
     * @param settings The settings for the plugin.
     */
    public FlightStreamPlugin(Settings settings) {
        if (FeatureFlags.isEnabled(ARROW_STREAMS_SETTING)) {
            this.delegate = new FlightStreamPluginImpl(settings);
        } else {
            this.delegate = new BaseFlightStreamPlugin() {
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
                    return List.of();
                }

                @Override
                public Map<String, Supplier<Transport>> getSecureTransports(
                    Settings settings,
                    ThreadPool threadPool,
                    PageCacheRecycler pageCacheRecycler,
                    CircuitBreakerService circuitBreakerService,
                    NamedWriteableRegistry namedWriteableRegistry,
                    NetworkService networkService,
                    SecureTransportSettingsProvider secureTransportSettingsProvider,
                    Tracer tracer
                ) {
                    return Map.of();
                }

                @Override
                public Supplier<StreamManager> getStreamManager() {
                    return () -> null;
                }

                @Override
                public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
                    return List.of();
                }

                @Override
                public List<Setting<?>> getSettings() {
                    return List.of();
                }

                @Override
                public void onNodeStarted(DiscoveryNode localNode) {

                }
            };
        }
    }

    /**
     * Creates components related to the Flight stream functionality.
     * @param client The OpenSearch client
     * @param clusterService The cluster service
     * @param threadPool The thread pool
     * @param resourceWatcherService The resource watcher service
     * @param scriptService The script service
     * @param xContentRegistry The named XContent registry
     * @param environment The environment
     * @param nodeEnvironment The node environment
     * @param namedWriteableRegistry The named writeable registry
     * @param indexNameExpressionResolver The index name expression resolver
     * @param repositoriesServiceSupplier The supplier for the repositories service
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
        return delegate.createComponents(
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

    /**
     * Gets the secure transports for Flight stream functionality.
     * @param settings The settings for the plugin
     * @param threadPool The thread pool
     * @param pageCacheRecycler The page cache recycler
     * @param circuitBreakerService The circuit breaker service
     * @param namedWriteableRegistry The named writeable registry
     * @param networkService The network service
     * @param secureTransportSettingsProvider The secure transport settings provider
     * @param tracer The tracer
     * @return A map of secure transports
     */
    @Override
    public Map<String, Supplier<Transport>> getSecureTransports(
        Settings settings,
        ThreadPool threadPool,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService,
        SecureTransportSettingsProvider secureTransportSettingsProvider,
        Tracer tracer
    ) {
        return delegate.getSecureTransports(
            settings,
            threadPool,
            pageCacheRecycler,
            circuitBreakerService,
            namedWriteableRegistry,
            networkService,
            secureTransportSettingsProvider,
            tracer
        );
    }

    /**
     * Gets the StreamManager instance for managing flight streams.
     */
    @Override
    public Supplier<StreamManager> getStreamManager() {
        return delegate.getStreamManager();
    }

    /**
     * Gets the list of ExecutorBuilder instances for building thread pools used for FlightServer.
     * @param settings The settings for the plugin
     */
    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return delegate.getExecutorBuilders(settings);
    }

    /**
     * Gets the list of settings for the Flight plugin.
     */
    @Override
    public List<Setting<?>> getSettings() {
        return delegate.getSettings();
    }

    /**
     * Called when a node is started. ClusterService is started by this time
     * @param localNode local Node info
     */
    @Override
    public void onNodeStarted(DiscoveryNode localNode) {
        delegate.onNodeStarted(localNode);
    }
}
