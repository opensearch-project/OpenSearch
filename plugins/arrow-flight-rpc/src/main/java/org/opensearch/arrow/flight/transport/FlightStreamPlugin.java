/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.Version;
import org.opensearch.arrow.flight.bootstrap.ServerConfig;
import org.opensearch.arrow.flight.bootstrap.tls.DefaultSslContextProvider;
import org.opensearch.arrow.flight.bootstrap.tls.SslContextProvider;
import org.opensearch.arrow.flight.stats.FlightStatsAction;
import org.opensearch.arrow.flight.stats.FlightStatsCollector;
import org.opensearch.arrow.flight.stats.FlightStatsRestHandler;
import org.opensearch.arrow.flight.stats.TransportFlightStatsAction;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ClusterPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.AuxTransport;
import org.opensearch.transport.Transport;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * FlightStreamPlugin class extends BaseFlightStreamPlugin and provides implementation for FlightStream plugin.
 */
public class FlightStreamPlugin extends Plugin implements NetworkPlugin, ActionPlugin, ClusterPlugin, ExtensiblePlugin {

    private final boolean isStreamTransportEnabled;
    private FlightStatsCollector statsCollector;

    /**
     * Constructor for FlightStreamPluginImpl.
     * @param settings The settings for the FlightStreamPlugin.
     */
    public FlightStreamPlugin(Settings settings) {
        this.isStreamTransportEnabled = FeatureFlags.isEnabled(FeatureFlags.STREAM_TRANSPORT);
        if (isStreamTransportEnabled) {
            try {
                ServerConfig.init(settings);
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize Arrow Flight server", e);
            }
        }
    }

    /**
     * Creates components for the FlightStream plugin.
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
     * @return Collection of components
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
        if (!isStreamTransportEnabled) {
            return Collections.emptyList();
        }

        List<Object> components = new ArrayList<>();
        statsCollector = new FlightStatsCollector();
        components.add(statsCollector);
        return components;
    }

    /**
     * Gets the secure transports for the FlightStream plugin.
     * @param settings The settings for the plugin.
     * @param threadPool The thread pool instance.
     * @param pageCacheRecycler The page cache recycler instance.
     * @param circuitBreakerService The circuit breaker service instance.
     * @param namedWriteableRegistry The named writeable registry.
     * @param networkService The network service instance.
     * @param secureTransportSettingsProvider The secure transport settings provider.
     * @param tracer The tracer instance.
     * @return A map of secure transports.
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
        if (isStreamTransportEnabled && ServerConfig.isSslEnabled()) {
            SslContextProvider sslContextProvider = new DefaultSslContextProvider(secureTransportSettingsProvider, settings);
            return Collections.singletonMap(
                "FLIGHT-SECURE",
                () -> new FlightTransport(
                    settings,
                    Version.CURRENT,
                    threadPool,
                    pageCacheRecycler,
                    circuitBreakerService,
                    namedWriteableRegistry,
                    networkService,
                    tracer,
                    sslContextProvider,
                    statsCollector
                )
            );
        }
        return Collections.emptyMap();
    }

    /**
     * Gets the secure transports for the FlightStream plugin.
     * @param settings The settings for the plugin.
     * @param threadPool The thread pool instance.
     * @param pageCacheRecycler The page cache recycler instance.
     * @param circuitBreakerService The circuit breaker service instance.
     * @param namedWriteableRegistry The named writeable registry.
     * @param networkService The network service instance.
     * @param tracer The tracer instance.
     * @return A map of secure transports.
     */
    @Override
    public Map<String, Supplier<Transport>> getTransports(
        Settings settings,
        ThreadPool threadPool,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService,
        Tracer tracer
    ) {
        if (isStreamTransportEnabled && !ServerConfig.isSslEnabled()) {
            return Collections.singletonMap(
                "FLIGHT",
                () -> new FlightTransport(
                    settings,
                    Version.CURRENT,
                    threadPool,
                    pageCacheRecycler,
                    circuitBreakerService,
                    namedWriteableRegistry,
                    networkService,
                    tracer,
                    null,
                    statsCollector
                )
            );
        }
        return Collections.emptyMap();
    }

    /**
     * Gets the auxiliary transports for the FlightStream plugin.
     * @param settings The settings for the plugin.
     * @param threadPool The thread pool instance.
     * @param circuitBreakerService The circuit breaker service instance.
     * @param networkService The network service instance.
     * @param clusterSettings The cluster settings instance.
     * @param tracer The tracer instance.
     * @return A map of auxiliary transports.
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
        return Collections.emptyMap();
    }

    /**
     * Gets the REST handlers for the FlightStream plugin.
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
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        if (!isStreamTransportEnabled) {
            return Collections.emptyList();
        }

        return Collections.singletonList(new FlightStatsRestHandler());
    }

    /**
     * Gets the list of action handlers for the FlightStream plugin.
     * @return A list of action handlers.
     */
    @Override
    public List<ActionHandler<?, ?>> getActions() {
        if (!isStreamTransportEnabled) {
            return Collections.emptyList();
        }

        return Collections.singletonList(new ActionHandler<>(FlightStatsAction.INSTANCE, TransportFlightStatsAction.class));
    }

    /**
     * Gets the list of ExecutorBuilder instances for building thread pools used for FlightServer.
     * @param settings The settings for the plugin
     */
    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        if (!isStreamTransportEnabled) {
            return Collections.emptyList();
        }
        return List.of(
            ServerConfig.getServerExecutorBuilder(),
            ServerConfig.getGrpcExecutorBuilder(),
            ServerConfig.getClientExecutorBuilder()
        );
    }

    /**
     * Gets the list of settings for the Flight plugin.
     */
    @Override
    public List<Setting<?>> getSettings() {
        if (!isStreamTransportEnabled) {
            return Collections.emptyList();
        }
        return new ArrayList<>(
            Arrays.asList(
                ServerConfig.SETTING_FLIGHT_PORTS,
                ServerConfig.SETTING_FLIGHT_HOST,
                ServerConfig.SETTING_FLIGHT_BIND_HOST,
                ServerConfig.SETTING_FLIGHT_PUBLISH_HOST,
                ServerConfig.SETTING_FLIGHT_PUBLISH_PORT
            )
        ) {
            {
                addAll(ServerConfig.getSettings());
            }
        };
    }
}
