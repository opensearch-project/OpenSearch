/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.AuxTransport;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.grpc.proto.request.search.query.AbstractQueryBuilderProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoConverterRegistryImpl;
import org.opensearch.transport.grpc.services.DocumentServiceImpl;
import org.opensearch.transport.grpc.services.SearchServiceImpl;
import org.opensearch.transport.grpc.spi.GrpcServiceFactory;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverter;
import org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import io.grpc.BindableService;

import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.GRPC_TRANSPORT_SETTING_KEY;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_BIND_HOST;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_EXECUTOR_COUNT;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_HOST;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_KEEPALIVE_TIMEOUT;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_MAX_CONCURRENT_CONNECTION_CALLS;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_MAX_CONNECTION_AGE;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_MAX_CONNECTION_IDLE;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_MAX_MSG_SIZE;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PORT;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PUBLISH_HOST;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PUBLISH_PORT;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_WORKER_COUNT;
import static org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport.GRPC_SECURE_TRANSPORT_SETTING_KEY;
import static org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport.SETTING_GRPC_SECURE_PORT;

/**
 * Main class for the gRPC plugin.
 */
public final class GrpcPlugin extends Plugin implements NetworkPlugin, ExtensiblePlugin {
    private static final Logger logger = LogManager.getLogger(GrpcPlugin.class);

    /** The name of the gRPC thread pool */
    public static final String GRPC_THREAD_POOL_NAME = "grpc";

    private final List<QueryBuilderProtoConverter> queryConverters = new ArrayList<>();
    private final List<GrpcServiceFactory> servicesFactory = new ArrayList<>();
    private QueryBuilderProtoConverterRegistryImpl queryRegistry;
    private AbstractQueryBuilderProtoUtils queryUtils;
    private Client client;

    /**
     * Creates a new GrpcPlugin instance.
     */
    public GrpcPlugin() {}

    /**
     * Loads extensions from other plugins.
     * This method is called by the OpenSearch plugin system to load extensions from other plugins.
     *
     * @param loader The extension loader to use for loading extensions
     */
    @Override
    public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
        // Load query converters from other plugins
        List<QueryBuilderProtoConverter> extensions = loader.loadExtensions(QueryBuilderProtoConverter.class);
        if (extensions != null && !extensions.isEmpty()) {
            logger.info("Loading {} QueryBuilderProtoConverter extensions from other plugins", extensions.size());
            for (QueryBuilderProtoConverter converter : extensions) {
                logger.info(
                    "Discovered QueryBuilderProtoConverter extension: {} (handles: {})",
                    converter.getClass().getName(),
                    converter.getHandledQueryCase()
                );
                queryConverters.add(converter);
            }
            logger.info("Successfully loaded {} QueryBuilderProtoConverter extensions", extensions.size());
        } else {
            logger.info("No QueryBuilderProtoConverter extensions found from other plugins");
        }

        // Load discovered gRPC service factories
        List<GrpcServiceFactory> services = loader.loadExtensions(GrpcServiceFactory.class);
        if (services != null) {
            servicesFactory.addAll(services);
            logger.info("Successfully loaded {} GrpcServiceFactory extensions", services.size());
        }
    }

    /**
     * Get the list of query converters, including those loaded from extensions.
     *
     * @return The list of query converters
     */
    public List<QueryBuilderProtoConverter> getQueryConverters() {
        return Collections.unmodifiableList(queryConverters);
    }

    /**
     * Get the query utils instance.
     *
     * @return The query utils instance
     * @throws IllegalStateException if queryUtils is not initialized
     */
    public AbstractQueryBuilderProtoUtils getQueryUtils() {
        if (queryUtils == null) {
            throw new IllegalStateException("Query utils not initialized. Make sure createComponents has been called.");
        }
        return queryUtils;
    }

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
     * @throws IllegalStateException if queryRegistry is not initialized
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
        if (client == null || queryRegistry == null) {
            throw new RuntimeException("createComponents must be called first to initialize server provided resources.");
        }

        return Collections.singletonMap(GRPC_TRANSPORT_SETTING_KEY, () -> {
            List<BindableService> grpcServices = new ArrayList<>(
                List.of(new DocumentServiceImpl(client), new SearchServiceImpl(client, queryUtils))
            );
            for (GrpcServiceFactory serviceFac : servicesFactory) {
                List<BindableService> pluginServices = serviceFac.initClient(client)
                    .initSettings(settings)
                    .initClusterSettings(clusterSettings)
                    .initThreadPool(threadPool)
                    .build();
                for (BindableService pluginService : pluginServices) {
                    logger.info(
                        "{} gRPC services loaded from plugin: {}",
                        pluginService.bindService().getServiceDescriptor().getName(),
                        serviceFac.plugin()
                    );
                }
                grpcServices.addAll(pluginServices);
            }
            return new Netty4GrpcServerTransport(settings, grpcServices, networkService, threadPool);
        });
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
     * @throws IllegalStateException if queryRegistry is not initialized
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
        if (client == null || queryRegistry == null) {
            throw new RuntimeException("createComponents must be called first to initialize server provided resources.");
        }

        return Collections.singletonMap(GRPC_SECURE_TRANSPORT_SETTING_KEY, () -> {
            List<BindableService> grpcServices = new ArrayList<>(
                List.of(new DocumentServiceImpl(client), new SearchServiceImpl(client, queryUtils))
            );
            for (GrpcServiceFactory serviceFac : servicesFactory) {
                List<BindableService> pluginServices = serviceFac.initClient(client)
                    .initSettings(settings)
                    .initClusterSettings(clusterSettings)
                    .initThreadPool(threadPool)
                    .build();
                for (BindableService pluginService : pluginServices) {
                    logger.info(
                        "{} gRPC services loaded from plugin: {}",
                        pluginService.bindService().getServiceDescriptor().getName(),
                        serviceFac.plugin()
                    );
                }
                grpcServices.addAll(pluginServices);
            }
            return new SecureNetty4GrpcServerTransport(
                settings,
                grpcServices,
                networkService,
                threadPool,
                secureAuxTransportSettingsProvider
            );
        });
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
            SETTING_GRPC_PUBLISH_PORT,
            SETTING_GRPC_SECURE_PORT,
            SETTING_GRPC_HOST,
            SETTING_GRPC_PUBLISH_HOST,
            SETTING_GRPC_BIND_HOST,
            SETTING_GRPC_WORKER_COUNT,
            SETTING_GRPC_EXECUTOR_COUNT,
            SETTING_GRPC_MAX_CONCURRENT_CONNECTION_CALLS,
            SETTING_GRPC_MAX_MSG_SIZE,
            SETTING_GRPC_MAX_CONNECTION_AGE,
            SETTING_GRPC_MAX_CONNECTION_IDLE,
            SETTING_GRPC_KEEPALIVE_TIMEOUT
        );
    }

    /**
     * Returns the executor builders for this plugin's custom thread pools.
     * Creates a dedicated thread pool for gRPC request processing that integrates
     * with OpenSearch's thread pool monitoring and management system.
     *
     * @param settings the current settings
     * @return executor builders for this plugin's custom thread pools
     */
    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final int executorCount = SETTING_GRPC_EXECUTOR_COUNT.get(settings);
        return List.of(
            new FixedExecutorBuilder(settings, GRPC_THREAD_POOL_NAME, executorCount, 1000, "thread_pool." + GRPC_THREAD_POOL_NAME)
        );
    }

    /**
     * Creates components used by the plugin.
     * Stores the client for later use in creating gRPC services, and the query registry which registers the types of supported GRPC Search queries.
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

        // Create the registry
        this.queryRegistry = new QueryBuilderProtoConverterRegistryImpl();

        // Create the query utils instance
        this.queryUtils = new AbstractQueryBuilderProtoUtils(queryRegistry);

        // Inject registry into external converters and register them
        if (!queryConverters.isEmpty()) {
            logger.info("Injecting registry and registering {} external QueryBuilderProtoConverter(s)", queryConverters.size());
            for (QueryBuilderProtoConverter converter : queryConverters) {
                logger.info(
                    "Processing external converter: {} (handles: {})",
                    converter.getClass().getName(),
                    converter.getHandledQueryCase()
                );

                // Inject the populated registry into the converter
                converter.setRegistry(queryRegistry);
                logger.info("Injected registry into converter: {}", converter.getClass().getName());

                // Register the converter
                queryRegistry.registerConverter(converter);
            }
            logger.info("Successfully injected registry and registered all {} external converters", queryConverters.size());

            // Update the registry on all converters (including built-in ones) so they can access external converters
            queryRegistry.updateRegistryOnAllConverters();
            logger.info("Updated registry on all converters to include external converters");
        } else {
            logger.info("No external QueryBuilderProtoConverter(s) to register");
        }

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
