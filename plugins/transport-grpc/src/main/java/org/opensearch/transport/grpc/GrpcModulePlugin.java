/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc;

import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.opensearch.common.settings.Setting.intSetting;
import static org.opensearch.common.settings.Setting.listSetting;

/**
 * Main class for the gRPC plugin
 */
public final class GrpcModulePlugin extends Plugin implements NetworkPlugin {
    public static final Setting<Integer> SETTING_GRPC_PUBLISH_PORT = intSetting("grpc.publish_port", -1, -1, Setting.Property.NodeScope);

    public static final Setting<List<String>> SETTING_GRPC_HOST = listSetting(
        "grpc.host",
        emptyList(),
        Function.identity(),
        Setting.Property.NodeScope
    );

    public static final Setting<List<String>> SETTING_GRPC_PUBLISH_HOST = listSetting(
        "grpc.publish_host",
        SETTING_GRPC_HOST,
        Function.identity(),
        Setting.Property.NodeScope
    );

    public static final Setting<List<String>> SETTING_GRPC_BIND_HOST = listSetting(
        "grpc.bind_host",
        SETTING_GRPC_HOST,
        Function.identity(),
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> SETTING_GRPC_WORKER_COUNT = new Setting<>(
        "grpc.netty.worker_count",
        (s) -> Integer.toString(OpenSearchExecutors.allocatedProcessors(s)),
        (s) -> Setting.parseInt(s, 1, "grpc.netty.worker_count"),
        Setting.Property.NodeScope
    );

    private Netty4GrpcServerTransport transport;

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
        if (FeatureFlags.isEnabled(FeatureFlags.GRPC_ENABLE_SETTING) == false) {
            throw new IllegalArgumentException("transport-grpc is experimental and feature flag must be enabled before use");
        }
        this.transport = new Netty4GrpcServerTransport(clusterService.getSettings(), Collections.emptyList());

        // The server will manage the lifecycle of any instance of LifecycleComponent that is returned here
        return List.of(transport);
    }

    @Override
    public Map<String, Supplier<HttpServerTransport>> getHttpTransports(
        Settings settings,
        ThreadPool threadPool,
        BigArrays bigArrays,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedXContentRegistry xContentRegistry,
        NetworkService networkService,
        HttpServerTransport.Dispatcher dispatcher,
        ClusterSettings clusterSettings,
        Tracer tracer
    ) {
        // This is something of a hack to capture the network service via the HTTP transport extension
        // point, but unless/until a specific gRPC extension point is needed in the future this will work.
        transport.setNetworkService(networkService);
        return Collections.emptyMap();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(SETTING_GRPC_HOST, SETTING_GRPC_PUBLISH_HOST, SETTING_GRPC_BIND_HOST, SETTING_GRPC_WORKER_COUNT);
    }
}
