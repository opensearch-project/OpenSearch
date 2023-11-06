/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.reactor;

import org.opensearch.common.SetOnce;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.reactor.netty4.ReactorNetty4HttpServerTransport;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The experimental network plugin that introduces new transport implementations based on Reactor Netty.
 */
public class ReactorNetty4Plugin extends Plugin implements NetworkPlugin {
    /**
     * The name of new experimental HTTP transport implementations based on Reactor Netty.
     */
    public static final String REACTOR_NETTY_HTTP_TRANSPORT_NAME = "reactor-netty4";

    private final SetOnce<SharedGroupFactory> groupFactory = new SetOnce<>();

    /**
     * Default constructor
     */
    public ReactorNetty4Plugin() {}

    /**
     * Returns a list of additional {@link Setting} definitions for this plugin.
     */
    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(/* no setting registered since we're picking the onces from Netty 4 transport */);
    }

    /**
     * Returns a map of {@link HttpServerTransport} suppliers.
     * See {@link org.opensearch.common.network.NetworkModule#HTTP_TYPE_SETTING} to configure a specific implementation.
     * @param settings settings
     * @param networkService network service
     * @param bigArrays big array allocator
     * @param pageCacheRecycler page cache recycler instance
     * @param circuitBreakerService circuit breaker service instance
     * @param threadPool thread pool instance
     * @param xContentRegistry XContent registry instance
     * @param dispatcher dispatcher instance
     * @param clusterSettings cluster settings
     * @param tracer tracer instance
     */
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
        return Collections.singletonMap(
            REACTOR_NETTY_HTTP_TRANSPORT_NAME,
            () -> new ReactorNetty4HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry,
                dispatcher,
                clusterSettings,
                getSharedGroupFactory(settings),
                tracer
            )
        );
    }

    private SharedGroupFactory getSharedGroupFactory(Settings settings) {
        final SharedGroupFactory groupFactory = this.groupFactory.get();
        if (groupFactory != null) {
            assert groupFactory.getSettings().equals(settings) : "Different settings than originally provided";
            return groupFactory;
        } else {
            this.groupFactory.set(new SharedGroupFactory(settings));
            return this.groupFactory.get();
        }
    }
}
