/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.transport;

import org.opensearch.Version;
import org.opensearch.common.SetOnce;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.netty4.Netty4HttpServerTransport;
import org.opensearch.indices.breaker.CircuitBreakerService;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.netty4.Netty4Transport;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class Netty4ModulePlugin extends Plugin implements NetworkPlugin {

    public static final String NETTY_TRANSPORT_NAME = "netty4";
    public static final String NETTY_HTTP_TRANSPORT_NAME = "netty4";

    private final SetOnce<SharedGroupFactory> groupFactory = new SetOnce<>();

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            Netty4HttpServerTransport.SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS,
            Netty4HttpServerTransport.SETTING_HTTP_WORKER_COUNT,
            Netty4HttpServerTransport.SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_SIZE,
            Netty4Transport.WORKER_COUNT,
            Netty4Transport.NETTY_RECEIVE_PREDICTOR_SIZE,
            Netty4Transport.NETTY_RECEIVE_PREDICTOR_MIN,
            Netty4Transport.NETTY_RECEIVE_PREDICTOR_MAX,
            Netty4Transport.NETTY_BOSS_COUNT
        );
    }

    @Override
    public Settings additionalSettings() {
        return Settings.builder()
            // here we set the netty4 transport and http transport as the default. This is a set once setting
            // ie. if another plugin does that as well the server will fail - only one default network can exist!
            .put(NetworkModule.HTTP_DEFAULT_TYPE_SETTING.getKey(), NETTY_HTTP_TRANSPORT_NAME)
            .put(NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING.getKey(), NETTY_TRANSPORT_NAME)
            .build();
    }

    @Override
    public Map<String, Supplier<Transport>> getTransports(
        Settings settings,
        ThreadPool threadPool,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService
    ) {
        return Collections.singletonMap(
            NETTY_TRANSPORT_NAME,
            () -> new Netty4Transport(
                settings,
                Version.CURRENT,
                threadPool,
                networkService,
                pageCacheRecycler,
                namedWriteableRegistry,
                circuitBreakerService,
                getSharedGroupFactory(settings)
            )
        );
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
        ClusterSettings clusterSettings
    ) {
        return Collections.singletonMap(
            NETTY_HTTP_TRANSPORT_NAME,
            () -> new Netty4HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry,
                dispatcher,
                clusterSettings,
                getSharedGroupFactory(settings)
            )
        );
    }

    private SharedGroupFactory getSharedGroupFactory(Settings settings) {
        SharedGroupFactory groupFactory = this.groupFactory.get();
        if (groupFactory != null) {
            assert groupFactory.getSettings().equals(settings) : "Different settings than originally provided";
            return groupFactory;
        } else {
            this.groupFactory.set(new SharedGroupFactory(settings));
            return this.groupFactory.get();
        }
    }
}
