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

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportInterceptor;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.opensearch.common.settings.Setting.affixKeySetting;

/**
 * Plugin for extending network and transport related classes
 *
 * @opensearch.api
 */
public interface NetworkPlugin {

    /**
     * Auxiliary transports are lifecycle components with an associated port range.
     * These pluggable client/server transport implementations have their lifecycle managed by Node.
     *
     * Auxiliary transports are additionally defined by a port range on which they bind. Opening permissions on these
     * ports is awkward as {@link org.opensearch.bootstrap.Security} is configured previous to Node initialization during
     * bootstrap. To allow pluggable AuxTransports access to configurable port ranges we require the port range be provided
     * through an {@link org.opensearch.common.settings.Setting.AffixSetting} of the form 'AUX_SETTINGS_PREFIX.{aux-transport-key}.ports'.
     */
    @ExperimentalApi
    abstract class AuxTransport extends AbstractLifecycleComponent {
        public static final String AUX_SETTINGS_PREFIX = "aux.transport.";
        public static final String AUX_TRANSPORT_TYPES_KEY = AUX_SETTINGS_PREFIX + "types";
        public static final String AUX_PORT_DEFAULTS = "9400-9500";
        public static final Setting.AffixSetting<PortsRange> AUX_TRANSPORT_PORT = affixKeySetting(
            AUX_SETTINGS_PREFIX,
            "port",
            key -> new Setting<>(key, AUX_PORT_DEFAULTS, PortsRange::new, Setting.Property.NodeScope)
        );

        public static final Setting<List<String>> AUX_TRANSPORT_TYPES_SETTING = Setting.listSetting(
            AUX_TRANSPORT_TYPES_KEY,
            emptyList(),
            Function.identity(),
            Setting.Property.NodeScope
        );
    }

    /**
     * Auxiliary transports are optional and run in parallel to the default HttpServerTransport.
     * Returns a map of AuxTransport suppliers.
     */
    @ExperimentalApi
    default Map<String, Supplier<AuxTransport>> getAuxTransports(
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
     * Returns a list of {@link TransportInterceptor} instances that are used to intercept incoming and outgoing
     * transport (inter-node) requests. This must not return <code>null</code>
     *
     * @param namedWriteableRegistry registry of all named writeables registered
     * @param threadContext a {@link ThreadContext} of the current nodes or clients {@link ThreadPool} that can be used to set additional
     *                      headers in the interceptors
     */
    default List<TransportInterceptor> getTransportInterceptors(
        NamedWriteableRegistry namedWriteableRegistry,
        ThreadContext threadContext
    ) {
        return Collections.emptyList();
    }

    /**
     * Returns a map of {@link Transport} suppliers.
     * See {@link org.opensearch.common.network.NetworkModule#TRANSPORT_TYPE_KEY} to configure a specific implementation.
     */
    default Map<String, Supplier<Transport>> getTransports(
        Settings settings,
        ThreadPool threadPool,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService,
        Tracer tracer
    ) {
        return Collections.emptyMap();
    }

    /**
     * Returns a map of {@link HttpServerTransport} suppliers.
     * See {@link org.opensearch.common.network.NetworkModule#HTTP_TYPE_SETTING} to configure a specific implementation.
     */
    default Map<String, Supplier<HttpServerTransport>> getHttpTransports(
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
        return Collections.emptyMap();
    }

    /**
     * Returns a map of secure {@link AuxTransport} suppliers.
     * See {@link org.opensearch.plugins.NetworkPlugin.AuxTransport#AUX_TRANSPORT_TYPES_SETTING} to configure a specific implementation.
     */
    @ExperimentalApi
    default Map<String, Supplier<AuxTransport>> getSecureAuxTransports(
        Settings settings,
        ThreadPool threadPool,
        CircuitBreakerService circuitBreakerService,
        NetworkService networkService,
        ClusterSettings clusterSettings,
        SecureAuxTransportSettingsProvider secureAuxTransportSettingsProvider,
        Tracer tracer
    ) {
        return Collections.emptyMap();
    }

    /**
     * Returns a map of secure {@link Transport} suppliers.
     * See {@link org.opensearch.common.network.NetworkModule#TRANSPORT_TYPE_KEY} to configure a specific implementation.
     */
    default Map<String, Supplier<Transport>> getSecureTransports(
        Settings settings,
        ThreadPool threadPool,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService,
        SecureTransportSettingsProvider secureTransportSettingsProvider,
        Tracer tracer
    ) {
        return Collections.emptyMap();
    }

    /**
     * Returns a map of secure {@link HttpServerTransport} suppliers.
     * See {@link org.opensearch.common.network.NetworkModule#HTTP_TYPE_SETTING} to configure a specific implementation.
     */
    default Map<String, Supplier<HttpServerTransport>> getSecureHttpTransports(
        Settings settings,
        ThreadPool threadPool,
        BigArrays bigArrays,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedXContentRegistry xContentRegistry,
        NetworkService networkService,
        HttpServerTransport.Dispatcher dispatcher,
        ClusterSettings clusterSettings,
        SecureHttpTransportSettingsProvider secureHttpTransportSettingsProvider,
        Tracer tracer
    ) {
        return Collections.emptyMap();
    }
}
