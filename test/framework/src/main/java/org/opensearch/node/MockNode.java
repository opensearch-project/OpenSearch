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

package org.opensearch.node;

import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.MockInternalClusterInfoService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.env.Environment;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.indices.IndicesService;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptService;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptService;
import org.opensearch.search.MockSearchService;
import org.opensearch.search.SearchService;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.test.MockHttpTransport;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportService;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A node for testing which allows:
 * <ul>
 *   <li>Overriding Version.CURRENT</li>
 *   <li>Adding test plugins that exist on the classpath</li>
 * </ul>
 */
public class MockNode extends Node {

    private final Collection<Class<? extends Plugin>> classpathPlugins;

    public MockNode(final Settings settings, final Collection<Class<? extends Plugin>> classpathPlugins) {
        this(settings, classpathPlugins, true);
    }

    public MockNode(
        final Settings settings,
        final Collection<Class<? extends Plugin>> classpathPlugins,
        final boolean forbidPrivateIndexSettings
    ) {
        this(settings, classpathPlugins, null, forbidPrivateIndexSettings);
    }

    public MockNode(
        final Settings settings,
        final Collection<Class<? extends Plugin>> classpathPlugins,
        final Path configPath,
        final boolean forbidPrivateIndexSettings
    ) {
        this(
            InternalSettingsPreparer.prepareEnvironment(settings, Collections.emptyMap(), configPath, () -> "mock_ node"),
            classpathPlugins,
            forbidPrivateIndexSettings
        );
    }

    private MockNode(
        final Environment environment,
        final Collection<Class<? extends Plugin>> classpathPlugins,
        final boolean forbidPrivateIndexSettings
    ) {
        super(environment, classpathPlugins, forbidPrivateIndexSettings);
        this.classpathPlugins = classpathPlugins;
    }

    /**
     * The classpath plugins this node was constructed with.
     */
    public Collection<Class<? extends Plugin>> getClasspathPlugins() {
        return classpathPlugins;
    }

    @Override
    protected BigArrays createBigArrays(PageCacheRecycler pageCacheRecycler, CircuitBreakerService circuitBreakerService) {
        if (getPluginsService().filterPlugins(NodeMocksPlugin.class).isEmpty()) {
            return super.createBigArrays(pageCacheRecycler, circuitBreakerService);
        }
        return new MockBigArrays(pageCacheRecycler, circuitBreakerService);
    }

    @Override
    PageCacheRecycler createPageCacheRecycler(Settings settings) {
        if (getPluginsService().filterPlugins(NodeMocksPlugin.class).isEmpty()) {
            return super.createPageCacheRecycler(settings);
        }
        return new MockPageCacheRecycler(settings);
    }

    @Override
    protected SearchService newSearchService(
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ScriptService scriptService,
        BigArrays bigArrays,
        QueryPhase queryPhase,
        FetchPhase fetchPhase,
        ResponseCollectorService responseCollectorService,
        CircuitBreakerService circuitBreakerService,
        Executor indexSearcherExecutor
    ) {
        if (getPluginsService().filterPlugins(MockSearchService.TestPlugin.class).isEmpty()) {
            return super.newSearchService(
                clusterService,
                indicesService,
                threadPool,
                scriptService,
                bigArrays,
                queryPhase,
                fetchPhase,
                responseCollectorService,
                circuitBreakerService,
                indexSearcherExecutor
            );
        }
        return new MockSearchService(
            clusterService,
            indicesService,
            threadPool,
            scriptService,
            bigArrays,
            queryPhase,
            fetchPhase,
            circuitBreakerService,
            indexSearcherExecutor
        );
    }

    @Override
    protected ScriptService newScriptService(Settings settings, Map<String, ScriptEngine> engines, Map<String, ScriptContext<?>> contexts) {
        if (getPluginsService().filterPlugins(MockScriptService.TestPlugin.class).isEmpty()) {
            return super.newScriptService(settings, engines, contexts);
        }
        return new MockScriptService(settings, engines, contexts);
    }

    @Override
    protected TransportService newTransportService(
        Settings settings,
        Transport transport,
        ThreadPool threadPool,
        TransportInterceptor interceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        ClusterSettings clusterSettings,
        Set<String> taskHeaders
    ) {
        // we use the MockTransportService.TestPlugin class as a marker to create a network
        // module with this MockNetworkService. NetworkService is such an integral part of the systme
        // we don't allow to plug it in from plugins or anything. this is a test-only override and
        // can't be done in a production env.
        if (getPluginsService().filterPlugins(MockTransportService.TestPlugin.class).isEmpty()) {
            return super.newTransportService(settings, transport, threadPool, interceptor, localNodeFactory, clusterSettings, taskHeaders);
        } else {
            return new MockTransportService(settings, transport, threadPool, interceptor, localNodeFactory, clusterSettings, taskHeaders);
        }
    }

    @Override
    protected void processRecoverySettings(ClusterSettings clusterSettings, RecoverySettings recoverySettings) {
        if (false == getPluginsService().filterPlugins(RecoverySettingsChunkSizePlugin.class).isEmpty()) {
            clusterSettings.addSettingsUpdateConsumer(RecoverySettingsChunkSizePlugin.CHUNK_SIZE_SETTING, recoverySettings::setChunkSize);
        }
    }

    @Override
    protected ClusterInfoService newClusterInfoService(
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        NodeClient client
    ) {
        if (getPluginsService().filterPlugins(MockInternalClusterInfoService.TestPlugin.class).isEmpty()) {
            return super.newClusterInfoService(settings, clusterService, threadPool, client);
        } else {
            final MockInternalClusterInfoService service = new MockInternalClusterInfoService(settings, clusterService, threadPool, client);
            clusterService.addListener(service);
            return service;
        }
    }

    @Override
    protected HttpServerTransport newHttpTransport(NetworkModule networkModule) {
        if (getPluginsService().filterPlugins(MockHttpTransport.TestPlugin.class).isEmpty()) {
            return super.newHttpTransport(networkModule);
        } else {
            return new MockHttpTransport();
        }
    }

    @Override
    protected void configureNodeAndClusterIdStateListener(ClusterService clusterService) {
        // do not configure this in tests as this is causing SetOnce to throw exceptions when jvm is used for multiple tests
    }

    public NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }
}
