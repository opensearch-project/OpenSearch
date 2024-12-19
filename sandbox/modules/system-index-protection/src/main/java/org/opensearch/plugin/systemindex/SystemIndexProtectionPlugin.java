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

package org.opensearch.plugin.systemindex;

import org.opensearch.action.support.ActionFilter;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.filter.ClusterInfoHolder;
import org.opensearch.index.filter.IndexResolverReplacer;
import org.opensearch.index.filter.SystemIndexFilter;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteClusterService;
import org.opensearch.transport.TransportService;
import org.opensearch.watcher.ResourceWatcherService;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class SystemIndexProtectionPlugin extends Plugin implements ActionPlugin {

    public static final String SYSTEM_INDEX_PROTECTION_ENABLED_KEY = "modules.system_index_protection.system_indices.enabled";

    private volatile SystemIndexFilter sif;
    private volatile IndexResolverReplacer irr;
    private volatile Settings settings;

    public SystemIndexProtectionPlugin(final Settings settings, final Path configPath) {
        this.settings = settings;
    }

    @Override
    public Collection<Object> createComponents(
        Client localClient,
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
        final ClusterInfoHolder cih = new ClusterInfoHolder(clusterService.getClusterName().value());

        final IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(threadPool.getThreadContext());
        irr = new IndexResolverReplacer(resolver, clusterService::state, cih);
        sif = new SystemIndexFilter(irr, localClient.threadPool());
        return Collections.emptySet();
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        List<ActionFilter> filters = new ArrayList<>(1);
        boolean isEnabled = settings.getAsBoolean(SYSTEM_INDEX_PROTECTION_ENABLED_KEY, false);
        if (isEnabled) {
            filters.add(Objects.requireNonNull(sif));
        }
        return filters;
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        // called for every index!
        boolean isEnabled = settings.getAsBoolean(SYSTEM_INDEX_PROTECTION_ENABLED_KEY, false);

        if (isEnabled) {
            indexModule.setReaderWrapper(indexService -> new SystemIndexSearcherWrapper(indexService, settings));
        }
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        final List<Class<? extends LifecycleComponent>> services = new ArrayList<>(1);
        services.add(GuiceHolder.class);
        return services;
    }

    public static class GuiceHolder implements LifecycleComponent {

        private static RepositoriesService repositoriesService;
        private static RemoteClusterService remoteClusterService;

        @Inject
        public GuiceHolder(final RepositoriesService repositoriesService, final TransportService remoteClusterService) {
            GuiceHolder.repositoriesService = repositoriesService;
            GuiceHolder.remoteClusterService = remoteClusterService.getRemoteClusterService();
        }

        public static RepositoriesService getRepositoriesService() {
            return repositoriesService;
        }

        public static RemoteClusterService getRemoteClusterService() {
            return remoteClusterService;
        }

        @Override
        public void close() {}

        @Override
        public Lifecycle.State lifecycleState() {
            return null;
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {}

        @Override
        public void removeLifecycleListener(LifecycleListener listener) {}

        @Override
        public void start() {}

        @Override
        public void stop() {}

    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<Setting<?>>();

        settings.add(
            Setting.boolSetting(
                SYSTEM_INDEX_PROTECTION_ENABLED_KEY,
                false,
                Setting.Property.NodeScope,
                Setting.Property.Filtered,
                Setting.Property.Final
            )
        );
        return settings;
    }
}
