/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.cluster.ClusterState;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.common.inject.Module;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.analytics.spi.AnalyticsBackEndPlugin;
import org.opensearch.analytics.spi.AnalyticsFrontEndPlugin;
import org.opensearch.analytics.spi.QueryPlanExecutorPlugin;
import org.opensearch.analytics.spi.SchemaProvider;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Analytics engine hub. Implements {@link ExtensiblePlugin} to discover
 * and wire query engine extensions via SPI.
 *
 * <p>Discovers {@link QueryPlanExecutorPlugin}, {@link AnalyticsBackEndPlugin},
 * and {@link AnalyticsFrontEndPlugin} implementations during
 * {@link #loadExtensions}. Defers actual wiring to {@link #createComponents}
 * where the {@link SchemaProvider} is built and the executor is initialized.
 * Binds {@link QueryPlanExecutor} and {@link SchemaProvider} into Guice
 * via lazy providers so that front-end plugins can inject them.
 *
 * @opensearch.internal
 */
public class BaseAnalyticsPlugin extends Plugin implements ExtensiblePlugin {

    private static final Logger logger = LogManager.getLogger(BaseAnalyticsPlugin.class);

    private final List<QueryPlanExecutorPlugin> executorPlugins = new ArrayList<>();
    private final List<AnalyticsBackEndPlugin> backEnds = new ArrayList<>();

    // Lazy references populated in createComponents, resolved by Guice providers
    private final AtomicReference<QueryPlanExecutor> executorRef = new AtomicReference<>();
    private final AtomicReference<SchemaProvider> schemaProviderRef = new AtomicReference<>();

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        executorPlugins.addAll(loader.loadExtensions(QueryPlanExecutorPlugin.class));
        backEnds.addAll(loader.loadExtensions(AnalyticsBackEndPlugin.class));

        if (executorPlugins.isEmpty()) {
            logger.warn("No QueryPlanExecutorPlugin found; query engine disabled");
        }
        if (executorPlugins.size() > 1) {
            logger.warn("Multiple QueryPlanExecutorPlugin found; using first: {}", executorPlugins.get(0).getClass().getName());
        }

        SchemaProvider schemaProvider = clusterState ->
            OpenSearchSchemaBuilder.buildSchema((ClusterState) clusterState);
        schemaProviderRef.set(schemaProvider);

        if (executorPlugins.isEmpty() == false) {
            QueryPlanExecutorPlugin executorPlugin = executorPlugins.get(0);
            QueryPlanExecutor executor = executorPlugin.createExecutor(backEnds);
            executorRef.set(executor);
        }
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return List.of(b -> {
            b.bind(QueryPlanExecutor.class).toProvider(executorRef::get);
            b.bind(SchemaProvider.class).toProvider(schemaProviderRef::get);
        });
    }
}
