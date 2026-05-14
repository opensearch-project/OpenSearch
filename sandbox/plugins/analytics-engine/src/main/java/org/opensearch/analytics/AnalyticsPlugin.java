/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.analytics.exec.AnalyticsSearchService;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.exec.QueryScheduler;
import org.opensearch.analytics.exec.Scheduler;
import org.opensearch.analytics.exec.action.AnalyticsQueryAction;
import org.opensearch.analytics.exec.join.JoinStrategyMetrics;
import org.opensearch.analytics.rest.RestJoinStrategyStatsAction;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Module;
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Analytics engine hub. Implements {@link ExtensiblePlugin} to discover
 * and wire query back-end extensions via SPI.
 *
 * @opensearch.internal
 */
public class AnalyticsPlugin extends Plugin implements ExtensiblePlugin, ActionPlugin {

    private static final Logger logger = LogManager.getLogger(AnalyticsPlugin.class);

    /**
     * Creates a new analytics engine hub plugin.
     */
    public AnalyticsPlugin() {}

    private final List<AnalyticsSearchBackendPlugin> backEnds = new ArrayList<>();
    private SqlOperatorTable operatorTable;
    private AnalyticsSearchService searchService;
    private final JoinStrategyMetrics joinStrategyMetrics = new JoinStrategyMetrics();

    @SuppressWarnings("rawtypes")
    @Override
    public void loadExtensions(ExtensionLoader loader) {
        backEnds.addAll(loader.loadExtensions(AnalyticsSearchBackendPlugin.class));
    }

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
        operatorTable = aggregateOperatorTables();
        DefaultEngineContext ctx = new DefaultEngineContext(clusterService, operatorTable);
        CapabilityRegistry capabilityRegistry = new CapabilityRegistry(backEnds, FieldStorageResolver::new);

        Map<String, AnalyticsSearchBackendPlugin> backEndsByName = new LinkedHashMap<>();
        for (AnalyticsSearchBackendPlugin be : backEnds) {
            backEndsByName.put(be.name(), be);
        }
        searchService = new AnalyticsSearchService(backEndsByName, namedWriteableRegistry);

        // Returned as components so Guice can inject them into DefaultPlanExecutor
        // (a HandledTransportAction registered via getActions() — constructed by Guice
        // after createComponents) and into AnalyticsSearchTransportService.
        return List.of(searchService, ctx, capabilityRegistry, joinStrategyMetrics);
    }

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
        return List.of(new RestJoinStrategyStatsAction(joinStrategyMetrics));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<Module> createGuiceModules() {
        return List.of(b -> {
            b.bind(new TypeLiteral<QueryPlanExecutor<RelNode, Iterable<Object[]>>>() {
            }).to(DefaultPlanExecutor.class);
            b.bind(EngineContext.class).to(DefaultEngineContext.class);
            b.bind(Scheduler.class).to(QueryScheduler.class);
        });
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(AnalyticsQueryAction.INSTANCE, DefaultPlanExecutor.class));
    }

    @Override
    public List<org.opensearch.common.settings.Setting<?>> getSettings() {
        return AnalyticsSettings.ALL_SETTINGS;
    }

    @Override
    public void close() {
        if (searchService != null) {
            searchService.close();
        }
    }

    private SqlOperatorTable aggregateOperatorTables() {
        // TODO: re-wire once operatorTable() is added back to AnalyticsSearchBackendPlugin
        return SqlOperatorTables.of();
    }

    /**
     * Default implementation of {@link EngineContext}.
     */
    record DefaultEngineContext(ClusterService clusterService, SqlOperatorTable operatorTable) implements EngineContext {

        @Override
        public SchemaPlus getSchema() {
            return OpenSearchSchemaBuilder.buildSchema(clusterService.state());
        }
    }
}
