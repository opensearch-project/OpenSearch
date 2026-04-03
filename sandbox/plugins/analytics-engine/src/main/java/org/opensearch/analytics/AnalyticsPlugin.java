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
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Module;
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
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
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Analytics engine hub. Implements {@link ExtensiblePlugin} to discover
 * and wire query back-end extensions via SPI.
 *
 * @opensearch.internal
 */
public class AnalyticsPlugin extends Plugin implements ExtensiblePlugin {

    private static final Logger logger = LogManager.getLogger(AnalyticsPlugin.class);

    /**
     * Creates a new analytics engine hub plugin.
     */
    public AnalyticsPlugin() {}

    private final List<AnalyticsSearchBackendPlugin> backEnds = new ArrayList<>();
    private final List<org.opensearch.plugins.SearchBackEndPlugin<?>> storageBackends = new ArrayList<>();
    private SqlOperatorTable operatorTable;

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void loadExtensions(ExtensionLoader loader) {
        backEnds.addAll(loader.loadExtensions(AnalyticsSearchBackendPlugin.class));
        storageBackends.addAll((List) loader.loadExtensions(org.opensearch.plugins.SearchBackEndPlugin.class));
        operatorTable = aggregateOperatorTables();
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
        DefaultEngineContext ctx = new DefaultEngineContext(clusterService, operatorTable);

        // Build scan format index and field storage factory from storage backends
        Map<String, List<String>> scanFormats = new LinkedHashMap<>();
        for (var sb : storageBackends) {
            for (var format : sb.getSupportedFormats()) {
                scanFormats.computeIfAbsent(format.name(), k -> new ArrayList<>()).add(sb.name());
            }
        }
        Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory =
            (indexMetadata) -> new FieldStorageResolver(indexMetadata, storageBackends);

        CapabilityRegistry capabilityRegistry = new CapabilityRegistry(backEnds, fieldStorageFactory, scanFormats);
        DefaultPlanExecutor executor = new DefaultPlanExecutor(backEnds, capabilityRegistry, clusterService);
        AnalyticsEngineService.setInstance(new AnalyticsEngineService(ctx, executor));
        return List.of(executor, ctx);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<Module> createGuiceModules() {
        return List.of(b -> {
            b.bind(new TypeLiteral<QueryPlanExecutor<RelNode, Iterable<Object[]>>>() {
            }).to(DefaultPlanExecutor.class);
            b.bind(EngineContext.class).to(DefaultEngineContext.class);
        });
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
