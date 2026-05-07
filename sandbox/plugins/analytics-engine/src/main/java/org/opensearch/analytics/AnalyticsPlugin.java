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
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.exec.QueryScheduler;
import org.opensearch.analytics.exec.Scheduler;
import org.opensearch.analytics.exec.action.AnalyticsQueryAction;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.arrow.memory.ArrowAllocatorService;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
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
import java.util.concurrent.atomic.AtomicReference;
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
    private final ArrowAllocatorServiceHolder allocatorHolder = new ArrowAllocatorServiceHolder();

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
        searchService = new AnalyticsSearchService(backEndsByName, allocatorHolder, namedWriteableRegistry);

        // Returned as components so Guice can inject them into DefaultPlanExecutor
        // (a HandledTransportAction registered via getActions() — constructed by Guice
        // after createComponents) and into AnalyticsSearchTransportService.
        return List.of(searchService, ctx, capabilityRegistry, allocatorHolder);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<Module> createGuiceModules() {
        return List.of(b -> {
            b.bind(new TypeLiteral<QueryPlanExecutor<RelNode, Iterable<Object[]>>>() {
            }).to(DefaultPlanExecutor.class);
            b.bind(EngineContext.class).to(DefaultEngineContext.class);
            // Singleton bind on the concrete class so node-injector lookups for
            // QueryScheduler.class don't fall back to a JIT binding (which would
            // re-instantiate AnalyticsSearchTransportService, whose ctor registers
            // transport handlers and is only legal to call once per node).
            b.bind(QueryScheduler.class).asEagerSingleton();
            b.bind(Scheduler.class).to(QueryScheduler.class);
            b.bind(ArrowAllocatorServiceBridge.class).asEagerSingleton();
        });
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(AnalyticsQueryAction.INSTANCE, DefaultPlanExecutor.class));
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

    /**
     * Per-plugin-instance holder populated by {@link ArrowAllocatorServiceBridge}. Services
     * inside this plugin (e.g., {@link AnalyticsSearchService}, {@link QueryContext}) read
     * through it instead of capturing the concrete {@link ArrowAllocatorService} at
     * {@code createComponents} time (which is too early — the injector is not yet built).
     */
    public static final class ArrowAllocatorServiceHolder implements Supplier<ArrowAllocatorService> {

        /** Creates an empty holder. Populated later by {@link ArrowAllocatorServiceBridge}. */
        public ArrowAllocatorServiceHolder() {}

        private final AtomicReference<ArrowAllocatorService> ref = new AtomicReference<>();

        void set(ArrowAllocatorService service) {
            ref.set(service);
        }

        @Override
        public ArrowAllocatorService get() {
            return ref.get();
        }
    }

    /** Guice-built bridge that stores the injected {@link ArrowAllocatorService} into the plugin's holder. */
    public static final class ArrowAllocatorServiceBridge {
        @Inject
        public ArrowAllocatorServiceBridge(ArrowAllocatorService service, ArrowAllocatorServiceHolder holder) {
            holder.set(service);
        }
    }
}
