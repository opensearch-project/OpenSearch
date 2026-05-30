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
import org.opensearch.analytics.exec.CoordinatorAllocatorHandle;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.exec.QueryScheduler;
import org.opensearch.analytics.exec.ReaderContextStore;
import org.opensearch.analytics.exec.Scheduler;
import org.opensearch.analytics.exec.action.AnalyticsQueryAction;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Module;
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginComponentRegistry;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
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

    public static final String SCHEDULER_THREAD_POOL_NAME = "analytics_scheduler";
    private static final int SCHEDULER_QUEUE_SIZE = 200;

    public static final String REDUCE_THREAD_POOL_NAME = "analytics_reduce";

    // The reduce pool exists to isolate coordinator-reduce drains from the SEARCH
    // pool, preventing deadlock when reduces and local shard fragments compete for
    // the same threads. Each reduce thread blocks on a synchronous FFM call
    // (streamNext) with negligible CPU usage — memory is the real constraint,
    // bounded by the DataFusion pool and phantom reservations. Size is generous
    // so the thread pool isn't the throughput bottleneck.
    private static final int REDUCE_POOL_SIZE = Math.max(8, Runtime.getRuntime().availableProcessors() * 4);
    private static final int REDUCE_QUEUE_SIZE = 200;

    public static final Setting<Long> COORDINATOR_BUFFER_LIMIT = Setting.longSetting(
        "analytics.coordinator.buffer_limit",
        256L * 1024 * 1024,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * When {@code true}, performance-delegated leaves (driver natively evaluable, peer also
     * viable) fuse with their correctness-delegated siblings even under {@code OR} / {@code NOT}.
     * The combiner ships the entire boolean structure as a single delegated expression instead
     * of throwing the dual-viable leaves back to native.
     *
     * <p>Default {@code false} — the carve-out exists for a reason. Under {@code OR}, the
     * {@code delegation_possible} pattern (driver evaluates natively, peer-consults
     * opportunistically) is incorrect: the driver can't tell whether a peer miss means "this
     * leaf didn't match" or "no leaf matched". Forcing fusion trades that semantic guarantee
     * for fewer round-trips. Flip to {@code true} only when measurements show the peer's
     * filter (e.g. Lucene's term dictionary) is decisively faster than the driver's column
     * scan AND the workload's OR shape benefits from the consolidation.
     */
    public static final Setting<Boolean> DELEGATION_FUSE_DUAL_VIABLE = Setting.boolSetting(
        "analytics.delegation.fuse_dual_viable",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * When {@code true}, prefer a metadata-only driving backend (e.g. Lucene's inverted index)
     * over value-producing backends (e.g. DataFusion) whenever both are viable for a stage —
     * i.e. the planner collapses {@code [lucene, datafusion]} alternatives into {@code [lucene]}
     * before shipping. Each stage ends up with exactly one {@link
     * org.opensearch.analytics.planner.dag.StagePlan}, so the data node skips per-request
     * alternative selection and the convertor runs once per stage instead of once per
     * alternative (saves coordinator CPU on conversion).
     *
     * <p>Default {@code true} — metadata-only backends are only viable end-to-end when the
     * fragment is fully metadata-soluble (count fast path today), in which case they are
     * strictly faster. Flip to {@code false} for A/B comparisons or to fall back to the
     * value-producing backend if the metadata driver hits a regression.
     */
    public static final Setting<Boolean> PREFER_METADATA_DRIVER = Setting.boolSetting(
        "analytics.planner.prefer_metadata_driver",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Creates a new analytics engine hub plugin.
     */
    public AnalyticsPlugin() {}

    private final List<AnalyticsSearchBackendPlugin> backEnds = new ArrayList<>();
    private AnalyticsSearchService searchService;
    private CoordinatorAllocatorHandle coordinatorAllocatorHandle;
    private ReaderContextStore readerContextStore;

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
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        PluginComponentRegistry pluginComponentRegistry
    ) {
        ArrowNativeAllocator nativeAllocator = pluginComponentRegistry.getComponent(ArrowNativeAllocator.class)
            .orElseThrow(() -> new IllegalStateException("ArrowNativeAllocator not available; arrow-base plugin must be installed"));

        CapabilityRegistry capabilityRegistry = new CapabilityRegistry(backEnds, FieldStorageResolver::new);

        Map<String, AnalyticsSearchBackendPlugin> backEndsByName = new LinkedHashMap<>();
        for (AnalyticsSearchBackendPlugin be : backEnds) {
            backEndsByName.put(be.name(), be);
        }
        readerContextStore = new ReaderContextStore(threadPool);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(ReaderContextStore.READER_CONTEXT_KEEP_ALIVE, readerContextStore::setKeepAlive);
        searchService = new AnalyticsSearchService(backEndsByName, nativeAllocator, namedWriteableRegistry, readerContextStore);
        DefaultEngineContextProvider ctx = new DefaultEngineContextProvider(clusterService, indexNameExpressionResolver, backEndsByName);
        // Build the coordinator allocator under POOL_QUERY here, in the plugin, so that the
        // plugin's lifecycle owns its lifetime. The Guice-bound DefaultPlanExecutor consumes
        // it via the handle without taking on close responsibility — mirroring how
        // AnalyticsSearchService's allocator is owned and closed by this plugin.
        coordinatorAllocatorHandle = new CoordinatorAllocatorHandle(
            nativeAllocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_QUERY).newChildAllocator("coordinator", 0, Long.MAX_VALUE)
        );

        return List.of(searchService, ctx, capabilityRegistry, coordinatorAllocatorHandle);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<Module> createGuiceModules() {
        return List.of(b -> {
            b.bind(new TypeLiteral<QueryPlanExecutor<RelNode, Iterable<Object[]>>>() {
            }).to(DefaultPlanExecutor.class);
            b.bind(EngineContextProvider.class).to(DefaultEngineContextProvider.class);
            // Singleton bind on the concrete class so node-injector lookups for
            // QueryScheduler.class don't fall back to a JIT binding (which would
            // re-instantiate AnalyticsSearchTransportService, whose ctor registers
            // transport handlers and is only legal to call once per node).
            b.bind(QueryScheduler.class).asEagerSingleton();
            b.bind(Scheduler.class).to(QueryScheduler.class);
        });
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(AnalyticsQueryAction.INSTANCE, DefaultPlanExecutor.class));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            COORDINATOR_BUFFER_LIMIT,
            DELEGATION_FUSE_DUAL_VIABLE,
            PREFER_METADATA_DRIVER,
            ReaderContextStore.READER_CONTEXT_KEEP_ALIVE
        );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        int poolSize = schedulerPoolSize();
        return List.of(
            new FixedExecutorBuilder(settings, SCHEDULER_THREAD_POOL_NAME, poolSize, SCHEDULER_QUEUE_SIZE, "analytics"),
            new FixedExecutorBuilder(settings, REDUCE_THREAD_POOL_NAME, REDUCE_POOL_SIZE, REDUCE_QUEUE_SIZE, "analytics_reduce")
        );
    }

    static int schedulerPoolSize() {
        return Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
    }

    @Override
    public void close() {
        if (searchService != null) {
            searchService.close();
        }
        if (coordinatorAllocatorHandle != null) {
            coordinatorAllocatorHandle.close();
        }
    }

    private SqlOperatorTable aggregateOperatorTables() {
        // TODO: re-wire once operatorTable() is added back to AnalyticsSearchBackendPlugin
        return SqlOperatorTables.of();
    }

    /**
     * Default implementation of {@link EngineContextProvider}. The {@link IndexNameExpressionResolver}
     * is the cluster's resolver — built by the OpenSearch server with security-plugin extensions,
     * system-index access checks, and ThreadContext threading. Building schemas with a fresh
     * resolver would silently bypass those checks.
     */
    record DefaultEngineContextProvider(ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver, Map<
        String,
        AnalyticsSearchBackendPlugin> backends) implements EngineContextProvider {

        @Override
        public QueryRequestContext getContext(ClusterState clusterState) {
            SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState, indexNameExpressionResolver);
            return new QueryRequestContext(clusterState, schema);
        }

        @Override
        public QueryRequestContext getContext() {
            return getContext(clusterService.state());
        }

        @Override
        public Exception convertException(Exception e) {
            for (AnalyticsSearchBackendPlugin backend : backends.values()) {
                Exception converted = backend.convertException(e);
                if (converted != e) {
                    return converted;
                }
            }
            return e;
        }
    }

}
