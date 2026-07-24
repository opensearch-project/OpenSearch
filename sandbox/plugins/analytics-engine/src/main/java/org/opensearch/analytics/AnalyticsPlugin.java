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
import org.opensearch.analytics.exec.AnalyticsFragmentSlowLog;
import org.opensearch.analytics.exec.AnalyticsSearchService;
import org.opensearch.analytics.exec.AnalyticsSearchSlowLog;
import org.opensearch.analytics.exec.CoordinatorAllocatorHandle;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.exec.QueryScheduler;
import org.opensearch.analytics.exec.ReaderContextStore;
import org.opensearch.analytics.exec.Scheduler;
import org.opensearch.analytics.exec.action.AnalyticsClearShuffleAction;
import org.opensearch.analytics.exec.action.AnalyticsQueryAction;
import org.opensearch.analytics.exec.action.AnalyticsShuffleDataAction;
import org.opensearch.analytics.exec.action.TransportAnalyticsClearShuffleAction;
import org.opensearch.analytics.exec.action.TransportAnalyticsShuffleDataAction;
import org.opensearch.analytics.exec.join.MppStrategyMetrics;
import org.opensearch.analytics.exec.shuffle.ShuffleBufferManager;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.rest.RestMppStrategyStatsAction;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.analytics.settings.AnalyticsApproximationSettings;
import org.opensearch.analytics.settings.AnalyticsQuerySettings;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.stats.AnalyticsStats;
import org.opensearch.analytics.stats.AnalyticsStatsCollector;
import org.opensearch.analytics.stats.RestAnalyticsStatsAction;
import org.opensearch.analytics.stats.transport.AnalyticsStatsAction;
import org.opensearch.analytics.stats.transport.TransportAnalyticsStatsAction;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Module;
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.search.stats.SearchStats;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginComponentRegistry;
import org.opensearch.plugins.SearchStatsContributor;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.nio.file.Path;
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
public class AnalyticsPlugin extends Plugin implements ExtensiblePlugin, ActionPlugin, SearchStatsContributor {

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

    // Per-query coordinator allocator cap in bytes. 0 (default) → no per-query child allocator;
    // queries share the coordinator allocator with no per-query cap.
    public static final Setting<Long> COORDINATOR_BUFFER_LIMIT = Setting.longSetting(
        "analytics.coordinator.buffer_limit",
        0L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Controls the metadata-only driver vs. value-producing peer choice when both are viable
     * for a stage:
     *
     * <ul>
     *   <li>{@code true} (default) — collapse to the metadata-only alternative (e.g. Lucene)
     *       whenever it can run the stage end-to-end (today: count fast path). Stage ships
     *       exactly one {@link org.opensearch.analytics.planner.dag.StagePlan}; convertor
     *       runs once per stage; data node skips per-request alternative selection.</li>
     *   <li>{@code false} — force the value-producing backend (DataFusion). All metadata-only
     *       alternatives are dropped from every stage. A/B comparison knob and regression
     *       escape hatch.</li>
     * </ul>
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
    private final MppStrategyMetrics mppStrategyMetrics = new MppStrategyMetrics();
    private final ShuffleBufferManager shuffleBufferManager = new ShuffleBufferManager();
    // Resolved once at startup: <path.data>/shuffle_spill, used when the spill.directory setting is
    // left at its empty default. Null only when the node has no data path (never in practice).
    private Path shuffleSpillDefaultRoot;
    private AnalyticsSearchSlowLog analyticsSearchSlowLog;
    private AnalyticsFragmentSlowLog analyticsFragmentSlowLog;
    private CoordinatorAllocatorHandle coordinatorAllocatorHandle;
    private ReaderContextStore readerContextStore;
    private final AnalyticsStatsCollector statsCollector = new AnalyticsStatsCollector();

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
        analyticsSearchSlowLog = new AnalyticsSearchSlowLog(clusterService);
        analyticsFragmentSlowLog = new AnalyticsFragmentSlowLog();
        searchService = new AnalyticsSearchService(
            backEndsByName,
            List.of(analyticsFragmentSlowLog),
            nativeAllocator,
            namedWriteableRegistry,
            readerContextStore
        );
        searchService.setShuffleBufferRegistry(shuffleBufferManager);
        // Wire the node-level shuffle budget from settings (initial value + dynamic updates). The
        // budget is a percent of max heap; node budget == per-query max (a lone query may use the
        // whole budget, but no single query may exceed it — that fails fast, non-retryably). Without
        // this the manager's budget stays Long.MAX_VALUE and a large shuffle accumulates its whole
        // input on-heap as byte[] until the node OOMs (the inert-cap bug; observed on TPC-H q17 sf=10).
        applyShuffleBudget(clusterService.getClusterSettings().get(AnalyticsSettings.MPP_SHUFFLE_NODE_BUDGET_PERCENT));
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(AnalyticsSettings.MPP_SHUFFLE_NODE_BUDGET_PERCENT, this::applyShuffleBudget);
        // Resolve the spill root once from the environment's first data path (settings default ""
        // means "use <path.data>/shuffle_spill"); a non-empty setting overrides it. Then wire the
        // spill config from settings (initial value + dynamic updates). Default OFF — when disabled
        // a per-query budget breach stays the fail-fast throw (behavior unchanged).
        this.shuffleSpillDefaultRoot = environment.dataFiles().length > 0 ? environment.dataFiles()[0].resolve("shuffle_spill") : null;
        applyShuffleSpillConfig(clusterService.getClusterSettings());
        ClusterSettings cs = clusterService.getClusterSettings();
        cs.addSettingsUpdateConsumer(AnalyticsSettings.MPP_SHUFFLE_SPILL_ENABLED, enabled -> applyShuffleSpillConfig(cs));
        cs.addSettingsUpdateConsumer(AnalyticsSettings.MPP_SHUFFLE_SPILL_DIRECTORY, dir -> applyShuffleSpillConfig(cs));
        cs.addSettingsUpdateConsumer(AnalyticsSettings.MPP_SHUFFLE_SPILL_MAX_BYTES, max -> applyShuffleSpillConfig(cs));
        searchService.setShuffleSenderDeps(client, threadPool, clusterService);
        DefaultEngineContextProvider ctx = new DefaultEngineContextProvider(clusterService, indexNameExpressionResolver, backEndsByName);
        // Build the coordinator allocator under POOL_QUERY here, in the plugin, so that the
        // plugin's lifecycle owns its lifetime. The Guice-bound DefaultPlanExecutor consumes
        // it via the handle without taking on close responsibility — mirroring how
        // AnalyticsSearchService's allocator is owned and closed by this plugin.
        coordinatorAllocatorHandle = new CoordinatorAllocatorHandle(
            nativeAllocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_QUERY).newChildAllocator("coordinator", 0, Long.MAX_VALUE)
        );

        // Returned as components so Guice can inject them into DefaultPlanExecutor
        // (a HandledTransportAction registered via getActions() — constructed by Guice
        // after createComponents) and into AnalyticsSearchTransportService. The shuffle
        // buffer manager is exposed both here (for Guice-injected consumers like
        // DefaultPlanExecutor) and via createGuiceModules' toInstance binding so the
        // transport handler that populates buffers from the wire side sees the same instance.
        return List.of(
            searchService,
            ctx,
            capabilityRegistry,
            mppStrategyMetrics,
            shuffleBufferManager,
            coordinatorAllocatorHandle,
            analyticsSearchSlowLog,
            statsCollector
        );
    }

    /**
     * Resolve the node-level shuffle byte budget from {@code percent}% of the JVM max heap and apply
     * it to the manager. Node budget == per-query max: a lone query may use the whole budget, but a
     * single query whose footprint exceeds it fails fast (non-retryable). {@code percent==0} disables
     * the budget (Long.MAX_VALUE — pre-fix behavior). Called at startup and on dynamic updates.
     */
    private void applyShuffleBudget(int percent) {
        long budget;
        if (percent <= 0) {
            budget = Long.MAX_VALUE;
        } else {
            long maxHeap = Runtime.getRuntime().maxMemory();
            budget = maxHeap == Long.MAX_VALUE ? Long.MAX_VALUE : (long) (maxHeap * (percent / 100.0));
        }
        shuffleBufferManager.setBudgets(budget, budget);
        logger.info("[analytics] hash-shuffle node budget set to {}% of max heap = {} bytes", percent, budget);
    }

    /**
     * Resolve and apply the hash-shuffle disk-spill config from current settings. The directory is
     * the {@code analytics.mpp.shuffle.spill.directory} setting when non-empty, else
     * {@code <path.data>/shuffle_spill}. When spill is enabled but no spill root can be resolved (no
     * data path), spill is left OFF (the manager falls back to the fail-fast budget path). Called at
     * startup and on any of the three spill settings changing.
     */
    private void applyShuffleSpillConfig(ClusterSettings clusterSettings) {
        boolean enabled = clusterSettings.get(AnalyticsSettings.MPP_SHUFFLE_SPILL_ENABLED);
        String configuredDir = clusterSettings.get(AnalyticsSettings.MPP_SHUFFLE_SPILL_DIRECTORY);
        long maxBytes = clusterSettings.get(AnalyticsSettings.MPP_SHUFFLE_SPILL_MAX_BYTES);
        Path dir;
        if (configuredDir != null && configuredDir.isEmpty() == false) {
            dir = Path.of(configuredDir);
        } else {
            dir = shuffleSpillDefaultRoot;
        }
        boolean effective = enabled && dir != null;
        shuffleBufferManager.setSpillConfig(effective, dir, maxBytes);
        if (enabled && dir == null) {
            logger.warn("[analytics] hash-shuffle spill requested but no data path is available; spill stays disabled");
        } else {
            logger.info("[analytics] hash-shuffle spill enabled={}, dir={}, max_bytes={}", effective, dir, maxBytes);
        }
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
        return List.of(new RestMppStrategyStatsAction(mppStrategyMetrics), new RestAnalyticsStatsAction());
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
            // ShuffleBufferManager is auto-bound by OpenSearch as a node-level component (we
            // return the singleton instance from createComponents), so DefaultPlanExecutor,
            // TransportAnalyticsShuffleDataAction, and AnalyticsSearchService all see the same
            // node-local registry. No explicit Guice binding needed here — adding one would
            // collide with the auto-bind ("already configured at _unknown_").
        });
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(AnalyticsQueryAction.INSTANCE, DefaultPlanExecutor.class),
            new ActionHandler<>(AnalyticsShuffleDataAction.INSTANCE, TransportAnalyticsShuffleDataAction.class),
            new ActionHandler<>(AnalyticsClearShuffleAction.INSTANCE, TransportAnalyticsClearShuffleAction.class),
            new ActionHandler<>(AnalyticsStatsAction.INSTANCE, TransportAnalyticsStatsAction.class)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>(AnalyticsSettings.ALL_SETTINGS);
        settings.add(COORDINATOR_BUFFER_LIMIT);
        settings.add(PREFER_METADATA_DRIVER);
        settings.add(ReaderContextStore.READER_CONTEXT_KEEP_ALIVE);
        settings.addAll(AnalyticsApproximationSettings.all());
        settings.addAll(AnalyticsQuerySettings.all());
        return List.copyOf(settings);
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
    public SearchStats contributeSearchStats() {
        // Contribute per-shard fragment task counts so the node-level search counters
        // exposed via _nodes/stats match Lucene's per-shard accounting (one increment
        // per shard query phase, not per user query).
        //
        // The queries.elapsed_ms bucket counts 1-per-query, which undercounts the
        // rate by the shard fan-out factor and drops most queries' latency (the
        // start/end window in AnalyticsStatsCollector#recordExecution is commonly
        // 0 when stage timestamps aren't populated). The SHARD_FRAGMENT stage
        // bucket counts 1-per-(query, node-hosting-shards), still missing the
        // per-shard granularity Lucene reports. fragments.total walks each
        // SHARD_FRAGMENT execution's per-shard StageTasks, giving per-shard
        // counts matching Lucene's onPreQueryPhase semantics.
        AnalyticsStats snapshot = statsCollector.snapshot();
        AnalyticsStats.Fragments fragments = snapshot.fragments();
        if (fragments == null || fragments.total() == 0) {
            return null;
        }
        SearchStats.Stats stats = new SearchStats.Stats.Builder().queryCount(fragments.total())
            .queryTimeInMillis(fragments.elapsedMs().sumMs())
            .build();
        return new SearchStats(stats, 0, null);
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
