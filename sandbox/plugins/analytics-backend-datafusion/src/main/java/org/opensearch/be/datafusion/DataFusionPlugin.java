/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.QueryExecutionMetrics;
import org.opensearch.be.datafusion.action.DataFusionStatsAction;
import org.opensearch.be.datafusion.cache.CacheSettings;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.search.backpressure.trackers.NativeMemoryUsageTracker;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;

/**
 * Main plugin class for the DataFusion native engine integration.
 * <p>
 * Owns the {@link DataFusionService} lifecycle (memory pool, native runtime).
 * Analytics query capabilities are declared in {@link DataFusionAnalyticsBackendPlugin},
 * which is SPI-discovered and receives this plugin instance via its constructor.
 */
public class DataFusionPlugin extends Plugin implements SearchBackEndPlugin<DatafusionReader>, AnalyticsSearchBackendPlugin, ActionPlugin {

    private static final Logger logger = LogManager.getLogger(DataFusionPlugin.class);

    /**
     * Memory pool limit for the DataFusion runtime.
     * <p>
     * Dynamic: changes take effect for new allocations only. Existing reservations
     * that exceed the new limit are not reclaimed — they drain naturally as queries complete.
     */
    public static final Setting<Long> DATAFUSION_MEMORY_POOL_LIMIT = Setting.longSetting(
        "datafusion.memory_pool_limit_bytes",
        Runtime.getRuntime().maxMemory() / 4,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Spill memory limit — when exceeded, DataFusion spills to disk. */
    public static final Setting<Long> DATAFUSION_SPILL_MEMORY_LIMIT = Setting.longSetting(
        "datafusion.spill_memory_limit_bytes",
        Runtime.getRuntime().maxMemory() / 8,
        0L,
        Setting.Property.NodeScope
    );

    /**
     * Selects how the coordinator-reduce sink hands shard responses to the native runtime.
     * <ul>
     *   <li>{@code streaming} (default) — use {@link DatafusionReduceSink}: each batch is pushed
     *       through a tokio mpsc, the native plan polls inputs as it executes.</li>
     *   <li>{@code memtable} — use {@link DatafusionMemtableReduceSink}: all batches are buffered
     *       in Java and handed across in one call as a {@code MemTable}. Trades memory for a
     *       simpler input lifecycle with no cross-runtime spawn or oneshot machinery.</li>
     * </ul>
     */
    public static final Setting<String> DATAFUSION_REDUCE_INPUT_MODE = Setting.simpleString(
        "datafusion.reduce.input_mode",
        "streaming",
        v -> {
            if (!"streaming".equals(v) && !"memtable".equals(v)) {
                throw new IllegalArgumentException("datafusion.reduce.input_mode must be 'streaming' or 'memtable', got: " + v);
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final String SUPPORTED_FORMAT = "parquet";

    /**
     * Cap on entries returned by {@link #getTopQueriesByMemory()}. Equal to the per-tick
     * cancellation budget on {@code SearchBackpressureService} ({@code cancellation_burst}
     * default), so the heaviest queries are always represented and SBP never sees fewer
     * candidates than it can act on in a tick.
     */
    private static final int ACTIVE_QUERY_METRICS_TOP_N = 10;

    private volatile DataFusionService dataFusionService;
    private volatile DataFormatRegistry dataFormatRegistry;
    private volatile SimpleExtension.ExtensionCollection substraitExtensions;
    private volatile ClusterService clusterService;
    private volatile DatafusionSettings datafusionSettings;

    /**
     * Creates the DataFusion plugin.
     */
    public DataFusionPlugin() {}

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
        DataFormatRegistry dataFormatRegistry
    ) {
        this.dataFormatRegistry = dataFormatRegistry;
        this.clusterService = clusterService;
        Settings settings = environment.settings();
        long memoryPoolLimit = DATAFUSION_MEMORY_POOL_LIMIT.get(settings);
        long spillMemoryLimit = DATAFUSION_SPILL_MEMORY_LIMIT.get(settings);
        String spillDir = environment.dataFiles()[0].getParent().resolve("tmp").toAbsolutePath().toString();

        dataFusionService = DataFusionService.builder()
            .memoryPoolLimit(memoryPoolLimit)
            .spillMemoryLimit(spillMemoryLimit)
            .spillDirectory(spillDir)
            .clusterSettings(clusterService.getClusterSettings())
            .build();
        dataFusionService.start();
        logger.debug("DataFusion plugin initialized — memory pool {}B, spill limit {}B", memoryPoolLimit, spillMemoryLimit);

        // Wire the dynamic memory pool limit setting to the native runtime so updates via the
        // cluster settings API take effect without restarting the node.
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DATAFUSION_MEMORY_POOL_LIMIT, this::updateMemoryPoolLimit);

        this.datafusionSettings = new DatafusionSettings(clusterService);

        // Expose per-task native-memory usage to search backpressure. The tracker calls
        // this supplier once per refresh (invoked by the backpressure service at the top of
        // doRun() and nodeStats()), snapshotting all live queries in one FFM call. Per-task
        // evaluation then reads from the tracker's cached map — no FFM call per task.
        //
        // The OpenSearch task id is used as the DataFusion context_id at query launch
        // (see ShardScanInstructionHandler / DatafusionSearchExecEngine), so the map is
        // already keyed by Task#getId on the consumer side.
        logger.info("installing native-memory snapshot supplier for search backpressure");
        NativeMemoryUsageTracker.setSnapshotSupplier(this::currentBytesByTaskId);
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(
            () -> DATAFUSION_MEMORY_POOL_LIMIT.get(clusterService.getSettings())
        );

        this.substraitExtensions = loadSubstraitExtensions();

        return Collections.singletonList(dataFusionService);
    }

    /**
     * Project the active-query metrics map down to {@code taskId -> currentBytes} for the
     * backpressure native-memory tracker. One FFM snapshot per call, capped at
     * {@link #ACTIVE_QUERY_METRICS_TOP_N} entries (the heaviest live queries).
     * Returns an empty map when the service isn't running, so startup/shutdown races
     * don't surface bad data.
     */
    private Map<Long, Long> currentBytesByTaskId() {
        if (dataFusionService == null) {
            return Collections.emptyMap();
        }
        Map<Long, QueryExecutionMetrics> metrics = getTopQueriesByMemory();
        if (metrics.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<Long, Long> out = new HashMap<>(metrics.size());
        for (Map.Entry<Long, QueryExecutionMetrics> e : metrics.entrySet()) {
            out.put(e.getKey(), e.getValue().currentBytes());
        }
        if (logger.isDebugEnabled()) {
            logger.debug("native memory snapshot: {} active queries", out.size());
        }
        return out;
    }

    /**
     * Loads the Substrait default extension catalog with the plugin's classloader as the
     * thread context classloader. Jackson polymorphic deserialization (used by Substrait
     * to load its {@code SimpleExtension} subclasses) consults the TCCL; in an OpenSearch
     * plugin context the TCCL is typically the server classloader, which cannot see the
     * plugin-local Substrait classes.
     */
    private static SimpleExtension.ExtensionCollection loadSubstraitExtensions() {
        Thread t = Thread.currentThread();
        ClassLoader previous = t.getContextClassLoader();
        try {
            t.setContextClassLoader(DataFusionPlugin.class.getClassLoader());
            SimpleExtension.ExtensionCollection delegationExtensions = SimpleExtension.load(List.of("/delegation_functions.yaml"));
            SimpleExtension.ExtensionCollection scalarExtensions = SimpleExtension.load(List.of("/opensearch_scalar_functions.yaml"));
            SimpleExtension.ExtensionCollection arrayExtensions = SimpleExtension.load(List.of("/opensearch_array_functions.yaml"));
            SimpleExtension.ExtensionCollection aggregateExtensions = SimpleExtension.load(List.of("/opensearch_aggregate_functions.yaml"));
            return DefaultExtensionCatalog.DEFAULT_COLLECTION.merge(delegationExtensions)
                .merge(scalarExtensions)
                .merge(arrayExtensions)
                .merge(aggregateExtensions);
        } finally {
            t.setContextClassLoader(previous);
        }
    }

    SimpleExtension.ExtensionCollection getSubstraitExtensions() {
        return substraitExtensions;
    }

    DataFormatRegistry getDataFormatRegistry() {
        return dataFormatRegistry;
    }

    DataFusionService getDataFusionService() {
        return dataFusionService;
    }

    ClusterService getClusterService() {
        return clusterService;
    }

    DatafusionSettings getDatafusionSettings() {
        return datafusionSettings;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return DatafusionSettings.ALL_SETTINGS;
    }

    /**
     * Applies a new memory pool limit to the running DataFusion runtime.
     * <p>
     * Takes effect for new allocations only. In-flight reservations that already
     * exceed the new limit are not reclaimed and drain as queries complete.
     * <p>
     * Safe to call during plugin startup before {@link #createComponents} returns
     * (service is null, ignored) and during shutdown after the native runtime has
     * been released (service throws {@link IllegalStateException}, caught and logged).
     * <p>
     * Package-private for testing.
     */
    void updateMemoryPoolLimit(long newLimitBytes) {
        DataFusionService service = dataFusionService;
        if (service == null) {
            logger.debug("DataFusion service not yet initialized; ignoring memory pool limit update to {}B", newLimitBytes);
            return;
        }
        try {
            service.setMemoryPoolLimit(newLimitBytes);
            logger.info("Updated DataFusion memory pool limit to {}B", newLimitBytes);
        } catch (IllegalStateException e) {
            // Service has been stopped/closed (e.g., during node shutdown). The listener is
            // still registered on ClusterSettings because there is no removeSettingsUpdateConsumer
            // API; swallow the race so cluster-state application does not log a spurious failure.
            logger.warn("Ignoring memory pool limit update to {}B; service is not running", newLimitBytes);
        }
    }

    @Override
    public String name() {
        return "datafusion";
    }

    @Override
    public EngineReaderManager<DatafusionReader> createReaderManager(ReaderManagerConfig settings) throws IOException {
        NativeStoreHandle dataformatAwareStoreHandle = settings.dataformatAwareStoreHandles().get(settings.format());
        return new DatafusionReaderManager(settings.format(), settings.shardPath(), dataFusionService, dataformatAwareStoreHandle);
    }

    @Override
    public List<String> getSupportedFormats() {
        return List.of(SUPPORTED_FORMAT);
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
        if (dataFusionService == null) {
            return Collections.emptyList();
        }
        return List.of(new DataFusionStatsAction(dataFusionService));
    }

    @Override
    public void close() throws IOException {
        if (dataFusionService != null) {
            dataFusionService.close();
        }
    }

    /**
     * Snapshot the native DataFusion per-query registry and return a {@code contextId -> metrics}
     * map. Returns an empty map when the service is not yet running (startup) or has been stopped
     * (shutdown), so callers never see a half-initialized view.
     *
     * <p>Each entry mirrors one {@code QueryTracker} on the Rust side — current and peak memory
     * reservation, wall time, and whether the query has completed but not yet been drained.
     * The map contains at most {@link #ACTIVE_QUERY_METRICS_TOP_N} entries — the heaviest live
     * queries by {@code current_bytes}, selected on the Rust side. Iteration order matches the
     * order Rust drained the bounded min-heap (unspecified but stable per snapshot).
     */
    @Override
    public Map<Long, QueryExecutionMetrics> getTopQueriesByMemory() {
        if (dataFusionService == null) {
            return Collections.emptyMap();
        }
        Map<Long, QueryExecutionMetrics> result = NativeBridge.getTopNQueriesByMemory(ACTIVE_QUERY_METRICS_TOP_N);
        if (logger.isDebugEnabled()) {
            logger.debug("getTopQueriesByMemory: {} entries from native registry", result.size());
        }
        return result;
    }
}
