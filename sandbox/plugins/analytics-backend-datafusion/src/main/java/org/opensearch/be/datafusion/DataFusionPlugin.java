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
import org.opensearch.be.datafusion.action.DataFusionStatsAction;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.RatioValue;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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

    /** Default value for {@link #DATAFUSION_MEMORY_POOL_LIMIT}. */
    public static final String DEFAULT_MEMORY_POOL_LIMIT = "25%";

    /** Default floor for {@link #DATAFUSION_MEMORY_POOL_LIMIT_MIN}. */
    public static final ByteSizeValue DEFAULT_MEMORY_POOL_LIMIT_MIN = new ByteSizeValue(512, ByteSizeUnit.MB);

    /** Default value for {@link #DATAFUSION_SPILL_MEMORY_LIMIT}. */
    public static final String DEFAULT_SPILL_MEMORY_LIMIT = "50%";

    /** Default floor for {@link #DATAFUSION_SPILL_MEMORY_LIMIT_MIN}. */
    public static final ByteSizeValue DEFAULT_SPILL_MEMORY_LIMIT_MIN = new ByteSizeValue(1, ByteSizeUnit.GB);

    /**
     * Memory pool limit for the DataFusion runtime. Accepts a percentage of non-heap memory
     * ({@code totalPhysicalMemory - configuredMaxHeap}, e.g. {@code "25%"}) or an absolute byte
     * size (e.g. {@code "10gb"}). When a percentage is supplied, the resolved value is floored
     * by {@link #DATAFUSION_MEMORY_POOL_LIMIT_MIN}.
     * <p>
     * Dynamic: changes take effect for new allocations only. Existing reservations that exceed
     * the new limit are not reclaimed — they drain naturally as queries complete.
     */
    public static final Setting<String> DATAFUSION_MEMORY_POOL_LIMIT = Setting.simpleString(
        "datafusion.memory_pool_limit_bytes",
        DEFAULT_MEMORY_POOL_LIMIT,
        DataFusionPlugin::validateMemorySizeOrPercentage,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Floor applied when {@link #DATAFUSION_MEMORY_POOL_LIMIT} is a percentage. */
    public static final Setting<ByteSizeValue> DATAFUSION_MEMORY_POOL_LIMIT_MIN = Setting.byteSizeSetting(
        "datafusion.memory_pool_limit.min",
        DEFAULT_MEMORY_POOL_LIMIT_MIN,
        new ByteSizeValue(0, ByteSizeUnit.BYTES),
        new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Spill memory limit — when exceeded, DataFusion spills to disk. Accepts a percentage of
     * non-heap memory or an absolute byte size. When a percentage is supplied, the resolved value
     * is floored by {@link #DATAFUSION_SPILL_MEMORY_LIMIT_MIN}.
     * <p>
     * Dynamic in the cluster-settings sense: PUTs are accepted and persisted, but the spill limit
     * is fixed in the native runtime at construction time (no setter). The new value takes effect
     * after the next node restart.
     */
    public static final Setting<String> DATAFUSION_SPILL_MEMORY_LIMIT = Setting.simpleString(
        "datafusion.spill_memory_limit_bytes",
        DEFAULT_SPILL_MEMORY_LIMIT,
        DataFusionPlugin::validateMemorySizeOrPercentage,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Floor applied when {@link #DATAFUSION_SPILL_MEMORY_LIMIT} is a percentage. */
    public static final Setting<ByteSizeValue> DATAFUSION_SPILL_MEMORY_LIMIT_MIN = Setting.byteSizeSetting(
        "datafusion.spill_memory_limit.min",
        DEFAULT_SPILL_MEMORY_LIMIT_MIN,
        new ByteSizeValue(0, ByteSizeUnit.BYTES),
        new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
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

    private volatile DataFusionService dataFusionService;
    private volatile DataFormatRegistry dataFormatRegistry;
    private volatile SimpleExtension.ExtensionCollection substraitExtensions;
    private volatile ClusterService clusterService;
    private volatile DatafusionSettings datafusionSettings;
    /** Most recent raw value of {@link #DATAFUSION_MEMORY_POOL_LIMIT}; updated by its listener. */
    private volatile String currentMemoryPoolLimitRaw = DEFAULT_MEMORY_POOL_LIMIT;

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
        this.currentMemoryPoolLimitRaw = DATAFUSION_MEMORY_POOL_LIMIT.get(settings);
        long memoryPoolLimit = resolveMemoryPoolBytes(settings);
        long spillMemoryLimit = resolveSpillMemoryBytes(settings);
        String spillDir = environment.dataFiles()[0].getParent().resolve("tmp").toAbsolutePath().toString();

        dataFusionService = DataFusionService.builder()
            .memoryPoolLimit(memoryPoolLimit)
            .spillMemoryLimit(spillMemoryLimit)
            .spillDirectory(spillDir)
            .clusterSettings(clusterService.getClusterSettings())
            .build();
        dataFusionService.start();
        logger.debug("DataFusion plugin initialized — memory pool {}B, spill limit {}B", memoryPoolLimit, spillMemoryLimit);

        // Wire the memory pool limit setting (and its dynamic min companion) to the native runtime
        // so cluster-settings PUTs take effect without restarting the node. Both listeners route
        // through the same resolver, which re-reads the latest raw value plus min on every update.
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DATAFUSION_MEMORY_POOL_LIMIT, this::onMemoryPoolLimitChanged);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DATAFUSION_MEMORY_POOL_LIMIT_MIN, this::onMemoryPoolMinChanged);

        this.datafusionSettings = new DatafusionSettings(clusterService);

        this.substraitExtensions = loadSubstraitExtensions();

        return Collections.singletonList(dataFusionService);
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
     * Resolves {@link #DATAFUSION_MEMORY_POOL_LIMIT}. Percentages apply against
     * {@code totalPhysicalMemory - configuredMaxHeap} and are floored by the {@code min}
     * companion setting; absolute byte sizes are used as-is.
     */
    static long resolveMemoryPoolBytes(Settings settings) {
        return resolveBytes(
            DATAFUSION_MEMORY_POOL_LIMIT.get(settings),
            DATAFUSION_MEMORY_POOL_LIMIT_MIN.get(settings),
            DATAFUSION_MEMORY_POOL_LIMIT.getKey()
        );
    }

    /**
     * Resolves {@link #DATAFUSION_SPILL_MEMORY_LIMIT}. Percentages apply against
     * {@code totalPhysicalMemory - configuredMaxHeap} and are floored by the {@code min}
     * companion setting; absolute byte sizes are used as-is.
     */
    static long resolveSpillMemoryBytes(Settings settings) {
        return resolveBytes(
            DATAFUSION_SPILL_MEMORY_LIMIT.get(settings),
            DATAFUSION_SPILL_MEMORY_LIMIT_MIN.get(settings),
            DATAFUSION_SPILL_MEMORY_LIMIT.getKey()
        );
    }

    /**
     * Shared parser for memory-pool-style settings. Mirrors {@code IndexingMemoryController}'s
     * native indexing buffer logic: percentages resolve against
     * {@code totalPhysicalMemory - configuredMaxHeap} and are floored by the supplied minimum;
     * absolute byte sizes are returned as-is.
     */
    private static long resolveBytes(String configured, ByteSizeValue floor, String settingKey) {
        if (configured.endsWith("%")) {
            long totalAvailableMemory = OsProbe.getInstance().getTotalPhysicalMemorySize() - JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
            if (totalAvailableMemory <= 0) {
                logger.warn("Non-heap memory not measurable while resolving [{}]; falling back to {}", settingKey, floor);
                return floor.getBytes();
            }
            RatioValue ratio = RatioValue.parseRatioValue(configured);
            long bytes = (long) (totalAvailableMemory * ratio.getAsRatio());
            if (bytes < floor.getBytes()) {
                bytes = floor.getBytes();
            }
            return bytes;
        }
        return ByteSizeValue.parseBytesSizeValue(configured, settingKey).getBytes();
    }

    /**
     * Cluster-settings listener for {@link #DATAFUSION_MEMORY_POOL_LIMIT}. Records the new raw
     * value, re-resolves against the current min, and propagates the result to the native runtime.
     */
    private void onMemoryPoolLimitChanged(String newValue) {
        this.currentMemoryPoolLimitRaw = newValue;
        applyMemoryPoolLimit(newValue);
    }

    /**
     * Cluster-settings listener for {@link #DATAFUSION_MEMORY_POOL_LIMIT_MIN}. Re-resolves the
     * current raw limit against the new floor and propagates the result. Only meaningful when the
     * limit is configured as a percentage — absolute byte sizes ignore the floor.
     */
    private void onMemoryPoolMinChanged(ByteSizeValue newMin) {
        applyMemoryPoolLimit(currentMemoryPoolLimitRaw);
    }

    private void applyMemoryPoolLimit(String rawValue) {
        ClusterService cs = clusterService;
        if (cs == null) {
            logger.debug("ClusterService not yet initialized; ignoring memory pool limit update");
            return;
        }
        ByteSizeValue floor = DATAFUSION_MEMORY_POOL_LIMIT_MIN.get(cs.getSettings());
        long newLimitBytes = resolveBytes(rawValue, floor, DATAFUSION_MEMORY_POOL_LIMIT.getKey());
        updateMemoryPoolLimit(newLimitBytes);
    }

    /**
     * Validates that {@code value} is either a percentage ({@code "25%"}) or an absolute byte
     * size accepted by {@link ByteSizeValue#parseBytesSizeValue(String, String)}. Used as the
     * setting-time validator for {@link #DATAFUSION_MEMORY_POOL_LIMIT} and
     * {@link #DATAFUSION_SPILL_MEMORY_LIMIT} so that malformed values fail at update time rather
     * than at the next read inside {@link #resolveBytes}.
     */
    private static void validateMemorySizeOrPercentage(String value) {
        try {
            if (value.endsWith("%")) {
                RatioValue.parseRatioValue(value);
            } else {
                ByteSizeValue.parseBytesSizeValue(value, "memory size");
            }
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                "value [" + value + "] must be a percentage (e.g. \"25%\") or a byte size (e.g. \"512mb\"): " + e.getMessage(),
                e
            );
        }
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
}
