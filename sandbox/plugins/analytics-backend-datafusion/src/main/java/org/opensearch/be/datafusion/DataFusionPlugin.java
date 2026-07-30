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
import org.opensearch.action.ActionRequest;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.QueryExecutionMetrics;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocator;
import org.opensearch.arrow.spi.PoolGroup;
import org.opensearch.be.datafusion.action.stats.DataFusionStatsActionType;
import org.opensearch.be.datafusion.action.stats.RestDataFusionStatsAction;
import org.opensearch.be.datafusion.action.stats.TransportDataFusionStatsAction;
import org.opensearch.be.datafusion.cache.CacheManager;
import org.opensearch.be.datafusion.cache.CacheSettings;
import org.opensearch.be.datafusion.cache.CacheUtils;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.indices.breaker.CircuitBreakerStats;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.IndexSortConfig;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.DocumentMetadataResolver;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.get.DocumentLookupResult;
import org.opensearch.indices.breaker.BreakerSettings;
import org.opensearch.nativebridge.spi.NativeMemoryFetcher;
import org.opensearch.nativebridge.spi.RustLoggerBridge;
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;
import org.opensearch.plugin.stats.AnalyticsBackendNativeMemoryStats;
import org.opensearch.plugin.stats.AnalyticsBackendTaskCancellationStats;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.CircuitBreakerPlugin;
import org.opensearch.plugins.DocumentLookupProvider;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.search.backpressure.trackers.NativeMemoryUsageTracker;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
public class DataFusionPlugin extends Plugin
    implements
        SearchBackEndPlugin<DatafusionReader>,
        AnalyticsSearchBackendPlugin,
        ActionPlugin,
        CircuitBreakerPlugin,
        DocumentLookupProvider {

    private static final Logger logger = LogManager.getLogger(DataFusionPlugin.class);

    /** Fraction of the spill volume's total capacity used as the default cap. */
    static final double SPILL_LIMIT_FRACTION = 0.90;

    /** Fallback when the spill volume's capacity cannot be probed. 8 GiB. */
    static final long SPILL_LIMIT_FALLBACK_BYTES = 8L * 1024 * 1024 * 1024;

    /**
     * Validates {@link #DATAFUSION_SPILL_MEMORY_LIMIT} against {@link #DATAFUSION_SPILL_DIRECTORY}:
     * <ul>
     *   <li>If spill is disabled (empty directory), the limit is a no-op and any value is accepted —
     *       the capacity check has no volume to probe, so it is skipped.</li>
     *   <li>If spill is enabled, the value must not exceed the spill volume's total capacity
     *       (probed live via {@link FileStore#getTotalSpace()}).</li>
     * </ul>
     * Probes the filesystem on each validate call. Cluster-settings updates are infrequent and
     * the probe is a single syscall (~µs), so caching is not worth the bookkeeping cost.
     * If the probe fails ({@link IOException}) the capacity check is skipped — operators retain
     * the ability to set the cap; only the safety-net validation is disabled.
     */
    static final class SpillLimitValidator implements Setting.Validator<Long> {
        @Override
        public void validate(Long value) {
            // Range check (>= 0) lives in the parser; nothing to do here without dependencies.
        }

        @Override
        public void validate(Long value, Map<Setting<?>, Object> dependencies) {
            String dir = (String) dependencies.get(DATAFUSION_SPILL_DIRECTORY);
            if (dir == null) {
                dir = "";
            }
            if (dir.isEmpty()) {
                // Spill disabled: the limit has no effect and there is no volume to size it against,
                // so accept any value rather than rejecting startup over an inert setting.
                return;
            }
            long total;
            try {
                total = Environment.getFileStore(Path.of(dir)).getTotalSpace();
            } catch (IOException e) {
                // Probe failed — skip the capacity check. Same fail-open behavior as the
                // boot-time default derivation.
                return;
            }
            if (total > 0 && value > total) {
                throw new IllegalArgumentException(
                    "Setting [datafusion.spill_memory_limit_bytes]=" + value + " exceeds spill volume capacity (" + total + " bytes)"
                );
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            return List.<Setting<?>>of(DATAFUSION_SPILL_DIRECTORY).iterator();
        }
    }

    /**
     * Memory pool limit for the DataFusion runtime.
     *
     * <p>When unset, the default is derived from the admission-control native-memory budget
     * ({@link ResourceTrackerSettings#NODE_NATIVE_MEMORY_LIMIT_SETTING}), which is
     * the same off-heap budget admission control throttles against. The DataFusion Rust
     * runtime is the dominant native-memory consumer for analytics workloads (see PR #21732
     * partitioning model), so the default takes 71% of {@code node.native_memory.limit}
     * (reduced from 74% to fund the 3% parquet cache budget).
     * If the AC limit is unset (== 0), the default is {@link Long#MAX_VALUE} — unbounded — to
     * preserve pre-AC behaviour rather than make up a number from JVM heap (which is a
     * separate, already-allocated region with no relation to native-memory sizing).
     *
     * <p>Dynamic: changes take effect for new allocations only. Existing reservations
     * that exceed the new limit are not reclaimed — they drain naturally as queries complete.
     */
    public static final Setting<Long> DATAFUSION_MEMORY_POOL_LIMIT = new Setting<>(
        "datafusion.memory_pool_limit_bytes",
        DataFusionPlugin::deriveMemoryPoolLimitDefault,
        s -> {
            long v = Long.parseLong(s);
            if (v < 0) {
                throw new IllegalArgumentException("Setting [datafusion.memory_pool_limit_bytes] must be >= 0, got " + v);
            }
            return v;
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Computes the default for {@link #DATAFUSION_MEMORY_POOL_LIMIT} as 71% of
     * {@link ResourceTrackerSettings#NODE_NATIVE_MEMORY_LIMIT_SETTING}, falling back to
     * {@link Long#MAX_VALUE} when AC is unconfigured.
     *
     * <p>Reduced from 74% to 71%: 3% of {@code node.native_memory.limit} is now reserved for
     * the DataFusion parquet caches (footer metadata, ColumnIndex, OffsetIndex). That 3% is
     * funded by 2% from the operator pool and 1% from the unmanaged headroom (which expanded
     * from 21% to 20% of off-heap via the 79→80% change to
     * {@code ResourceTrackerSettings.deriveNativeMemoryLimitDefault}).
     *
     * <p>The fraction is taken straight from {@code node.native_memory.limit}, not from
     * {@code limit - buffer_percent}. {@code buffer_percent} is an admission-control throttle
     * margin, not a framework budget reduction; subtracting it here would collapse AC's safety
     * margin into the framework's hard cap.
     *
     * <p>Returns the bytes-as-string representation expected by the {@link Setting} parser.
     */
    static String deriveMemoryPoolLimitDefault(Settings settings) {
        ByteSizeValue nativeLimit = ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.get(settings);
        if (nativeLimit.getBytes() <= 0) {
            return Long.toString(Long.MAX_VALUE);
        }
        // 71% of node.native_memory.limit. DataFusion is the dominant native consumer for
        // analytics workloads; operators tune via the dynamic setting once they characterize
        // their workload.
        long pool = Math.max(0L, nativeLimit.getBytes() * 71 / 100);
        return Long.toString(pool);
    }

    /**
     * Spill directory used by DataFusion's {@code DiskManager} for intermediate state when
     * operators (HashAggregate, Sort, TopK) exceed {@link #DATAFUSION_MEMORY_POOL_LIMIT}.
     *
     * <p>Optional. When set, DataFusion uses {@code DiskManagerMode::Directories} to spill
     * to the configured path. When unset (empty), DataFusion runs in
     * {@code DiskManagerMode::Disabled} — spill is off and queries that exceed
     * {@link #DATAFUSION_MEMORY_POOL_LIMIT} fail with a clear "DiskManager is disabled" error
     * rather than silently spilling somewhere unexpected.
     *
     * <p>{@code Final} because DataFusion's {@code DiskManager} is built once at runtime
     * startup; changing the directory mid-flight would orphan in-progress spill files.
     *
     * <p>Declared before {@link #DATAFUSION_SPILL_MEMORY_LIMIT} because the {@code Setting}
     * constructor evaluates the default-value supplier eagerly (under {@code -ea}); the
     * spill-limit default reads this setting, so it must be initialized first.
     */
    public static final Setting<String> DATAFUSION_SPILL_DIRECTORY = new Setting<>(
        "datafusion.spill_directory",
        "",
        Function.identity(),
        DataFusionPlugin::validateSpillDirectory,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    /**
     * Disk-staging budget for DataFusion spill. When in-memory operations (HashAggregate, Sort,
     * TopK) exceed {@link #DATAFUSION_MEMORY_POOL_LIMIT}, DataFusion writes working state to disk;
     * this setting caps how much disk space that staging can consume.
     *
     * <p><strong>Default: 80% of the spill volume's total disk capacity.</strong> Spill is a disk
     * budget, not a memory budget, so the default is derived from the disk volume itself rather
     * than from physical RAM or the off-heap carve-out. This sizes correctly on both supported
     * deployment patterns: a dedicated EBS volume mounted at the spill directory, or the spill
     * directory on the same disk as the OpenSearch process.
     *
     * <p>When {@link #DATAFUSION_SPILL_DIRECTORY} is unset (empty), returns {@code 0} — spill is
     * disabled and the cap is irrelevant. Falls back to {@link #SPILL_LIMIT_FALLBACK_BYTES}
     * (8 GiB) when the spill volume cannot be probed.
     *
     * <p>Dynamic only when the loaded native library exports {@code df_set_spill_limit}
     * (see {@link org.opensearch.be.datafusion.nativelib.NativeBridge#isSpillLimitDynamic()}).
     * When the symbol is absent the setting can still be updated at the cluster level,
     * but the new value only takes effect after a node restart — the live update consumer
     * logs a warning in that case.
     */
    public static final Setting<Long> DATAFUSION_SPILL_MEMORY_LIMIT = new Setting<>(
        new Setting.SimpleKey("datafusion.spill_memory_limit_bytes"),
        DataFusionPlugin::deriveSpillLimitDefault,
        s -> {
            long v = Long.parseLong(s);
            if (v < 0) {
                throw new IllegalArgumentException("Setting [datafusion.spill_memory_limit_bytes] must be >= 0, got " + v);
            }
            return v;
        },
        new SpillLimitValidator(),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Validates {@link #DATAFUSION_SPILL_DIRECTORY}. Empty (the unset sentinel) is accepted
     * and signals that spill should be disabled. Non-empty values must parse as a {@link Path}.
     *
     * <p>Existence and writability are checked at boot time by the core
     * {@code Node.assertCanWritePluginHealthPaths} probe (which consumes the path returned
     * by {@link #getAdditionalHealthPaths(Settings)}), and at runtime by
     * {@code FsHealthService}. This validator only constrains the syntactic form of the setting.
     */
    static String validateSpillDirectory(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        try {
            Path.of(value).toAbsolutePath().normalize();
        } catch (java.nio.file.InvalidPathException e) {
            throw new IllegalArgumentException("Setting [datafusion.spill_directory] is not a valid path: [" + value + "]", e);
        }
        return value;
    }

    /**
     * Computes the default for {@link #DATAFUSION_SPILL_MEMORY_LIMIT}.
     *
     * <ul>
     *   <li>When {@link #DATAFUSION_SPILL_DIRECTORY} is unset (empty), returns {@code "0"} —
     *       spill is disabled and the cap is irrelevant.</li>
     *   <li>When set, returns {@link #SPILL_LIMIT_FRACTION} of the spill volume's
     *       {@link FileStore#getTotalSpace() total space}.</li>
     *   <li>When the file store cannot be probed (transient FS hiccup after boot probe),
     *       returns {@link #SPILL_LIMIT_FALLBACK_BYTES} — a conservative 8 GiB.</li>
     * </ul>
     *
     * <p>Spill is a disk budget, so the default is derived from the disk volume itself
     * rather than from physical RAM. This sizes correctly on both supported deployment
     * patterns: a dedicated EBS volume mounted at the spill directory, or the spill
     * directory on the same disk as the OpenSearch process.
     */
    static String deriveSpillLimitDefault(Settings settings) {
        String dir = DATAFUSION_SPILL_DIRECTORY.get(settings);
        if (dir == null || dir.isEmpty()) {
            return "0";
        }
        try {
            long total = Environment.getFileStore(Path.of(dir)).getTotalSpace();
            if (total <= 0) {
                return Long.toString(SPILL_LIMIT_FALLBACK_BYTES);
            }
            return Long.toString((long) (total * SPILL_LIMIT_FRACTION));
        } catch (IOException e) {
            return Long.toString(SPILL_LIMIT_FALLBACK_BYTES);
        }
    }

    /**
     * Minimum target partitions floor for the adaptive budget system.
     * When memory pressure forces partition reduction, this is the lowest value allowed.
     * Setting this equal to the configured target_partitions effectively disables
     * adaptive reduction (the budget system will never reduce below this floor).
     * Default: 1 (allow full reduction range).
     */
    public static final Setting<Integer> DATAFUSION_MIN_TARGET_PARTITIONS = Setting.intSetting(
        "datafusion.min_target_partitions",
        1,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Number of partitions used by the coordinator-reduce DataFusion plan.
     * More partitions = more parallelism = more memory (each partition holds its own hash table).
     * Lower values reduce peak memory at the cost of slower single-query latency.
     */
    public static final Setting<Integer> DATAFUSION_REDUCE_TARGET_PARTITIONS = Setting.intSetting(
        "datafusion.reduce.target_partitions",
        4,
        1,
        32,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Kill-switch for the scoped page-index feature.
     * When false, the scoped CI/OI caches are bypassed and the metadata cache
     * retains the full page index (fallback mode). Default: true.
     */
    public static final Setting<Boolean> SCOPED_PAGE_INDEX_ENABLED = Setting.boolSetting(
        "datafusion.scoped_page_index.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Admission threshold for the jemalloc memory guard (0.0–1.0).
     * When pool accounting rejects a phantom reservation but jemalloc reports
     * actual RSS below this fraction of the pool limit, the reservation proceeds
     * at full parallelism (false-positive override). Lower = more conservative.
     * Default: 0.75.
     */
    public static final Setting<Double> DATAFUSION_MEMORY_GUARD_ADMISSION_THROTTLE_THRESHOLD = Setting.doubleSetting(
        "datafusion.memory_guard.admission_throttle_threshold",
        0.75,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * RSS fraction above which new queries are rejected (429 backpressure).
     * Protects running queries from new arrivals when memory is elevated.
     * Default: 0.85.
     */
    public static final Setting<Double> DATAFUSION_MEMORY_GUARD_ADMISSION_REJECT_THRESHOLD = Setting.doubleSetting(
        "datafusion.memory_guard.admission_reject_threshold",
        0.85,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * RSS fraction above which the execution hard guard forces spill and the
     * override (which allows allocations despite pool rejection) is disabled.
     * Default: 0.85.
     */
    public static final Setting<Double> DATAFUSION_MEMORY_GUARD_EXECUTION_SPILL_THRESHOLD = Setting.doubleSetting(
        "datafusion.memory_guard.execution.spill_threshold",
        0.85,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * RSS fraction at which memory is considered critical. Serves dual purpose:
     * - Hard guard (pre-CAS): forces spill when pool accounting lags jemalloc (recoverable)
     * - Cancel path (post-CAS-fail): terminates query when spill can't help (last resort)
     * Default: 0.95.
     */
    public static final Setting<Double> DATAFUSION_MEMORY_GUARD_EXECUTION_CRITICAL_THRESHOLD = Setting.doubleSetting(
        "datafusion.memory_guard.execution.critical_threshold",
        0.95,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Total in-flight allocation in bytes allowed through the 85% spill gate by spillable consumers
     * so they can finish spilling before the pool rejects them. Bounds how much concurrent spilling
     * can collectively borrow above the spill threshold while staying below the 95% critical limit.
     * Default 536870912 (512MB) expressed in raw bytes. Live-tunable — takes effect on the next
     * allocation decision.
     */
    public static final Setting<Long> DATAFUSION_MEMORY_GUARD_SPILL_EXEMPT_CAP = Setting.longSetting(
        "datafusion.memory_guard.spill_exempt_cap_bytes",
        536870912L,
        0L,
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
    // DocumentLookupProvider implementation. Construction deferred until the DataFusion service is live.
    private volatile GetService getService;
    private volatile CircuitBreaker datafusionBreaker;

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
        return createComponents(
            client,
            clusterService,
            threadPool,
            resourceWatcherService,
            scriptService,
            xContentRegistry,
            environment,
            nodeEnvironment,
            namedWriteableRegistry,
            indexNameExpressionResolver,
            repositoriesServiceSupplier,
            dataFormatRegistry,
            null
        );
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
        DataFormatRegistry dataFormatRegistry,
        @Nullable NativeAllocator nativeAllocator
    ) {
        this.dataFormatRegistry = dataFormatRegistry;
        this.clusterService = clusterService;
        Settings settings = environment.settings();
        long memoryPoolLimit = DATAFUSION_MEMORY_POOL_LIMIT.get(settings);
        long spillMemoryLimit = DATAFUSION_SPILL_MEMORY_LIMIT.get(settings);
        String spillDir = DATAFUSION_SPILL_DIRECTORY.get(settings);

        dataFusionService = DataFusionService.builder()
            .memoryPoolLimit(memoryPoolLimit)
            .spillMemoryLimit(spillMemoryLimit)
            .spillDirectory(spillDir)
            .datanodeMultiplier(DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER.get(settings))
            .clusterSettings(clusterService.getClusterSettings())
            .build();
        dataFusionService.start();
        logger.debug("DataFusion plugin initialized — memory pool {}B, spill limit {}B", memoryPoolLimit, spillMemoryLimit);

        // Build the get-by-id service now that the DataFusion runtime is live. The
        // DocumentMetadataResolver is supplied per-call by the engine, so it is not needed here.
        this.getService = new GetService(this);

        // Wire the dynamic spill limit setting to the native runtime so updates via the
        // cluster settings API take effect without restarting the node.
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DATAFUSION_SPILL_MEMORY_LIMIT, this::updateSpillMemoryLimit);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DATAFUSION_MIN_TARGET_PARTITIONS, this::updateMinTargetPartitions);
        // Recompute and push absolute cache limits whenever the total budget or any
        // sub-cache percentage changes. Validates that percentages sum to 100 first.
        // The total-size budget and the four sub-cache percentages together determine the
        // absolute CI/OI limits, so they are wired through a single grouped Consumer<Settings>.
        // This MUST use the grouped overload (not `v -> recompute(getClusterSettings())`): during
        // an apply cycle the per-setting getter resolves against `lastSettingsApplied`, which the
        // settings framework only swaps in AFTER all update consumers have run — so a callback that
        // re-reads via getClusterSettings().get() sees the stale/previous value and pushes the
        // limit derived from the OLD budget (an off-by-one update). The grouped consumer receives a
        // Settings built from the cycle's new values, so reading each setting from it is correct.
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                this::recomputePageCacheLimits,
                List.of(
                    CacheSettings.METADATA_INDEX_CACHE_TOTAL_SIZE,
                    CacheSettings.FOOTER_METADATA_CACHE_PERCENT,
                    CacheSettings.OFFSET_INDEX_CACHE_PERCENT,
                    CacheSettings.COLUMN_INDEX_CACHE_PERCENT,
                    CacheSettings.STATISTICS_CACHE_PERCENT
                )
            );
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DATAFUSION_REDUCE_TARGET_PARTITIONS, NativeBridge::setReduceTargetPartitions);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(SCOPED_PAGE_INDEX_ENABLED, enabled -> {
            NativeBridge.setScopedPageIndexEnabled(enabled);
            if (!enabled) {
                // Clear scoped caches immediately when disabling — entries are
                // useless in fallback mode and would waste native heap.
                NativeBridge.clearColumnIndexCache();
                NativeBridge.clearOffsetIndexCache();
                logger.info("Scoped page-index disabled: cleared CI and OI caches");
            }
        });
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DATAFUSION_MEMORY_GUARD_SPILL_EXEMPT_CAP, NativeBridge::setSpillExemptCapBytes);
        // The four memory-guard thresholds are pushed to the native pool together via a single
        // grouped consumer. This MUST use the grouped Consumer<Settings> overload (not
        // `v -> updateMemoryGuardThresholds()` re-reading via getClusterSettings().get()): during
        // an apply cycle the per-setting getter resolves against `lastSettingsApplied`, which the
        // settings framework only swaps in AFTER all update consumers have run — so a callback that
        // re-reads sees the stale/previous value and would push defaults instead of the new value.
        // The grouped consumer receives a Settings built from the cycle's new (`current`) settings,
        // so reading each threshold from it yields the value just set.
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                this::updateMemoryGuardThresholds,
                List.of(
                    DATAFUSION_MEMORY_GUARD_ADMISSION_THROTTLE_THRESHOLD,
                    DATAFUSION_MEMORY_GUARD_ADMISSION_REJECT_THRESHOLD,
                    DATAFUSION_MEMORY_GUARD_EXECUTION_SPILL_THRESHOLD,
                    DATAFUSION_MEMORY_GUARD_EXECUTION_CRITICAL_THRESHOLD
                )
            );

        // Push Rust log level whenever any logger.* cluster setting changes, so Rust macros
        // can short-circuit format!() for suppressed levels without polling.
        clusterService.getClusterSettings()
            .addAffixUpdateConsumer(Loggers.LOG_LEVEL_SETTING, (namespace, level) -> RustLoggerBridge.pushLevel(), (k, v) -> {});

        // Wire dynamic concurrency gate multiplier settings
        int cpuThreads = DataFusionService.cpuThreadCount();

        clusterService.getClusterSettings().addSettingsUpdateConsumer(DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER, multiplier -> {
            int newMax = Math.max(1, (int) (cpuThreads * multiplier));
            NativeBridge.updateConcurrencyGate("fragment_executor", newMax);
        });

        // Apply initial values
        NativeBridge.setMinTargetPartitions(DATAFUSION_MIN_TARGET_PARTITIONS.get(settings));
        NativeBridge.setReduceTargetPartitions(DATAFUSION_REDUCE_TARGET_PARTITIONS.get(settings));
        NativeBridge.setSpillExemptCapBytes(DATAFUSION_MEMORY_GUARD_SPILL_EXEMPT_CAP.get(settings));
        NativeBridge.setScopedPageIndexEnabled(SCOPED_PAGE_INDEX_ENABLED.get(settings));
        NativeBridge.setMemoryGuardThresholds(
            DATAFUSION_MEMORY_GUARD_ADMISSION_THROTTLE_THRESHOLD.get(settings),
            DATAFUSION_MEMORY_GUARD_ADMISSION_REJECT_THRESHOLD.get(settings),
            DATAFUSION_MEMORY_GUARD_EXECUTION_SPILL_THRESHOLD.get(settings),
            DATAFUSION_MEMORY_GUARD_EXECUTION_CRITICAL_THRESHOLD.get(settings)
        );

        this.datafusionSettings = new DatafusionSettings(clusterService);

        // Expose per-task native-memory usage to search backpressure.
        NativeMemoryUsageTracker.setSnapshotSupplier(this::currentBytesByTaskId);
        NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(() -> DATAFUSION_MEMORY_POOL_LIMIT.get(clusterService.getSettings()));

        this.substraitExtensions = loadSubstraitExtensions();

        // Register with the unified allocator if available
        if (nativeAllocator != null) {
            ClusterSettings clusterSettings = clusterService.getClusterSettings();
            ArrowNativeAllocator arrowAllocator = (ArrowNativeAllocator) nativeAllocator;

            NativeAllocator.VirtualPoolHandle dfPool = arrowAllocator.registerVirtualPool(
                DatafusionSettings.POOL_DATAFUSION,
                DatafusionSettings.DATAFUSION_MEMORY_POOL_MIN.get(settings),
                DATAFUSION_MEMORY_POOL_LIMIT.get(settings),
                PoolGroup.SEARCH,
                this::updateMemoryPoolLimit
            );

            arrowAllocator.addStatsRefresher(() -> {
                if (dataFusionService != null) {
                    long usage = dataFusionService.getMemoryPoolUsage();
                    dfPool.updateStats(usage, usage);
                }
            });

            arrowAllocator.setNativeMemoryStatsSupplier(() -> {
                AnalyticsBackendNativeMemoryStats s = NativeMemoryFetcher.fetch();
                return new long[] { s.getAllocatedBytes(), s.getResidentBytes() };
            });

            // Wire dynamic setting consumers for pool min/max
            clusterSettings.addSettingsUpdateConsumer(DATAFUSION_MEMORY_POOL_LIMIT, newMax -> {
                arrowAllocator.setPoolLimit(DatafusionSettings.POOL_DATAFUSION, newMax);
                updateMemoryPoolLimit(newMax);
            });
            clusterSettings.addSettingsUpdateConsumer(
                DatafusionSettings.DATAFUSION_MEMORY_POOL_MIN,
                newMin -> arrowAllocator.setPoolMin(DatafusionSettings.POOL_DATAFUSION, newMin)
            );
        }

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
            SimpleExtension.ExtensionCollection windowExtensions = SimpleExtension.load(List.of("/opensearch_window_functions.yaml"));
            SimpleExtension.ExtensionCollection arithmeticOverloads = SimpleExtension.load(
                List.of("/opensearch_arithmetic_overloads.yaml")
            );
            return DefaultExtensionCatalog.DEFAULT_COLLECTION.merge(delegationExtensions)
                .merge(scalarExtensions)
                .merge(arrayExtensions)
                .merge(aggregateExtensions)
                .merge(windowExtensions)
                .merge(arithmeticOverloads);
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

    @Override
    public List<Path> getAdditionalHealthPaths(Settings settings) {
        String dir = DATAFUSION_SPILL_DIRECTORY.get(settings);
        if (dir == null || dir.isEmpty()) {
            return List.of();
        }
        return List.of(Path.of(dir));
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

    /**
     * Applies a new spill memory limit to the running DataFusion runtime when the loaded
     * native library exports {@code df_set_spill_limit}; otherwise emits a warning. The
     * cluster-settings update is accepted unconditionally because the value is read at next
     * node startup. Package-private for testing.
     */
    void updateSpillMemoryLimit(long newLimitBytes) {
        DataFusionService service = dataFusionService;
        if (service == null) {
            logger.debug("DataFusion service not yet initialized; ignoring spill limit update to {}B", newLimitBytes);
            return;
        }
        if (!service.isSpillLimitDynamic()) {
            logger.warn(
                "Updated DataFusion spill memory limit to {}B at the cluster level; the loaded native library does not "
                    + "support runtime spill resize, so the new value will only take effect after a node restart",
                newLimitBytes
            );
            return;
        }
        try {
            service.setSpillMemoryLimit(newLimitBytes);
            logger.info("Updated DataFusion spill memory limit to {}B", newLimitBytes);
        } catch (IllegalStateException e) {
            logger.warn("Ignoring spill memory limit update to {}B; service is not running", newLimitBytes);
        } catch (UnsupportedOperationException e) {
            // isSpillLimitDynamic() guard above should make this unreachable, but defend
            // against a race between probe and call.
            logger.warn("Ignoring spill memory limit update to {}B; native runtime does not support live updates", newLimitBytes);
        }
    }

    void updateMinTargetPartitions(int value) {
        NativeBridge.setMinTargetPartitions(value);
        logger.info("Updated DataFusion min_target_partitions to {}", value);
    }

    /**
     * Recompute absolute ColumnIndex and OffsetIndex cache limits from the updated
     * {@link CacheSettings#METADATA_INDEX_CACHE_TOTAL_SIZE} and percent settings, then push them
     * to native. Validates that percentages sum to 100 before applying.
     *
     * <p>Reads each value from the {@code updated} Settings supplied by the grouped settings-update
     * consumer — NOT via {@code clusterService.getClusterSettings().get(...)}, which during an apply
     * cycle still resolves against the previous settings and would derive limits from the stale
     * budget (an off-by-one update).
     */
    private void recomputePageCacheLimits(org.opensearch.common.settings.Settings updated) {
        long total = CacheSettings.METADATA_INDEX_CACHE_TOTAL_SIZE.get(updated).getBytes();
        int metaPct = CacheSettings.FOOTER_METADATA_CACHE_PERCENT.get(updated);
        int oiPct = CacheSettings.OFFSET_INDEX_CACHE_PERCENT.get(updated);
        int ciPct = CacheSettings.COLUMN_INDEX_CACHE_PERCENT.get(updated);
        int statsPct = CacheSettings.STATISTICS_CACHE_PERCENT.get(updated);
        CacheSettings.validatePercentSum(metaPct, oiPct, ciPct, statsPct);
        long metaLimit = total * metaPct / 100;
        long ciLimit = total * ciPct / 100;
        long oiLimit = total * oiPct / 100;
        long statsLimit = total * statsPct / 100;
        logger.info(
            "Updating cache limits: footer_metadata={} bytes, " + "column_index={} bytes, offset_index={} bytes, statistics={} bytes",
            metaLimit,
            ciLimit,
            oiLimit,
            statsLimit
        );
        NativeBridge.setColumnIndexCacheLimit(ciLimit);
        NativeBridge.setOffsetIndexCacheLimit(oiLimit);
        DataFusionService service = dataFusionService;
        if (service != null) {
            CacheManager cm = service.getCacheManager();
            if (cm != null) {
                cm.updateSizeLimit(CacheUtils.CacheType.METADATA, metaLimit);
                cm.updateSizeLimit(CacheUtils.CacheType.STATISTICS, statsLimit);
            }
        }
    }

    /**
     * Pushes the four memory-guard thresholds to the native pool. Reads each value from the
     * `updated` Settings supplied by the grouped settings-update consumer — which the framework
     * builds from the cycle's new settings — rather than re-reading via
     * `clusterService.getClusterSettings().get(...)`. The latter resolves against
     * `lastSettingsApplied`, which is not swapped in until after all update consumers have run, so
     * re-reading mid-cycle would yield the stale/previous value (the root cause of thresholds
     * silently not updating at runtime). Unchanged thresholds are filled with their registered
     * defaults in `updated`, so passing all four every time is correct.
     */
    private void updateMemoryGuardThresholds(Settings updated) {
        double admissionThrottle = DATAFUSION_MEMORY_GUARD_ADMISSION_THROTTLE_THRESHOLD.get(updated);
        double admissionReject = DATAFUSION_MEMORY_GUARD_ADMISSION_REJECT_THRESHOLD.get(updated);
        double executionSpill = DATAFUSION_MEMORY_GUARD_EXECUTION_SPILL_THRESHOLD.get(updated);
        double executionCritical = DATAFUSION_MEMORY_GUARD_EXECUTION_CRITICAL_THRESHOLD.get(updated);
        NativeBridge.setMemoryGuardThresholds(admissionThrottle, admissionReject, executionSpill, executionCritical);
        logger.info(
            "Updated DataFusion memory guard thresholds: admission_throttle={}, admission_reject={}, execution_spill={}, execution_critical={}",
            admissionThrottle,
            admissionReject,
            executionSpill,
            executionCritical
        );
    }

    @Override
    public String name() {
        return "datafusion";
    }

    @Override
    public EngineReaderManager<DatafusionReader> createReaderManager(ReaderManagerConfig settings) throws IOException {
        NativeStoreHandle dataformatAwareStoreHandle = settings.dataformatAwareStoreHandles().get(settings.format());
        // Pull index.sort.field / index.sort.order off IndexSettings so the native reader can declare
        // file sort order to DataFusion. Empty lists when the index has no index sort configured.
        // Two consumers downstream:
        // - Vanilla path: ListingOptions.with_file_sort_order so the planner can drop SortExec.
        // - Indexed path: indexed_executor reverses segment iteration when the query's leading
        // ORDER BY runs counter to the catalog direction.
        List<String> sortFields = List.of();
        List<String> sortOrders = List.of();
        IndexSettings indexSettings = settings.indexSettings();
        if (indexSettings != null) {
            Settings rawSettings = indexSettings.getSettings();
            List<String> fields = IndexSortConfig.INDEX_SORT_FIELD_SETTING.get(rawSettings);
            if (!fields.isEmpty()) {
                sortFields = List.copyOf(fields);
                // IndexSortConfig validates size match at index creation, so when
                // index.sort.order is set its length equals fields.length. When omitted,
                // every field defaults to asc — matches IndexSortConfig behavior.
                if (IndexSortConfig.INDEX_SORT_ORDER_SETTING.exists(rawSettings)) {
                    sortOrders = IndexSortConfig.INDEX_SORT_ORDER_SETTING.get(rawSettings)
                        .stream()
                        .map(o -> o == SortOrder.DESC ? "desc" : "asc")
                        .toList();
                } else {
                    sortOrders = fields.stream().map(f -> "asc").toList();
                }
            }
        }
        return new DatafusionReaderManager(
            settings.format(),
            settings.shardPath(),
            dataFusionService,
            dataformatAwareStoreHandle,
            sortFields,
            sortOrders
        );
    }

    @Override
    public List<String> getSupportedFormats() {
        return List.of(SUPPORTED_FORMAT);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(DataFusionStatsActionType.INSTANCE, TransportDataFusionStatsAction.class),
            new ActionHandler<>(
                org.opensearch.be.datafusion.action.stats.ClearCacheActionType.INSTANCE,
                org.opensearch.be.datafusion.action.stats.TransportClearCacheAction.class
            )
        );
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
        return List.of(new RestDataFusionStatsAction(), new org.opensearch.be.datafusion.action.stats.RestClearCacheAction());
    }

    @Override
    public BreakerSettings getCircuitBreaker(Settings settings) {
        long limit = DATAFUSION_MEMORY_POOL_LIMIT.get(settings);
        return new BreakerSettings(
            "analytics_backend_datafusion",
            limit,
            1.0,
            CircuitBreaker.Type.MEMORY,
            CircuitBreaker.Durability.TRANSIENT,
            () -> {
                long currentLimit = dataFusionService != null ? dataFusionService.getMemoryPoolLimit() : limit;
                long[] stats = dataFusionService != null ? dataFusionService.getMemoryPoolStats() : new long[] { 0, 0 };
                return new CircuitBreakerStats("analytics_backend_datafusion", currentLimit, stats[0], 1.0, stats[1]);
            }
        );
    }

    @Override
    public void setCircuitBreaker(CircuitBreaker circuitBreaker) {
        this.datafusionBreaker = circuitBreaker;
    }

    public Supplier<AnalyticsBackendTaskCancellationStats> getAnalyticsBackendTaskCancellationStats() {
        return () -> {
            try {
                return NativeBridge.nativeNodeStats();
            } catch (Exception e) {
                return new AnalyticsBackendTaskCancellationStats(0, 0, 0, 0);
            }
        };
    }

    @Override
    public Supplier<AnalyticsBackendNativeMemoryStats> getAnalyticsBackendNativeMemoryStats() {
        return () -> {
            try {
                return NativeMemoryFetcher.fetch();
            } catch (Exception e) {
                return new AnalyticsBackendNativeMemoryStats(-1, -1, 0);
            }
        };
    }

    @Override
    public void close() throws IOException {
        if (getService != null) {
            getService.close();
        }
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

    /**
     * Get-by-id entry point. Delegates to {@link GetService}, which fetches the row through the native runtime.
     */
    @Override
    public DocumentLookupResult getById(Engine.Get get, IndexReaderProvider.Reader reader, Index index, DocumentMetadataResolver resolver)
        throws IOException {
        GetService getService = getServiceOrThrow();
        return getService.documentLookupService(resolver).getById(get.id(), reader, index);
    }

    @Override
    public DocumentLookupResult getVersionMetadata(
        String id,
        IndexReaderProvider.Reader reader,
        Index index,
        DocumentMetadataResolver resolver
    ) throws IOException {
        GetService svc = getServiceOrThrow();
        return svc.documentLookupService(resolver).getVersionMetadata(id, reader, index);
    }

    @Override
    public List<DocumentLookupResult> getDocsAboveSeqNo(
        long fromSeqNoExclusive,
        IndexReaderProvider.Reader reader,
        Index index,
        DocumentMetadataResolver resolver
    ) throws IOException {
        GetService svc = getService;
        if (svc == null) return List.of();
        return svc.documentLookupService(resolver).getDocsAboveSeqNo(fromSeqNoExclusive, reader, index);
    }

    /**
     * Returns the {@link GetService} , throwing IllegalStateException if not initialized.
     */
    private GetService getServiceOrThrow() {
        GetService svc = getService;
        if (svc == null) {
            throw new IllegalStateException("GetService is not initialized. ");
        }
        return svc;
    }
}
