/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.cache.CacheSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;
import org.opensearch.search.SearchService;

import java.util.List;

/**
 * Consolidates all DataFusion plugin settings (existing memory/spill/reduce/cache settings
 * plus the new indexed query settings) and manages the pre-computed {@link WireConfigSnapshot}.
 * <p>
 * Each dynamic indexed setting registers an {@code addSettingsUpdateConsumer} callback that
 * atomically rebuilds the volatile snapshot on change. At query time, the instruction handler
 * reads the snapshot with zero per-query overhead — no {@code ClusterService} lookup on the
 * hot path.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class DatafusionSettings {

    public static final String POOL_DATAFUSION = "datafusion";

    // ── New indexed query settings ──

    /** Number of rows per batch in the indexed query execution path. */
    public static final Setting<Integer> BATCH_SIZE = Setting.intSetting(
        "datafusion.batch_size",
        8192,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Whether DataFusion applies its own decode-time predicate pushdown on the
     * ListingTable (non-indexed) query path. Maps to DataFusion's
     * {@code execution.parquet.pushdown_filters} session option.
     */
    public static final Setting<Boolean> LISTING_TABLE_PUSHDOWN_FILTERS = Setting.boolSetting(
        "datafusion.listing_table.pushdown_filters",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Whether the indexed stream asks parquet to apply the residual predicate during
     * decode (via {@code RowFilter} pushdown). When true (default), narrow row-granular
     * selections benefit from decode-time filtering; when false, the indexed stream
     * handles filtering externally via bitmap-based row selection.
     * <p>
     * Note: ideally this decision should be taken by the planner on a per-query basis
     * (e.g., based on filter shape and estimated selectivity). This setting acts as
     * the node-wide default until per-query planner support is added.
     */
    public static final Setting<Boolean> INDEXED_PUSHDOWN_FILTERS = Setting.boolSetting(
        "datafusion.indexed.pushdown_filters",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Default minimum run length (in rows) below which the indexed stream skips
     * row-selection optimizations and falls back to sequential decode. Shorter runs
     * have higher per-row overhead from selection vector maintenance.
     */
    public static final Setting<Integer> INDEXED_MIN_SKIP_RUN_DEFAULT = Setting.intSetting(
        "datafusion.indexed.min_skip_run_default",
        1024,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Selectivity threshold [0.0, 1.0] that controls when the indexed stream switches
     * from row-selection mode to full-decode mode. A low threshold (e.g., 0.03) means
     * "only use row-selection when the filter is very selective (few rows match)."
     * <p>
     * Example: with threshold 0.03, a filter that matches 2% of rows uses row-selection
     * (skip non-matching rows), but a filter matching 5% switches to full-decode
     * (cheaper to just read everything sequentially).
     */
    public static final Setting<Double> INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD = Setting.doubleSetting(
        "datafusion.indexed.min_skip_run_selectivity_threshold",
        0.03,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Pins the indexed stream's per-row-group {@code min_skip_run} strategy instead of
     * letting candidate selectivity decide. Accepts:
     * <ul>
     *   <li>{@code "none"} (default) — selectivity heuristic decides (wire {@code -1});</li>
     *   <li>{@code "row_selection"} — force row-granular selection, {@code min_skip_run = 1} (wire {@code 0});</li>
     *   <li>{@code "boolean_mask"} — force a single whole-RG select (wire {@code 1}).</li>
     * </ul>
     * Node-wide override intended for benchmarking/diagnostics; production normally
     * leaves this at {@code "none"}.
     */
    public static final Setting<String> INDEXED_FORCE_STRATEGY = Setting.simpleString(
        "datafusion.indexed.force_strategy",
        "none",
        DatafusionSettings::validateForceStrategy,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static void validateForceStrategy(String value) {
        forceStrategyToWire(value);
    }

    /** Maps the {@link #INDEXED_FORCE_STRATEGY} string to the Rust wire code (-1/0/1). */
    static int forceStrategyToWire(String value) {
        switch (value) {
            case "none":
                return -1;
            case "row_selection":
                return 0;
            case "boolean_mask":
                return 1;
            default:
                throw new IllegalArgumentException(
                    "Setting [datafusion.indexed.force_strategy] must be one of [none, row_selection, boolean_mask], got [" + value + "]"
                );
        }
    }

    // ── Concurrency gate settings ──

    /** Minimum guaranteed bytes for the DataFusion memory pool. Default is half of datafusion max (37% of budget). */
    public static final Setting<Long> DATAFUSION_MEMORY_POOL_MIN = new Setting<>(
        "datafusion.memory_pool_min_bytes",
        s -> derivePoolMinDefault(s, 37),
        s -> {
            long v = Long.parseLong(s);
            if (v < 0) {
                throw new IllegalArgumentException("Setting [datafusion.memory_pool_min_bytes] must be >= 0, got " + v);
            }
            return v;
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Fragment executor concurrency gate multiplier: max concurrent partition-equivalents = cpu_threads × multiplier. */
    public static final Setting<Double> CONCURRENCY_DATANODE_MULTIPLIER = Setting.doubleSetting(
        "datafusion.concurrency.fragment_executor_multiplier",
        1.5,
        0.1,
        10.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Computes the default for a pool min as a percentage of
     * {@link ResourceTrackerSettings#NODE_NATIVE_MEMORY_LIMIT_SETTING}.
     * Returns 0 when AC is unconfigured.
     */
    static String derivePoolMinDefault(Settings settings, int percent) {
        ByteSizeValue nativeLimit = ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.get(settings);
        if (nativeLimit.getBytes() <= 0) {
            return "0";
        }
        return Long.toString(Math.max(0L, nativeLimit.getBytes() * percent / 100));
    }

    // ── All settings registered by the plugin ──

    /**
     * Dynamically enables or disables Liquid Cache for new queries.
     * When false, optimizers are not injected and queries bypass the cache.
     * The cache remains initialized (for fast re-enable) but idle.
     */
    public static final Setting<Boolean> LIQUID_CACHE_ENABLED = Setting.boolSetting(
        "datafusion.liquid_cache.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Controls the Liquid Cache max memory size in bytes for byte-level Parquet caching.
     * Only used when liquid cache is enabled via the experimental feature flag.
     */
    public static final Setting<Long> LIQUID_CACHE_SIZE = Setting.longSetting(
        "datafusion.liquid_cache.size_bytes",
        1L * 1024 * 1024 * 1024, // 1GB default
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Liquid Cache eviction policy.
     * <ul>
     *   <li>{@code liquid} (default) — Independent FIFO queues per batch type (LiquidPolicy)</li>
     *   <li>{@code lru} — Standard LRU eviction across all entry types</li>
     * </ul>
     */
    public static final Setting<String> LIQUID_CACHE_EVICTION_POLICY = Setting.simpleString(
        "datafusion.liquid_cache.eviction_policy",
        "lru",
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    /**
     * Selectivity threshold for LC STREAM vs DELEGATE (0.0 to 1.0).
     * Files with selectivity below this threshold are delegated to plain parquet.
     */
    public static final Setting<Float> LIQUID_CACHE_SELECTIVITY_THRESHOLD = Setting.floatSetting(
        "datafusion.liquid_cache.selectivity_threshold",
        0.8f,
        0.0f,
        1.0f,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Maximum number of output columns for LC engagement.
     * Queries projecting more columns than this are skipped by the optimizer.
     */
    public static final Setting<Integer> LIQUID_CACHE_MAX_COLUMNS = Setting.intSetting(
        "datafusion.liquid_cache.max_columns",
        10,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final List<Setting<?>> ALL_SETTINGS = List.of(

        // Runtime settings — memory pool, spill, reduce input mode, and budget tuning
        DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT,
        DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT,
        DataFusionPlugin.DATAFUSION_SPILL_DIRECTORY,
        DataFusionPlugin.DATAFUSION_REDUCE_INPUT_MODE,
        DataFusionPlugin.DATAFUSION_REDUCE_TARGET_PARTITIONS,
        DataFusionPlugin.DATAFUSION_MIN_TARGET_PARTITIONS,
        DataFusionPlugin.DATAFUSION_MEMORY_GUARD_ADMISSION_THROTTLE_THRESHOLD,
        DataFusionPlugin.DATAFUSION_MEMORY_GUARD_ADMISSION_REJECT_THRESHOLD,
        DataFusionPlugin.DATAFUSION_MEMORY_GUARD_EXECUTION_SPILL_THRESHOLD,
        DataFusionPlugin.DATAFUSION_MEMORY_GUARD_EXECUTION_CRITICAL_THRESHOLD,
        DataFusionPlugin.DATAFUSION_MEMORY_GUARD_SPILL_EXEMPT_CAP,
        DATAFUSION_MEMORY_POOL_MIN,

        // Cache settings — metadata, statistics, column index, offset index configuration
        CacheSettings.METADATA_CACHE_EVICTION_TYPE,
        CacheSettings.STATISTICS_CACHE_EVICTION_TYPE,
        CacheSettings.COLUMN_INDEX_CACHE_EVICTION_TYPE,
        CacheSettings.OFFSET_INDEX_CACHE_EVICTION_TYPE,
        CacheSettings.METADATA_CACHE_ENABLED,
        CacheSettings.STATISTICS_CACHE_ENABLED,
        CacheSettings.METADATA_INDEX_CACHE_TOTAL_SIZE,
        CacheSettings.FOOTER_METADATA_CACHE_PERCENT,
        CacheSettings.OFFSET_INDEX_CACHE_PERCENT,
        CacheSettings.COLUMN_INDEX_CACHE_PERCENT,
        CacheSettings.STATISTICS_CACHE_PERCENT,

        // Scoped page-index feature flag
        DataFusionPlugin.SCOPED_PAGE_INDEX_ENABLED,

        // Concurrency gate settings
        CONCURRENCY_DATANODE_MULTIPLIER,

        // Indexed query settings — per-query tuning knobs for the indexed execution path
        BATCH_SIZE,
        LISTING_TABLE_PUSHDOWN_FILTERS,
        INDEXED_PUSHDOWN_FILTERS,
        INDEXED_MIN_SKIP_RUN_DEFAULT,
        INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD,
        INDEXED_FORCE_STRATEGY,

        LIQUID_CACHE_ENABLED,
        LIQUID_CACHE_SIZE,
        LIQUID_CACHE_EVICTION_POLICY,
        LIQUID_CACHE_SELECTIVITY_THRESHOLD,
        LIQUID_CACHE_MAX_COLUMNS
    );

    // ── Snapshot management ──

    private volatile WireConfigSnapshot snapshot;

    /**
     * Tracks the current value of {@code search.concurrent.max_slice_count} for
     * deriving {@code target_partitions}. Updated by the registered listener.
     */
    private volatile int maxSliceCount;

    /**
     * Tracks the current concurrent search mode ({@code "auto"}, {@code "all"}, or {@code "none"}).
     * When mode is {@code "none"}, target_partitions is forced to 1.
     */
    private volatile String concurrentSearchMode;

    /**
     * Creates the settings holder, builds the initial {@link WireConfigSnapshot} from
     * the cluster service's settings, and registers listeners for dynamic updates.
     *
     * @param clusterService the cluster service providing settings and listener registration
     */
    public DatafusionSettings(ClusterService clusterService) {
        Settings settings = clusterService.getSettings();
        ClusterSettings clusterSettings = clusterService.getClusterSettings();

        this.concurrentSearchMode = SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.get(settings);
        this.maxSliceCount = SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.get(settings);

        this.snapshot = WireConfigSnapshot.builder()
            .batchSize(BATCH_SIZE.get(settings))
            .targetPartitions(deriveTargetPartitions(this.concurrentSearchMode, this.maxSliceCount))
            .listingTablePushdownFilters(LISTING_TABLE_PUSHDOWN_FILTERS.get(settings))
            .indexedPushdownFilters(INDEXED_PUSHDOWN_FILTERS.get(settings))
            .minSkipRunDefault(INDEXED_MIN_SKIP_RUN_DEFAULT.get(settings))
            .minSkipRunSelectivityThreshold(INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.get(settings))
            .forceStrategy(forceStrategyToWire(INDEXED_FORCE_STRATEGY.get(settings)))
            .build();

        registerListeners(clusterSettings);
    }

    /**
     * Package-private constructor for testing — builds the initial snapshot from
     * raw settings without registering dynamic update listeners.
     */
    DatafusionSettings(Settings settings) {
        this.concurrentSearchMode = SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.get(settings);
        this.maxSliceCount = SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.get(settings);

        this.snapshot = WireConfigSnapshot.builder()
            .batchSize(BATCH_SIZE.get(settings))
            .targetPartitions(deriveTargetPartitions(this.concurrentSearchMode, this.maxSliceCount))
            .listingTablePushdownFilters(LISTING_TABLE_PUSHDOWN_FILTERS.get(settings))
            .indexedPushdownFilters(INDEXED_PUSHDOWN_FILTERS.get(settings))
            .minSkipRunDefault(INDEXED_MIN_SKIP_RUN_DEFAULT.get(settings))
            .minSkipRunSelectivityThreshold(INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.get(settings))
            .forceStrategy(forceStrategyToWire(INDEXED_FORCE_STRATEGY.get(settings)))
            .build();
    }

    void registerListeners(ClusterSettings clusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(BATCH_SIZE, newValue -> {
            snapshot = WireConfigSnapshot.builder(snapshot).batchSize(newValue).build();
        });

        clusterSettings.addSettingsUpdateConsumer(LISTING_TABLE_PUSHDOWN_FILTERS, newValue -> {
            snapshot = WireConfigSnapshot.builder(snapshot).listingTablePushdownFilters(newValue).build();
        });

        clusterSettings.addSettingsUpdateConsumer(INDEXED_PUSHDOWN_FILTERS, newValue -> {
            snapshot = WireConfigSnapshot.builder(snapshot).indexedPushdownFilters(newValue).build();
        });

        clusterSettings.addSettingsUpdateConsumer(INDEXED_MIN_SKIP_RUN_DEFAULT, newValue -> {
            snapshot = WireConfigSnapshot.builder(snapshot).minSkipRunDefault(newValue).build();
        });

        clusterSettings.addSettingsUpdateConsumer(INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD, newValue -> {
            snapshot = WireConfigSnapshot.builder(snapshot).minSkipRunSelectivityThreshold(newValue).build();
        });

        clusterSettings.addSettingsUpdateConsumer(INDEXED_FORCE_STRATEGY, newValue -> {
            snapshot = WireConfigSnapshot.builder(snapshot).forceStrategy(forceStrategyToWire(newValue)).build();
        });

        clusterSettings.addSettingsUpdateConsumer(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING, newValue -> {
            this.maxSliceCount = newValue;
            snapshot = WireConfigSnapshot.builder(snapshot)
                .targetPartitions(deriveTargetPartitions(this.concurrentSearchMode, newValue))
                .build();
        });

        clusterSettings.addSettingsUpdateConsumer(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE, newValue -> {
            this.concurrentSearchMode = newValue;
            snapshot = WireConfigSnapshot.builder(snapshot).targetPartitions(deriveTargetPartitions(newValue, this.maxSliceCount)).build();
        });
    }

    /**
     * Returns the current pre-computed wire config snapshot. This is a single
     * volatile read — safe for the query hot path with zero overhead.
     *
     * @return the current snapshot (never null after construction)
     */
    public WireConfigSnapshot getSnapshot() {
        return snapshot;
    }

    /**
     * Derives {@code target_partitions} from the concurrent search mode and
     * {@code search.concurrent.max_slice_count} setting value.
     * <p>
     * When mode is {@code "none"}, forces target_partitions to 1 (no concurrency).
     * When {@code max_slice_count} is 0, uses 50% of available CPU cores.
     * Otherwise caps the value at 100% of available CPU cores.
     */
    private static int deriveTargetPartitions(String mode, int maxSliceCount) {
        if (SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE.equals(mode)) {
            return 1;
        }

        // For maxSliceCount == 0 also, we will be owning the concurrency level
        if (maxSliceCount == 0) {
            return Runtime.getRuntime().availableProcessors() / 2;
        }

        // Even if the user set's a higher value, we will still want to limit the number
        // of slices to the number of available processors
        // to avoid over-subscription and ensure reasonable performance
        return Math.min(maxSliceCount, Runtime.getRuntime().availableProcessors());
    }
}
