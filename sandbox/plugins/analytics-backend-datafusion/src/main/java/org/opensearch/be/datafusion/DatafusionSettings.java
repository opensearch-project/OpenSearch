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

    // ── New indexed query settings ──

    /** Number of rows per batch in the indexed query execution path. */
    public static final Setting<Integer> INDEXED_BATCH_SIZE = Setting.intSetting(
        "datafusion.indexed.batch_size",
        8192,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Whether DataFusion applies residual predicate pushdown during parquet decode
     * on the indexed path. When true, narrow row-granular selections benefit from
     * decode-time filtering via {@code RowFilter}. When false (default), the indexed
     * stream handles filtering externally via bitmap-based row selection.
     * <p>
     * Note: ideally this decision should be taken by the planner on a per-query basis
     * (e.g., based on filter shape and estimated selectivity). This setting acts as
     * the node-wide default until per-query planner support is added.
     */
    public static final Setting<Boolean> INDEXED_PARQUET_PUSHDOWN_FILTERS = Setting.boolSetting(
        "datafusion.indexed.parquet_pushdown_filters",
        false,
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

    // Strategy constants for CollectorCallStrategy
    public static final String STRATEGY_FULL_RANGE = "full_range";
    public static final String STRATEGY_TIGHTEN_OUTER_BOUNDS = "tighten_outer_bounds";
    public static final String STRATEGY_PAGE_RANGE_SPLIT = "page_range_split";

    /**
     * How the SingleCollectorEvaluator narrows collector doc ranges relative to
     * page-pruning results. Valid values: full_range, tighten_outer_bounds, page_range_split.
     * Default is page_range_split — only one collector, so multiple FFM calls per RG is acceptable.
     */
    public static final Setting<String> INDEXED_SINGLE_COLLECTOR_STRATEGY = Setting.simpleString(
        "datafusion.indexed.single_collector_strategy",
        STRATEGY_PAGE_RANGE_SPLIT,
        value -> {
            switch (value) {
                case STRATEGY_FULL_RANGE:
                case STRATEGY_TIGHTEN_OUTER_BOUNDS:
                case STRATEGY_PAGE_RANGE_SPLIT:
                    break;
                default:
                    throw new IllegalArgumentException(
                        "datafusion.indexed.single_collector_strategy must be one of "
                            + "[full_range, tighten_outer_bounds, page_range_split], got: "
                            + value
                    );
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How the bitmap tree evaluator narrows collector doc ranges when multiple collectors
     * are present. Valid values: full_range, tighten_outer_bounds, page_range_split.
     * Default is tighten_outer_bounds — multiple collectors make page_range_split expensive.
     */
    public static final Setting<String> INDEXED_TREE_COLLECTOR_STRATEGY = Setting.simpleString(
        "datafusion.indexed.tree_collector_strategy",
        STRATEGY_TIGHTEN_OUTER_BOUNDS,
        value -> {
            switch (value) {
                case STRATEGY_FULL_RANGE:
                case STRATEGY_TIGHTEN_OUTER_BOUNDS:
                case STRATEGY_PAGE_RANGE_SPLIT:
                    break;
                default:
                    throw new IllegalArgumentException(
                        "datafusion.indexed.tree_collector_strategy must be one of "
                            + "[full_range, tighten_outer_bounds, page_range_split], got: "
                            + value
                    );
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Maximum number of Collector-leaf FFM calls issued in parallel per row-group
     * prefetch. 1 = fully sequential (lowest CPU, fastest short-circuit). Higher
     * values sacrifice short-circuit savings in AND/OR groups but reduce latency
     * for independent collector leaves.
     */
    public static final Setting<Integer> INDEXED_MAX_COLLECTOR_PARALLELISM = Setting.intSetting(
        "datafusion.indexed.max_collector_parallelism",
        1,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // ── All settings registered by the plugin ──

    public static final List<Setting<?>> ALL_SETTINGS = List.of(

        // Runtime settings — memory pool, spill, and reduce input mode
        DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT,
        DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT,
        DataFusionPlugin.DATAFUSION_REDUCE_INPUT_MODE,

        // Cache settings — metadata and statistics cache configuration
        CacheSettings.METADATA_CACHE_SIZE_LIMIT,
        CacheSettings.STATISTICS_CACHE_SIZE_LIMIT,
        CacheSettings.METADATA_CACHE_EVICTION_TYPE,
        CacheSettings.STATISTICS_CACHE_EVICTION_TYPE,
        CacheSettings.METADATA_CACHE_ENABLED,
        CacheSettings.STATISTICS_CACHE_ENABLED,

        // Indexed query settings — per-query tuning knobs for the indexed execution path
        INDEXED_BATCH_SIZE,
        INDEXED_PARQUET_PUSHDOWN_FILTERS,
        INDEXED_MIN_SKIP_RUN_DEFAULT,
        INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD,
        INDEXED_SINGLE_COLLECTOR_STRATEGY,
        INDEXED_TREE_COLLECTOR_STRATEGY,
        INDEXED_MAX_COLLECTOR_PARALLELISM
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
            .batchSize(INDEXED_BATCH_SIZE.get(settings))
            .targetPartitions(deriveTargetPartitions(this.concurrentSearchMode, this.maxSliceCount))
            .parquetPushdownFilters(INDEXED_PARQUET_PUSHDOWN_FILTERS.get(settings))
            .minSkipRunDefault(INDEXED_MIN_SKIP_RUN_DEFAULT.get(settings))
            .minSkipRunSelectivityThreshold(INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.get(settings))
            .singleCollectorStrategy(strategyToWireValue(INDEXED_SINGLE_COLLECTOR_STRATEGY.get(settings)))
            .treeCollectorStrategy(strategyToWireValue(INDEXED_TREE_COLLECTOR_STRATEGY.get(settings)))
            .maxCollectorParallelism(INDEXED_MAX_COLLECTOR_PARALLELISM.get(settings))
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
            .batchSize(INDEXED_BATCH_SIZE.get(settings))
            .targetPartitions(deriveTargetPartitions(this.concurrentSearchMode, this.maxSliceCount))
            .parquetPushdownFilters(INDEXED_PARQUET_PUSHDOWN_FILTERS.get(settings))
            .minSkipRunDefault(INDEXED_MIN_SKIP_RUN_DEFAULT.get(settings))
            .minSkipRunSelectivityThreshold(INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.get(settings))
            .singleCollectorStrategy(strategyToWireValue(INDEXED_SINGLE_COLLECTOR_STRATEGY.get(settings)))
            .treeCollectorStrategy(strategyToWireValue(INDEXED_TREE_COLLECTOR_STRATEGY.get(settings)))
            .maxCollectorParallelism(INDEXED_MAX_COLLECTOR_PARALLELISM.get(settings))
            .build();
    }

    void registerListeners(ClusterSettings clusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(INDEXED_BATCH_SIZE, newValue -> {
            snapshot = WireConfigSnapshot.builder(snapshot).batchSize(newValue).build();
        });

        clusterSettings.addSettingsUpdateConsumer(INDEXED_PARQUET_PUSHDOWN_FILTERS, newValue -> {
            snapshot = WireConfigSnapshot.builder(snapshot).parquetPushdownFilters(newValue).build();
        });

        clusterSettings.addSettingsUpdateConsumer(INDEXED_MIN_SKIP_RUN_DEFAULT, newValue -> {
            snapshot = WireConfigSnapshot.builder(snapshot).minSkipRunDefault(newValue).build();
        });

        clusterSettings.addSettingsUpdateConsumer(INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD, newValue -> {
            snapshot = WireConfigSnapshot.builder(snapshot).minSkipRunSelectivityThreshold(newValue).build();
        });

        clusterSettings.addSettingsUpdateConsumer(INDEXED_SINGLE_COLLECTOR_STRATEGY, newValue -> {
            snapshot = WireConfigSnapshot.builder(snapshot).singleCollectorStrategy(strategyToWireValue(newValue)).build();
        });

        clusterSettings.addSettingsUpdateConsumer(INDEXED_TREE_COLLECTOR_STRATEGY, newValue -> {
            snapshot = WireConfigSnapshot.builder(snapshot).treeCollectorStrategy(strategyToWireValue(newValue)).build();
        });

        clusterSettings.addSettingsUpdateConsumer(INDEXED_MAX_COLLECTOR_PARALLELISM, newValue -> {
            snapshot = WireConfigSnapshot.builder(snapshot).maxCollectorParallelism(newValue).build();
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
     * Converts a strategy string to its wire format integer value.
     * <p>
     * Mapping: full_range = 0, tighten_outer_bounds = 1, page_range_split = 2.
     */
    static int strategyToWireValue(String strategy) {
        switch (strategy) {
            case STRATEGY_FULL_RANGE:
                return 0;
            case STRATEGY_TIGHTEN_OUTER_BOUNDS:
                return 1;
            case STRATEGY_PAGE_RANGE_SPLIT:
                return 2;
            default:
                throw new IllegalArgumentException("Unknown strategy: " + strategy);
        }
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
