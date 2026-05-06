/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.cache.CacheSettings;
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

    // ── Computed default for max_collector_parallelism and target_partitions fallback ──
    private static final int DEFAULT_PARALLELISM = Math.max(1, Math.min(Runtime.getRuntime().availableProcessors() / 2, 4));

    // ── New indexed query settings ──

    public static final Setting<Integer> INDEXED_BATCH_SIZE = Setting.intSetting(
        "datafusion.indexed.batch_size",
        8192,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> INDEXED_PARQUET_PUSHDOWN_FILTERS = Setting.boolSetting(
        "datafusion.indexed.parquet_pushdown_filters",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> INDEXED_MIN_SKIP_RUN_DEFAULT = Setting.intSetting(
        "datafusion.indexed.min_skip_run_default",
        1024,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Double> INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD = Setting.doubleSetting(
        "datafusion.indexed.min_skip_run_selectivity_threshold",
        0.03,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> INDEXED_COST_PREDICATE = Setting.intSetting(
        "datafusion.indexed.cost_predicate",
        1,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> INDEXED_COST_COLLECTOR = Setting.intSetting(
        "datafusion.indexed.cost_collector",
        10,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> INDEXED_MAX_COLLECTOR_PARALLELISM = Setting.intSetting(
        "datafusion.indexed.max_collector_parallelism",
        DEFAULT_PARALLELISM,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // ── All settings list (existing re-exports + new indexed settings) ──

    public static final List<Setting<?>> ALL_SETTINGS = List.of(
        // Existing settings re-exported from DataFusionPlugin
        DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT,
        DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT,
        DataFusionPlugin.DATAFUSION_REDUCE_INPUT_MODE,
        // Existing settings re-exported from CacheSettings
        CacheSettings.METADATA_CACHE_SIZE_LIMIT,
        CacheSettings.STATISTICS_CACHE_SIZE_LIMIT,
        CacheSettings.METADATA_CACHE_EVICTION_TYPE,
        CacheSettings.STATISTICS_CACHE_EVICTION_TYPE,
        CacheSettings.METADATA_CACHE_ENABLED,
        CacheSettings.STATISTICS_CACHE_ENABLED,
        // New indexed query settings
        INDEXED_BATCH_SIZE,
        INDEXED_PARQUET_PUSHDOWN_FILTERS,
        INDEXED_MIN_SKIP_RUN_DEFAULT,
        INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD,
        INDEXED_COST_PREDICATE,
        INDEXED_COST_COLLECTOR,
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
     * Creates the settings holder and builds the initial {@link WireConfigSnapshot}
     * from the provided node settings.
     *
     * @param initialSettings the node-level settings at startup
     */
    public DatafusionSettings(Settings initialSettings) {
        int batchSize = INDEXED_BATCH_SIZE.get(initialSettings);
        boolean parquetPushdownFilters = INDEXED_PARQUET_PUSHDOWN_FILTERS.get(initialSettings);
        int minSkipRunDefault = INDEXED_MIN_SKIP_RUN_DEFAULT.get(initialSettings);
        double minSkipRunSelectivityThreshold = INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.get(initialSettings);
        int costPredicate = INDEXED_COST_PREDICATE.get(initialSettings);
        int costCollector = INDEXED_COST_COLLECTOR.get(initialSettings);
        int maxCollectorParallelism = INDEXED_MAX_COLLECTOR_PARALLELISM.get(initialSettings);

        this.maxSliceCount = SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.get(initialSettings);
        int targetPartitions = deriveTargetPartitions(this.maxSliceCount);

        this.snapshot = new WireConfigSnapshot(
            batchSize,
            targetPartitions,
            parquetPushdownFilters,
            minSkipRunDefault,
            minSkipRunSelectivityThreshold,
            costPredicate,
            costCollector,
            maxCollectorParallelism
        );
    }

    /**
     * Registers {@code addSettingsUpdateConsumer} listeners for each dynamic indexed
     * setting and for {@code search.concurrent.max_slice_count}. Each listener
     * atomically rebuilds the snapshot with the updated value.
     *
     * @param clusterSettings the cluster settings to register listeners on
     */
    public void registerListeners(ClusterSettings clusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(INDEXED_BATCH_SIZE, newValue -> {
            WireConfigSnapshot current = snapshot;
            snapshot = new WireConfigSnapshot(
                newValue,
                current.targetPartitions(),
                current.parquetPushdownFilters(),
                current.minSkipRunDefault(),
                current.minSkipRunSelectivityThreshold(),
                current.costPredicate(),
                current.costCollector(),
                current.maxCollectorParallelism()
            );
        });

        clusterSettings.addSettingsUpdateConsumer(INDEXED_PARQUET_PUSHDOWN_FILTERS, newValue -> {
            WireConfigSnapshot current = snapshot;
            snapshot = new WireConfigSnapshot(
                current.batchSize(),
                current.targetPartitions(),
                newValue,
                current.minSkipRunDefault(),
                current.minSkipRunSelectivityThreshold(),
                current.costPredicate(),
                current.costCollector(),
                current.maxCollectorParallelism()
            );
        });

        clusterSettings.addSettingsUpdateConsumer(INDEXED_MIN_SKIP_RUN_DEFAULT, newValue -> {
            WireConfigSnapshot current = snapshot;
            snapshot = new WireConfigSnapshot(
                current.batchSize(),
                current.targetPartitions(),
                current.parquetPushdownFilters(),
                newValue,
                current.minSkipRunSelectivityThreshold(),
                current.costPredicate(),
                current.costCollector(),
                current.maxCollectorParallelism()
            );
        });

        clusterSettings.addSettingsUpdateConsumer(INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD, newValue -> {
            WireConfigSnapshot current = snapshot;
            snapshot = new WireConfigSnapshot(
                current.batchSize(),
                current.targetPartitions(),
                current.parquetPushdownFilters(),
                current.minSkipRunDefault(),
                newValue,
                current.costPredicate(),
                current.costCollector(),
                current.maxCollectorParallelism()
            );
        });

        clusterSettings.addSettingsUpdateConsumer(INDEXED_COST_PREDICATE, newValue -> {
            WireConfigSnapshot current = snapshot;
            snapshot = new WireConfigSnapshot(
                current.batchSize(),
                current.targetPartitions(),
                current.parquetPushdownFilters(),
                current.minSkipRunDefault(),
                current.minSkipRunSelectivityThreshold(),
                newValue,
                current.costCollector(),
                current.maxCollectorParallelism()
            );
        });

        clusterSettings.addSettingsUpdateConsumer(INDEXED_COST_COLLECTOR, newValue -> {
            WireConfigSnapshot current = snapshot;
            snapshot = new WireConfigSnapshot(
                current.batchSize(),
                current.targetPartitions(),
                current.parquetPushdownFilters(),
                current.minSkipRunDefault(),
                current.minSkipRunSelectivityThreshold(),
                current.costPredicate(),
                newValue,
                current.maxCollectorParallelism()
            );
        });

        clusterSettings.addSettingsUpdateConsumer(INDEXED_MAX_COLLECTOR_PARALLELISM, newValue -> {
            WireConfigSnapshot current = snapshot;
            snapshot = new WireConfigSnapshot(
                current.batchSize(),
                current.targetPartitions(),
                current.parquetPushdownFilters(),
                current.minSkipRunDefault(),
                current.minSkipRunSelectivityThreshold(),
                current.costPredicate(),
                current.costCollector(),
                newValue
            );
        });

        clusterSettings.addSettingsUpdateConsumer(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING, newValue -> {
            this.maxSliceCount = newValue;
            WireConfigSnapshot current = snapshot;
            snapshot = new WireConfigSnapshot(
                current.batchSize(),
                deriveTargetPartitions(newValue),
                current.parquetPushdownFilters(),
                current.minSkipRunDefault(),
                current.minSkipRunSelectivityThreshold(),
                current.costPredicate(),
                current.costCollector(),
                current.maxCollectorParallelism()
            );
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
     * Derives {@code target_partitions} from the {@code search.concurrent.max_slice_count}
     * setting value. When the value is greater than 0, it is used directly (passthrough).
     * When 0 (meaning Lucene default slice computation), falls back to
     * {@code max(1, min(availableProcessors/2, 4))}.
     */
    private static int deriveTargetPartitions(int maxSliceCount) {
        if (maxSliceCount > 0) {
            return maxSliceCount;
        }
        return DEFAULT_PARALLELISM;
    }
}
