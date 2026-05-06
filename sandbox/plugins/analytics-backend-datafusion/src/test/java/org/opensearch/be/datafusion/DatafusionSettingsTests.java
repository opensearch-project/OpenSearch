/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.settings.Settings;
import org.opensearch.search.SearchService;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link DatafusionSettings} — verifies setting definitions (keys,
 * defaults, bounds, properties), the {@code ALL_SETTINGS} list, default snapshot
 * values, {@code target_partitions} derivation logic, and validation rejection for
 * boundary values.
 * <p>
 * These are example-based tests using fixed/known values (not randomized).
 * <p>
 * Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 4.1, 4.2, 4.3
 */
public class DatafusionSettingsTests extends OpenSearchTestCase {

    private static final int DEFAULT_PARALLELISM = Math.max(1, Math.min(Runtime.getRuntime().availableProcessors() / 2, 4));

    // ── Setting definition tests ──

    /**
     * Validates: Requirement 1.1
     * batch_size key is "datafusion.indexed.batch_size", default 8192, isDynamic, hasNodeScope
     */
    public void testBatchSizeSettingDefinition() {
        assertEquals("datafusion.indexed.batch_size", DatafusionSettings.INDEXED_BATCH_SIZE.getKey());
        assertEquals(Integer.valueOf(8192), DatafusionSettings.INDEXED_BATCH_SIZE.get(Settings.EMPTY));
        assertTrue("batch_size must be dynamic", DatafusionSettings.INDEXED_BATCH_SIZE.isDynamic());
        assertTrue("batch_size must have node scope", DatafusionSettings.INDEXED_BATCH_SIZE.hasNodeScope());
    }

    /**
     * Validates: Requirement 1.3
     * parquet_pushdown_filters key, default false, isDynamic, hasNodeScope
     */
    public void testParquetPushdownFiltersSettingDefinition() {
        assertEquals("datafusion.indexed.parquet_pushdown_filters", DatafusionSettings.INDEXED_PARQUET_PUSHDOWN_FILTERS.getKey());
        assertEquals(Boolean.FALSE, DatafusionSettings.INDEXED_PARQUET_PUSHDOWN_FILTERS.get(Settings.EMPTY));
        assertTrue("parquet_pushdown_filters must be dynamic", DatafusionSettings.INDEXED_PARQUET_PUSHDOWN_FILTERS.isDynamic());
        assertTrue("parquet_pushdown_filters must have node scope", DatafusionSettings.INDEXED_PARQUET_PUSHDOWN_FILTERS.hasNodeScope());
    }

    /**
     * Validates: Requirement 1.4
     * min_skip_run_default key, default 1024, isDynamic, hasNodeScope
     */
    public void testMinSkipRunDefaultSettingDefinition() {
        assertEquals("datafusion.indexed.min_skip_run_default", DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT.getKey());
        assertEquals(Integer.valueOf(1024), DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT.get(Settings.EMPTY));
        assertTrue("min_skip_run_default must be dynamic", DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT.isDynamic());
        assertTrue("min_skip_run_default must have node scope", DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT.hasNodeScope());
    }

    /**
     * Validates: Requirement 1.5
     * min_skip_run_selectivity_threshold key, default 0.03, isDynamic, hasNodeScope
     */
    public void testMinSkipRunSelectivityThresholdSettingDefinition() {
        assertEquals(
            "datafusion.indexed.min_skip_run_selectivity_threshold",
            DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.getKey()
        );
        assertEquals(0.03, DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.get(Settings.EMPTY), 1e-15);
        assertTrue(
            "min_skip_run_selectivity_threshold must be dynamic",
            DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.isDynamic()
        );
        assertTrue(
            "min_skip_run_selectivity_threshold must have node scope",
            DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.hasNodeScope()
        );
    }

    /**
     * Validates: Requirement 1.6
     * cost_predicate key, default 1, isDynamic, hasNodeScope
     */
    public void testCostPredicateSettingDefinition() {
        assertEquals("datafusion.indexed.cost_predicate", DatafusionSettings.INDEXED_COST_PREDICATE.getKey());
        assertEquals(Integer.valueOf(1), DatafusionSettings.INDEXED_COST_PREDICATE.get(Settings.EMPTY));
        assertTrue("cost_predicate must be dynamic", DatafusionSettings.INDEXED_COST_PREDICATE.isDynamic());
        assertTrue("cost_predicate must have node scope", DatafusionSettings.INDEXED_COST_PREDICATE.hasNodeScope());
    }

    /**
     * Validates: Requirement 1.7
     * cost_collector key, default 10, isDynamic, hasNodeScope
     */
    public void testCostCollectorSettingDefinition() {
        assertEquals("datafusion.indexed.cost_collector", DatafusionSettings.INDEXED_COST_COLLECTOR.getKey());
        assertEquals(Integer.valueOf(10), DatafusionSettings.INDEXED_COST_COLLECTOR.get(Settings.EMPTY));
        assertTrue("cost_collector must be dynamic", DatafusionSettings.INDEXED_COST_COLLECTOR.isDynamic());
        assertTrue("cost_collector must have node scope", DatafusionSettings.INDEXED_COST_COLLECTOR.hasNodeScope());
    }

    /**
     * Validates: Requirement 1.8
     * max_collector_parallelism key, default max(1, min(availableProcessors/2, 4)), isDynamic, hasNodeScope
     */
    public void testMaxCollectorParallelismSettingDefinition() {
        assertEquals("datafusion.indexed.max_collector_parallelism", DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM.getKey());
        assertEquals(Integer.valueOf(DEFAULT_PARALLELISM), DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM.get(Settings.EMPTY));
        assertTrue("max_collector_parallelism must be dynamic", DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM.isDynamic());
        assertTrue("max_collector_parallelism must have node scope", DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM.hasNodeScope());
    }

    // ── ALL_SETTINGS list test ──

    /**
     * Validates: Requirements 1.1–1.8, 5.2
     * ALL_SETTINGS has 16 entries (9 existing + 7 new) and contains each new setting.
     */
    public void testAllSettingsContainsAllExpectedSettings() {
        assertEquals("ALL_SETTINGS must contain 16 entries (9 existing + 7 new)", 16, DatafusionSettings.ALL_SETTINGS.size());
        assertTrue(
            "ALL_SETTINGS must contain INDEXED_BATCH_SIZE",
            DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_BATCH_SIZE)
        );
        assertTrue(
            "ALL_SETTINGS must contain INDEXED_PARQUET_PUSHDOWN_FILTERS",
            DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_PARQUET_PUSHDOWN_FILTERS)
        );
        assertTrue(
            "ALL_SETTINGS must contain INDEXED_MIN_SKIP_RUN_DEFAULT",
            DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT)
        );
        assertTrue(
            "ALL_SETTINGS must contain INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD",
            DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD)
        );
        assertTrue(
            "ALL_SETTINGS must contain INDEXED_COST_PREDICATE",
            DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_COST_PREDICATE)
        );
        assertTrue(
            "ALL_SETTINGS must contain INDEXED_COST_COLLECTOR",
            DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_COST_COLLECTOR)
        );
        assertTrue(
            "ALL_SETTINGS must contain INDEXED_MAX_COLLECTOR_PARALLELISM",
            DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM)
        );
    }

    // ── Default snapshot values test ──

    /**
     * Validates: Requirements 1.1–1.8, 3.5
     * Creating DatafusionSettings with Settings.EMPTY produces a snapshot with all default values.
     */
    public void testDefaultSnapshotValuesMatchDefaults() {
        DatafusionSettings ds = new DatafusionSettings(Settings.EMPTY);
        WireConfigSnapshot snapshot = ds.getSnapshot();

        assertEquals("default batchSize must be 8192", 8192, snapshot.batchSize());
        assertEquals("default parquetPushdownFilters must be false", false, snapshot.parquetPushdownFilters());
        assertEquals("default minSkipRunDefault must be 1024", 1024, snapshot.minSkipRunDefault());
        assertEquals("default minSkipRunSelectivityThreshold must be 0.03", 0.03, snapshot.minSkipRunSelectivityThreshold(), 1e-15);
        assertEquals("default costPredicate must be 1", 1, snapshot.costPredicate());
        assertEquals("default costCollector must be 10", 10, snapshot.costCollector());
        assertEquals(
            "default maxCollectorParallelism must be DEFAULT_PARALLELISM",
            DEFAULT_PARALLELISM,
            snapshot.maxCollectorParallelism()
        );
        // target_partitions falls back to DEFAULT_PARALLELISM when max_slice_count is 0
        assertEquals("default targetPartitions must be DEFAULT_PARALLELISM", DEFAULT_PARALLELISM, snapshot.targetPartitions());
    }

    // ── target_partitions derivation tests ──

    /**
     * Validates: Requirement 1.2
     * When search.concurrent.max_slice_count is non-zero, target_partitions passes through directly.
     */
    public void testTargetPartitionsPassthroughWhenNonZero() {
        Settings settings = Settings.builder()
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey(), 8)
            .build();
        DatafusionSettings ds = new DatafusionSettings(settings);
        WireConfigSnapshot snapshot = ds.getSnapshot();

        assertEquals("targetPartitions must pass through max_slice_count when non-zero", 8, snapshot.targetPartitions());
    }

    /**
     * Validates: Requirement 1.2
     * When search.concurrent.max_slice_count is 0 (default), target_partitions falls back to
     * max(1, min(availableProcessors/2, 4)).
     */
    public void testTargetPartitionsFallbackWhenZero() {
        // Settings.EMPTY means max_slice_count defaults to 0
        DatafusionSettings ds = new DatafusionSettings(Settings.EMPTY);
        WireConfigSnapshot snapshot = ds.getSnapshot();

        assertEquals(
            "targetPartitions must fall back to max(1, min(availableProcessors/2, 4)) when max_slice_count is 0",
            DEFAULT_PARALLELISM,
            snapshot.targetPartitions()
        );
    }

    // ── Validation rejection tests ──

    /**
     * Validates: Requirement 4.1
     * batch_size=0 is below the minimum of 1 and must be rejected.
     */
    public void testBatchSizeZeroIsRejected() {
        Settings settings = Settings.builder().put("datafusion.indexed.batch_size", 0).build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.INDEXED_BATCH_SIZE.get(settings));
    }

    /**
     * Validates: Requirement 4.3
     * max_collector_parallelism=-1 is below the minimum of 1 and must be rejected.
     */
    public void testMaxCollectorParallelismNegativeIsRejected() {
        Settings settings = Settings.builder().put("datafusion.indexed.max_collector_parallelism", -1).build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM.get(settings));
    }

    /**
     * Validates: Requirement 4.2
     * min_skip_run_selectivity_threshold=1.1 is above the maximum of 1.0 and must be rejected.
     */
    public void testSelectivityThresholdAboveBoundIsRejected() {
        Settings settings = Settings.builder().put("datafusion.indexed.min_skip_run_selectivity_threshold", 1.1).build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.get(settings));
    }
}
