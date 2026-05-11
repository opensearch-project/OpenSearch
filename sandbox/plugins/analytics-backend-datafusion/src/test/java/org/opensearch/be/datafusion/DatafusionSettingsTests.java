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

public class DatafusionSettingsTests extends OpenSearchTestCase {

    private static final int DEFAULT_PARALLELISM = Math.max(1, Math.min(Runtime.getRuntime().availableProcessors() / 2, 4));

    public void testBatchSizeSettingDefinition() {
        assertEquals("datafusion.indexed.batch_size", DatafusionSettings.INDEXED_BATCH_SIZE.getKey());
        assertEquals(Integer.valueOf(8192), DatafusionSettings.INDEXED_BATCH_SIZE.get(Settings.EMPTY));
        assertTrue(DatafusionSettings.INDEXED_BATCH_SIZE.isDynamic());
        assertTrue(DatafusionSettings.INDEXED_BATCH_SIZE.hasNodeScope());
    }

    public void testParquetPushdownFiltersSettingDefinition() {
        assertEquals("datafusion.indexed.parquet_pushdown_filters", DatafusionSettings.INDEXED_PARQUET_PUSHDOWN_FILTERS.getKey());
        assertEquals(Boolean.FALSE, DatafusionSettings.INDEXED_PARQUET_PUSHDOWN_FILTERS.get(Settings.EMPTY));
        assertTrue(DatafusionSettings.INDEXED_PARQUET_PUSHDOWN_FILTERS.isDynamic());
        assertTrue(DatafusionSettings.INDEXED_PARQUET_PUSHDOWN_FILTERS.hasNodeScope());
    }

    public void testMinSkipRunDefaultSettingDefinition() {
        assertEquals("datafusion.indexed.min_skip_run_default", DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT.getKey());
        assertEquals(Integer.valueOf(1024), DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT.get(Settings.EMPTY));
        assertTrue(DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT.isDynamic());
        assertTrue(DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT.hasNodeScope());
    }

    public void testMinSkipRunSelectivityThresholdSettingDefinition() {
        assertEquals(
            "datafusion.indexed.min_skip_run_selectivity_threshold",
            DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.getKey()
        );
        assertEquals(0.03, DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.get(Settings.EMPTY), 1e-15);
        assertTrue(DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.isDynamic());
        assertTrue(DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.hasNodeScope());
    }

    public void testSingleCollectorStrategySettingDefinition() {
        assertEquals("datafusion.indexed.single_collector_strategy", DatafusionSettings.INDEXED_SINGLE_COLLECTOR_STRATEGY.getKey());
        assertEquals("page_range_split", DatafusionSettings.INDEXED_SINGLE_COLLECTOR_STRATEGY.get(Settings.EMPTY));
        assertTrue(DatafusionSettings.INDEXED_SINGLE_COLLECTOR_STRATEGY.isDynamic());
        assertTrue(DatafusionSettings.INDEXED_SINGLE_COLLECTOR_STRATEGY.hasNodeScope());
    }

    public void testTreeCollectorStrategySettingDefinition() {
        assertEquals("datafusion.indexed.tree_collector_strategy", DatafusionSettings.INDEXED_TREE_COLLECTOR_STRATEGY.getKey());
        assertEquals("tighten_outer_bounds", DatafusionSettings.INDEXED_TREE_COLLECTOR_STRATEGY.get(Settings.EMPTY));
        assertTrue(DatafusionSettings.INDEXED_TREE_COLLECTOR_STRATEGY.isDynamic());
        assertTrue(DatafusionSettings.INDEXED_TREE_COLLECTOR_STRATEGY.hasNodeScope());
    }

    public void testMaxCollectorParallelismSettingDefinition() {
        assertEquals("datafusion.indexed.max_collector_parallelism", DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM.getKey());
        assertEquals(Integer.valueOf(1), DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM.get(Settings.EMPTY));
        assertTrue(DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM.isDynamic());
        assertTrue(DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM.hasNodeScope());
    }

    public void testAllSettingsContainsAllExpectedSettings() {
        assertEquals(18, DatafusionSettings.ALL_SETTINGS.size());
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT_MIN));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT_MIN));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_BATCH_SIZE));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_PARQUET_PUSHDOWN_FILTERS));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_SINGLE_COLLECTOR_STRATEGY));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_TREE_COLLECTOR_STRATEGY));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM));
    }

    public void testDefaultSnapshotValuesMatchDefaults() {
        DatafusionSettings ds = new DatafusionSettings(Settings.EMPTY);
        WireConfigSnapshot snapshot = ds.getSnapshot();

        assertEquals(8192, snapshot.batchSize());
        assertEquals(false, snapshot.parquetPushdownFilters());
        assertEquals(1024, snapshot.minSkipRunDefault());
        assertEquals(0.03, snapshot.minSkipRunSelectivityThreshold(), 1e-15);
        assertEquals(2, snapshot.singleCollectorStrategy()); // page_range_split
        assertEquals(1, snapshot.treeCollectorStrategy()); // tighten_outer_bounds
        assertEquals(1, snapshot.maxCollectorParallelism());
        assertEquals(DEFAULT_PARALLELISM, snapshot.targetPartitions());
    }

    public void testTargetPartitionsPassthroughWhenNonZero() {
        Settings settings = Settings.builder()
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey(), 8)
            .build();
        DatafusionSettings ds = new DatafusionSettings(settings);

        assertEquals(Math.min(8, Runtime.getRuntime().availableProcessors()), ds.getSnapshot().targetPartitions());
    }

    public void testTargetPartitionsFallbackWhenZero() {
        DatafusionSettings ds = new DatafusionSettings(Settings.EMPTY);

        assertEquals(DEFAULT_PARALLELISM, ds.getSnapshot().targetPartitions());
    }

    public void testTargetPartitionsForcedToOneWhenModeNone() {
        Settings settings = Settings.builder()
            .put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), "none")
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey(), 16)
            .build();
        DatafusionSettings ds = new DatafusionSettings(settings);

        assertEquals(1, ds.getSnapshot().targetPartitions());
    }

    public void testTargetPartitionsCappedAtAvailableProcessors() {
        int processors = Runtime.getRuntime().availableProcessors();
        Settings settings = Settings.builder()
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey(), processors + 10)
            .build();
        DatafusionSettings ds = new DatafusionSettings(settings);

        assertEquals(processors, ds.getSnapshot().targetPartitions());
    }

    public void testStrategyToWireValueMapping() {
        assertEquals(0, DatafusionSettings.strategyToWireValue("full_range"));
        assertEquals(1, DatafusionSettings.strategyToWireValue("tighten_outer_bounds"));
        assertEquals(2, DatafusionSettings.strategyToWireValue("page_range_split"));
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.strategyToWireValue("invalid"));
    }

    public void testBatchSizeZeroIsRejected() {
        Settings settings = Settings.builder().put("datafusion.indexed.batch_size", 0).build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.INDEXED_BATCH_SIZE.get(settings));
    }

    public void testMaxCollectorParallelismNegativeIsRejected() {
        Settings settings = Settings.builder().put("datafusion.indexed.max_collector_parallelism", -1).build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM.get(settings));
    }

    public void testSelectivityThresholdAboveBoundIsRejected() {
        Settings settings = Settings.builder().put("datafusion.indexed.min_skip_run_selectivity_threshold", 1.1).build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.get(settings));
    }

    public void testInvalidSingleCollectorStrategyIsRejected() {
        Settings settings = Settings.builder().put("datafusion.indexed.single_collector_strategy", "bogus").build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.INDEXED_SINGLE_COLLECTOR_STRATEGY.get(settings));
    }

    public void testInvalidTreeCollectorStrategyIsRejected() {
        Settings settings = Settings.builder().put("datafusion.indexed.tree_collector_strategy", "bogus").build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.INDEXED_TREE_COLLECTOR_STRATEGY.get(settings));
    }
}
