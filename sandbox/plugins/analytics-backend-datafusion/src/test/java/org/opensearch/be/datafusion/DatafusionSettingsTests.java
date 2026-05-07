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
        assertTrue("batch_size must be dynamic", DatafusionSettings.INDEXED_BATCH_SIZE.isDynamic());
        assertTrue("batch_size must have node scope", DatafusionSettings.INDEXED_BATCH_SIZE.hasNodeScope());
    }

    public void testParquetPushdownFiltersSettingDefinition() {
        assertEquals("datafusion.indexed.parquet_pushdown_filters", DatafusionSettings.INDEXED_PARQUET_PUSHDOWN_FILTERS.getKey());
        assertEquals(Boolean.FALSE, DatafusionSettings.INDEXED_PARQUET_PUSHDOWN_FILTERS.get(Settings.EMPTY));
        assertTrue("parquet_pushdown_filters must be dynamic", DatafusionSettings.INDEXED_PARQUET_PUSHDOWN_FILTERS.isDynamic());
        assertTrue("parquet_pushdown_filters must have node scope", DatafusionSettings.INDEXED_PARQUET_PUSHDOWN_FILTERS.hasNodeScope());
    }

    public void testMinSkipRunDefaultSettingDefinition() {
        assertEquals("datafusion.indexed.min_skip_run_default", DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT.getKey());
        assertEquals(Integer.valueOf(1024), DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT.get(Settings.EMPTY));
        assertTrue("min_skip_run_default must be dynamic", DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT.isDynamic());
        assertTrue("min_skip_run_default must have node scope", DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT.hasNodeScope());
    }

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

    public void testCostPredicateSettingDefinition() {
        assertEquals("datafusion.indexed.cost_predicate", DatafusionSettings.INDEXED_COST_PREDICATE.getKey());
        assertEquals(Integer.valueOf(1), DatafusionSettings.INDEXED_COST_PREDICATE.get(Settings.EMPTY));
        assertTrue("cost_predicate must be dynamic", DatafusionSettings.INDEXED_COST_PREDICATE.isDynamic());
        assertTrue("cost_predicate must have node scope", DatafusionSettings.INDEXED_COST_PREDICATE.hasNodeScope());
    }

    public void testCostCollectorSettingDefinition() {
        assertEquals("datafusion.indexed.cost_collector", DatafusionSettings.INDEXED_COST_COLLECTOR.getKey());
        assertEquals(Integer.valueOf(10), DatafusionSettings.INDEXED_COST_COLLECTOR.get(Settings.EMPTY));
        assertTrue("cost_collector must be dynamic", DatafusionSettings.INDEXED_COST_COLLECTOR.isDynamic());
        assertTrue("cost_collector must have node scope", DatafusionSettings.INDEXED_COST_COLLECTOR.hasNodeScope());
    }

    public void testMaxCollectorParallelismSettingDefinition() {
        assertEquals("datafusion.indexed.max_collector_parallelism", DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM.getKey());
        assertEquals(Integer.valueOf(1), DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM.get(Settings.EMPTY));
        assertTrue("max_collector_parallelism must be dynamic", DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM.isDynamic());
        assertTrue("max_collector_parallelism must have node scope", DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM.hasNodeScope());
    }

    public void testAllSettingsContainsAllExpectedSettings() {
        assertEquals("ALL_SETTINGS must contain 16 entries (9 existing + 7 new)", 16, DatafusionSettings.ALL_SETTINGS.size());
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_BATCH_SIZE));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_PARQUET_PUSHDOWN_FILTERS));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_COST_PREDICATE));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_COST_COLLECTOR));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM));
    }

    public void testDefaultSnapshotValuesMatchDefaults() {
        DatafusionSettings ds = new DatafusionSettings(Settings.EMPTY);
        WireConfigSnapshot snapshot = ds.getSnapshot();

        assertEquals(8192, snapshot.batchSize());
        assertEquals(false, snapshot.parquetPushdownFilters());
        assertEquals(1024, snapshot.minSkipRunDefault());
        assertEquals(0.03, snapshot.minSkipRunSelectivityThreshold(), 1e-15);
        assertEquals(1, snapshot.costPredicate());
        assertEquals(10, snapshot.costCollector());
        assertEquals(1, snapshot.maxCollectorParallelism());
        assertEquals(DEFAULT_PARALLELISM, snapshot.targetPartitions());
    }

    public void testTargetPartitionsPassthroughWhenNonZero() {
        Settings settings = Settings.builder()
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey(), 8)
            .build();
        DatafusionSettings ds = new DatafusionSettings(settings);
        WireConfigSnapshot snapshot = ds.getSnapshot();

        assertEquals(Math.min(8, Runtime.getRuntime().availableProcessors()), snapshot.targetPartitions());
    }

    public void testTargetPartitionsFallbackWhenZero() {
        DatafusionSettings ds = new DatafusionSettings(Settings.EMPTY);
        WireConfigSnapshot snapshot = ds.getSnapshot();

        assertEquals(DEFAULT_PARALLELISM, snapshot.targetPartitions());
    }

    public void testTargetPartitionsForcedToOneWhenModeNone() {
        Settings settings = Settings.builder()
            .put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), "none")
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey(), 16)
            .build();
        DatafusionSettings ds = new DatafusionSettings(settings);
        WireConfigSnapshot snapshot = ds.getSnapshot();

        assertEquals(1, snapshot.targetPartitions());
    }

    public void testTargetPartitionsCappedAtAvailableProcessors() {
        int processors = Runtime.getRuntime().availableProcessors();
        int overCommit = processors + 10;
        Settings settings = Settings.builder()
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey(), overCommit)
            .build();
        DatafusionSettings ds = new DatafusionSettings(settings);
        WireConfigSnapshot snapshot = ds.getSnapshot();

        assertEquals(processors, snapshot.targetPartitions());
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
}
