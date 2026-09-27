/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.SearchService;
import org.opensearch.test.OpenSearchTestCase;

public class DatafusionSettingsTests extends OpenSearchTestCase {

    private static final int DEFAULT_PARALLELISM = Math.max(1, Math.min(Runtime.getRuntime().availableProcessors() / 2, 4));

    public void testBatchSizeSettingDefinition() {
        assertEquals("datafusion.batch_size", DatafusionSettings.BATCH_SIZE.getKey());
        assertEquals(Integer.valueOf(8192), DatafusionSettings.BATCH_SIZE.get(Settings.EMPTY));
        assertTrue(DatafusionSettings.BATCH_SIZE.isDynamic());
        assertTrue(DatafusionSettings.BATCH_SIZE.hasNodeScope());
    }

    public void testListingTablePushdownFiltersSettingDefinition() {
        assertEquals("datafusion.listing_table.pushdown_filters", DatafusionSettings.LISTING_TABLE_PUSHDOWN_FILTERS.getKey());
        assertEquals(Boolean.FALSE, DatafusionSettings.LISTING_TABLE_PUSHDOWN_FILTERS.get(Settings.EMPTY));
        assertTrue(DatafusionSettings.LISTING_TABLE_PUSHDOWN_FILTERS.isDynamic());
        assertTrue(DatafusionSettings.LISTING_TABLE_PUSHDOWN_FILTERS.hasNodeScope());
    }

    public void testIndexedPushdownFiltersSettingDefinition() {
        assertEquals("datafusion.indexed.pushdown_filters", DatafusionSettings.INDEXED_PUSHDOWN_FILTERS.getKey());
        assertEquals(Boolean.TRUE, DatafusionSettings.INDEXED_PUSHDOWN_FILTERS.get(Settings.EMPTY));
        assertTrue(DatafusionSettings.INDEXED_PUSHDOWN_FILTERS.isDynamic());
        assertTrue(DatafusionSettings.INDEXED_PUSHDOWN_FILTERS.hasNodeScope());
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

    public void testNodeScopedSettingsContainsAllExpectedSettings() {
        assertEquals(31, DatafusionSettings.NODE_SCOPED_SETTINGS.size());
        assertTrue(DatafusionSettings.NODE_SCOPED_SETTINGS.contains(DataFusionPlugin.DATAFUSION_REDUCE_TARGET_PARTITIONS));
        assertTrue(DatafusionSettings.NODE_SCOPED_SETTINGS.contains(DataFusionPlugin.DATAFUSION_MEMORY_GUARD_SPILL_EXEMPT_CAP));
        assertTrue(DatafusionSettings.NODE_SCOPED_SETTINGS.contains(DataFusionPlugin.DATAFUSION_SPILL_DIRECTORY));
        assertTrue(DatafusionSettings.NODE_SCOPED_SETTINGS.contains(DatafusionSettings.BATCH_SIZE));
        assertTrue(DatafusionSettings.NODE_SCOPED_SETTINGS.contains(DatafusionSettings.LISTING_TABLE_PUSHDOWN_FILTERS));
        assertTrue(DatafusionSettings.NODE_SCOPED_SETTINGS.contains(DatafusionSettings.INDEXED_PUSHDOWN_FILTERS));
        assertTrue(DatafusionSettings.NODE_SCOPED_SETTINGS.contains(DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT));
        assertTrue(DatafusionSettings.NODE_SCOPED_SETTINGS.contains(DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD));
        assertTrue(DatafusionSettings.NODE_SCOPED_SETTINGS.contains(DatafusionSettings.INDEXED_FORCE_STRATEGY));
    }

    /**
     * {@code NODE_SCOPED_SETTINGS} is handed to a {@code ClusterSettings}, which rejects anything that is
     * not {@code NodeScope}, so index-scoped settings must stay in {@code INDEX_SCOPED_SETTINGS}.
     */
    public void testNodeScopedSettingsHoldOnlyNodeScope() {
        for (Setting<?> setting : DatafusionSettings.NODE_SCOPED_SETTINGS) {
            assertTrue("NODE_SCOPED_SETTINGS must be NodeScope only, found: " + setting.getKey(), setting.hasNodeScope());
        }
    }

    public void testIndexScopedSettingsHoldsTheDocValuesBatchSizes() {
        assertEquals(2, DatafusionSettings.INDEX_SCOPED_SETTINGS.size());
        assertTrue(DatafusionSettings.INDEX_SCOPED_SETTINGS.contains(DatafusionSettings.DOCVALUES_INITIAL_BATCH_SIZE));
        assertTrue(DatafusionSettings.INDEX_SCOPED_SETTINGS.contains(DatafusionSettings.DOCVALUES_MAX_BATCH_SIZE));
    }

    public void testDocValuesBatchSizeSettingsAreIndexScopedAndDynamic() {
        assertTrue(DatafusionSettings.DOCVALUES_INITIAL_BATCH_SIZE.hasIndexScope());
        assertTrue(DatafusionSettings.DOCVALUES_INITIAL_BATCH_SIZE.isDynamic());
        assertTrue(DatafusionSettings.DOCVALUES_MAX_BATCH_SIZE.hasIndexScope());
        assertTrue(DatafusionSettings.DOCVALUES_MAX_BATCH_SIZE.isDynamic());
    }

    public void testDocValuesInitialBatchSizeIsResolvedDownToTheCeiling() {
        // The two settings are independent, so a configuration that asks to start above the ceiling
        // is resolved down rather than rejected.
        Settings settings = Settings.builder()
            .put(DatafusionSettings.DOCVALUES_INITIAL_BATCH_SIZE.getKey(), 4096)
            .put(DatafusionSettings.DOCVALUES_MAX_BATCH_SIZE.getKey(), 64)
            .build();
        assertEquals(64, DatafusionSettings.docValuesInitialBatchSize(settings));
        assertEquals(64, DatafusionSettings.docValuesMaxBatchSize(settings));
    }

    public void testDocValuesBatchSizeDefaults() {
        assertEquals(DatafusionSettings.DEFAULT_DOCVALUES_INITIAL_BATCH_SIZE, DatafusionSettings.docValuesInitialBatchSize(Settings.EMPTY));
        assertEquals(DatafusionSettings.DEFAULT_DOCVALUES_MAX_BATCH_SIZE, DatafusionSettings.docValuesMaxBatchSize(Settings.EMPTY));
    }

    public void testForceStrategySettingDefaultsAndMapping() {
        assertEquals("datafusion.indexed.force_strategy", DatafusionSettings.INDEXED_FORCE_STRATEGY.getKey());
        assertEquals("none", DatafusionSettings.INDEXED_FORCE_STRATEGY.get(Settings.EMPTY));
        assertTrue(DatafusionSettings.INDEXED_FORCE_STRATEGY.isDynamic());
        assertTrue(DatafusionSettings.INDEXED_FORCE_STRATEGY.hasNodeScope());

        assertEquals(-1, DatafusionSettings.forceStrategyToWire("none"));
        assertEquals(0, DatafusionSettings.forceStrategyToWire("row_selection"));
        assertEquals(1, DatafusionSettings.forceStrategyToWire("boolean_mask"));

        // Default snapshot encodes None (-1).
        assertEquals(-1, new DatafusionSettings(Settings.EMPTY).getSnapshot().forceStrategy());

        // A configured value flows into the snapshot.
        DatafusionSettings ds = new DatafusionSettings(Settings.builder().put("datafusion.indexed.force_strategy", "boolean_mask").build());
        assertEquals(1, ds.getSnapshot().forceStrategy());

        // Invalid values are rejected at parse time.
        expectThrows(
            IllegalArgumentException.class,
            () -> DatafusionSettings.INDEXED_FORCE_STRATEGY.get(
                Settings.builder().put("datafusion.indexed.force_strategy", "bogus").build()
            )
        );
    }

    public void testDefaultSnapshotValuesMatchDefaults() {
        DatafusionSettings ds = new DatafusionSettings(Settings.EMPTY);
        WireConfigSnapshot snapshot = ds.getSnapshot();

        assertEquals(8192, snapshot.batchSize());
        assertEquals(false, snapshot.listingTablePushdownFilters());
        assertEquals(1024, snapshot.minSkipRunDefault());
        assertEquals(0.03, snapshot.minSkipRunSelectivityThreshold(), 1e-15);
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

    public void testBatchSizeZeroIsRejected() {
        Settings settings = Settings.builder().put("datafusion.batch_size", 0).build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.BATCH_SIZE.get(settings));
    }

    public void testSelectivityThresholdAboveBoundIsRejected() {
        Settings settings = Settings.builder().put("datafusion.indexed.min_skip_run_selectivity_threshold", 1.1).build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.get(settings));
    }
}
