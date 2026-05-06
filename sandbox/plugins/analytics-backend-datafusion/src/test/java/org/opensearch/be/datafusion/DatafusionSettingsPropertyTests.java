/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.SearchService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;

/**
 * Property-based test for {@link DatafusionSettings} snapshot update consistency.
 * <p>
 * Feature: dynamic-indexed-query-settings, Property 1: Snapshot update consistency
 * <p>
 * Validates: Requirements 3.2
 * <p>
 * For any indexed query setting and for any valid value within that setting's defined
 * bounds, when the setting's update consumer fires with the new value, the resulting
 * {@link WireConfigSnapshot} SHALL contain that new value in the corresponding field
 * while all other fields remain unchanged from their prior values.
 */
public class DatafusionSettingsPropertyTests extends OpenSearchTestCase {

    private static final int ITERATIONS = 200;

    /** Setting index constants for random selection. */
    private static final int SETTING_BATCH_SIZE = 0;
    private static final int SETTING_PARQUET_PUSHDOWN_FILTERS = 1;
    private static final int SETTING_MIN_SKIP_RUN_DEFAULT = 2;
    private static final int SETTING_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD = 3;
    private static final int SETTING_COST_PREDICATE = 4;
    private static final int SETTING_COST_COLLECTOR = 5;
    private static final int SETTING_MAX_COLLECTOR_PARALLELISM = 6;
    private static final int SETTING_MAX_SLICE_COUNT = 7;
    private static final int NUM_SETTINGS = 8;

    /**
     * Creates a {@link ClusterSettings} instance with all required settings registered.
     */
    private ClusterSettings createClusterSettings() {
        Set<Setting<?>> settingsSet = new HashSet<>(DatafusionSettings.ALL_SETTINGS);
        settingsSet.add(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING);
        return new ClusterSettings(Settings.EMPTY, settingsSet);
    }

    /**
     * **Validates: Requirements 3.2**
     * <p>
     * Property 1: Snapshot update consistency — for any indexed query setting and
     * for any valid value, when the setting's update consumer fires, the resulting
     * snapshot contains the new value in the corresponding field while all other
     * fields remain unchanged.
     */
    public void testSnapshotUpdateConsistencyProperty() {
        for (int i = 0; i < ITERATIONS; i++) {
            DatafusionSettings datafusionSettings = new DatafusionSettings(Settings.EMPTY);
            ClusterSettings clusterSettings = createClusterSettings();
            datafusionSettings.registerListeners(clusterSettings);

            WireConfigSnapshot before = datafusionSettings.getSnapshot();

            int settingIndex = randomIntBetween(0, NUM_SETTINGS - 1);
            Settings newSettings;

            switch (settingIndex) {
                case SETTING_BATCH_SIZE:
                    int newBatchSize = randomIntBetween(1, 1_000_000);
                    newSettings = Settings.builder().put("datafusion.indexed.batch_size", newBatchSize).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterBatch = datafusionSettings.getSnapshot();
                    assertEquals("batchSize must reflect new value", newBatchSize, afterBatch.batchSize());
                    assertEquals("targetPartitions must be unchanged", before.targetPartitions(), afterBatch.targetPartitions());
                    assertEquals(
                        "parquetPushdownFilters must be unchanged",
                        before.parquetPushdownFilters(),
                        afterBatch.parquetPushdownFilters()
                    );
                    assertEquals("minSkipRunDefault must be unchanged", before.minSkipRunDefault(), afterBatch.minSkipRunDefault());
                    assertEquals(
                        "minSkipRunSelectivityThreshold must be unchanged",
                        before.minSkipRunSelectivityThreshold(),
                        afterBatch.minSkipRunSelectivityThreshold(),
                        0.0
                    );
                    assertEquals("costPredicate must be unchanged", before.costPredicate(), afterBatch.costPredicate());
                    assertEquals("costCollector must be unchanged", before.costCollector(), afterBatch.costCollector());
                    assertEquals(
                        "maxCollectorParallelism must be unchanged",
                        before.maxCollectorParallelism(),
                        afterBatch.maxCollectorParallelism()
                    );
                    break;

                case SETTING_PARQUET_PUSHDOWN_FILTERS:
                    boolean newPushdown = before.parquetPushdownFilters() == false;
                    newSettings = Settings.builder().put("datafusion.indexed.parquet_pushdown_filters", newPushdown).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterPushdown = datafusionSettings.getSnapshot();
                    assertEquals("parquetPushdownFilters must reflect new value", newPushdown, afterPushdown.parquetPushdownFilters());
                    assertEquals("batchSize must be unchanged", before.batchSize(), afterPushdown.batchSize());
                    assertEquals("targetPartitions must be unchanged", before.targetPartitions(), afterPushdown.targetPartitions());
                    assertEquals("minSkipRunDefault must be unchanged", before.minSkipRunDefault(), afterPushdown.minSkipRunDefault());
                    assertEquals(
                        "minSkipRunSelectivityThreshold must be unchanged",
                        before.minSkipRunSelectivityThreshold(),
                        afterPushdown.minSkipRunSelectivityThreshold(),
                        0.0
                    );
                    assertEquals("costPredicate must be unchanged", before.costPredicate(), afterPushdown.costPredicate());
                    assertEquals("costCollector must be unchanged", before.costCollector(), afterPushdown.costCollector());
                    assertEquals(
                        "maxCollectorParallelism must be unchanged",
                        before.maxCollectorParallelism(),
                        afterPushdown.maxCollectorParallelism()
                    );
                    break;

                case SETTING_MIN_SKIP_RUN_DEFAULT:
                    int newMinSkipRun = randomIntBetween(1, 100_000);
                    newSettings = Settings.builder().put("datafusion.indexed.min_skip_run_default", newMinSkipRun).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterSkipRun = datafusionSettings.getSnapshot();
                    assertEquals("minSkipRunDefault must reflect new value", newMinSkipRun, afterSkipRun.minSkipRunDefault());
                    assertEquals("batchSize must be unchanged", before.batchSize(), afterSkipRun.batchSize());
                    assertEquals("targetPartitions must be unchanged", before.targetPartitions(), afterSkipRun.targetPartitions());
                    assertEquals(
                        "parquetPushdownFilters must be unchanged",
                        before.parquetPushdownFilters(),
                        afterSkipRun.parquetPushdownFilters()
                    );
                    assertEquals(
                        "minSkipRunSelectivityThreshold must be unchanged",
                        before.minSkipRunSelectivityThreshold(),
                        afterSkipRun.minSkipRunSelectivityThreshold(),
                        0.0
                    );
                    assertEquals("costPredicate must be unchanged", before.costPredicate(), afterSkipRun.costPredicate());
                    assertEquals("costCollector must be unchanged", before.costCollector(), afterSkipRun.costCollector());
                    assertEquals(
                        "maxCollectorParallelism must be unchanged",
                        before.maxCollectorParallelism(),
                        afterSkipRun.maxCollectorParallelism()
                    );
                    break;

                case SETTING_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD:
                    double newThreshold = randomDoubleBetween(0.0, 1.0, true);
                    newSettings = Settings.builder().put("datafusion.indexed.min_skip_run_selectivity_threshold", newThreshold).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterThreshold = datafusionSettings.getSnapshot();
                    assertEquals(
                        "minSkipRunSelectivityThreshold must reflect new value",
                        newThreshold,
                        afterThreshold.minSkipRunSelectivityThreshold(),
                        1e-15
                    );
                    assertEquals("batchSize must be unchanged", before.batchSize(), afterThreshold.batchSize());
                    assertEquals("targetPartitions must be unchanged", before.targetPartitions(), afterThreshold.targetPartitions());
                    assertEquals(
                        "parquetPushdownFilters must be unchanged",
                        before.parquetPushdownFilters(),
                        afterThreshold.parquetPushdownFilters()
                    );
                    assertEquals("minSkipRunDefault must be unchanged", before.minSkipRunDefault(), afterThreshold.minSkipRunDefault());
                    assertEquals("costPredicate must be unchanged", before.costPredicate(), afterThreshold.costPredicate());
                    assertEquals("costCollector must be unchanged", before.costCollector(), afterThreshold.costCollector());
                    assertEquals(
                        "maxCollectorParallelism must be unchanged",
                        before.maxCollectorParallelism(),
                        afterThreshold.maxCollectorParallelism()
                    );
                    break;

                case SETTING_COST_PREDICATE:
                    int newCostPredicate = randomIntBetween(0, 1000);
                    newSettings = Settings.builder().put("datafusion.indexed.cost_predicate", newCostPredicate).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterCostPred = datafusionSettings.getSnapshot();
                    assertEquals("costPredicate must reflect new value", newCostPredicate, afterCostPred.costPredicate());
                    assertEquals("batchSize must be unchanged", before.batchSize(), afterCostPred.batchSize());
                    assertEquals("targetPartitions must be unchanged", before.targetPartitions(), afterCostPred.targetPartitions());
                    assertEquals(
                        "parquetPushdownFilters must be unchanged",
                        before.parquetPushdownFilters(),
                        afterCostPred.parquetPushdownFilters()
                    );
                    assertEquals("minSkipRunDefault must be unchanged", before.minSkipRunDefault(), afterCostPred.minSkipRunDefault());
                    assertEquals(
                        "minSkipRunSelectivityThreshold must be unchanged",
                        before.minSkipRunSelectivityThreshold(),
                        afterCostPred.minSkipRunSelectivityThreshold(),
                        0.0
                    );
                    assertEquals("costCollector must be unchanged", before.costCollector(), afterCostPred.costCollector());
                    assertEquals(
                        "maxCollectorParallelism must be unchanged",
                        before.maxCollectorParallelism(),
                        afterCostPred.maxCollectorParallelism()
                    );
                    break;

                case SETTING_COST_COLLECTOR:
                    int newCostCollector = randomIntBetween(0, 1000);
                    newSettings = Settings.builder().put("datafusion.indexed.cost_collector", newCostCollector).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterCostColl = datafusionSettings.getSnapshot();
                    assertEquals("costCollector must reflect new value", newCostCollector, afterCostColl.costCollector());
                    assertEquals("batchSize must be unchanged", before.batchSize(), afterCostColl.batchSize());
                    assertEquals("targetPartitions must be unchanged", before.targetPartitions(), afterCostColl.targetPartitions());
                    assertEquals(
                        "parquetPushdownFilters must be unchanged",
                        before.parquetPushdownFilters(),
                        afterCostColl.parquetPushdownFilters()
                    );
                    assertEquals("minSkipRunDefault must be unchanged", before.minSkipRunDefault(), afterCostColl.minSkipRunDefault());
                    assertEquals(
                        "minSkipRunSelectivityThreshold must be unchanged",
                        before.minSkipRunSelectivityThreshold(),
                        afterCostColl.minSkipRunSelectivityThreshold(),
                        0.0
                    );
                    assertEquals("costPredicate must be unchanged", before.costPredicate(), afterCostColl.costPredicate());
                    assertEquals(
                        "maxCollectorParallelism must be unchanged",
                        before.maxCollectorParallelism(),
                        afterCostColl.maxCollectorParallelism()
                    );
                    break;

                case SETTING_MAX_COLLECTOR_PARALLELISM:
                    int newMaxParallelism = randomIntBetween(1, 64);
                    newSettings = Settings.builder().put("datafusion.indexed.max_collector_parallelism", newMaxParallelism).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterParallelism = datafusionSettings.getSnapshot();
                    assertEquals(
                        "maxCollectorParallelism must reflect new value",
                        newMaxParallelism,
                        afterParallelism.maxCollectorParallelism()
                    );
                    assertEquals("batchSize must be unchanged", before.batchSize(), afterParallelism.batchSize());
                    assertEquals("targetPartitions must be unchanged", before.targetPartitions(), afterParallelism.targetPartitions());
                    assertEquals(
                        "parquetPushdownFilters must be unchanged",
                        before.parquetPushdownFilters(),
                        afterParallelism.parquetPushdownFilters()
                    );
                    assertEquals("minSkipRunDefault must be unchanged", before.minSkipRunDefault(), afterParallelism.minSkipRunDefault());
                    assertEquals(
                        "minSkipRunSelectivityThreshold must be unchanged",
                        before.minSkipRunSelectivityThreshold(),
                        afterParallelism.minSkipRunSelectivityThreshold(),
                        0.0
                    );
                    assertEquals("costPredicate must be unchanged", before.costPredicate(), afterParallelism.costPredicate());
                    assertEquals("costCollector must be unchanged", before.costCollector(), afterParallelism.costCollector());
                    break;

                case SETTING_MAX_SLICE_COUNT:
                    int newSliceCount = randomIntBetween(1, 32);
                    newSettings = Settings.builder().put("search.concurrent.max_slice_count", newSliceCount).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterSlice = datafusionSettings.getSnapshot();
                    assertEquals("targetPartitions must reflect new slice count", newSliceCount, afterSlice.targetPartitions());
                    assertEquals("batchSize must be unchanged", before.batchSize(), afterSlice.batchSize());
                    assertEquals(
                        "parquetPushdownFilters must be unchanged",
                        before.parquetPushdownFilters(),
                        afterSlice.parquetPushdownFilters()
                    );
                    assertEquals("minSkipRunDefault must be unchanged", before.minSkipRunDefault(), afterSlice.minSkipRunDefault());
                    assertEquals(
                        "minSkipRunSelectivityThreshold must be unchanged",
                        before.minSkipRunSelectivityThreshold(),
                        afterSlice.minSkipRunSelectivityThreshold(),
                        0.0
                    );
                    assertEquals("costPredicate must be unchanged", before.costPredicate(), afterSlice.costPredicate());
                    assertEquals("costCollector must be unchanged", before.costCollector(), afterSlice.costCollector());
                    assertEquals(
                        "maxCollectorParallelism must be unchanged",
                        before.maxCollectorParallelism(),
                        afterSlice.maxCollectorParallelism()
                    );
                    break;

                default:
                    fail("Unexpected setting index: " + settingIndex);
            }
        }
    }

    /**
     * **Validates: Requirements 3.2**
     * <p>
     * Property 1 supplementary: sequential updates to different settings accumulate
     * correctly — each update only modifies its own field.
     */
    public void testSequentialUpdatesAccumulateCorrectly() {
        for (int i = 0; i < ITERATIONS; i++) {
            DatafusionSettings datafusionSettings = new DatafusionSettings(Settings.EMPTY);
            ClusterSettings clusterSettings = createClusterSettings();
            datafusionSettings.registerListeners(clusterSettings);

            // Generate random values for three different settings
            int newBatchSize = randomIntBetween(1, 1_000_000);
            int newCostCollector = randomIntBetween(0, 1000);
            double newThreshold = randomDoubleBetween(0.0, 1.0, true);

            // Apply all three settings together (ClusterSettings.applySettings treats the
            // provided settings as the full transient set — omitted keys revert to defaults)
            clusterSettings.applySettings(
                Settings.builder()
                    .put("datafusion.indexed.batch_size", newBatchSize)
                    .put("datafusion.indexed.cost_collector", newCostCollector)
                    .put("datafusion.indexed.min_skip_run_selectivity_threshold", newThreshold)
                    .build()
            );

            WireConfigSnapshot finalSnapshot = datafusionSettings.getSnapshot();

            // All three updates must be reflected
            assertEquals("batchSize must reflect accumulated update", newBatchSize, finalSnapshot.batchSize());
            assertEquals("costCollector must reflect accumulated update", newCostCollector, finalSnapshot.costCollector());
            assertEquals(
                "minSkipRunSelectivityThreshold must reflect accumulated update",
                newThreshold,
                finalSnapshot.minSkipRunSelectivityThreshold(),
                1e-15
            );

            // Fields not updated must retain defaults
            assertEquals("parquetPushdownFilters must retain default", false, finalSnapshot.parquetPushdownFilters());
            assertEquals("minSkipRunDefault must retain default", 1024, finalSnapshot.minSkipRunDefault());
            assertEquals("costPredicate must retain default", 1, finalSnapshot.costPredicate());
        }
    }
}
