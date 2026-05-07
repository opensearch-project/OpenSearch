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

public class DatafusionSettingsPropertyTests extends OpenSearchTestCase {

    private static final int ITERATIONS = 200;

    private static final int SETTING_BATCH_SIZE = 0;
    private static final int SETTING_PARQUET_PUSHDOWN_FILTERS = 1;
    private static final int SETTING_MIN_SKIP_RUN_DEFAULT = 2;
    private static final int SETTING_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD = 3;
    private static final int SETTING_COST_PREDICATE = 4;
    private static final int SETTING_COST_COLLECTOR = 5;
    private static final int SETTING_MAX_COLLECTOR_PARALLELISM = 6;
    private static final int SETTING_MAX_SLICE_COUNT = 7;
    private static final int SETTING_CONCURRENT_SEARCH_MODE = 8;
    private static final int NUM_SETTINGS = 9;

    private ClusterSettings createClusterSettings() {
        Set<Setting<?>> settingsSet = new HashSet<>(DatafusionSettings.ALL_SETTINGS);
        settingsSet.add(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING);
        settingsSet.add(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE);
        return new ClusterSettings(Settings.EMPTY, settingsSet);
    }

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
                    assertEquals(newBatchSize, afterBatch.batchSize());
                    assertEquals(before.targetPartitions(), afterBatch.targetPartitions());
                    assertEquals(before.parquetPushdownFilters(), afterBatch.parquetPushdownFilters());
                    assertEquals(before.minSkipRunDefault(), afterBatch.minSkipRunDefault());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterBatch.minSkipRunSelectivityThreshold(), 0.0);
                    assertEquals(before.costPredicate(), afterBatch.costPredicate());
                    assertEquals(before.costCollector(), afterBatch.costCollector());
                    assertEquals(before.maxCollectorParallelism(), afterBatch.maxCollectorParallelism());
                    break;

                case SETTING_PARQUET_PUSHDOWN_FILTERS:
                    boolean newPushdown = before.parquetPushdownFilters() == false;
                    newSettings = Settings.builder().put("datafusion.indexed.parquet_pushdown_filters", newPushdown).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterPushdown = datafusionSettings.getSnapshot();
                    assertEquals(newPushdown, afterPushdown.parquetPushdownFilters());
                    assertEquals(before.batchSize(), afterPushdown.batchSize());
                    assertEquals(before.targetPartitions(), afterPushdown.targetPartitions());
                    assertEquals(before.minSkipRunDefault(), afterPushdown.minSkipRunDefault());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterPushdown.minSkipRunSelectivityThreshold(), 0.0);
                    assertEquals(before.costPredicate(), afterPushdown.costPredicate());
                    assertEquals(before.costCollector(), afterPushdown.costCollector());
                    assertEquals(before.maxCollectorParallelism(), afterPushdown.maxCollectorParallelism());
                    break;

                case SETTING_MIN_SKIP_RUN_DEFAULT:
                    int newMinSkipRun = randomIntBetween(1, 100_000);
                    newSettings = Settings.builder().put("datafusion.indexed.min_skip_run_default", newMinSkipRun).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterSkipRun = datafusionSettings.getSnapshot();
                    assertEquals(newMinSkipRun, afterSkipRun.minSkipRunDefault());
                    assertEquals(before.batchSize(), afterSkipRun.batchSize());
                    assertEquals(before.targetPartitions(), afterSkipRun.targetPartitions());
                    assertEquals(before.parquetPushdownFilters(), afterSkipRun.parquetPushdownFilters());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterSkipRun.minSkipRunSelectivityThreshold(), 0.0);
                    assertEquals(before.costPredicate(), afterSkipRun.costPredicate());
                    assertEquals(before.costCollector(), afterSkipRun.costCollector());
                    assertEquals(before.maxCollectorParallelism(), afterSkipRun.maxCollectorParallelism());
                    break;

                case SETTING_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD:
                    double newThreshold = randomDoubleBetween(0.0, 1.0, true);
                    newSettings = Settings.builder().put("datafusion.indexed.min_skip_run_selectivity_threshold", newThreshold).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterThreshold = datafusionSettings.getSnapshot();
                    assertEquals(newThreshold, afterThreshold.minSkipRunSelectivityThreshold(), 1e-15);
                    assertEquals(before.batchSize(), afterThreshold.batchSize());
                    assertEquals(before.targetPartitions(), afterThreshold.targetPartitions());
                    assertEquals(before.parquetPushdownFilters(), afterThreshold.parquetPushdownFilters());
                    assertEquals(before.minSkipRunDefault(), afterThreshold.minSkipRunDefault());
                    assertEquals(before.costPredicate(), afterThreshold.costPredicate());
                    assertEquals(before.costCollector(), afterThreshold.costCollector());
                    assertEquals(before.maxCollectorParallelism(), afterThreshold.maxCollectorParallelism());
                    break;

                case SETTING_COST_PREDICATE:
                    int newCostPredicate = randomIntBetween(0, 1000);
                    newSettings = Settings.builder().put("datafusion.indexed.cost_predicate", newCostPredicate).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterCostPred = datafusionSettings.getSnapshot();
                    assertEquals(newCostPredicate, afterCostPred.costPredicate());
                    assertEquals(before.batchSize(), afterCostPred.batchSize());
                    assertEquals(before.targetPartitions(), afterCostPred.targetPartitions());
                    assertEquals(before.parquetPushdownFilters(), afterCostPred.parquetPushdownFilters());
                    assertEquals(before.minSkipRunDefault(), afterCostPred.minSkipRunDefault());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterCostPred.minSkipRunSelectivityThreshold(), 0.0);
                    assertEquals(before.costCollector(), afterCostPred.costCollector());
                    assertEquals(before.maxCollectorParallelism(), afterCostPred.maxCollectorParallelism());
                    break;

                case SETTING_COST_COLLECTOR:
                    int newCostCollector = randomIntBetween(0, 1000);
                    newSettings = Settings.builder().put("datafusion.indexed.cost_collector", newCostCollector).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterCostColl = datafusionSettings.getSnapshot();
                    assertEquals(newCostCollector, afterCostColl.costCollector());
                    assertEquals(before.batchSize(), afterCostColl.batchSize());
                    assertEquals(before.targetPartitions(), afterCostColl.targetPartitions());
                    assertEquals(before.parquetPushdownFilters(), afterCostColl.parquetPushdownFilters());
                    assertEquals(before.minSkipRunDefault(), afterCostColl.minSkipRunDefault());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterCostColl.minSkipRunSelectivityThreshold(), 0.0);
                    assertEquals(before.costPredicate(), afterCostColl.costPredicate());
                    assertEquals(before.maxCollectorParallelism(), afterCostColl.maxCollectorParallelism());
                    break;

                case SETTING_MAX_COLLECTOR_PARALLELISM:
                    int newMaxParallelism = randomIntBetween(1, 64);
                    newSettings = Settings.builder().put("datafusion.indexed.max_collector_parallelism", newMaxParallelism).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterParallelism = datafusionSettings.getSnapshot();
                    assertEquals(newMaxParallelism, afterParallelism.maxCollectorParallelism());
                    assertEquals(before.batchSize(), afterParallelism.batchSize());
                    assertEquals(before.targetPartitions(), afterParallelism.targetPartitions());
                    assertEquals(before.parquetPushdownFilters(), afterParallelism.parquetPushdownFilters());
                    assertEquals(before.minSkipRunDefault(), afterParallelism.minSkipRunDefault());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterParallelism.minSkipRunSelectivityThreshold(), 0.0);
                    assertEquals(before.costPredicate(), afterParallelism.costPredicate());
                    assertEquals(before.costCollector(), afterParallelism.costCollector());
                    break;

                case SETTING_MAX_SLICE_COUNT:
                    int newSliceCount = randomIntBetween(1, 32);
                    newSettings = Settings.builder().put("search.concurrent.max_slice_count", newSliceCount).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterSlice = datafusionSettings.getSnapshot();
                    assertEquals(Math.min(newSliceCount, Runtime.getRuntime().availableProcessors()), afterSlice.targetPartitions());
                    assertEquals(before.batchSize(), afterSlice.batchSize());
                    assertEquals(before.parquetPushdownFilters(), afterSlice.parquetPushdownFilters());
                    assertEquals(before.minSkipRunDefault(), afterSlice.minSkipRunDefault());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterSlice.minSkipRunSelectivityThreshold(), 0.0);
                    assertEquals(before.costPredicate(), afterSlice.costPredicate());
                    assertEquals(before.costCollector(), afterSlice.costCollector());
                    assertEquals(before.maxCollectorParallelism(), afterSlice.maxCollectorParallelism());
                    break;

                case SETTING_CONCURRENT_SEARCH_MODE:
                    newSettings = Settings.builder().put("search.concurrent_segment_search.mode", "none").build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterMode = datafusionSettings.getSnapshot();
                    assertEquals(1, afterMode.targetPartitions());
                    assertEquals(before.batchSize(), afterMode.batchSize());
                    assertEquals(before.parquetPushdownFilters(), afterMode.parquetPushdownFilters());
                    assertEquals(before.minSkipRunDefault(), afterMode.minSkipRunDefault());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterMode.minSkipRunSelectivityThreshold(), 0.0);
                    assertEquals(before.costPredicate(), afterMode.costPredicate());
                    assertEquals(before.costCollector(), afterMode.costCollector());
                    assertEquals(before.maxCollectorParallelism(), afterMode.maxCollectorParallelism());
                    break;

                default:
                    fail("Unexpected setting index: " + settingIndex);
            }
        }
    }

    public void testSequentialUpdatesAccumulateCorrectly() {
        for (int i = 0; i < ITERATIONS; i++) {
            DatafusionSettings datafusionSettings = new DatafusionSettings(Settings.EMPTY);
            ClusterSettings clusterSettings = createClusterSettings();
            datafusionSettings.registerListeners(clusterSettings);

            int newBatchSize = randomIntBetween(1, 1_000_000);
            int newCostCollector = randomIntBetween(0, 1000);
            double newThreshold = randomDoubleBetween(0.0, 1.0, true);

            clusterSettings.applySettings(
                Settings.builder()
                    .put("datafusion.indexed.batch_size", newBatchSize)
                    .put("datafusion.indexed.cost_collector", newCostCollector)
                    .put("datafusion.indexed.min_skip_run_selectivity_threshold", newThreshold)
                    .build()
            );

            WireConfigSnapshot finalSnapshot = datafusionSettings.getSnapshot();

            assertEquals(newBatchSize, finalSnapshot.batchSize());
            assertEquals(newCostCollector, finalSnapshot.costCollector());
            assertEquals(newThreshold, finalSnapshot.minSkipRunSelectivityThreshold(), 1e-15);
            assertEquals(false, finalSnapshot.parquetPushdownFilters());
            assertEquals(1024, finalSnapshot.minSkipRunDefault());
            assertEquals(1, finalSnapshot.costPredicate());
        }
    }
}
