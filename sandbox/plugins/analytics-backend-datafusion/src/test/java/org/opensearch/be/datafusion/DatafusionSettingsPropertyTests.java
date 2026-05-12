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
    private static final String[] STRATEGIES = { "full_range", "tighten_outer_bounds", "page_range_split" };

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

            int settingIndex = randomIntBetween(0, 8);
            Settings newSettings;

            switch (settingIndex) {
                case 0: // batch_size
                    int newBatchSize = randomIntBetween(1, 1_000_000);
                    newSettings = Settings.builder().put("datafusion.indexed.batch_size", newBatchSize).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterBatch = datafusionSettings.getSnapshot();
                    assertEquals(newBatchSize, afterBatch.batchSize());
                    assertEquals(before.targetPartitions(), afterBatch.targetPartitions());
                    assertEquals(before.parquetPushdownFilters(), afterBatch.parquetPushdownFilters());
                    assertEquals(before.minSkipRunDefault(), afterBatch.minSkipRunDefault());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterBatch.minSkipRunSelectivityThreshold(), 0.0);
                    assertEquals(before.singleCollectorStrategy(), afterBatch.singleCollectorStrategy());
                    assertEquals(before.treeCollectorStrategy(), afterBatch.treeCollectorStrategy());
                    assertEquals(before.maxCollectorParallelism(), afterBatch.maxCollectorParallelism());
                    break;

                case 1: // parquet_pushdown_filters
                    boolean newPushdown = before.parquetPushdownFilters() == false;
                    newSettings = Settings.builder().put("datafusion.indexed.parquet_pushdown_filters", newPushdown).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterPushdown = datafusionSettings.getSnapshot();
                    assertEquals(newPushdown, afterPushdown.parquetPushdownFilters());
                    assertEquals(before.batchSize(), afterPushdown.batchSize());
                    assertEquals(before.targetPartitions(), afterPushdown.targetPartitions());
                    assertEquals(before.minSkipRunDefault(), afterPushdown.minSkipRunDefault());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterPushdown.minSkipRunSelectivityThreshold(), 0.0);
                    assertEquals(before.singleCollectorStrategy(), afterPushdown.singleCollectorStrategy());
                    assertEquals(before.treeCollectorStrategy(), afterPushdown.treeCollectorStrategy());
                    assertEquals(before.maxCollectorParallelism(), afterPushdown.maxCollectorParallelism());
                    break;

                case 2: // min_skip_run_default
                    int newMinSkipRun = randomIntBetween(1, 100_000);
                    newSettings = Settings.builder().put("datafusion.indexed.min_skip_run_default", newMinSkipRun).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterSkipRun = datafusionSettings.getSnapshot();
                    assertEquals(newMinSkipRun, afterSkipRun.minSkipRunDefault());
                    assertEquals(before.batchSize(), afterSkipRun.batchSize());
                    assertEquals(before.targetPartitions(), afterSkipRun.targetPartitions());
                    assertEquals(before.parquetPushdownFilters(), afterSkipRun.parquetPushdownFilters());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterSkipRun.minSkipRunSelectivityThreshold(), 0.0);
                    assertEquals(before.singleCollectorStrategy(), afterSkipRun.singleCollectorStrategy());
                    assertEquals(before.treeCollectorStrategy(), afterSkipRun.treeCollectorStrategy());
                    assertEquals(before.maxCollectorParallelism(), afterSkipRun.maxCollectorParallelism());
                    break;

                case 3: // min_skip_run_selectivity_threshold
                    double newThreshold = randomDoubleBetween(0.0, 1.0, true);
                    newSettings = Settings.builder().put("datafusion.indexed.min_skip_run_selectivity_threshold", newThreshold).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterThreshold = datafusionSettings.getSnapshot();
                    assertEquals(newThreshold, afterThreshold.minSkipRunSelectivityThreshold(), 1e-15);
                    assertEquals(before.batchSize(), afterThreshold.batchSize());
                    assertEquals(before.targetPartitions(), afterThreshold.targetPartitions());
                    assertEquals(before.parquetPushdownFilters(), afterThreshold.parquetPushdownFilters());
                    assertEquals(before.minSkipRunDefault(), afterThreshold.minSkipRunDefault());
                    assertEquals(before.singleCollectorStrategy(), afterThreshold.singleCollectorStrategy());
                    assertEquals(before.treeCollectorStrategy(), afterThreshold.treeCollectorStrategy());
                    assertEquals(before.maxCollectorParallelism(), afterThreshold.maxCollectorParallelism());
                    break;

                case 4: // single_collector_strategy
                    String newSingle = STRATEGIES[randomIntBetween(0, 2)];
                    newSettings = Settings.builder().put("datafusion.indexed.single_collector_strategy", newSingle).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterSingle = datafusionSettings.getSnapshot();
                    assertEquals(DatafusionSettings.strategyToWireValue(newSingle), afterSingle.singleCollectorStrategy());
                    assertEquals(before.batchSize(), afterSingle.batchSize());
                    assertEquals(before.targetPartitions(), afterSingle.targetPartitions());
                    assertEquals(before.parquetPushdownFilters(), afterSingle.parquetPushdownFilters());
                    assertEquals(before.minSkipRunDefault(), afterSingle.minSkipRunDefault());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterSingle.minSkipRunSelectivityThreshold(), 0.0);
                    assertEquals(before.treeCollectorStrategy(), afterSingle.treeCollectorStrategy());
                    assertEquals(before.maxCollectorParallelism(), afterSingle.maxCollectorParallelism());
                    break;

                case 5: // tree_collector_strategy
                    String newTree = STRATEGIES[randomIntBetween(0, 2)];
                    newSettings = Settings.builder().put("datafusion.indexed.tree_collector_strategy", newTree).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterTree = datafusionSettings.getSnapshot();
                    assertEquals(DatafusionSettings.strategyToWireValue(newTree), afterTree.treeCollectorStrategy());
                    assertEquals(before.batchSize(), afterTree.batchSize());
                    assertEquals(before.targetPartitions(), afterTree.targetPartitions());
                    assertEquals(before.parquetPushdownFilters(), afterTree.parquetPushdownFilters());
                    assertEquals(before.minSkipRunDefault(), afterTree.minSkipRunDefault());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterTree.minSkipRunSelectivityThreshold(), 0.0);
                    assertEquals(before.singleCollectorStrategy(), afterTree.singleCollectorStrategy());
                    assertEquals(before.maxCollectorParallelism(), afterTree.maxCollectorParallelism());
                    break;

                case 6: // max_collector_parallelism
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
                    assertEquals(before.singleCollectorStrategy(), afterParallelism.singleCollectorStrategy());
                    assertEquals(before.treeCollectorStrategy(), afterParallelism.treeCollectorStrategy());
                    break;

                case 7: // max_slice_count
                    int newSliceCount = randomIntBetween(1, 32);
                    newSettings = Settings.builder().put("search.concurrent.max_slice_count", newSliceCount).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterSlice = datafusionSettings.getSnapshot();
                    assertEquals(Math.min(newSliceCount, Runtime.getRuntime().availableProcessors()), afterSlice.targetPartitions());
                    assertEquals(before.batchSize(), afterSlice.batchSize());
                    assertEquals(before.parquetPushdownFilters(), afterSlice.parquetPushdownFilters());
                    assertEquals(before.minSkipRunDefault(), afterSlice.minSkipRunDefault());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterSlice.minSkipRunSelectivityThreshold(), 0.0);
                    assertEquals(before.singleCollectorStrategy(), afterSlice.singleCollectorStrategy());
                    assertEquals(before.treeCollectorStrategy(), afterSlice.treeCollectorStrategy());
                    assertEquals(before.maxCollectorParallelism(), afterSlice.maxCollectorParallelism());
                    break;

                case 8: // concurrent_search_mode
                    newSettings = Settings.builder().put("search.concurrent_segment_search.mode", "none").build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterMode = datafusionSettings.getSnapshot();
                    assertEquals(1, afterMode.targetPartitions());
                    assertEquals(before.batchSize(), afterMode.batchSize());
                    assertEquals(before.parquetPushdownFilters(), afterMode.parquetPushdownFilters());
                    assertEquals(before.minSkipRunDefault(), afterMode.minSkipRunDefault());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterMode.minSkipRunSelectivityThreshold(), 0.0);
                    assertEquals(before.singleCollectorStrategy(), afterMode.singleCollectorStrategy());
                    assertEquals(before.treeCollectorStrategy(), afterMode.treeCollectorStrategy());
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
            String newSingleStrategy = STRATEGIES[randomIntBetween(0, 2)];
            double newThreshold = randomDoubleBetween(0.0, 1.0, true);

            clusterSettings.applySettings(
                Settings.builder()
                    .put("datafusion.indexed.batch_size", newBatchSize)
                    .put("datafusion.indexed.single_collector_strategy", newSingleStrategy)
                    .put("datafusion.indexed.min_skip_run_selectivity_threshold", newThreshold)
                    .build()
            );

            WireConfigSnapshot finalSnapshot = datafusionSettings.getSnapshot();

            assertEquals(newBatchSize, finalSnapshot.batchSize());
            assertEquals(DatafusionSettings.strategyToWireValue(newSingleStrategy), finalSnapshot.singleCollectorStrategy());
            assertEquals(newThreshold, finalSnapshot.minSkipRunSelectivityThreshold(), 1e-15);
            assertEquals(false, finalSnapshot.parquetPushdownFilters());
            assertEquals(1024, finalSnapshot.minSkipRunDefault());
            assertEquals(1, finalSnapshot.maxCollectorParallelism());
        }
    }
}
