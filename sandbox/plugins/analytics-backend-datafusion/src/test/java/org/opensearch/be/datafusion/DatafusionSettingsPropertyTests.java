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

            int settingIndex = randomIntBetween(0, 5);
            Settings newSettings;

            switch (settingIndex) {
                case 0: // batch_size
                    int newBatchSize = randomIntBetween(1, 1_000_000);
                    newSettings = Settings.builder().put("datafusion.batch_size", newBatchSize).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterBatch = datafusionSettings.getSnapshot();
                    assertEquals(newBatchSize, afterBatch.batchSize());
                    assertEquals(before.targetPartitions(), afterBatch.targetPartitions());
                    assertEquals(before.listingTablePushdownFilters(), afterBatch.listingTablePushdownFilters());
                    assertEquals(before.minSkipRunDefault(), afterBatch.minSkipRunDefault());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterBatch.minSkipRunSelectivityThreshold(), 0.0);
                    break;

                case 1: // listing_table.pushdown_filters
                    boolean newPushdown = before.listingTablePushdownFilters() == false;
                    newSettings = Settings.builder().put("datafusion.listing_table.pushdown_filters", newPushdown).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterPushdown = datafusionSettings.getSnapshot();
                    assertEquals(newPushdown, afterPushdown.listingTablePushdownFilters());
                    assertEquals(before.batchSize(), afterPushdown.batchSize());
                    assertEquals(before.targetPartitions(), afterPushdown.targetPartitions());
                    assertEquals(before.minSkipRunDefault(), afterPushdown.minSkipRunDefault());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterPushdown.minSkipRunSelectivityThreshold(), 0.0);
                    break;

                case 2: // min_skip_run_default
                    int newMinSkipRun = randomIntBetween(1, 100_000);
                    newSettings = Settings.builder().put("datafusion.indexed.min_skip_run_default", newMinSkipRun).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterSkipRun = datafusionSettings.getSnapshot();
                    assertEquals(newMinSkipRun, afterSkipRun.minSkipRunDefault());
                    assertEquals(before.batchSize(), afterSkipRun.batchSize());
                    assertEquals(before.targetPartitions(), afterSkipRun.targetPartitions());
                    assertEquals(before.listingTablePushdownFilters(), afterSkipRun.listingTablePushdownFilters());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterSkipRun.minSkipRunSelectivityThreshold(), 0.0);
                    break;

                case 3: // min_skip_run_selectivity_threshold
                    double newThreshold = randomDoubleBetween(0.0, 1.0, true);
                    newSettings = Settings.builder().put("datafusion.indexed.min_skip_run_selectivity_threshold", newThreshold).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterThreshold = datafusionSettings.getSnapshot();
                    assertEquals(newThreshold, afterThreshold.minSkipRunSelectivityThreshold(), 1e-15);
                    assertEquals(before.batchSize(), afterThreshold.batchSize());
                    assertEquals(before.targetPartitions(), afterThreshold.targetPartitions());
                    assertEquals(before.listingTablePushdownFilters(), afterThreshold.listingTablePushdownFilters());
                    assertEquals(before.minSkipRunDefault(), afterThreshold.minSkipRunDefault());
                    break;

                case 4: // max_slice_count
                    int newSliceCount = randomIntBetween(1, 32);
                    newSettings = Settings.builder().put("search.concurrent.max_slice_count", newSliceCount).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterSlice = datafusionSettings.getSnapshot();
                    assertEquals(Math.min(newSliceCount, Runtime.getRuntime().availableProcessors()), afterSlice.targetPartitions());
                    assertEquals(before.batchSize(), afterSlice.batchSize());
                    assertEquals(before.listingTablePushdownFilters(), afterSlice.listingTablePushdownFilters());
                    assertEquals(before.minSkipRunDefault(), afterSlice.minSkipRunDefault());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterSlice.minSkipRunSelectivityThreshold(), 0.0);
                    break;

                case 5: // concurrent_search_mode
                    newSettings = Settings.builder().put("search.concurrent_segment_search.mode", "none").build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterMode = datafusionSettings.getSnapshot();
                    assertEquals(1, afterMode.targetPartitions());
                    assertEquals(before.batchSize(), afterMode.batchSize());
                    assertEquals(before.listingTablePushdownFilters(), afterMode.listingTablePushdownFilters());
                    assertEquals(before.minSkipRunDefault(), afterMode.minSkipRunDefault());
                    assertEquals(before.minSkipRunSelectivityThreshold(), afterMode.minSkipRunSelectivityThreshold(), 0.0);
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
            double newThreshold = randomDoubleBetween(0.0, 1.0, true);

            clusterSettings.applySettings(
                Settings.builder()
                    .put("datafusion.batch_size", newBatchSize)
                    .put("datafusion.indexed.min_skip_run_selectivity_threshold", newThreshold)
                    .build()
            );

            WireConfigSnapshot finalSnapshot = datafusionSettings.getSnapshot();

            assertEquals(newBatchSize, finalSnapshot.batchSize());
            assertEquals(newThreshold, finalSnapshot.minSkipRunSelectivityThreshold(), 1e-15);
            assertEquals(false, finalSnapshot.listingTablePushdownFilters());
            assertEquals(1024, finalSnapshot.minSkipRunDefault());
        }
    }
}
