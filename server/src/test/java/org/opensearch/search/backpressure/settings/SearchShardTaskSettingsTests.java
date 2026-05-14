/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.settings;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests covering the native-memory threshold settings added to
 * {@link SearchShardTaskSettings}.
 */
public class SearchShardTaskSettingsTests extends OpenSearchTestCase {

    public void testDefaultNativeMemoryThresholdsKeepTrackerInert() {
        SearchShardTaskSettings settings = new SearchShardTaskSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        assertEquals(0L, settings.getTotalNativeMemoryBytesThreshold());
        assertEquals(0.0d, settings.getNativeMemoryPercentThreshold(), 0.0d);
    }

    public void testInitialNativeMemoryThresholdsRespectSettings() {
        long bytesThreshold = 512L * 1024L * 1024L;
        double fractionThreshold = 0.6d;
        Settings raw = Settings.builder()
            .put(SearchShardTaskSettings.SETTING_TOTAL_NATIVE_MEMORY_BYTES_THRESHOLD.getKey(), bytesThreshold)
            .put(SearchShardTaskSettings.SETTING_NATIVE_MEMORY_PERCENT_THRESHOLD.getKey(), fractionThreshold)
            .build();
        SearchShardTaskSettings settings = new SearchShardTaskSettings(
            raw,
            new ClusterSettings(raw, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        assertEquals(bytesThreshold, settings.getTotalNativeMemoryBytesThreshold());
        assertEquals(fractionThreshold, settings.getNativeMemoryPercentThreshold(), 0.0d);
    }

    public void testNativeMemoryThresholdsAreDynamic() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchShardTaskSettings settings = new SearchShardTaskSettings(Settings.EMPTY, clusterSettings);
        clusterSettings.applySettings(
            Settings.builder()
                .put(SearchShardTaskSettings.SETTING_TOTAL_NATIVE_MEMORY_BYTES_THRESHOLD.getKey(), 8192L)
                .put(SearchShardTaskSettings.SETTING_NATIVE_MEMORY_PERCENT_THRESHOLD.getKey(), 0.25d)
                .build()
        );
        assertEquals(8192L, settings.getTotalNativeMemoryBytesThreshold());
        assertEquals(0.25d, settings.getNativeMemoryPercentThreshold(), 0.0d);
    }

    public void testNativeMemoryPercentThresholdRejectsOutOfRange() {
        Settings raw = Settings.builder().put(SearchShardTaskSettings.SETTING_NATIVE_MEMORY_PERCENT_THRESHOLD.getKey(), 2.0d).build();
        expectThrows(
            IllegalArgumentException.class,
            () -> new SearchShardTaskSettings(raw, new ClusterSettings(raw, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );

        Settings raw2 = Settings.builder().put(SearchShardTaskSettings.SETTING_NATIVE_MEMORY_PERCENT_THRESHOLD.getKey(), -0.5d).build();
        expectThrows(
            IllegalArgumentException.class,
            () -> new SearchShardTaskSettings(raw2, new ClusterSettings(raw2, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
    }

    public void testTotalNativeMemoryBytesThresholdRejectsNegative() {
        Settings raw = Settings.builder().put(SearchShardTaskSettings.SETTING_TOTAL_NATIVE_MEMORY_BYTES_THRESHOLD.getKey(), -1L).build();
        expectThrows(
            IllegalArgumentException.class,
            () -> new SearchShardTaskSettings(raw, new ClusterSettings(raw, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
    }
}
