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

    public void testDefaultNativeMemoryPercentThresholdMatchesDefaults() {
        // Production default: a small non-zero fraction so the tracker engages once a backend
        // installs a non-zero budget. Test pinned to the same value as Defaults.NATIVE_MEMORY_PERCENT_THRESHOLD
        // so a default change here trips this assertion deliberately.
        SearchShardTaskSettings settings = new SearchShardTaskSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        assertEquals(0.05d, settings.getNativeMemoryPercentThreshold(), 0.0d);
    }

    public void testInitialNativeMemoryPercentThresholdRespectsSetting() {
        double fractionThreshold = 0.6d;
        Settings raw = Settings.builder()
            .put(SearchShardTaskSettings.SETTING_NATIVE_MEMORY_PERCENT_THRESHOLD.getKey(), fractionThreshold)
            .build();
        SearchShardTaskSettings settings = new SearchShardTaskSettings(
            raw,
            new ClusterSettings(raw, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        assertEquals(fractionThreshold, settings.getNativeMemoryPercentThreshold(), 0.0d);
    }

    public void testNativeMemoryPercentThresholdIsDynamic() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchShardTaskSettings settings = new SearchShardTaskSettings(Settings.EMPTY, clusterSettings);
        clusterSettings.applySettings(
            Settings.builder().put(SearchShardTaskSettings.SETTING_NATIVE_MEMORY_PERCENT_THRESHOLD.getKey(), 0.25d).build()
        );
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
}
