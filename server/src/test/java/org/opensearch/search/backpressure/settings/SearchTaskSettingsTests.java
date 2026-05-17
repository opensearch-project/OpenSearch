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
 * Unit tests covering the native-memory threshold settings added to {@link SearchTaskSettings}.
 */
public class SearchTaskSettingsTests extends OpenSearchTestCase {

    private SearchTaskSettings buildDefault() {
        return new SearchTaskSettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    public void testDefaultNativeMemoryPercentThresholdMatchesDefaults() {
        // Production default: a small non-zero fraction so the tracker engages once a backend
        // installs a non-zero budget. Test pinned to the same value as Defaults.NATIVE_MEMORY_PERCENT_THRESHOLD
        // so a default change here trips this assertion deliberately.
        SearchTaskSettings settings = buildDefault();
        assertEquals(0.05d, settings.getNativeMemoryPercentThreshold(), 0.0d);
    }

    public void testInitialNativeMemoryPercentThresholdRespectsSetting() {
        double fractionThreshold = 0.75d;
        Settings raw = Settings.builder()
            .put(SearchTaskSettings.SETTING_NATIVE_MEMORY_PERCENT_THRESHOLD.getKey(), fractionThreshold)
            .build();
        SearchTaskSettings settings = new SearchTaskSettings(raw, new ClusterSettings(raw, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        assertEquals(fractionThreshold, settings.getNativeMemoryPercentThreshold(), 0.0d);
    }

    public void testNativeMemoryPercentThresholdIsDynamic() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchTaskSettings settings = new SearchTaskSettings(Settings.EMPTY, clusterSettings);
        clusterSettings.applySettings(
            Settings.builder().put(SearchTaskSettings.SETTING_NATIVE_MEMORY_PERCENT_THRESHOLD.getKey(), 0.33d).build()
        );
        assertEquals(0.33d, settings.getNativeMemoryPercentThreshold(), 0.0d);
    }

    public void testNativeMemoryPercentThresholdRejectsOutOfRange() {
        Settings raw = Settings.builder().put(SearchTaskSettings.SETTING_NATIVE_MEMORY_PERCENT_THRESHOLD.getKey(), 1.5d).build();
        expectThrows(
            IllegalArgumentException.class,
            () -> new SearchTaskSettings(raw, new ClusterSettings(raw, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );

        Settings raw2 = Settings.builder().put(SearchTaskSettings.SETTING_NATIVE_MEMORY_PERCENT_THRESHOLD.getKey(), -0.1d).build();
        expectThrows(
            IllegalArgumentException.class,
            () -> new SearchTaskSettings(raw2, new ClusterSettings(raw2, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
    }
}
