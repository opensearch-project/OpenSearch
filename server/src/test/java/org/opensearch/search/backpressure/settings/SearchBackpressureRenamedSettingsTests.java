/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.settings;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Set;

public class SearchBackpressureRenamedSettingsTests extends OpenSearchTestCase {

    /**
     * Validate the both settings are known and supported.
     */
    public void testOldSettingsExist() {
        Set<Setting<?>> settings = ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
        assertTrue(
            "Both 'search_backpressure.search_shard_task.cancellation_ratio' and its predecessor should be supported built-in settings",
            settings.containsAll(
                Arrays.asList(SearchBackpressureSettings.SETTING_CANCELLATION_RATIO, SearchShardTaskSettings.SETTING_CANCELLATION_RATIO)
            )
        );

        assertTrue(
            "Both 'search_backpressure.search_shard_task.cancellation_rate' and its predecessor should be supported built-in settings",
            settings.containsAll(
                Arrays.asList(SearchBackpressureSettings.SETTING_CANCELLATION_RATE, SearchShardTaskSettings.SETTING_CANCELLATION_RATE)
            )
        );

        assertTrue(
            "Both 'search_backpressure.search_shard_task.cancellation_burst' and its predecessor should be supported built-in settings",
            settings.containsAll(
                Arrays.asList(SearchBackpressureSettings.SETTING_CANCELLATION_BURST, SearchShardTaskSettings.SETTING_CANCELLATION_BURST)
            )
        );
    }

    /**
     * Validate the default value of the both settings is the same.
     */
    public void testSettingFallback() {
        assertEquals(
            SearchBackpressureSettings.SETTING_CANCELLATION_RATIO.get(Settings.EMPTY),
            SearchShardTaskSettings.SETTING_CANCELLATION_RATIO.get(Settings.EMPTY)
        );

        assertEquals(
            SearchBackpressureSettings.SETTING_CANCELLATION_RATE.get(Settings.EMPTY),
            SearchShardTaskSettings.SETTING_CANCELLATION_RATE.get(Settings.EMPTY)
        );

        assertEquals(
            SearchBackpressureSettings.SETTING_CANCELLATION_BURST.get(Settings.EMPTY),
            SearchShardTaskSettings.SETTING_CANCELLATION_BURST.get(Settings.EMPTY)
        );
    }

    /**
     * Validate the new setting can be configured correctly, and it doesn't impact the old setting.
     */
    public void testSettingGetValue() {
        Settings settings = Settings.builder().put("search_backpressure.search_shard_task.cancellation_ratio", "0.5").build();
        assertEquals(Double.valueOf(0.5), SearchShardTaskSettings.SETTING_CANCELLATION_RATIO.get(settings));
        assertEquals(
            SearchBackpressureSettings.SETTING_CANCELLATION_RATIO.getDefault(Settings.EMPTY),
            SearchBackpressureSettings.SETTING_CANCELLATION_RATIO.get(settings)
        );
    }

    /**
     * Validate the value of the old setting will be applied to the new setting, if the new setting is not configured.
     */
    public void testSettingGetValueWithFallback() {
        Settings settings = Settings.builder().put("search_backpressure.cancellation_ratio", "0.3").build();
        assertEquals(Double.valueOf(0.3), SearchShardTaskSettings.SETTING_CANCELLATION_RATIO.get(settings));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { SearchBackpressureSettings.SETTING_CANCELLATION_RATIO });
    }

    /**
     * Validate the value of the old setting will be ignored, if the new setting is configured.
     */
    public void testSettingGetValueWhenBothAreConfigured() {
        Settings settings = Settings.builder()
            .put("search_backpressure.search_shard_task.cancellation_ratio", "0.2")
            .put("search_backpressure.cancellation_ratio", "0.4")
            .build();
        assertEquals(Double.valueOf(0.2), SearchShardTaskSettings.SETTING_CANCELLATION_RATIO.get(settings));
        assertEquals(Double.valueOf(0.4), SearchBackpressureSettings.SETTING_CANCELLATION_RATIO.get(settings));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { SearchBackpressureSettings.SETTING_CANCELLATION_RATIO });
    }
}
