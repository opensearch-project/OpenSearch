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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicReference;

public class SearchBackpressureSettingsTests extends OpenSearchTestCase {

    /**
     * Validate proper construction of SearchBackpressureSettings object with a valid mode.
     */
    public void testSearchBackpressureSettings() {
        Settings settings = Settings.builder().put("search_backpressure.mode", "monitor_only").build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchBackpressureSettings sbs = new SearchBackpressureSettings(settings, cs);
        assertEquals(SearchBackpressureMode.MONITOR_ONLY, sbs.getMode());
        assertEquals(settings, sbs.getSettings());
        assertEquals(cs, sbs.getClusterSettings());
    }

    /**
     * Validate construction of SearchBackpressureSettings object gets rejected
     * on invalid SearchBackpressureMode value.
     */
    public void testSearchBackpressureSettingValidateInvalidMode() {
        Settings settings = Settings.builder().put("search_backpressure.mode", "foo").build();
        assertThrows(
            IllegalArgumentException.class,
            () -> new SearchBackpressureSettings(settings, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
    }

    public void testInvalidCancellationRate() {
        Settings settings1 = Settings.builder().put("search_backpressure.search_task.cancellation_rate", randomFrom(-1, 0)).build();
        assertThrows(
            IllegalArgumentException.class,
            () -> new SearchBackpressureSettings(settings1, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );

        Settings settings2 = Settings.builder().put("search_backpressure.search_shard_task.cancellation_rate", randomFrom(-1, 0)).build();
        assertThrows(
            IllegalArgumentException.class,
            () -> new SearchBackpressureSettings(settings2, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
    }

    public void testInvalidCancellationRatio() {
        Settings settings1 = Settings.builder().put("search_backpressure.search_task.cancellation_ratio", randomFrom(-1, 0)).build();
        assertThrows(
            IllegalArgumentException.class,
            () -> new SearchBackpressureSettings(settings1, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );

        Settings settings2 = Settings.builder().put("search_backpressure.search_shard_task.cancellation_ratio", randomFrom(-1, 0)).build();
        assertThrows(
            IllegalArgumentException.class,
            () -> new SearchBackpressureSettings(settings2, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
    }

    /**
     * Validate that the interval setting is read correctly from settings.
     */
    public void testIntervalSetting() {
        long intervalMillis = randomLongBetween(100, 5000);
        Settings settings = Settings.builder().put("search_backpressure.interval_millis", intervalMillis).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchBackpressureSettings sbs = new SearchBackpressureSettings(settings, cs);
        assertEquals(new TimeValue(intervalMillis), sbs.getInterval());
    }

    /**
     * Validate that the interval setting defaults to 1000ms.
     */
    public void testIntervalSettingDefault() {
        Settings settings = Settings.EMPTY;
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchBackpressureSettings sbs = new SearchBackpressureSettings(settings, cs);
        assertEquals(new TimeValue(1000), sbs.getInterval());
    }

    /**
     * Validate that the interval setting can be updated dynamically.
     */
    public void testDynamicIntervalUpdate() {
        long initialInterval = 1000;
        long newInterval = 2000;
        Settings settings = Settings.builder().put("search_backpressure.interval_millis", initialInterval).build();
        ClusterSettings cs = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchBackpressureSettings sbs = new SearchBackpressureSettings(settings, cs);

        // Verify initial interval
        assertEquals(new TimeValue(initialInterval), sbs.getInterval());

        // Register a listener to track interval changes
        AtomicReference<TimeValue> listenerCalledWith = new AtomicReference<>();
        sbs.addIntervalListener(listenerCalledWith::set);

        // Update the interval dynamically
        Settings newSettings = Settings.builder().put("search_backpressure.interval_millis", newInterval).build();
        cs.applySettings(newSettings);

        // Verify the interval was updated
        assertEquals(new TimeValue(newInterval), sbs.getInterval());

        // Verify the listener was notified
        assertEquals(new TimeValue(newInterval), listenerCalledWith.get());
    }

    /**
     * Validate that the interval setting rejects invalid values.
     */
    public void testInvalidIntervalSetting() {
        Settings settings = Settings.builder().put("search_backpressure.interval_millis", 0).build();
        assertThrows(
            IllegalArgumentException.class,
            () -> new SearchBackpressureSettings(settings, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );

        Settings negativeSettings = Settings.builder().put("search_backpressure.interval_millis", -1).build();
        assertThrows(
            IllegalArgumentException.class,
            () -> new SearchBackpressureSettings(
                negativeSettings,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            )
        );
    }
}
