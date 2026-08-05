/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.resource.tracker;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.containsString;

/**
 * Verifies that a {@code *.window_duration} shorter than its paired {@code *.polling_interval} is rejected at
 * settings-update validation time, rather than throwing while the committed cluster state is applied.
 */
public class ResourceTrackerSettingsTests extends OpenSearchTestCase {

    private ClusterSettings clusterSettings() {
        return new ClusterSettings(
            Settings.EMPTY,
            Set.of(
                ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_POLLING_INTERVAL_SETTING,
                ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING,
                ResourceTrackerSettings.GLOBAL_JVM_USAGE_AC_POLLING_INTERVAL_SETTING,
                ResourceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING,
                ResourceTrackerSettings.GLOBAL_IO_USAGE_AC_POLLING_INTERVAL_SETTING,
                ResourceTrackerSettings.GLOBAL_IO_USAGE_AC_WINDOW_DURATION_SETTING,
                ResourceTrackerSettings.GLOBAL_NATIVE_MEMORY_USAGE_AC_POLLING_INTERVAL_SETTING,
                ResourceTrackerSettings.GLOBAL_NATIVE_MEMORY_USAGE_AC_WINDOW_DURATION_SETTING
            )
        );
    }

    public void testWindowShorterThanPollingRejectedAtValidation() {
        ClusterSettings cs = clusterSettings();
        // cpu polling default is 500ms; a 200ms window would floor the moving-average size to 0.
        Settings update = Settings.builder()
            .put(ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), "200ms")
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> cs.validate(update, true));
        assertThat(e.getMessage(), containsString(ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.getKey()));
    }

    public void testWindowEqualToPollingAccepted() {
        ClusterSettings cs = clusterSettings();
        // window == polling (500ms) => moving-average size floors to exactly 1, which is valid.
        Settings update = Settings.builder()
            .put(ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), "500ms")
            .build();
        cs.validate(update, true);
        cs.applySettings(update); // must not throw
    }

    public void testWindowShorterThanPollingRejectedForAllTrackers() {
        ClusterSettings cs = clusterSettings();
        // Each tracker: a 1ms window is below every tracker's polling interval (>=500ms), so all must reject.
        String[] windowKeys = new String[] {
            ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.getKey(),
            ResourceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING.getKey(),
            ResourceTrackerSettings.GLOBAL_IO_USAGE_AC_WINDOW_DURATION_SETTING.getKey(),
            ResourceTrackerSettings.GLOBAL_NATIVE_MEMORY_USAGE_AC_WINDOW_DURATION_SETTING.getKey() };
        for (String key : windowKeys) {
            Settings update = Settings.builder().put(key, "1ms").build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> cs.validate(update, true));
            assertThat(e.getMessage(), containsString(key));
        }
    }

    public void testWindowAbovePollingAccepted() {
        ClusterSettings cs = clusterSettings();
        // A 3s window comfortably exceeds the 500ms cpu polling interval and must be accepted end-to-end.
        Settings update = Settings.builder()
            .put(ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), "3s")
            .build();
        cs.validate(update, true);
        cs.applySettings(update); // must not throw
    }
}
