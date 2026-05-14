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
 * Unit tests covering the native-memory threshold added to {@link NodeDuressSettings}.
 */
public class NodeDuressSettingsTests extends OpenSearchTestCase {

    public void testDefaultNativeMemoryThreshold() {
        NodeDuressSettings settings = new NodeDuressSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        // Default per NodeDuressSettings.Defaults.NATIVE_MEMORY_THRESHOLD.
        assertEquals(0.85d, settings.getNativeMemoryThreshold(), 0.0d);
    }

    public void testInitialNativeMemoryThresholdRespectsSetting() {
        Settings raw = Settings.builder().put(NodeDuressSettings.SETTING_NATIVE_MEMORY_THRESHOLD.getKey(), 0.42d).build();
        NodeDuressSettings settings = new NodeDuressSettings(raw, new ClusterSettings(raw, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        assertEquals(0.42d, settings.getNativeMemoryThreshold(), 0.0d);
    }

    public void testNativeMemoryThresholdIsDynamic() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        NodeDuressSettings settings = new NodeDuressSettings(Settings.EMPTY, clusterSettings);
        assertEquals(0.85d, settings.getNativeMemoryThreshold(), 0.0d);

        // Apply a runtime update — the consumer should propagate to the field.
        clusterSettings.applySettings(Settings.builder().put(NodeDuressSettings.SETTING_NATIVE_MEMORY_THRESHOLD.getKey(), 0.55d).build());
        assertEquals(0.55d, settings.getNativeMemoryThreshold(), 0.0d);
    }

    public void testNativeMemoryThresholdRejectsOutOfRange() {
        // Range [0.0, 1.0] enforced by Setting.doubleSetting(min, max).
        Settings raw = Settings.builder().put(NodeDuressSettings.SETTING_NATIVE_MEMORY_THRESHOLD.getKey(), 1.5d).build();
        expectThrows(
            IllegalArgumentException.class,
            () -> new NodeDuressSettings(raw, new ClusterSettings(raw, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );

        Settings raw2 = Settings.builder().put(NodeDuressSettings.SETTING_NATIVE_MEMORY_THRESHOLD.getKey(), -0.1d).build();
        expectThrows(
            IllegalArgumentException.class,
            () -> new NodeDuressSettings(raw2, new ClusterSettings(raw2, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
    }
}
