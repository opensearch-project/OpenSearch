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
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests covering the native-memory threshold and node native-memory limit
 * settings on {@link NodeDuressSettings}.
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

    public void testDefaultNodeNativeMemoryLimitIsZero() {
        // Default ByteSizeValue.ZERO — operator hasn't configured a native pool, so the
        // duress probe must self-report "unconfigured" and the SBP path treats it as 0.
        NodeDuressSettings settings = new NodeDuressSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        assertEquals(0L, settings.getNodeNativeMemory());
    }

    public void testInitialNodeNativeMemoryLimitRespectsSetting() {
        // ByteSizeValue parses unit-suffixed strings; verify the configured value reaches the field.
        Settings raw = Settings.builder().put(NodeDuressSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "2gb").build();
        NodeDuressSettings settings = new NodeDuressSettings(raw, new ClusterSettings(raw, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        assertEquals(2L * 1024 * 1024 * 1024, settings.getNodeNativeMemory());
    }

    public void testNodeNativeMemoryLimitIsDynamic() {
        // Operator must be able to flip the limit at runtime without a node restart.
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        NodeDuressSettings settings = new NodeDuressSettings(Settings.EMPTY, clusterSettings);
        assertEquals(0L, settings.getNodeNativeMemory());

        clusterSettings.applySettings(
            Settings.builder().put(NodeDuressSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "512mb").build()
        );
        assertEquals(512L * 1024 * 1024, settings.getNodeNativeMemory());

        clusterSettings.applySettings(Settings.builder().put(NodeDuressSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "1gb").build());
        assertEquals(1024L * 1024 * 1024, settings.getNodeNativeMemory());
    }

    public void testNodeNativeMemoryLimitSetterAcceptsByteSizeValue() {
        // Direct setter contract — the cluster-settings consumer routes through this setter,
        // so assert the contract directly to catch regressions in the consumer wiring here
        // rather than only in an integration test.
        NodeDuressSettings settings = new NodeDuressSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        settings.setNodeNativeMemory(new ByteSizeValue(4096L));
        assertEquals(4096L, settings.getNodeNativeMemory());
    }
}
