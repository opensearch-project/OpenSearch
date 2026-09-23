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
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;
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

    public void testDefaultNodeNativeMemoryLimitUsesNodeLevelSetting() {
        // Setting node.native_memory.limit to 0b disables the native-memory budget and
        // the duress probe reports an unconfigured limit.
        Settings raw = Settings.builder().put(ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "0b").build();
        NodeDuressSettings settings = new NodeDuressSettings(raw, new ClusterSettings(raw, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        assertEquals(0L, settings.getNodeNativeMemory());
    }

    public void testInitialNodeNativeMemoryLimitRespectsSetting() {
        // Configuring node.native_memory.limit is picked up by the duress probe.
        Settings raw = Settings.builder().put(ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "2gb").build();
        NodeDuressSettings settings = new NodeDuressSettings(raw, new ClusterSettings(raw, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        assertEquals(2L * 1024 * 1024 * 1024, settings.getNodeNativeMemory());
    }

    public void testNodeNativeMemoryLimitIsDynamic() {
        // A dynamic update to node.native_memory.limit must propagate to the duress probe.
        Settings initial = Settings.builder().put(ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "0b").build();
        ClusterSettings clusterSettings = new ClusterSettings(initial, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        NodeDuressSettings settings = new NodeDuressSettings(initial, clusterSettings);
        assertEquals(0L, settings.getNodeNativeMemory());

        clusterSettings.applySettings(
            Settings.builder().put(ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "512mb").build()
        );
        assertEquals(512L * 1024 * 1024, settings.getNodeNativeMemory());

        clusterSettings.applySettings(
            Settings.builder().put(ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "1gb").build()
        );
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

    /**
     * BWC: a cluster that had {@code search_backpressure.node_duress.native_memory_limit} set
     * in its persistent cluster state must have that value transparently migrated to
     * {@code node.native_memory.limit} by {@link NodeDuressSettings#NATIVE_MEMORY_LIMIT_UPGRADER}
     * during cluster-state recovery, so the SBP duress probe still honours the operator's
     * original intent after upgrade.
     */
    public void testLegacySettingUpgraderMigratesKeyToNodeNativeMemoryLimit() {
        // Simulate old cluster-state that contains the legacy SBP-specific key.
        Settings legacyClusterState = Settings.builder().put(NodeDuressSettings.SETTING_NATIVE_MEMORY_LIMIT_LEGACY.getKey(), "4gb").build();

        // Must pass BUILT_IN_SETTING_UPGRADERS explicitly — the two-arg ClusterSettings
        // constructor defaults to an empty upgrader set, mirroring the path SettingsModule takes.
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
            new java.util.HashSet<>(ClusterSettings.BUILT_IN_SETTING_UPGRADERS)
        );
        Settings upgraded = clusterSettings.upgradeSettings(legacyClusterState);

        // Old key must be gone after upgrade.
        assertNull(upgraded.get(NodeDuressSettings.SETTING_NATIVE_MEMORY_LIMIT_LEGACY.getKey()));
        // New unified key must carry the original value, compared as bytes to avoid
        // depending on the internal string representation produced by the upgrader.
        assertEquals(4L * 1024 * 1024 * 1024, ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.get(upgraded).getBytes());
    }
}
