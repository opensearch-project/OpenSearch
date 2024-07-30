/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query_group;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.search.query_group.QueryGroupServiceSettings.NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME;
import static org.opensearch.search.query_group.QueryGroupServiceSettings.NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME;
import static org.opensearch.search.query_group.QueryGroupServiceSettings.NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME;
import static org.opensearch.search.query_group.QueryGroupServiceSettings.NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME;
import static org.opensearch.search.query_group.QueryGroupServiceSettings.QUERY_GROUP_COUNT_SETTING_NAME;

public class QueryGroupServiceSettingsTests extends OpenSearchTestCase {

    /**
     * Tests the valid value of {@code node.query_group.max_count}
     */
    public void testValidMaxSandboxCountSetting() {
        Settings settings = Settings.builder().put(QUERY_GROUP_COUNT_SETTING_NAME, 100).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryGroupServiceSettings queryGroupServiceSettings = new QueryGroupServiceSettings(settings, cs);
        assertEquals(100, queryGroupServiceSettings.getMaxQueryGroupCount());
    }

    /**
     * test the invalid value of {@code node.query_group.max_count}
     */
    public void testInValidMaxSandboxCountSetting() {
        Settings settings = Settings.builder().put(QUERY_GROUP_COUNT_SETTING_NAME, -100).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        assertThrows(IllegalArgumentException.class, () -> new QueryGroupServiceSettings(settings, cs));
    }

    /**
     * Tests the valid value for {@code query_group.node.memory_rejection_threshold}
     */
    public void testValidNodeLevelMemoryRejectionThreshold() {
        Settings settings = Settings.builder().put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.80).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryGroupServiceSettings queryGroupServiceSettings = new QueryGroupServiceSettings(settings, cs);
        assertEquals(0.80, queryGroupServiceSettings.getNodeLevelMemoryRejectionThreshold(), 1e-9);
    }

    /**
     * Tests the valid value for {@code query_group.node.cpu__rejection_threshold}
     */
    public void testValidNodeLevelCpuRejectionThreshold() {
        Settings settings = Settings.builder().put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.80).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryGroupServiceSettings queryGroupServiceSettings = new QueryGroupServiceSettings(settings, cs);
        assertEquals(0.80, queryGroupServiceSettings.getNodeLevelCpuRejectionThreshold(), 1e-9);
    }

    /**
     * Tests the invalid value for {@code query_group.node.memory_rejection_threshold}
     * When the value is set more than {@literal 0.90}
     */
    public void testInValidNodeLevelMemoryRejectionThresholdCase1() {
        Settings settings = Settings.builder().put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.80).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryGroupServiceSettings queryGroupServiceSettings = new QueryGroupServiceSettings(settings, cs);
        assertThrows(IllegalArgumentException.class, () -> queryGroupServiceSettings.setNodeLevelMemoryRejectionThreshold(0.95));
    }

    /**
     * Tests the invalid value for {@code query_group.node.cpu_rejection_threshold}
     * When the value is set more than {@literal 0.90}
     */
    public void testInValidNodeLevelCpuRejectionThresholdCase1() {
        Settings settings = Settings.builder().put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.80).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryGroupServiceSettings queryGroupServiceSettings = new QueryGroupServiceSettings(settings, cs);
        assertThrows(IllegalArgumentException.class, () -> queryGroupServiceSettings.setNodeLevelCpuRejectionThreshold(0.95));
    }

    /**
     * Tests the invalid value for {@code query_group.node.memory_rejection_threshold}
     * When the value is set more than {@code query_group.node.memory_cancellation_threshold}
     */
    public void testInValidNodeLevelMemoryRejectionThresholdCase2() {
        Settings settings = Settings.builder()
            .put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.70)
            .put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.80)
            .build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryGroupServiceSettings queryGroupServiceSettings = new QueryGroupServiceSettings(settings, cs);
        assertThrows(IllegalArgumentException.class, () -> queryGroupServiceSettings.setNodeLevelMemoryRejectionThreshold(0.85));
    }

    /**
     * Tests the invalid value for {@code query_group.node.cpu_rejection_threshold}
     * When the value is set more than {@code query_group.node.cpu_cancellation_threshold}
     */
    public void testInValidNodeLevelCpuRejectionThresholdCase2() {
        Settings settings = Settings.builder()
            .put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.70)
            .put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.80)
            .build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryGroupServiceSettings queryGroupServiceSettings = new QueryGroupServiceSettings(settings, cs);
        assertThrows(IllegalArgumentException.class, () -> queryGroupServiceSettings.setNodeLevelCpuRejectionThreshold(0.85));
    }

    /**
     * Tests the invalid value for {@code query_group.node.memory_rejection_threshold}
     * When the value is set more than {@code query_group.node.memory_cancellation_threshold} accidentally during
     * new feature development. This test is to ensure that {@link QueryGroupServiceSettings} holds the
     * invariant {@code nodeLevelRejectionThreshold < nodeLevelCancellationThreshold}
     */
    public void testInValidMemoryInstantiationOfQueryGroupServiceSettings() {
        Settings settings = Settings.builder()
            .put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.80)
            .put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.70)
            .build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        assertThrows(IllegalArgumentException.class, () -> new QueryGroupServiceSettings(settings, cs));
    }

    /**
     * Tests the invalid value for {@code query_group.node.cpu_rejection_threshold}
     * When the value is set more than {@code query_group.node.cpu_cancellation_threshold} accidentally during
     * new feature development. This test is to ensure that {@link QueryGroupServiceSettings} holds the
     * invariant {@code nodeLevelRejectionThreshold < nodeLevelCancellationThreshold}
     */
    public void testInValidCpuInstantiationOfQueryGroupServiceSettings() {
        Settings settings = Settings.builder()
            .put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.80)
            .put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.70)
            .build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        assertThrows(IllegalArgumentException.class, () -> new QueryGroupServiceSettings(settings, cs));
    }

    /**
     * Tests the valid value for {@code query_group.node.memory_cancellation_threshold}
     */
    public void testValidNodeLevelMemoryCancellationThreshold() {
        Settings settings = Settings.builder().put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.80).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryGroupServiceSettings queryGroupServiceSettings = new QueryGroupServiceSettings(settings, cs);
        assertEquals(0.80, queryGroupServiceSettings.getNodeLevelMemoryRejectionThreshold(), 1e-9);
    }

    /**
     * Tests the valid value for {@code query_group.node.cpu_cancellation_threshold}
     */
    public void testValidNodeLevelCpuCancellationThreshold() {
        Settings settings = Settings.builder().put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.80).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryGroupServiceSettings queryGroupServiceSettings = new QueryGroupServiceSettings(settings, cs);
        assertEquals(0.80, queryGroupServiceSettings.getNodeLevelCpuRejectionThreshold(), 1e-9);
    }

    /**
     * Tests the invalid value for {@code query_group.node.memory_cancellation_threshold}
     * When the value is set more than {@literal 0.95}
     */
    public void testInValidNodeLevelMemoryCancellationThresholdCase1() {
        Settings settings = Settings.builder().put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.80).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryGroupServiceSettings queryGroupServiceSettings = new QueryGroupServiceSettings(settings, cs);
        assertThrows(IllegalArgumentException.class, () -> queryGroupServiceSettings.setNodeLevelMemoryRejectionThreshold(0.96));
    }

    /**
     * Tests the invalid value for {@code query_group.node.memory_cancellation_threshold}
     * When the value is set more than {@literal 0.95}
     */
    public void testInValidNodeLevelCpuCancellationThresholdCase1() {
        Settings settings = Settings.builder().put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.80).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryGroupServiceSettings queryGroupServiceSettings = new QueryGroupServiceSettings(settings, cs);
        assertThrows(IllegalArgumentException.class, () -> queryGroupServiceSettings.setNodeLevelCpuRejectionThreshold(0.96));
    }

    /**
     * Tests the invalid value for {@code query_group.node.memory_cancellation_threshold}
     * When the value is set less than {@code query_group.node.memory_rejection_threshold}
     */
    public void testInValidNodeLevelMemoryCancellationThresholdCase2() {
        Settings settings = Settings.builder()
            .put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.70)
            .put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.80)
            .build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryGroupServiceSettings queryGroupServiceSettings = new QueryGroupServiceSettings(settings, cs);
        assertThrows(IllegalArgumentException.class, () -> queryGroupServiceSettings.setNodeLevelMemoryRejectionThreshold(0.85));
    }

    /**
     * Tests the invalid value for {@code query_group.node.cpu_cancellation_threshold}
     * When the value is set less than {@code query_group.node.cpu_rejection_threshold}
     */
    public void testInValidNodeLevelCpuCancellationThresholdCase2() {
        Settings settings = Settings.builder()
            .put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.70)
            .put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.80)
            .build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryGroupServiceSettings queryGroupServiceSettings = new QueryGroupServiceSettings(settings, cs);
        assertThrows(IllegalArgumentException.class, () -> queryGroupServiceSettings.setNodeLevelCpuRejectionThreshold(0.85));
    }

}
