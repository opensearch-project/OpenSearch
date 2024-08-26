/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.wlm.WorkloadManagementSettings.NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME;
import static org.opensearch.wlm.WorkloadManagementSettings.NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME;
import static org.opensearch.wlm.WorkloadManagementSettings.NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME;
import static org.opensearch.wlm.WorkloadManagementSettings.NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME;

public class WorkloadManagementSettingsTests extends OpenSearchTestCase {

    /**
     * Tests the invalid value for {@code wlm.query_group.node.memory_rejection_threshold}
     * When the value is set more than {@code wlm.query_group.node.memory_cancellation_threshold} accidentally during
     * new feature development. This test is to ensure that {@link WorkloadManagementSettings} holds the
     * invariant {@code nodeLevelRejectionThreshold < nodeLevelCancellationThreshold}
     */
    public void testInvalidMemoryInstantiationOfWorkloadManagementSettings() {
        Settings settings = Settings.builder()
            .put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.8)
            .put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.7)
            .build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        assertThrows(IllegalArgumentException.class, () -> new WorkloadManagementSettings(settings, cs));
    }

    /**
     * Tests the invalid value for {@code wlm.query_group.node.cpu_rejection_threshold}
     * When the value is set more than {@code wlm.query_group.node.cpu_cancellation_threshold} accidentally during
     * new feature development. This test is to ensure that {@link WorkloadManagementSettings} holds the
     * invariant {@code nodeLevelRejectionThreshold < nodeLevelCancellationThreshold}
     */
    public void testInvalidCpuInstantiationOfWorkloadManagementSettings() {
        Settings settings = Settings.builder()
            .put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.8)
            .put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.7)
            .build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        assertThrows(IllegalArgumentException.class, () -> new WorkloadManagementSettings(settings, cs));
    }

    /**
     * Tests the valid value for {@code wlm.query_group.node.cpu_rejection_threshold}
     * Using setNodeLevelCpuRejectionThreshold function
     */
    public void testValidNodeLevelCpuRejectionThresholdCase1() {
        Settings settings = Settings.builder().put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.8).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        workloadManagementSettings.setNodeLevelCpuRejectionThreshold(0.7);
        assertEquals(0.7, workloadManagementSettings.getNodeLevelCpuRejectionThreshold(), 1e-9);
    }

    /**
     * Tests the valid value for {@code wlm.query_group.node.cpu_rejection_threshold}
     */
    public void testValidNodeLevelCpuRejectionThresholdCase2() {
        Settings settings = Settings.builder().put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.79).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        assertEquals(0.79, workloadManagementSettings.getNodeLevelCpuRejectionThreshold(), 1e-9);
    }

    /**
     * Tests the invalid value for {@code wlm.query_group.node.cpu_rejection_threshold}
     * When the value is set more than {@literal 0.9}
     */
    public void testInvalidNodeLevelCpuRejectionThresholdCase1() {
        Settings settings = Settings.builder().put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.8).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        assertThrows(IllegalArgumentException.class, () -> workloadManagementSettings.setNodeLevelCpuRejectionThreshold(0.95));
    }

    /**
     * Tests the invalid value for {@code wlm.query_group.node.cpu_rejection_threshold}
     * When the value is set more than {@code wlm.query_group.node.cpu_cancellation_threshold}
     */
    public void testInvalidNodeLevelCpuRejectionThresholdCase2() {
        Settings settings = Settings.builder()
            .put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.7)
            .put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.8)
            .build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        assertThrows(IllegalArgumentException.class, () -> workloadManagementSettings.setNodeLevelCpuRejectionThreshold(0.85));
    }

    /**
     * Tests the valid value for {@code wlm.query_group.node.cpu_cancellation_threshold}
     */
    public void testValidNodeLevelCpuCancellationThresholdCase1() {
        Settings settings = Settings.builder().put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.8).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        assertEquals(0.8, workloadManagementSettings.getNodeLevelCpuRejectionThreshold(), 1e-9);
    }

    /**
     * Tests the valid value for {@code wlm.query_group.node.cpu_cancellation_threshold}
     * Using setNodeLevelCpuCancellationThreshold function
     */
    public void testValidNodeLevelCpuCancellationThresholdCase2() {
        Settings settings = Settings.builder().put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.8).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        workloadManagementSettings.setNodeLevelCpuCancellationThreshold(0.83);
        assertEquals(0.83, workloadManagementSettings.getNodeLevelCpuCancellationThreshold(), 1e-9);
    }

    /**
     * Tests the invalid value for {@code wlm.query_group.node.cpu_cancellation_threshold}
     * When the value is set more than {@literal 0.95}
     */
    public void testInvalidNodeLevelCpuCancellationThresholdCase1() {
        Settings settings = Settings.builder().put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.9).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        assertThrows(IllegalArgumentException.class, () -> workloadManagementSettings.setNodeLevelCpuCancellationThreshold(0.96));
    }

    /**
     * Tests the invalid value for {@code wlm.query_group.node.cpu_cancellation_threshold}
     * When the value is set less than {@code wlm.query_group.node.cpu_rejection_threshold}
     */
    public void testInvalidNodeLevelCpuCancellationThresholdCase2() {
        Settings settings = Settings.builder()
            .put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.7)
            .put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.8)
            .build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        assertThrows(IllegalArgumentException.class, () -> workloadManagementSettings.setNodeLevelCpuCancellationThreshold(0.65));
    }

    /**
     * Tests the valid value for {@code wlm.query_group.node.memory_cancellation_threshold}
     */
    public void testValidNodeLevelMemoryCancellationThresholdCase1() {
        Settings settings = Settings.builder().put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.8).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        assertEquals(0.8, workloadManagementSettings.getNodeLevelMemoryCancellationThreshold(), 1e-9);
    }

    /**
     * Tests the valid value for {@code wlm.query_group.node.memory_cancellation_threshold}
     * Using setNodeLevelMemoryCancellationThreshold function
     */
    public void testValidNodeLevelMemoryCancellationThresholdCase2() {
        Settings settings = Settings.builder().put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.8).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        workloadManagementSettings.setNodeLevelMemoryCancellationThreshold(0.83);
        assertEquals(0.83, workloadManagementSettings.getNodeLevelMemoryCancellationThreshold(), 1e-9);
    }

    /**
     * Tests the invalid value for {@code wlm.query_group.node.memory_cancellation_threshold}
     * When the value is set more than {@literal 0.95}
     */
    public void testInvalidNodeLevelMemoryCancellationThresholdCase1() {
        Settings settings = Settings.builder().put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.9).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        assertThrows(IllegalArgumentException.class, () -> workloadManagementSettings.setNodeLevelMemoryCancellationThreshold(0.96));
    }

    /**
     * Tests the invalid value for {@code wlm.query_group.node.memory_cancellation_threshold}
     * When the value is set less than {@code wlm.query_group.node.memory_rejection_threshold}
     */
    public void testInvalidNodeLevelMemoryCancellationThresholdCase2() {
        Settings settings = Settings.builder()
            .put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.7)
            .put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.8)
            .build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        assertThrows(IllegalArgumentException.class, () -> workloadManagementSettings.setNodeLevelMemoryCancellationThreshold(0.65));
    }

    /**
     * Tests the valid value for {@code wlm.query_group.node.memory_rejection_threshold}
     */
    public void testValidNodeLevelMemoryRejectionThresholdCase1() {
        Settings settings = Settings.builder().put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.79).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        assertEquals(0.79, workloadManagementSettings.getNodeLevelMemoryRejectionThreshold(), 1e-9);
    }

    /**
     * Tests the valid value for {@code wlm.query_group.node.memory_rejection_threshold}
     * Using setNodeLevelMemoryRejectionThreshold function
     */
    public void testValidNodeLevelMemoryRejectionThresholdCase2() {
        Settings settings = Settings.builder().put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.9).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        workloadManagementSettings.setNodeLevelMemoryRejectionThreshold(0.86);
        assertEquals(0.86, workloadManagementSettings.getNodeLevelMemoryRejectionThreshold(), 1e-9);
    }

    /**
     * Tests the invalid value for {@code wlm.query_group.node.memory_rejection_threshold}
     * When the value is set more than {@literal 0.9}
     */
    public void testInvalidNodeLevelMemoryRejectionThresholdCase1() {
        Settings settings = Settings.builder().put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.9).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        assertThrows(IllegalArgumentException.class, () -> workloadManagementSettings.setNodeLevelMemoryRejectionThreshold(0.92));
    }

    /**
     * Tests the invalid value for {@code wlm.query_group.node.memory_rejection_threshold}
     * When the value is set more than {@code wlm.query_group.node.memory_cancellation_threshold}
     */
    public void testInvalidNodeLevelMemoryRejectionThresholdCase2() {
        Settings settings = Settings.builder()
            .put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.7)
            .put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.8)
            .build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        assertThrows(IllegalArgumentException.class, () -> workloadManagementSettings.setNodeLevelMemoryRejectionThreshold(0.85));
    }
}
