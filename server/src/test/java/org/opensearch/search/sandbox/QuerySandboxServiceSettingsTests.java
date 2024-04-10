/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandbox;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.search.sandbox.QuerySandboxServiceSettings.*;

public class QuerySandboxServiceSettingsTests extends OpenSearchTestCase {

    /**
     * Tests the valid value of {@code node.sandbox.max_count}
     */
    public void testValidMaxSandboxCountSetting() {
        Settings settings = Settings.builder().put(SANDBOX_COUNT_SETTING_NAME, 100).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QuerySandboxServiceSettings querySandboxServiceSettings = new QuerySandboxServiceSettings(settings, cs);
        assertEquals(100, querySandboxServiceSettings.getMaxSandboxCount());
    }


    /**
     * test the invalid value of {@code node.sandbox.max_count}
     */
    public void testInValidMaxSandboxCountSetting() {
        Settings settings = Settings.builder().put(SANDBOX_COUNT_SETTING_NAME, -100).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        assertThrows(IllegalArgumentException.class,
            () -> new QuerySandboxServiceSettings(settings, cs));
    }

    /**
     * Tests the valid value for {@code query_sandbox.node.rejection_threshold}
     */
    public void testValidNodeLevelRejectionThreshold() {
        Settings settings = Settings.builder().put(QUERY_SANDBOX_NODE_REJECTION_THRESHOLD_SETTING_NAME, 0.80).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QuerySandboxServiceSettings querySandboxServiceSettings = new QuerySandboxServiceSettings(settings, cs);
        assertEquals(0.80, querySandboxServiceSettings.getNodeLevelJvmRejectionThreshold(), 1e-9);
    }

    /**
     * Tests the invalid value for {@code query_sandbox.node.rejection_threshold}
     * When the value is set more than {@literal 0.90}
     */
    public void testInValidNodeLevelRejectionThresholdCase1() {
        Settings settings = Settings.builder().put(QUERY_SANDBOX_NODE_REJECTION_THRESHOLD_SETTING_NAME, 0.80).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QuerySandboxServiceSettings querySandboxServiceSettings = new QuerySandboxServiceSettings(settings, cs);
        assertThrows(IllegalArgumentException.class,
            () -> querySandboxServiceSettings.setNodeLevelJvmRejectionThreshold(0.95));
    }

    /**
     * Tests the invalid value for {@code query_sandbox.node.rejection_threshold}
     * When the value is set more than {@code query_sandbox.node.cancellation_threshold}
     */
    public void testInValidNodeLevelRejectionThresholdCase2() {
        Settings settings = Settings.builder().put(QUERY_SANDBOX_NODE_REJECTION_THRESHOLD_SETTING_NAME, 0.70)
            .put(QUERY_SANDBOX_NODE_CANCELLATION_THRESHOLD_SETTING_NAME, 0.80)
            .build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QuerySandboxServiceSettings querySandboxServiceSettings = new QuerySandboxServiceSettings(settings, cs);
        assertThrows(IllegalArgumentException.class,
            () -> querySandboxServiceSettings.setNodeLevelJvmRejectionThreshold(0.85));
    }


    /**
     * Tests the invalid value for {@code query_sandbox.node.rejection_threshold}
     * When the value is set more than {@code query_sandbox.node.cancellation_threshold} accidentally during
     * new feature development. This test is to ensure that {@link QuerySandboxServiceSettings} holds the
     * invariant {@code nodeLevelRejectionThreshold < nodeLevelCancellationThreshold}
     */
    public void testInValidInstantiationOfQuerySandboxServiceSettings() {
        Settings settings = Settings.builder().put(QUERY_SANDBOX_NODE_REJECTION_THRESHOLD_SETTING_NAME, 0.80)
            .put(QUERY_SANDBOX_NODE_CANCELLATION_THRESHOLD_SETTING_NAME, 0.70)
            .build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        assertThrows(IllegalArgumentException.class,
            () -> new QuerySandboxServiceSettings(settings, cs));
    }

    /**
     * Tests the valid value for {@code query_sandbox.node.cancellation_threshold}
     */
    public void testValidNodeLevelCancellationThreshold() {
        Settings settings = Settings.builder().put(QUERY_SANDBOX_NODE_CANCELLATION_THRESHOLD_SETTING_NAME, 0.80).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QuerySandboxServiceSettings querySandboxServiceSettings = new QuerySandboxServiceSettings(settings, cs);
        assertEquals(0.80, querySandboxServiceSettings.getNodeLevelJvmRejectionThreshold(), 1e-9);
    }

    /**
     * Tests the invalid value for {@code query_sandbox.node.cancellation_threshold}
     * When the value is set more than {@literal 0.95}
     */
    public void testInValidNodeLevelCancellationThresholdCase1() {
        Settings settings = Settings.builder().put(QUERY_SANDBOX_NODE_CANCELLATION_THRESHOLD_SETTING_NAME, 0.80).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QuerySandboxServiceSettings querySandboxServiceSettings = new QuerySandboxServiceSettings(settings, cs);
        assertThrows(IllegalArgumentException.class,
            () -> querySandboxServiceSettings.setNodeLevelJvmRejectionThreshold(0.96));
    }

    /**
     * Tests the invalid value for {@code query_sandbox.node.cancellation_threshold}
     * When the value is set less than {@code query_sandbox.node.rejection_threshold}
     */
    public void testInValidNodeLevelCancellationThresholdCase2() {
        Settings settings = Settings.builder().put(QUERY_SANDBOX_NODE_REJECTION_THRESHOLD_SETTING_NAME, 0.70)
            .put(QUERY_SANDBOX_NODE_CANCELLATION_THRESHOLD_SETTING_NAME, 0.80)
            .build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QuerySandboxServiceSettings querySandboxServiceSettings = new QuerySandboxServiceSettings(settings, cs);
        assertThrows(IllegalArgumentException.class,
            () -> querySandboxServiceSettings.setNodeLevelJvmRejectionThreshold(0.85));
    }

}
