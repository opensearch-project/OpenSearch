/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Set;

/**
 * A unit test to validate the former name of the setting 'cluster.initial_cluster_manager_nodes' still take effect,
 * after it is deprecated, so that the backwards compatibility is maintained.
 * The test can be removed along with removing support of the deprecated setting.
 */
public class ClusterBootstrapServiceRenamedSettingTests extends OpenSearchTestCase {
    /**
     * Validate the both settings are known and supported.
     */
    public void testClusterBootstrapServiceSettingsExist() {
        Set<Setting<?>> settings = ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
        assertTrue(
            "Both 'cluster.initial_cluster_manager_nodes' and its predecessor should be supported built-in settings.",
            settings.containsAll(
                Arrays.asList(
                    ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING,
                    ClusterBootstrapService.INITIAL_CLUSTER_MANAGER_NODES_SETTING
                )
            )
        );
    }

    /**
     * Validate the default value of the both settings is the same.
     */
    public void testSettingFallback() {
        assertEquals(
            ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.get(Settings.EMPTY),
            ClusterBootstrapService.INITIAL_CLUSTER_MANAGER_NODES_SETTING.get(Settings.EMPTY)
        );
    }

    /**
     * Validate the new setting can be configured correctly, and it doesn't impact the old setting.
     */
    public void testSettingGetValue() {
        Settings settings = Settings.builder().put("cluster.initial_cluster_manager_nodes", "node-a").build();
        assertEquals(Arrays.asList("node-a"), ClusterBootstrapService.INITIAL_CLUSTER_MANAGER_NODES_SETTING.get(settings));
        assertEquals(
            ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.getDefault(Settings.EMPTY),
            ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.get(settings)
        );
    }

    /**
     * Validate the value of the old setting will be applied to the new setting, if the new setting is not configured.
     */
    public void testSettingGetValueWithFallback() {
        Settings settings = Settings.builder().put("cluster.initial_master_nodes", "node-a").build();
        assertEquals(Arrays.asList("node-a"), ClusterBootstrapService.INITIAL_CLUSTER_MANAGER_NODES_SETTING.get(settings));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING });
    }

    /**
     * Validate the value of the old setting will be ignored, if the new setting is configured.
     */
    public void testSettingGetValueWhenBothAreConfigured() {
        Settings settings = Settings.builder()
            .put("cluster.initial_cluster_manager_nodes", "node-a")
            .put("cluster.initial_master_nodes", "node-a, node-b")
            .build();
        assertEquals(Arrays.asList("node-a"), ClusterBootstrapService.INITIAL_CLUSTER_MANAGER_NODES_SETTING.get(settings));
        assertEquals(Arrays.asList("node-a", "node-b"), ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.get(settings));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING });
    }
}
