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
 * A unit test to validate the former name of the setting 'cluster.no_cluster_manager_block' still take effect,
 * after it is deprecated, so that the backwards compatibility is maintained.
 * The test can be removed along with removing support of the deprecated setting.
 */
public class NoClusterManagerBlockServiceRenamedSettingTests extends OpenSearchTestCase {

    /**
     * Validate the both settings are known and supported.
     */
    public void testReindexSettingsExist() {
        Set<Setting<?>> settings = ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
        assertTrue(
            "Both 'cluster.no_cluster_manager_block' and its predecessor should be supported built-in settings.",
            settings.containsAll(
                Arrays.asList(
                    NoClusterManagerBlockService.NO_MASTER_BLOCK_SETTING,
                    NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_SETTING
                )
            )
        );
    }

    /**
     * Validate the default value of the both settings is the same.
     */
    public void testSettingFallback() {
        assertEquals(
            NoClusterManagerBlockService.NO_MASTER_BLOCK_SETTING.get(Settings.EMPTY),
            NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_SETTING.get(Settings.EMPTY)

        );
    }

    /**
     * Validate the new setting can be configured correctly, and it doesn't impact the old setting.
     */
    public void testSettingGetValue() {
        Settings settings = Settings.builder().put("cluster.no_cluster_manager_block", "all").build();
        assertEquals(
            NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ALL,
            NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_SETTING.get(settings)
        );
        assertEquals(
            NoClusterManagerBlockService.NO_MASTER_BLOCK_SETTING.getDefault(Settings.EMPTY),
            NoClusterManagerBlockService.NO_MASTER_BLOCK_SETTING.get(settings)
        );
    }

    /**
     * Validate the value of the old setting will be applied to the new setting, if the new setting is not configured.
     */
    public void testSettingGetValueWithFallback() {
        Settings settings = Settings.builder().put("cluster.no_master_block", "metadata_write").build();
        assertEquals(
            NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_METADATA_WRITES,
            NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_SETTING.get(settings)
        );
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { NoClusterManagerBlockService.NO_MASTER_BLOCK_SETTING });
    }

    /**
     * Validate the value of the old setting will be ignored, if the new setting is configured.
     */
    public void testSettingGetValueWhenBothAreConfigured() {
        Settings settings = Settings.builder()
            .put("cluster.no_cluster_manager_block", "all")
            .put("cluster.no_master_block", "metadata_write")
            .build();
        assertEquals(
            NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ALL,
            NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_SETTING.get(settings)
        );
        assertEquals(
            NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_METADATA_WRITES,
            NoClusterManagerBlockService.NO_MASTER_BLOCK_SETTING.get(settings)
        );
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { NoClusterManagerBlockService.NO_MASTER_BLOCK_SETTING });
    }

}
