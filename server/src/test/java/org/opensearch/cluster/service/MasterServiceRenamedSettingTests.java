/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Set;

/**
 * A unit test to validate the former name of the setting 'cluster.service.slow_cluster_manager_task_logging_threshold' still take effect,
 * after it is deprecated, so that the backwards compatibility is maintained.
 * The test can be removed along with removing support of the deprecated setting.
 */
public class MasterServiceRenamedSettingTests extends OpenSearchTestCase {

    /**
     * Validate the both settings are known and supported.
     */
    public void testClusterManagerServiceSettingsExist() {
        Set<Setting<?>> settings = ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
        assertTrue(
            "Both 'cluster.service.slow_cluster_manager_task_logging_threshold' and its predecessor should be supported built-in settings",
            settings.containsAll(
                Arrays.asList(
                    MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                    MasterService.CLUSTER_MANAGER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING
                )
            )
        );
    }

    /**
     * Validate the default value of the both settings is the same.
     */
    public void testSettingFallback() {
        assertEquals(
            MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY),
            MasterService.CLUSTER_MANAGER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY)
        );
    }

    /**
     * Validate the new setting can be configured correctly, and it doesn't impact the old setting.
     */
    public void testSettingGetValue() {
        Settings settings = Settings.builder().put("cluster.service.slow_cluster_manager_task_logging_threshold", "9s").build();
        assertEquals(
            TimeValue.timeValueSeconds(9),
            MasterService.CLUSTER_MANAGER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings)
        );
        assertEquals(
            MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.getDefault(Settings.EMPTY),
            MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings)

        );
    }

    /**
     * Validate the value of the old setting will be applied to the new setting, if the new setting is not configured.
     */
    public void testSettingGetValueWithFallback() {
        Settings settings = Settings.builder().put("cluster.service.slow_master_task_logging_threshold", "8s").build();
        assertEquals(
            TimeValue.timeValueSeconds(8),
            MasterService.CLUSTER_MANAGER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings)

        );
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING });
    }

    /**
     * Validate the value of the old setting will be ignored, if the new setting is configured.
     */
    public void testSettingGetValueWhenBothAreConfigured() {
        Settings settings = Settings.builder()
            .put("cluster.service.slow_cluster_manager_task_logging_threshold", "9s")
            .put("cluster.service.slow_master_task_logging_threshold", "8s")
            .build();
        assertEquals(
            TimeValue.timeValueSeconds(9),
            MasterService.CLUSTER_MANAGER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings)

        );
        assertEquals(TimeValue.timeValueSeconds(8), MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING });
    }

}
