/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.settings;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Set;

public class CPUBasedAdmissionControlSettingsTests extends OpenSearchTestCase {
    private ClusterService clusterService;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("admission_controller_settings_test");
        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testSettingsExists() {
        Set<Setting<?>> settings = ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
        assertTrue(
            "All the cpu based admission controller settings should be supported built in settings",
            settings.containsAll(
                Arrays.asList(
                    CPUBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE,
                    CPUBasedAdmissionControllerSettings.SEARCH_CPU_USAGE_LIMIT,
                    CPUBasedAdmissionControllerSettings.INDEXING_CPU_USAGE_LIMIT
                )
            )
        );
    }

    public void testDefaultSettings() {
        CPUBasedAdmissionControllerSettings cpuBasedAdmissionControllerSettings = new CPUBasedAdmissionControllerSettings(
            clusterService.getClusterSettings(),
            Settings.EMPTY
        );
        long percent = 95;
        assertEquals(cpuBasedAdmissionControllerSettings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.DISABLED);
        assertEquals(cpuBasedAdmissionControllerSettings.getIndexingCPULimit().longValue(), percent);
        assertEquals(cpuBasedAdmissionControllerSettings.getSearchCPULimit().longValue(), percent);
        assertEquals(cpuBasedAdmissionControllerSettings.getTransportActionsList(), Arrays.asList("indexing", "search"));
    }

    public void testGetConfiguredSettings() {
        long percent = 95;
        long indexingPercent = 85;
        Settings settings = Settings.builder()
            .put(
                CPUBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .put(CPUBasedAdmissionControllerSettings.INDEXING_CPU_USAGE_LIMIT.getKey(), indexingPercent)
            .build();

        CPUBasedAdmissionControllerSettings cpuBasedAdmissionControllerSettings = new CPUBasedAdmissionControllerSettings(
            clusterService.getClusterSettings(),
            settings
        );
        assertEquals(cpuBasedAdmissionControllerSettings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.ENFORCED);
        assertEquals(cpuBasedAdmissionControllerSettings.getSearchCPULimit().longValue(), percent);
        assertEquals(cpuBasedAdmissionControllerSettings.getIndexingCPULimit().longValue(), indexingPercent);
    }

    public void testUpdateAfterGetDefaultSettings() {
        long percent = 95;
        long searchPercent = 80;
        CPUBasedAdmissionControllerSettings cpuBasedAdmissionControllerSettings = new CPUBasedAdmissionControllerSettings(
            clusterService.getClusterSettings(),
            Settings.EMPTY
        );
        Settings settings = Settings.builder()
            .put(
                CPUBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .put(CPUBasedAdmissionControllerSettings.SEARCH_CPU_USAGE_LIMIT.getKey(), searchPercent)
            .build();
        clusterService.getClusterSettings().applySettings(settings);
        assertEquals(cpuBasedAdmissionControllerSettings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.ENFORCED);
        assertEquals(cpuBasedAdmissionControllerSettings.getSearchCPULimit().longValue(), searchPercent);
        assertEquals(cpuBasedAdmissionControllerSettings.getIndexingCPULimit().longValue(), percent);
    }

    public void testUpdateAfterGetConfiguredSettings() {
        long percent = 95;
        long indexingPercent = 85;
        long searchPercent = 80;
        Settings settings = Settings.builder()
            .put(
                CPUBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .put(CPUBasedAdmissionControllerSettings.SEARCH_CPU_USAGE_LIMIT.getKey(), searchPercent)
            .build();

        CPUBasedAdmissionControllerSettings cpuBasedAdmissionControllerSettings = new CPUBasedAdmissionControllerSettings(
            clusterService.getClusterSettings(),
            settings
        );
        assertEquals(cpuBasedAdmissionControllerSettings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.ENFORCED);
        assertEquals(cpuBasedAdmissionControllerSettings.getSearchCPULimit().longValue(), searchPercent);
        assertEquals(cpuBasedAdmissionControllerSettings.getIndexingCPULimit().longValue(), percent);

        Settings updatedSettings = Settings.builder()
            .put(
                CPUBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.MONITOR.getMode()
            )
            .put(CPUBasedAdmissionControllerSettings.INDEXING_CPU_USAGE_LIMIT.getKey(), indexingPercent)
            .build();
        clusterService.getClusterSettings().applySettings(updatedSettings);
        assertEquals(cpuBasedAdmissionControllerSettings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.MONITOR);
        assertEquals(cpuBasedAdmissionControllerSettings.getSearchCPULimit().longValue(), searchPercent);
        assertEquals(cpuBasedAdmissionControllerSettings.getIndexingCPULimit().longValue(), indexingPercent);

        searchPercent = 70;

        updatedSettings = Settings.builder()
            .put(updatedSettings)
            .put(CPUBasedAdmissionControllerSettings.SEARCH_CPU_USAGE_LIMIT.getKey(), searchPercent)
            .build();
        clusterService.getClusterSettings().applySettings(updatedSettings);

        assertEquals(cpuBasedAdmissionControllerSettings.getSearchCPULimit().longValue(), searchPercent);
        assertEquals(cpuBasedAdmissionControllerSettings.getIndexingCPULimit().longValue(), indexingPercent);
    }
}
