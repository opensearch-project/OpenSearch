/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

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
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Set;

public class IoBasedAdmissionControllerSettingsTests extends OpenSearchTestCase {
    private ClusterService clusterService;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("io_based_admission_controller_settings_test");
        clusterService = ClusterServiceUtils.createClusterService(
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
            "All the IO based admission controller settings should be supported built in settings",
            settings.containsAll(
                Arrays.asList(
                    IoBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE,
                    IoBasedAdmissionControllerSettings.SEARCH_IO_USAGE_LIMIT,
                    IoBasedAdmissionControllerSettings.INDEXING_IO_USAGE_LIMIT
                )
            )
        );
    }

    public void testDefaultSettings() {
        IoBasedAdmissionControllerSettings ioBasedAdmissionControllerSettings = new IoBasedAdmissionControllerSettings(
            clusterService.getClusterSettings(),
            Settings.EMPTY
        );
        long percent = 95;
        assertEquals(ioBasedAdmissionControllerSettings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.DISABLED);
        assertEquals(ioBasedAdmissionControllerSettings.getIndexingIOUsageLimit().longValue(), percent);
        assertEquals(ioBasedAdmissionControllerSettings.getSearchIOUsageLimit().longValue(), percent);
        assertEquals(
            ioBasedAdmissionControllerSettings.getClusterAdminIOUsageLimit().longValue(),
            IoBasedAdmissionControllerSettings.Defaults.CLUSTER_ADMIN_IO_USAGE_LIMIT
        );
    }

    public void testGetConfiguredSettings() {
        long percent = 95;
        long indexingPercent = 85;
        Settings settings = Settings.builder()
            .put(
                IoBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .put(IoBasedAdmissionControllerSettings.INDEXING_IO_USAGE_LIMIT.getKey(), indexingPercent)
            .build();

        IoBasedAdmissionControllerSettings ioBasedAdmissionControllerSettings = new IoBasedAdmissionControllerSettings(
            clusterService.getClusterSettings(),
            settings
        );
        assertEquals(ioBasedAdmissionControllerSettings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.ENFORCED);
        assertEquals(ioBasedAdmissionControllerSettings.getSearchIOUsageLimit().longValue(), percent);
        assertEquals(ioBasedAdmissionControllerSettings.getIndexingIOUsageLimit().longValue(), indexingPercent);
    }

    public void testUpdateAfterGetDefaultSettings() {
        long percent = 95;
        long searchPercent = 80;
        IoBasedAdmissionControllerSettings ioBasedAdmissionControllerSettings = new IoBasedAdmissionControllerSettings(
            clusterService.getClusterSettings(),
            Settings.EMPTY
        );
        Settings settings = Settings.builder()
            .put(
                IoBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .put(IoBasedAdmissionControllerSettings.SEARCH_IO_USAGE_LIMIT.getKey(), searchPercent)
            .build();

        clusterService.getClusterSettings().applySettings(settings);
        assertEquals(ioBasedAdmissionControllerSettings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.ENFORCED);
        assertEquals(ioBasedAdmissionControllerSettings.getSearchIOUsageLimit().longValue(), searchPercent);
        assertEquals(ioBasedAdmissionControllerSettings.getIndexingIOUsageLimit().longValue(), percent);
    }

    public void testUpdateAfterGetConfiguredSettings() {
        long percent = 95;
        long indexingPercent = 85;
        long searchPercent = 80;
        Settings settings = Settings.builder()
            .put(
                IoBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .put(IoBasedAdmissionControllerSettings.SEARCH_IO_USAGE_LIMIT.getKey(), searchPercent)
            .build();

        IoBasedAdmissionControllerSettings ioBasedAdmissionControllerSettings = new IoBasedAdmissionControllerSettings(
            clusterService.getClusterSettings(),
            settings
        );
        assertEquals(ioBasedAdmissionControllerSettings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.ENFORCED);
        assertEquals(ioBasedAdmissionControllerSettings.getSearchIOUsageLimit().longValue(), searchPercent);
        assertEquals(ioBasedAdmissionControllerSettings.getIndexingIOUsageLimit().longValue(), percent);
        assertEquals(
            ioBasedAdmissionControllerSettings.getClusterAdminIOUsageLimit().longValue(),
            IoBasedAdmissionControllerSettings.Defaults.CLUSTER_ADMIN_IO_USAGE_LIMIT
        );

        Settings updatedSettings = Settings.builder()
            .put(
                IoBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.MONITOR.getMode()
            )
            .put(IoBasedAdmissionControllerSettings.INDEXING_IO_USAGE_LIMIT.getKey(), indexingPercent)
            .build();
        clusterService.getClusterSettings().applySettings(updatedSettings);
        assertEquals(ioBasedAdmissionControllerSettings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.MONITOR);
        assertEquals(ioBasedAdmissionControllerSettings.getSearchIOUsageLimit().longValue(), searchPercent);
        assertEquals(ioBasedAdmissionControllerSettings.getIndexingIOUsageLimit().longValue(), indexingPercent);
        assertEquals(
            ioBasedAdmissionControllerSettings.getClusterAdminIOUsageLimit().longValue(),
            IoBasedAdmissionControllerSettings.Defaults.CLUSTER_ADMIN_IO_USAGE_LIMIT
        );

        searchPercent = 70;
        updatedSettings = Settings.builder()
            .put(updatedSettings)
            .put(IoBasedAdmissionControllerSettings.SEARCH_IO_USAGE_LIMIT.getKey(), searchPercent)
            .build();

        clusterService.getClusterSettings().applySettings(updatedSettings);
        assertEquals(ioBasedAdmissionControllerSettings.getSearchIOUsageLimit().longValue(), searchPercent);
        assertEquals(ioBasedAdmissionControllerSettings.getIndexingIOUsageLimit().longValue(), indexingPercent);
        assertEquals(
            ioBasedAdmissionControllerSettings.getClusterAdminIOUsageLimit().longValue(),
            IoBasedAdmissionControllerSettings.Defaults.CLUSTER_ADMIN_IO_USAGE_LIMIT
        );
    }
}
