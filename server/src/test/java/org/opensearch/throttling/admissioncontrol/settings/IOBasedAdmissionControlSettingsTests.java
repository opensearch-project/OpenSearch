/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.throttling.admissioncontrol.settings;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.throttling.admissioncontrol.controllers.IOBasedAdmissionController;
import org.opensearch.throttling.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.throttling.admissioncontrol.enums.TransportActionType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static org.mockito.Mockito.mock;

public class IOBasedAdmissionControlSettingsTests extends OpenSearchTestCase {
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
            "All the io based admission controller settings should be supported built in settings",
            settings.containsAll(
                Arrays.asList(
                    IOBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE
                )
            )
        );
    }

    public void testDefaultSettings() {
        IOBasedAdmissionControllerSettings ioBasedAdmissionControllerSettings = new IOBasedAdmissionControllerSettings(
            clusterService.getClusterSettings(),
            Settings.EMPTY
        );
        assertEquals(ioBasedAdmissionControllerSettings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.DISABLED);
    }

    public void testGetConfiguredSettings() {
        Settings settings = Settings.builder()
            .put(
                IOBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();

        IOBasedAdmissionControllerSettings ioBasedAdmissionControllerSettings = new IOBasedAdmissionControllerSettings(
            clusterService.getClusterSettings(),
            settings
        );
        assertEquals(ioBasedAdmissionControllerSettings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.ENFORCED);
    }

    public void testUpdateAfterGetDefaultSettings() {
        IOBasedAdmissionControllerSettings ioBasedAdmissionControllerSettings = new IOBasedAdmissionControllerSettings(
            clusterService.getClusterSettings(),
            Settings.EMPTY
        );
        Settings settings = Settings.builder()
            .put(
                IOBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        clusterService.getClusterSettings().applySettings(settings);
        assertEquals(ioBasedAdmissionControllerSettings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.ENFORCED);
    }

    public void testUpdateAfterGetConfiguredSettings() {
        Settings settings = Settings.builder()
            .put(
                IOBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();

        IOBasedAdmissionControllerSettings ioBasedAdmissionControllerSettings = new IOBasedAdmissionControllerSettings(
            clusterService.getClusterSettings(),
            settings
        );
        assertEquals(ioBasedAdmissionControllerSettings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.ENFORCED);
        Settings newSettings = Settings.builder()
            .put(settings)
            .put(
                IOBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.MONITOR.getMode()
            )
            .build();
        clusterService.getClusterSettings().applySettings(newSettings);
        assertEquals(ioBasedAdmissionControllerSettings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.MONITOR);
    }
}
