/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.throttling.admissioncontrol.controllers;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.throttling.admissioncontrol.AdmissionControlSettings;
import org.opensearch.throttling.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.throttling.admissioncontrol.settings.IOBasedAdmissionControllerSettings;

import java.util.Set;

public class IOBasedAdmissionControllerTests extends OpenSearchTestCase {
    private ClusterService clusterService;
    private ThreadPool threadPool;
    IOBasedAdmissionController ioBasedAdmissionController = null;

    String action = "TEST_ACTION";

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

    public void testCheckDefaultParameters() {
        ioBasedAdmissionController = new IOBasedAdmissionController(
            AdmissionControlSettings.IO_BASED_ADMISSION_CONTROLLER,
            Settings.EMPTY,
            clusterService.getClusterSettings()
        );
        assertEquals(ioBasedAdmissionController.getName(), AdmissionControlSettings.IO_BASED_ADMISSION_CONTROLLER);
        assertEquals(ioBasedAdmissionController.getRejectionCount(), 0);
        assertEquals(ioBasedAdmissionController.settings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.DISABLED);
        assertFalse(ioBasedAdmissionController.isEnabledForTransportLayer());
    }

    public void testCheckUpdateSettings() {
        ioBasedAdmissionController = new IOBasedAdmissionController(
            AdmissionControlSettings.IO_BASED_ADMISSION_CONTROLLER,
            Settings.EMPTY,
            clusterService.getClusterSettings()
        );
        Settings settings = Settings.builder()
            .put(
                IOBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        clusterService.getClusterSettings().applySettings(settings);

        assertEquals(ioBasedAdmissionController.getName(), AdmissionControlSettings.IO_BASED_ADMISSION_CONTROLLER);
        assertEquals(ioBasedAdmissionController.getRejectionCount(), 0);
        assertEquals(ioBasedAdmissionController.settings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.ENFORCED);
        assertTrue(ioBasedAdmissionController.isEnabledForTransportLayer());
    }

    public void testApplyControllerWithDefaultSettings() {
        ioBasedAdmissionController = new IOBasedAdmissionController(
            AdmissionControlSettings.IO_BASED_ADMISSION_CONTROLLER,
            Settings.EMPTY,
            clusterService.getClusterSettings()
        );
        assertEquals(ioBasedAdmissionController.getRejectionCount(), 0);
        assertEquals(ioBasedAdmissionController.settings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.DISABLED);
        action = "indices:data/write/bulk[s][p]";
        ioBasedAdmissionController.acquire(action);
        assertEquals(ioBasedAdmissionController.getRejectionCount(), 0);
    }

    public void testApplyControllerWhenSettingsEnabled() {
        Settings settings = Settings.builder()
            .put(
                IOBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        ioBasedAdmissionController = new IOBasedAdmissionController(
            AdmissionControlSettings.IO_BASED_ADMISSION_CONTROLLER,
            settings,
            clusterService.getClusterSettings()
        );
        assertTrue(ioBasedAdmissionController.isEnabledForTransportLayer());
        assertEquals(ioBasedAdmissionController.getRejectionCount(), 0);
        action = "indices:data/write/bulk[s][p]";
        ioBasedAdmissionController.acquire(action);
        assertEquals(ioBasedAdmissionController.getRejectionCount(), 1);
        Set<String> actions = ioBasedAdmissionController.getAllowedTransportActions();
        assertTrue(actions.contains(action));
    }

    public void testApplyControllerWhenSettingsEnabledWithInvalidAction() {
        Settings settings = Settings.builder()
            .put(
                IOBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        ioBasedAdmissionController = new IOBasedAdmissionController(
            AdmissionControlSettings.IO_BASED_ADMISSION_CONTROLLER,
            settings,
            clusterService.getClusterSettings()
        );
        assertTrue(ioBasedAdmissionController.isEnabledForTransportLayer());
        assertEquals(ioBasedAdmissionController.getRejectionCount(), 0);
        action = "TEST";
        ioBasedAdmissionController.acquire(action);
        assertEquals(ioBasedAdmissionController.getRejectionCount(), 0);
    }
}
