/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.throttling.admissioncontrol;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.throttling.admissioncontrol.controllers.AdmissionController;
import org.opensearch.throttling.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.throttling.admissioncontrol.settings.IOBasedAdmissionControllerSettings;

import java.util.List;

public class AdmissionControlServiceTests extends OpenSearchTestCase {
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private AdmissionControlService admissionControlService;
    private String action = "";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("admission_controller_settings_test");
        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        action = "indexing";
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testWhenAdmissionControllerDisabled() {
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService.getClusterSettings(), threadPool);
        assertFalse(admissionControlService.isTransportLayerAdmissionControlEnabled());
        assertFalse(admissionControlService.isTransportLayerAdmissionControlEnforced());

        Settings settings = Settings.builder()
            .put(AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.MONITOR.getMode())
            .build();

        clusterService.getClusterSettings().applySettings(settings);
        assertTrue(admissionControlService.isTransportLayerAdmissionControlEnabled());
        assertFalse(admissionControlService.isTransportLayerAdmissionControlEnforced());

        settings = Settings.builder()
            .put(AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.ENFORCED.getMode())
            .build();

        clusterService.getClusterSettings().applySettings(settings);
        assertTrue(admissionControlService.isTransportLayerAdmissionControlEnabled());
        assertTrue(admissionControlService.isTransportLayerAdmissionControlEnforced());
        assertTrue(admissionControlService.isTransportLayerAdmissionControlEnforced());
    }

    public void testWhenAdmissionControllerRegistered() {
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService.getClusterSettings(), threadPool);
        assertEquals(admissionControlService.getAdmissionControllers().size(), 1);
    }

    public void testApplyAdmissionControllerDisabled() {
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService.getClusterSettings(), threadPool);
        admissionControlService.applyTransportAdmissionControl(this.action);
        List<AdmissionController> admissionControllerList = admissionControlService.getAdmissionControllers();
        admissionControllerList.forEach(admissionController -> { assertEquals(admissionController.getRejectionCount(), 0); });
    }

    public void testApplyAdmissionControllerEnabled() {
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService.getClusterSettings(), threadPool);
        admissionControlService.applyTransportAdmissionControl(this.action);
        assertEquals(
            admissionControlService.getAdmissionController(AdmissionControlSettings.IO_BASED_ADMISSION_CONTROLLER).getRejectionCount(),
            0
        );

        Settings settings = Settings.builder()
            .put(
                IOBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.MONITOR.getMode()
            )
            .build();
        clusterService.getClusterSettings().applySettings(settings);
        this.action = "indices:data/write/bulk[s][p]";
        admissionControlService.applyTransportAdmissionControl(this.action);
        List<AdmissionController> admissionControllerList = admissionControlService.getAdmissionControllers();
        assertEquals(admissionControllerList.size(), 1);
        assertEquals(
            admissionControlService.getAdmissionController(AdmissionControlSettings.IO_BASED_ADMISSION_CONTROLLER).getRejectionCount(),
            1
        );
    }

    public void testApplyAdmissionControllerEnabledInvalidAction() {
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService.getClusterSettings(), threadPool);
        admissionControlService.applyTransportAdmissionControl(this.action);
        assertEquals(
            admissionControlService.getAdmissionController(AdmissionControlSettings.IO_BASED_ADMISSION_CONTROLLER).getRejectionCount(),
            0
        );

        Settings settings = Settings.builder()
            .put(
                IOBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.MONITOR.getMode()
            )
            .build();
        clusterService.getClusterSettings().applySettings(settings);
        this.action = "TEST";
        admissionControlService.applyTransportAdmissionControl(this.action);
        List<AdmissionController> admissionControllerList = admissionControlService.getAdmissionControllers();
        assertEquals(admissionControllerList.size(), 1);
        assertEquals(
            admissionControlService.getAdmissionController(AdmissionControlSettings.IO_BASED_ADMISSION_CONTROLLER).getRejectionCount(),
            0
        );
    }

    public void testWhenForceEnablementDisabled() {
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService.getClusterSettings(), threadPool);
        Settings settings = Settings.builder()
            .put(
                IOBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.MONITOR.getMode()
            )
            .put(AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.DISABLED.getMode())
            .build();
        clusterService.getClusterSettings().applySettings(settings);
        this.action = "indices:data/write/bulk[s][p]";
        admissionControlService.applyTransportAdmissionControl(this.action);
        assertEquals(
            admissionControlService.getAdmissionController(AdmissionControlSettings.IO_BASED_ADMISSION_CONTROLLER).getRejectionCount(),
            1
        );
    }

    public void testWhenForceEnablementEnabled() {
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService.getClusterSettings(), threadPool);
        Settings settings = Settings.builder()
            .put(
                IOBasedAdmissionControllerSettings.IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.MONITOR.getMode()
            )
            .put(AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.DISABLED.getMode())
            .build();
        clusterService.getClusterSettings().applySettings(settings);
        this.action = "indices:data/write/bulk[s][p]";
        admissionControlService.applyTransportAdmissionControl(this.action);
        assertEquals(
            admissionControlService.getAdmissionController(AdmissionControlSettings.IO_BASED_ADMISSION_CONTROLLER).getRejectionCount(),
            0
        );

        Settings newSettings = Settings.builder()
            .put(settings)
            .build();
        clusterService.getClusterSettings().applySettings(newSettings);
        admissionControlService.applyTransportAdmissionControl(this.action);
        assertEquals(
            admissionControlService.getAdmissionController(AdmissionControlSettings.IO_BASED_ADMISSION_CONTROLLER).getRejectionCount(),
            1
        );
    }
}
