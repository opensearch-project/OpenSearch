/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.ratelimitting.admissioncontrol.controllers.AdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.controllers.CpuBasedAdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

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

    public void testWhenAdmissionControllerRegistered() {
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService, threadPool, null);
        assertEquals(admissionControlService.getAdmissionControllers().size(), 1);
    }

    public void testRegisterInvalidAdmissionController() {
        String test = "TEST";
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService, threadPool, null);
        assertEquals(admissionControlService.getAdmissionControllers().size(), 1);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> admissionControlService.registerAdmissionController(test)
        );
        assertEquals(ex.getMessage(), "Not Supported AdmissionController : " + test);
    }

    public void testAdmissionControllerSettings() {
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService, threadPool, null);
        AdmissionControlSettings admissionControlSettings = admissionControlService.admissionControlSettings;
        List<AdmissionController> admissionControllerList = admissionControlService.getAdmissionControllers();
        assertEquals(admissionControllerList.size(), 1);
        CpuBasedAdmissionController cpuBasedAdmissionController = (CpuBasedAdmissionController) admissionControlService
            .getAdmissionController(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER);
        assertEquals(
            admissionControlSettings.isTransportLayerAdmissionControlEnabled(),
            cpuBasedAdmissionController.isEnabledForTransportLayer(
                cpuBasedAdmissionController.settings.getTransportLayerAdmissionControllerMode()
            )
        );

        Settings settings = Settings.builder()
            .put(AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.DISABLED.getMode())
            .build();
        clusterService.getClusterSettings().applySettings(settings);
        assertEquals(
            admissionControlSettings.isTransportLayerAdmissionControlEnabled(),
            cpuBasedAdmissionController.isEnabledForTransportLayer(
                cpuBasedAdmissionController.settings.getTransportLayerAdmissionControllerMode()
            )
        );
        assertFalse(admissionControlSettings.isTransportLayerAdmissionControlEnabled());

        Settings newSettings = Settings.builder()
            .put(settings)
            .put(
                CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        clusterService.getClusterSettings().applySettings(newSettings);
        assertFalse(admissionControlSettings.isTransportLayerAdmissionControlEnabled());
        assertTrue(
            cpuBasedAdmissionController.isEnabledForTransportLayer(
                cpuBasedAdmissionController.settings.getTransportLayerAdmissionControllerMode()
            )
        );
    }

    public void testApplyAdmissionControllerDisabled() {
        this.action = "indices:data/write/bulk[s][p]";
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService, threadPool, null);
        admissionControlService.applyTransportAdmissionControl(this.action, null);
        List<AdmissionController> admissionControllerList = admissionControlService.getAdmissionControllers();
        admissionControllerList.forEach(admissionController -> {
            assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
        });
    }

    public void testApplyAdmissionControllerEnabled() {
        this.action = "indices:data/write/bulk[s][p]";
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService, threadPool, null);
        admissionControlService.applyTransportAdmissionControl(this.action, null);
        assertEquals(
            admissionControlService.getAdmissionController(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER)
                .getRejectionCount(AdmissionControlActionType.INDEXING.getType()),
            0
        );

        Settings settings = Settings.builder()
            .put(
                CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.MONITOR.getMode()
            )
            .build();
        clusterService.getClusterSettings().applySettings(settings);
        List<AdmissionController> admissionControllerList = admissionControlService.getAdmissionControllers();
        assertEquals(admissionControllerList.size(), 1);
    }

    public void testApplyAdmissionControllerEnforced() {
        this.action = "indices:data/write/bulk[s][p]";
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService, threadPool, null);
        admissionControlService.applyTransportAdmissionControl(this.action, null);
        assertEquals(
            admissionControlService.getAdmissionController(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER)
                .getRejectionCount(AdmissionControlActionType.INDEXING.getType()),
            0
        );

        Settings settings = Settings.builder()
            .put(
                CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.MONITOR.getMode()
            )
            .build();
        clusterService.getClusterSettings().applySettings(settings);
        List<AdmissionController> admissionControllerList = admissionControlService.getAdmissionControllers();
        assertEquals(admissionControllerList.size(), 1);
    }
}
