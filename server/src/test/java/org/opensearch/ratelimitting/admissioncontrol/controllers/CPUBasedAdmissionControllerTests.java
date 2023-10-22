/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.controllers;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.ratelimitting.admissioncontrol.settings.CPUBasedAdmissionControllerSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import org.mockito.Mockito;

public class CPUBasedAdmissionControllerTests extends OpenSearchTestCase {
    private ClusterService clusterService;
    private ThreadPool threadPool;
    CPUBasedAdmissionController admissionController = null;

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
        admissionController = new CPUBasedAdmissionController(
            CPUBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
            null,
            clusterService,
            Settings.EMPTY
        );
        assertEquals(admissionController.getName(), CPUBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
        assertEquals(admissionController.settings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.DISABLED);
        assertFalse(
            admissionController.isEnabledForTransportLayer(admissionController.settings.getTransportLayerAdmissionControllerMode())
        );
    }

    public void testCheckUpdateSettings() {
        admissionController = new CPUBasedAdmissionController(
            CPUBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
            null,
            clusterService,
            Settings.EMPTY
        );
        Settings settings = Settings.builder()
            .put(
                CPUBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        clusterService.getClusterSettings().applySettings(settings);

        assertEquals(admissionController.getName(), CPUBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
        assertEquals(admissionController.settings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.ENFORCED);
        assertTrue(admissionController.isEnabledForTransportLayer(admissionController.settings.getTransportLayerAdmissionControllerMode()));
    }

    public void testApplyControllerWithDefaultSettings() {
        ResourceUsageCollectorService rs = Mockito.mock(ResourceUsageCollectorService.class);
        admissionController = new CPUBasedAdmissionController(
            CPUBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
            rs,
            clusterService,
            Settings.EMPTY
        );
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
        assertEquals(admissionController.settings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.DISABLED);
        action = "indices:data/write/bulk[s][p]";
        admissionController.apply(action, AdmissionControlActionType.INDEXING);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
    }

    public void testApplyControllerWhenSettingsEnabled() throws Exception {
        Settings settings = Settings.builder()
            .put(
                CPUBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        ResourceUsageCollectorService rs = Mockito.mock(ResourceUsageCollectorService.class);
        admissionController = new CPUBasedAdmissionController(
            CPUBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
            rs,
            clusterService,
            settings
        );
        assertTrue(admissionController.isEnabledForTransportLayer(admissionController.settings.getTransportLayerAdmissionControllerMode()));
        assertTrue(
            admissionController.isAdmissionControllerEnforced(admissionController.settings.getTransportLayerAdmissionControllerMode())
        );
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
        // we can assert admission control and rejections as part of ITs
    }
}
