/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.controllers;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.node.NodeResourceUsageStats;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Optional;

import org.mockito.Mockito;

public class CpuBasedAdmissionControllerTests extends OpenSearchTestCase {
    private ClusterService clusterService;
    private ThreadPool threadPool;
    CpuBasedAdmissionController admissionController = null;
    String action = "TEST_ACTION";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("admission_controller_settings_test");
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        threadPool.shutdownNow();
    }

    public void testCheckDefaultParameters() {
        admissionController = new CpuBasedAdmissionController(
            CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
            null,
            clusterService,
            Settings.EMPTY
        );
        assertEquals(admissionController.getName(), CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
        assertEquals(admissionController.settings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.DISABLED);
        assertFalse(
            admissionController.isEnabledForTransportLayer(admissionController.settings.getTransportLayerAdmissionControllerMode())
        );
    }

    public void testCheckUpdateSettings() {
        admissionController = new CpuBasedAdmissionController(
            CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
            null,
            clusterService,
            Settings.EMPTY
        );
        Settings settings = Settings.builder()
            .put(
                CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        clusterService.getClusterSettings().applySettings(settings);

        assertEquals(admissionController.getName(), CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
        assertEquals(admissionController.settings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.ENFORCED);
        assertTrue(admissionController.isEnabledForTransportLayer(admissionController.settings.getTransportLayerAdmissionControllerMode()));
    }

    public void testApplyControllerWithDefaultSettings() {
        ResourceUsageCollectorService rs = Mockito.mock(ResourceUsageCollectorService.class);
        admissionController = new CpuBasedAdmissionController(
            CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
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
                CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        ResourceUsageCollectorService rs = Mockito.mock(ResourceUsageCollectorService.class);
        admissionController = new CpuBasedAdmissionController(
            CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
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

    public void testRejectionCount() {
        Settings settings = Settings.builder()
            .put(
                CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        ResourceUsageCollectorService rs = Mockito.mock(ResourceUsageCollectorService.class);
        admissionController = new CpuBasedAdmissionController(
            CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
            rs,
            clusterService,
            settings
        );
        admissionController.addRejectionCount(AdmissionControlActionType.SEARCH.getType(), 1);
        admissionController.addRejectionCount(AdmissionControlActionType.INDEXING.getType(), 3);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.SEARCH.getType()), 1);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 3);
        admissionController.addRejectionCount(AdmissionControlActionType.SEARCH.getType(), 1);
        admissionController.addRejectionCount(AdmissionControlActionType.INDEXING.getType(), 2);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.SEARCH.getType()), 2);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 5);
    }

    public void testApplyForTransportLayerEnforcedRejectsRegardlessOfMode() {
        // The mode-override is left at its default (DISABLED); the enforced-apply used by the CPU-only flow
        // must still reject when the CPU usage limit is breached, i.e. it does not consult the configured mode.
        Settings settings = Settings.builder().put(CpuBasedAdmissionControllerSettings.INDEXING_CPU_USAGE_LIMIT.getKey(), 0).build();
        ResourceUsageCollectorService rs = Mockito.mock(ResourceUsageCollectorService.class);
        NodeResourceUsageStats stats = Mockito.mock(NodeResourceUsageStats.class);
        Mockito.when(stats.getCpuUtilizationPercent()).thenReturn(50.0);
        Mockito.when(rs.getNodeStatistics(Mockito.anyString())).thenReturn(Optional.of(stats));
        admissionController = new CpuBasedAdmissionController(
            CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
            rs,
            clusterService,
            settings
        );
        assertEquals(AdmissionControlMode.DISABLED, admissionController.settings.getTransportLayerAdmissionControllerMode());
        expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> admissionController.applyForTransportLayerEnforced(action, AdmissionControlActionType.INDEXING)
        );
        assertEquals(1, admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()));
    }

    public void testApplyForTransportLayerEnforcedNoRejectionBelowLimit() {
        // Default CPU limit is 95; a usage of 10 is below it, so no rejection should occur.
        ResourceUsageCollectorService rs = Mockito.mock(ResourceUsageCollectorService.class);
        NodeResourceUsageStats stats = Mockito.mock(NodeResourceUsageStats.class);
        Mockito.when(stats.getCpuUtilizationPercent()).thenReturn(10.0);
        Mockito.when(rs.getNodeStatistics(Mockito.anyString())).thenReturn(Optional.of(stats));
        admissionController = new CpuBasedAdmissionController(
            CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
            rs,
            clusterService,
            Settings.EMPTY
        );
        admissionController.applyForTransportLayerEnforced(action, AdmissionControlActionType.INDEXING);
        assertEquals(0, admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()));
    }
}
