/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol;

import org.apache.lucene.util.Constants;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.node.IoUsageStats;
import org.opensearch.node.NodeResourceUsageStats;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.ratelimitting.admissioncontrol.controllers.AdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.controllers.CpuBasedAdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.controllers.IoBasedAdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.controllers.NativeMemoryBasedAdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings;
import org.opensearch.ratelimitting.admissioncontrol.settings.IoBasedAdmissionControllerSettings;
import org.opensearch.ratelimitting.admissioncontrol.settings.NativeMemoryBasedAdmissionControllerSettings;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Optional;

import org.mockito.Mockito;

public class AdmissionControlServiceTests extends OpenSearchTestCase {
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private AdmissionControlService admissionControlService;
    private String action = "";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("admission_controller_settings_test");
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        action = "indexing";
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        threadPool.shutdownNow();
    }

    public void testWhenAdmissionControllerRegistered() {
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService, threadPool, null, null);
        if (Constants.LINUX) {
            assertEquals(admissionControlService.getAdmissionControllers().size(), 3);
        } else {
            assertEquals(admissionControlService.getAdmissionControllers().size(), 1);
        }
    }

    public void testRegisterInvalidAdmissionController() {
        String test = "TEST";
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService, threadPool, null, null);
        if (Constants.LINUX) {
            assertEquals(admissionControlService.getAdmissionControllers().size(), 3);
        } else {
            assertEquals(admissionControlService.getAdmissionControllers().size(), 1);
        }
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> admissionControlService.registerAdmissionController(test)
        );
        assertEquals(ex.getMessage(), "Not Supported AdmissionController : " + test);
    }

    public void testAdmissionControllerSettings() {
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService, threadPool, null, null);
        AdmissionControlSettings admissionControlSettings = admissionControlService.admissionControlSettings;
        List<AdmissionController> admissionControllerList = admissionControlService.getAdmissionControllers();
        if (Constants.LINUX) {
            assertEquals(admissionControllerList.size(), 3);
        } else {
            assertEquals(admissionControllerList.size(), 1);
        }
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
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService, threadPool, null, null);
        admissionControlService.applyTransportAdmissionControl(this.action, null);
        List<AdmissionController> admissionControllerList = admissionControlService.getAdmissionControllers();
        admissionControllerList.forEach(admissionController -> {
            assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
        });
    }

    public void testApplyAdmissionControllerEnabled() {
        this.action = "indices:data/write/bulk[s][p]";
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService, threadPool, null, null);
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
        if (Constants.LINUX) {
            assertEquals(admissionControllerList.size(), 3);
        } else {
            assertEquals(admissionControllerList.size(), 1);
        }
    }

    public void testApplyAdmissionControllerEnforced() {
        this.action = "indices:data/write/bulk[s][p]";
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService, threadPool, null, null);
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
        List<AdmissionController> admissionControllerList2 = admissionControlService.getAdmissionControllers();
        if (Constants.LINUX) {
            assertEquals(admissionControllerList2.size(), 3);
        } else {
            assertEquals(admissionControllerList2.size(), 1);
        }
    }

    public void testNativeMemoryBasedAdmissionControllerRegistered() {
        assumeTrue("native memory controller is Linux-only", Constants.LINUX);
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService, threadPool, null, null);
        AdmissionController nativeMemoryController = admissionControlService.getAdmissionController(
            NativeMemoryBasedAdmissionController.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER
        );
        assertNotNull(nativeMemoryController);
        assertEquals(nativeMemoryController.getName(), NativeMemoryBasedAdmissionController.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER);
    }

    public void testNativeMemoryAdmissionControllerSettings() {
        assumeTrue("native memory controller is Linux-only", Constants.LINUX);
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService, threadPool, null, null);
        NativeMemoryBasedAdmissionController nativeMemoryController = (NativeMemoryBasedAdmissionController) admissionControlService
            .getAdmissionController(NativeMemoryBasedAdmissionController.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER);
        assertNotNull(nativeMemoryController);
        assertEquals(nativeMemoryController.getSettings().getTransportLayerAdmissionControllerMode(), AdmissionControlMode.DISABLED);

        Settings settings = Settings.builder()
            .put(
                NativeMemoryBasedAdmissionControllerSettings.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        clusterService.getClusterSettings().applySettings(settings);
        assertEquals(nativeMemoryController.getSettings().getTransportLayerAdmissionControllerMode(), AdmissionControlMode.ENFORCED);
        assertTrue(
            nativeMemoryController.isEnabledForTransportLayer(
                nativeMemoryController.getSettings().getTransportLayerAdmissionControllerMode()
            )
        );
    }

    public void testApplyNativeMemoryAdmissionControllerDisabled() {
        assumeTrue("native memory controller is Linux-only", Constants.LINUX);
        this.action = "indices:data/write/bulk[s][p]";
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService, threadPool, null, null);
        admissionControlService.applyTransportAdmissionControl(this.action, null);
        assertEquals(
            admissionControlService.getAdmissionController(NativeMemoryBasedAdmissionController.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER)
                .getRejectionCount(AdmissionControlActionType.INDEXING.getType()),
            0
        );
    }

    /**
     * New CPU-only flow enabled with the legacy transport mode disabled: only the CPU controller enforces and
     * rejects; the IO controller must not participate.
     */
    public void testNewCpuOnlyFlowEnforcesCpuAndExcludesIo() {
        this.action = "indices:data/write/bulk[s][p]";
        Settings settings = Settings.builder()
            .put(AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_CPU_ENABLED.getKey(), true)
            .put(CpuBasedAdmissionControllerSettings.INDEXING_CPU_USAGE_LIMIT.getKey(), 0)
            .put(IoBasedAdmissionControllerSettings.INDEXING_IO_USAGE_LIMIT.getKey(), 0)
            .build();
        ResourceUsageCollectorService rs = mockResourceCollector(50.0, 50.0);
        admissionControlService = new AdmissionControlService(settings, clusterService, threadPool, rs, null);
        expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> admissionControlService.applyTransportAdmissionControl(this.action, AdmissionControlActionType.INDEXING)
        );
        assertEquals(
            1,
            admissionControlService.getAdmissionController(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER)
                .getRejectionCount(AdmissionControlActionType.INDEXING.getType())
        );
        if (Constants.LINUX) {
            // IO controller is registered but must NOT participate in the CPU-only flow.
            assertEquals(
                0,
                admissionControlService.getAdmissionController(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER)
                    .getRejectionCount(AdmissionControlActionType.INDEXING.getType())
            );
        }
    }

    /**
     * New CPU-only flow disabled (the default): the legacy multi-controller flow runs. With all legacy modes
     * disabled, no admission control is applied even at high CPU usage - i.e. existing default behavior is preserved.
     */
    public void testNewCpuOnlyFlowDisabledUsesLegacyPath() {
        this.action = "indices:data/write/bulk[s][p]";
        ResourceUsageCollectorService rs = mockResourceCollector(100.0, 100.0);
        admissionControlService = new AdmissionControlService(Settings.EMPTY, clusterService, threadPool, rs, null);
        admissionControlService.applyTransportAdmissionControl(this.action, AdmissionControlActionType.INDEXING);
        assertEquals(
            0,
            admissionControlService.getAdmissionController(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER)
                .getRejectionCount(AdmissionControlActionType.INDEXING.getType())
        );
    }

    /**
     * When both the legacy transport mode and the new CPU-only flow are enabled, the legacy flow takes
     * precedence. Proven by having only the IO controller breach: a rejection from IO shows the legacy
     * multi-controller path ran (the CPU-only flow would have skipped IO entirely).
     */
    public void testLegacyTakesPrecedenceWhenBothEnabled() {
        assumeTrue("IO/native controllers are Linux-only", Constants.LINUX);
        this.action = "indices:data/write/bulk[s][p]";
        Settings settings = Settings.builder()
            .put(AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_CPU_ENABLED.getKey(), true)
            .put(AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.ENFORCED.getMode())
            .put(
                CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.DISABLED.getMode()
            )
            .put(
                NativeMemoryBasedAdmissionControllerSettings.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.DISABLED.getMode()
            )
            .put(IoBasedAdmissionControllerSettings.INDEXING_IO_USAGE_LIMIT.getKey(), 0)
            .build();
        ResourceUsageCollectorService rs = mockResourceCollector(50.0, 50.0);
        admissionControlService = new AdmissionControlService(settings, clusterService, threadPool, rs, null);
        expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> admissionControlService.applyTransportAdmissionControl(this.action, AdmissionControlActionType.INDEXING)
        );
        // Rejection came from the IO controller (legacy path), not CPU.
        assertEquals(
            1,
            admissionControlService.getAdmissionController(IoBasedAdmissionController.IO_BASED_ADMISSION_CONTROLLER)
                .getRejectionCount(AdmissionControlActionType.INDEXING.getType())
        );
        assertEquals(
            0,
            admissionControlService.getAdmissionController(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER)
                .getRejectionCount(AdmissionControlActionType.INDEXING.getType())
        );
    }

    private ResourceUsageCollectorService mockResourceCollector(double cpuPercent, double ioPercent) {
        ResourceUsageCollectorService rs = Mockito.mock(ResourceUsageCollectorService.class);
        NodeResourceUsageStats stats = Mockito.mock(NodeResourceUsageStats.class);
        Mockito.when(stats.getCpuUtilizationPercent()).thenReturn(cpuPercent);
        IoUsageStats ioStats = Mockito.mock(IoUsageStats.class);
        Mockito.when(ioStats.getIoUtilisationPercent()).thenReturn(ioPercent);
        Mockito.when(stats.getIoUsageStats()).thenReturn(ioStats);
        Mockito.when(rs.getNodeStatistics(Mockito.anyString())).thenReturn(Optional.of(stats));
        return rs;
    }
}
