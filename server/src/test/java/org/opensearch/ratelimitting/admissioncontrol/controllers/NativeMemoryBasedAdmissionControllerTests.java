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
import org.opensearch.node.NodeResourceUsageStats;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.ratelimitting.admissioncontrol.settings.NativeMemoryBasedAdmissionControllerSettings;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Optional;

import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class NativeMemoryBasedAdmissionControllerTests extends OpenSearchTestCase {
    private ClusterService clusterService;
    private ThreadPool threadPool;
    NativeMemoryBasedAdmissionController admissionController = null;
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
        admissionController = new NativeMemoryBasedAdmissionController(
            NativeMemoryBasedAdmissionController.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER,
            null,
            clusterService,
            Settings.EMPTY
        );
        assertEquals(admissionController.getName(), NativeMemoryBasedAdmissionController.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
        assertEquals(admissionController.getSettings().getTransportLayerAdmissionControllerMode(), AdmissionControlMode.DISABLED);
        assertFalse(
            admissionController.isEnabledForTransportLayer(admissionController.getSettings().getTransportLayerAdmissionControllerMode())
        );
    }

    public void testCheckDefaultLimits() {
        admissionController = new NativeMemoryBasedAdmissionController(
            NativeMemoryBasedAdmissionController.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER,
            null,
            clusterService,
            Settings.EMPTY
        );
        assertEquals(
            admissionController.getSettings().getSearchNativeMemoryUsageLimit().longValue(),
            NativeMemoryBasedAdmissionControllerSettings.Defaults.NATIVE_MEMORY_USAGE_LIMIT
        );
        assertEquals(
            admissionController.getSettings().getIndexingNativeMemoryUsageLimit().longValue(),
            NativeMemoryBasedAdmissionControllerSettings.Defaults.NATIVE_MEMORY_USAGE_LIMIT
        );
        assertEquals(
            admissionController.getSettings().getClusterAdminNativeMemoryUsageLimit().longValue(),
            NativeMemoryBasedAdmissionControllerSettings.Defaults.CLUSTER_ADMIN_NATIVE_MEMORY_USAGE_LIMIT
        );
    }

    public void testCheckUpdateSettings() {
        admissionController = new NativeMemoryBasedAdmissionController(
            NativeMemoryBasedAdmissionController.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER,
            null,
            clusterService,
            Settings.EMPTY
        );
        Settings settings = Settings.builder()
            .put(
                NativeMemoryBasedAdmissionControllerSettings.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        clusterService.getClusterSettings().applySettings(settings);
        assertEquals(admissionController.getName(), NativeMemoryBasedAdmissionController.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
        assertEquals(admissionController.getSettings().getTransportLayerAdmissionControllerMode(), AdmissionControlMode.ENFORCED);
        assertTrue(
            admissionController.isEnabledForTransportLayer(admissionController.getSettings().getTransportLayerAdmissionControllerMode())
        );
    }

    public void testCheckUpdateLimitSettings() {
        admissionController = new NativeMemoryBasedAdmissionController(
            NativeMemoryBasedAdmissionController.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER,
            null,
            clusterService,
            Settings.EMPTY
        );
        Settings settings = Settings.builder()
            .put(NativeMemoryBasedAdmissionControllerSettings.SEARCH_NATIVE_MEMORY_USAGE_LIMIT.getKey(), 80)
            .put(NativeMemoryBasedAdmissionControllerSettings.INDEXING_NATIVE_MEMORY_USAGE_LIMIT.getKey(), 70)
            .build();
        clusterService.getClusterSettings().applySettings(settings);
        assertEquals(admissionController.getSettings().getSearchNativeMemoryUsageLimit().longValue(), 80);
        assertEquals(admissionController.getSettings().getIndexingNativeMemoryUsageLimit().longValue(), 70);
    }

    public void testApplyControllerWithDefaultSettings() {
        ResourceUsageCollectorService rs = Mockito.mock(ResourceUsageCollectorService.class);
        admissionController = new NativeMemoryBasedAdmissionController(
            NativeMemoryBasedAdmissionController.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER,
            rs,
            clusterService,
            Settings.EMPTY
        );
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
        assertEquals(admissionController.getSettings().getTransportLayerAdmissionControllerMode(), AdmissionControlMode.DISABLED);
        action = "indices:data/write/bulk[s][p]";
        admissionController.apply(action, AdmissionControlActionType.INDEXING);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
    }

    public void testApplyControllerWhenSettingsEnabled() {
        Settings settings = Settings.builder()
            .put(
                NativeMemoryBasedAdmissionControllerSettings.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        ResourceUsageCollectorService rs = Mockito.mock(ResourceUsageCollectorService.class);
        admissionController = new NativeMemoryBasedAdmissionController(
            NativeMemoryBasedAdmissionController.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER,
            rs,
            clusterService,
            settings
        );
        assertTrue(
            admissionController.isEnabledForTransportLayer(admissionController.getSettings().getTransportLayerAdmissionControllerMode())
        );
        assertTrue(
            admissionController.isAdmissionControllerEnforced(admissionController.getSettings().getTransportLayerAdmissionControllerMode())
        );
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
    }

    public void testApplyControllerWhenMemoryUsageBreached() {
        Settings settings = Settings.builder()
            .put(
                NativeMemoryBasedAdmissionControllerSettings.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .put(NativeMemoryBasedAdmissionControllerSettings.SEARCH_NATIVE_MEMORY_USAGE_LIMIT.getKey(), 50)
            .build();

        ResourceUsageCollectorService rs = Mockito.mock(ResourceUsageCollectorService.class);
        admissionController = new NativeMemoryBasedAdmissionController(
            NativeMemoryBasedAdmissionController.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER,
            rs,
            clusterService,
            settings
        );

        // Mock node stats with native memory usage above the threshold
        String localNodeId = clusterService.state().nodes().getLocalNodeId();
        NodeResourceUsageStats stats = new NodeResourceUsageStats(localNodeId, System.currentTimeMillis(), 50, 50, null, 80);
        when(rs.getNodeStatistics(localNodeId)).thenReturn(Optional.of(stats));

        action = "indices:data/read/search";
        expectThrows(
            org.opensearch.core.concurrency.OpenSearchRejectedExecutionException.class,
            () -> admissionController.apply(action, AdmissionControlActionType.SEARCH)
        );
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.SEARCH.getType()), 1);
    }

    public void testApplyControllerWhenMemoryUsageNotBreached() {
        Settings settings = Settings.builder()
            .put(
                NativeMemoryBasedAdmissionControllerSettings.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .put(NativeMemoryBasedAdmissionControllerSettings.SEARCH_NATIVE_MEMORY_USAGE_LIMIT.getKey(), 50)
            .build();

        ResourceUsageCollectorService rs = Mockito.mock(ResourceUsageCollectorService.class);
        admissionController = new NativeMemoryBasedAdmissionController(
            NativeMemoryBasedAdmissionController.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER,
            rs,
            clusterService,
            settings
        );

        // Mock node stats with native memory usage below the threshold
        String localNodeId = clusterService.state().nodes().getLocalNodeId();
        NodeResourceUsageStats stats = new NodeResourceUsageStats(localNodeId, System.currentTimeMillis(), 50, 50, null, 30);
        when(rs.getNodeStatistics(localNodeId)).thenReturn(Optional.of(stats));

        action = "indices:data/read/search";
        admissionController.apply(action, AdmissionControlActionType.SEARCH);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.SEARCH.getType()), 0);
    }

    public void testApplyControllerInMonitorMode() {
        Settings settings = Settings.builder()
            .put(
                NativeMemoryBasedAdmissionControllerSettings.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.MONITOR.getMode()
            )
            .put(NativeMemoryBasedAdmissionControllerSettings.SEARCH_NATIVE_MEMORY_USAGE_LIMIT.getKey(), 50)
            .build();

        ResourceUsageCollectorService rs = Mockito.mock(ResourceUsageCollectorService.class);
        admissionController = new NativeMemoryBasedAdmissionController(
            NativeMemoryBasedAdmissionController.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER,
            rs,
            clusterService,
            settings
        );

        // Mock node stats with native memory usage above the threshold
        String localNodeId = clusterService.state().nodes().getLocalNodeId();
        NodeResourceUsageStats stats = new NodeResourceUsageStats(localNodeId, System.currentTimeMillis(), 50, 50, null, 80);
        when(rs.getNodeStatistics(localNodeId)).thenReturn(Optional.of(stats));

        // In monitor mode, should not throw but should still count rejections
        action = "indices:data/read/search";
        admissionController.apply(action, AdmissionControlActionType.SEARCH);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.SEARCH.getType()), 1);
    }

    public void testRejectionCount() {
        Settings settings = Settings.builder()
            .put(
                NativeMemoryBasedAdmissionControllerSettings.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        ResourceUsageCollectorService rs = Mockito.mock(ResourceUsageCollectorService.class);
        admissionController = new NativeMemoryBasedAdmissionController(
            NativeMemoryBasedAdmissionController.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER,
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
}
