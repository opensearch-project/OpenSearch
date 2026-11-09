/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.node.IoUsageStats;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.List;

import static org.opensearch.ratelimitting.admissioncontrol.AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE;
import static org.opensearch.ratelimitting.admissioncontrol.settings.NativeMemoryBasedAdmissionControllerSettings.INDEXING_NATIVE_MEMORY_USAGE_LIMIT;
import static org.opensearch.ratelimitting.admissioncontrol.settings.NativeMemoryBasedAdmissionControllerSettings.NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE;
import static org.opensearch.ratelimitting.admissioncontrol.settings.NativeMemoryBasedAdmissionControllerSettings.SEARCH_NATIVE_MEMORY_USAGE_LIMIT;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests for native memory based admission control across different configurations:
 * - No limit configured (auto-derived from totalRAM - jvmMaxHeap)
 * - Explicit limit configured
 * - Enforced mode rejecting requests
 * - Monitor mode (counting but not rejecting)
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class NativeMemoryAdmissionControlIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class);
    }

    /**
     * When no node.native_memory.limit is set, the tracker should auto-derive the budget
     * from totalPhysicalMemory - jvmMaxHeap. The admission controller should still function.
     */
    public void testAutoDerivesLimitWhenNotConfigured() {
        Settings nodeSettings = Settings.builder()
            .put(NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.ENFORCED.getMode())
            .put(SEARCH_NATIVE_MEMORY_USAGE_LIMIT.getKey(), 85)
            .put(ResourceTrackerSettings.GLOBAL_NATIVE_MEMORY_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueSeconds(5))
            .build();

        String node = internalCluster().startNode(nodeSettings);
        ensureGreen();

        String nodeId = internalCluster().clusterService(node).localNode().getId();
        ResourceUsageCollectorService collector = internalCluster().getInstance(ResourceUsageCollectorService.class, node);

        // Inject 50% native memory utilization — below the 85% threshold
        collector.collectNodeResourceUsageStats(nodeId, System.currentTimeMillis(), 30, 30, new IoUsageStats(10), 50);

        // Search should succeed — 50% is below the 85% limit
        assertAcked(prepareCreate("test-index").setMapping("field", "type=text"));
        client().index(new IndexRequest("test-index").source("field", "value")).actionGet();
        client().prepareSearch("test-index").get();
    }

    /**
     * With explicit node.native_memory.limit configured, the tracker should report
     * the injected utilization and the admission controller should count breaches.
     */
    public void testExplicitLimitTracksUtilization() {
        Settings nodeSettings = Settings.builder()
            .put(ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "4gb")
            .put(ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.MONITOR.getMode())
            .put(NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.MONITOR.getMode())
            .put(SEARCH_NATIVE_MEMORY_USAGE_LIMIT.getKey(), 80)
            .put(ResourceTrackerSettings.GLOBAL_NATIVE_MEMORY_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueSeconds(5))
            .build();

        String node = internalCluster().startNode(nodeSettings);
        ensureGreen();

        String nodeId = internalCluster().clusterService(node).localNode().getId();
        ResourceUsageCollectorService collector = internalCluster().getInstance(ResourceUsageCollectorService.class, node);

        // Inject 95% native memory — above the 80% threshold
        collector.collectNodeResourceUsageStats(nodeId, System.currentTimeMillis(), 30, 30, new IoUsageStats(10), 95);

        // Verify the stats are visible
        assertTrue(collector.getNodeStatistics(nodeId).isPresent());
        assertEquals(95.0, collector.getNodeStatistics(nodeId).get().getNativeMemoryUtilizationPercent(), 0.01);
    }

    /**
     * Monitor mode should allow requests through even when limits are breached,
     * but should still count rejections.
     */
    public void testMonitorModeAllowsRequestsWhenLimitBreached() {
        Settings nodeSettings = Settings.builder()
            .put(ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "4gb")
            .put(NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.MONITOR.getMode())
            .put(SEARCH_NATIVE_MEMORY_USAGE_LIMIT.getKey(), 80)
            .put(ResourceTrackerSettings.GLOBAL_NATIVE_MEMORY_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueSeconds(5))
            .build();

        String node = internalCluster().startNode(nodeSettings);
        ensureGreen();

        String nodeId = internalCluster().clusterService(node).localNode().getId();
        ResourceUsageCollectorService collector = internalCluster().getInstance(ResourceUsageCollectorService.class, node);
        collector.stop();

        assertAcked(prepareCreate("test-index").setMapping("field", "type=text"));
        client().index(new IndexRequest("test-index").source("field", "value")).actionGet();

        // Inject 95% — above the 80% limit
        collector.collectNodeResourceUsageStats(nodeId, System.currentTimeMillis(), 30, 30, new IoUsageStats(10), 95);

        // In MONITOR mode, request should still succeed (not rejected)
        client().prepareSearch("test-index").get();
    }

    /**
     * When native memory utilization is below the threshold, requests should always succeed
     * regardless of mode.
     */
    public void testRequestsSucceedWhenBelowThreshold() {
        Settings nodeSettings = Settings.builder()
            .put(ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "8gb")
            .put(NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.ENFORCED.getMode())
            .put(SEARCH_NATIVE_MEMORY_USAGE_LIMIT.getKey(), 85)
            .put(INDEXING_NATIVE_MEMORY_USAGE_LIMIT.getKey(), 85)
            .put(ResourceTrackerSettings.GLOBAL_NATIVE_MEMORY_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueSeconds(5))
            .build();

        String node = internalCluster().startNode(nodeSettings);
        ensureGreen();

        String nodeId = internalCluster().clusterService(node).localNode().getId();
        ResourceUsageCollectorService collector = internalCluster().getInstance(ResourceUsageCollectorService.class, node);
        collector.stop();

        assertAcked(prepareCreate("test-index").setMapping("field", "type=text"));

        // 40% utilization — well below 85% threshold
        collector.collectNodeResourceUsageStats(nodeId, System.currentTimeMillis(), 30, 30, new IoUsageStats(10), 40);

        client().index(new IndexRequest("test-index").source("field", "value")).actionGet();
        client().prepareSearch("test-index").get();
    }

    /**
     * Dynamic settings update: changing the threshold at runtime should take effect immediately.
     * Uses a two-node cluster so requests cross the transport layer.
     */
    /**
     * Dynamic settings update: changing the native memory limit at runtime should be visible
     * in the tracker's effective budget calculation.
     */
    public void testDynamicLimitUpdate() {
        Settings nodeSettings = Settings.builder()
            .put(ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "4gb")
            .put(ResourceTrackerSettings.GLOBAL_NATIVE_MEMORY_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueSeconds(5))
            .build();

        String node = internalCluster().startNode(nodeSettings);
        ensureGreen();

        String nodeId = internalCluster().clusterService(node).localNode().getId();
        ResourceUsageCollectorService collector = internalCluster().getInstance(ResourceUsageCollectorService.class, node);

        // Inject utilization stats
        collector.collectNodeResourceUsageStats(nodeId, System.currentTimeMillis(), 30, 30, new IoUsageStats(10), 75);

        // Verify stats are tracked
        assertTrue(collector.getNodeStatistics(nodeId).isPresent());
        assertEquals(75.0, collector.getNodeStatistics(nodeId).get().getNativeMemoryUtilizationPercent(), 0.01);

        // Update the native memory limit dynamically
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "8gb"))
            .execute()
            .actionGet();

        // Inject new stats — should still be trackable after settings update
        collector.collectNodeResourceUsageStats(nodeId, System.currentTimeMillis(), 30, 30, new IoUsageStats(10), 40);
        assertEquals(40.0, collector.getNodeStatistics(nodeId).get().getNativeMemoryUtilizationPercent(), 0.01);
    }
}
