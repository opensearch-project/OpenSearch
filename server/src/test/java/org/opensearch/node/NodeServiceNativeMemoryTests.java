/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.search.SearchTransportService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.discovery.Discovery;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.index.SegmentReplicationStatsTracker;
import org.opensearch.indices.IndicesService;
import org.opensearch.ingest.IngestService;
import org.opensearch.monitor.MonitorService;
import org.opensearch.monitor.memory.MemoryReportingService;
import org.opensearch.plugin.stats.AnalyticsBackendNativeMemoryStats;
import org.opensearch.plugins.PluginsService;
import org.opensearch.ratelimitting.admissioncontrol.AdmissionControlService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.backpressure.SearchBackpressureService;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.tasks.TaskCancellationMonitoringService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for NodeService native memory stats delegation logic.
 * <p>
 * Validates that NodeService correctly delegates to the native memory stats
 * supplier when nativeMemory=true and the supplier is non-null,
 * and returns null otherwise.
 */
public class NodeServiceNativeMemoryTests extends OpenSearchTestCase {

    private NodeService createNodeService(AnalyticsBackendNativeMemoryStats nativeStats) {
        TransportService transportService = mock(TransportService.class);
        DiscoveryNode localNode = new DiscoveryNode("test_node", buildNewFakeTransportAddress(), Version.CURRENT);
        when(transportService.getLocalNode()).thenReturn(localNode);

        ClusterService clusterService = mock(ClusterService.class);
        IngestService ingestService = mock(IngestService.class);
        SearchPipelineService searchPipelineService = mock(SearchPipelineService.class);

        MemoryReportingService memoryReportingService = mock(MemoryReportingService.class);
        when(memoryReportingService.nativeStats()).thenReturn(nativeStats);

        MonitorService monitorService = mock(MonitorService.class);
        when(monitorService.memoryReportingService()).thenReturn(memoryReportingService);

        return new NodeService(
            Settings.EMPTY,
            mock(ThreadPool.class),
            monitorService,
            mock(Discovery.class),
            transportService,
            mock(IndicesService.class),
            mock(PluginsService.class),
            mock(CircuitBreakerService.class),
            mock(ScriptService.class),
            null, // httpServerTransport
            ingestService,
            clusterService,
            new SettingsFilter(Collections.emptyList()),
            null, // responseCollectorService - not needed when adaptiveSelection=false
            mock(SearchTransportService.class),
            mock(IndexingPressureService.class),
            null, // aggregationUsageService
            mock(SearchBackpressureService.class),
            searchPipelineService,
            null, // fileCache
            mock(TaskCancellationMonitoringService.class),
            null, // resourceUsageCollectorService
            mock(SegmentReplicationStatsTracker.class),
            mock(RepositoriesService.class),
            mock(AdmissionControlService.class),
            null  // cacheService
        );
    }

    /**
     * Tests that stats() with nativeMemory=true and a non-null supplier
     * returns the stats from the supplier.
     */
    public void testStatsWithNativeMemoryTrueAndServicePresent() {
        AnalyticsBackendNativeMemoryStats expectedStats = new AnalyticsBackendNativeMemoryStats(1024L, 2048L);

        NodeService nodeService = createNodeService(expectedStats);

        NodeStats nodeStats = nodeService.stats(
            CommonStatsFlags.NONE,
            false, // os
            false, // process
            false, // jvm
            false, // threadPool
            false, // fs
            false, // transport
            false, // http
            false, // circuitBreaker
            false, // script
            false, // discoveryStats
            false, // ingest
            false, // adaptiveSelection
            false, // scriptCache
            false, // indexingPressure
            false, // shardIndexingPressure
            false, // searchBackpressure
            false, // clusterManagerThrottling
            false, // weightedRoutingStats
            false, // fileCacheStats
            false, // taskCancellation
            false, // searchPipelineStats
            false, // resourceUsageStats
            false, // segmentReplicationTrackerStats
            false, // repositoriesStats
            false, // admissionControl
            false, // cacheService
            false, // remoteStoreNodeStats
            true   // nativeMemory
        );

        assertNotNull(nodeStats.getAnalyticsBackendNativeMemoryStats());
        assertSame(expectedStats, nodeStats.getAnalyticsBackendNativeMemoryStats());
        assertEquals(1024L, nodeStats.getAnalyticsBackendNativeMemoryStats().getAllocatedBytes());
        assertEquals(2048L, nodeStats.getAnalyticsBackendNativeMemoryStats().getResidentBytes());
    }

    /**
     * Tests that stats() with nativeMemory=true and a null supplier
     * returns null for the nativeMemoryStats field.
     */
    public void testStatsWithNativeMemoryTrueAndNullService() {
        NodeService nodeService = createNodeService(null);

        NodeStats nodeStats = nodeService.stats(
            CommonStatsFlags.NONE,
            false, // os
            false, // process
            false, // jvm
            false, // threadPool
            false, // fs
            false, // transport
            false, // http
            false, // circuitBreaker
            false, // script
            false, // discoveryStats
            false, // ingest
            false, // adaptiveSelection
            false, // scriptCache
            false, // indexingPressure
            false, // shardIndexingPressure
            false, // searchBackpressure
            false, // clusterManagerThrottling
            false, // weightedRoutingStats
            false, // fileCacheStats
            false, // taskCancellation
            false, // searchPipelineStats
            false, // resourceUsageStats
            false, // segmentReplicationTrackerStats
            false, // repositoriesStats
            false, // admissionControl
            false, // cacheService
            false, // remoteStoreNodeStats
            true   // nativeMemory
        );

        assertNull(nodeStats.getAnalyticsBackendNativeMemoryStats());
    }

    /**
     * Tests that stats() with nativeMemory=false returns null for the
     * nativeMemoryStats field regardless of whether the supplier is present.
     */
    public void testStatsWithNativeMemoryFalse() {
        AnalyticsBackendNativeMemoryStats expectedStats = new AnalyticsBackendNativeMemoryStats(4096L, 8192L);

        NodeService nodeService = createNodeService(expectedStats);

        NodeStats nodeStats = nodeService.stats(
            CommonStatsFlags.NONE,
            false, // os
            false, // process
            false, // jvm
            false, // threadPool
            false, // fs
            false, // transport
            false, // http
            false, // circuitBreaker
            false, // script
            false, // discoveryStats
            false, // ingest
            false, // adaptiveSelection
            false, // scriptCache
            false, // indexingPressure
            false, // shardIndexingPressure
            false, // searchBackpressure
            false, // clusterManagerThrottling
            false, // weightedRoutingStats
            false, // fileCacheStats
            false, // taskCancellation
            false, // searchPipelineStats
            false, // resourceUsageStats
            false, // segmentReplicationTrackerStats
            false, // repositoriesStats
            false, // admissionControl
            false, // cacheService
            false, // remoteStoreNodeStats
            false  // nativeMemory
        );

        assertNull(nodeStats.getAnalyticsBackendNativeMemoryStats());
    }

    /**
     * Integration test: verifies that the _nodes/stats/native_memory response format
     * contains the expected "native_memory" object with "allocated_bytes" and "resident_bytes" fields.
     * This ensures the response format is unchanged after the refactor.
     */
    @SuppressWarnings("unchecked")
    public void testNativeMemoryResponseFormatUnchanged() throws Exception {
        AnalyticsBackendNativeMemoryStats expectedStats = new AnalyticsBackendNativeMemoryStats(123456789L, 987654321L);

        NodeService nodeService = createNodeService(expectedStats);

        NodeStats nodeStats = nodeService.stats(
            CommonStatsFlags.NONE,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            true   // nativeMemory
        );

        assertNotNull("nativeMemoryStats should be present", nodeStats.getAnalyticsBackendNativeMemoryStats());

        // Render the AnalyticsBackendNativeMemoryStats to JSON and verify the format
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        nodeStats.getAnalyticsBackendNativeMemoryStats().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = Strings.toString(MediaTypeRegistry.JSON, nodeStats.getAnalyticsBackendNativeMemoryStats());

        Map<String, Object> root = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        // Verify "native_memory" object is present
        assertTrue("Response should contain 'native_memory' key", root.containsKey("native_memory"));

        Map<String, Object> nativeMemory = (Map<String, Object>) root.get("native_memory");
        assertNotNull("native_memory object should not be null", nativeMemory);

        // Verify "allocated_bytes" and "resident_bytes" fields are present with correct values
        assertTrue("native_memory should contain 'allocated_bytes'", nativeMemory.containsKey("allocated_bytes"));
        assertTrue("native_memory should contain 'resident_bytes'", nativeMemory.containsKey("resident_bytes"));
        assertEquals(123456789L, ((Number) nativeMemory.get("allocated_bytes")).longValue());
        assertEquals(987654321L, ((Number) nativeMemory.get("resident_bytes")).longValue());
    }

    /**
     * Integration test: verifies that when native stats are unavailable (null supplier),
     * the response omits the native_memory object entirely.
     */
    public void testNativeMemoryOmittedWhenUnavailable() throws Exception {
        NodeService nodeService = createNodeService(null);

        NodeStats nodeStats = nodeService.stats(
            CommonStatsFlags.NONE,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            true   // nativeMemory
        );

        assertNull("nativeMemoryStats should be null when supplier is null", nodeStats.getAnalyticsBackendNativeMemoryStats());
    }
}
