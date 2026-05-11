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
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.discovery.Discovery;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.index.SegmentReplicationStatsTracker;
import org.opensearch.indices.IndicesService;
import org.opensearch.ingest.IngestService;
import org.opensearch.monitor.MonitorService;
import org.opensearch.nativebridge.spi.NativeMemoryService;
import org.opensearch.nativebridge.spi.NativeMemoryStats;
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for NodeService native memory stats delegation logic.
 * <p>
 * Validates Requirements 5.3 and 5.4: NodeService correctly delegates to
 * NativeMemoryService when nativeMemory=true and the service is non-null,
 * and returns null otherwise.
 */
public class NodeServiceNativeMemoryTests extends OpenSearchTestCase {

    private NodeService createNodeService(NativeMemoryService nativeMemoryService) {
        TransportService transportService = mock(TransportService.class);
        DiscoveryNode localNode = new DiscoveryNode("test_node", buildNewFakeTransportAddress(), Version.CURRENT);
        when(transportService.getLocalNode()).thenReturn(localNode);

        ClusterService clusterService = mock(ClusterService.class);
        IngestService ingestService = mock(IngestService.class);
        SearchPipelineService searchPipelineService = mock(SearchPipelineService.class);

        return new NodeService(
            Settings.EMPTY,
            mock(ThreadPool.class),
            mock(MonitorService.class),
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
            null, // cacheService
            nativeMemoryService
        );
    }

    /**
     * Tests that stats() with nativeMemory=true and a non-null NativeMemoryService
     * returns the stats from the service.
     */
    public void testStatsWithNativeMemoryTrueAndServicePresent() {
        NativeMemoryStats expectedStats = new NativeMemoryStats(1024L, 2048L);
        NativeMemoryService nativeMemoryService = mock(NativeMemoryService.class);
        when(nativeMemoryService.stats()).thenReturn(expectedStats);

        NodeService nodeService = createNodeService(nativeMemoryService);

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

        assertNotNull(nodeStats.getNativeMemoryStats());
        assertSame(expectedStats, nodeStats.getNativeMemoryStats());
        assertEquals(1024L, nodeStats.getNativeMemoryStats().getAllocatedBytes());
        assertEquals(2048L, nodeStats.getNativeMemoryStats().getResidentBytes());
    }

    /**
     * Tests that stats() with nativeMemory=true and a null NativeMemoryService
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

        assertNull(nodeStats.getNativeMemoryStats());
    }

    /**
     * Tests that stats() with nativeMemory=false returns null for the
     * nativeMemoryStats field regardless of whether the service is present.
     */
    public void testStatsWithNativeMemoryFalse() {
        NativeMemoryStats expectedStats = new NativeMemoryStats(4096L, 8192L);
        NativeMemoryService nativeMemoryService = mock(NativeMemoryService.class);
        when(nativeMemoryService.stats()).thenReturn(expectedStats);

        NodeService nodeService = createNodeService(nativeMemoryService);

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

        assertNull(nodeStats.getNativeMemoryStats());
    }
}
