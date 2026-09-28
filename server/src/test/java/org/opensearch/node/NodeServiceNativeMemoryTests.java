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
import org.opensearch.plugin.stats.NativeAllocatorPoolStats;
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
import java.util.List;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for NodeService native memory stats delegation logic.
 * <p>
 * Validates that NodeService correctly delegates to the native allocator stats
 * supplier when nativeMemory=true and the supplier is non-null,
 * and returns null otherwise.
 */
public class NodeServiceNativeMemoryTests extends OpenSearchTestCase {

    private NodeService createNodeService(Supplier<NativeAllocatorPoolStats> nativeAllocatorStatsSupplier) {
        TransportService transportService = mock(TransportService.class);
        DiscoveryNode localNode = new DiscoveryNode("test_node", buildNewFakeTransportAddress(), Version.CURRENT);
        when(transportService.getLocalNode()).thenReturn(localNode);

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
            mock(IngestService.class),
            mock(ClusterService.class),
            new SettingsFilter(Collections.emptyList()),
            null, // responseCollectorService - not needed when adaptiveSelection=false
            mock(SearchTransportService.class),
            mock(IndexingPressureService.class),
            null, // aggregationUsageService
            mock(SearchBackpressureService.class),
            mock(SearchPipelineService.class),
            null, // nodeCacheService
            mock(TaskCancellationMonitoringService.class),
            null, // resourceUsageCollectorService
            mock(SegmentReplicationStatsTracker.class),
            mock(RepositoriesService.class),
            mock(AdmissionControlService.class),
            null, // cacheService
            nativeAllocatorStatsSupplier,
            null // concurrencyLimiterStatsSupplier
        );
    }

    /**
     * Tests that stats() with nativeMemory=true and a non-null supplier
     * returns the stats from the supplier.
     */
    public void testStatsWithNativeMemoryTrueAndSupplierPresent() {
        NativeAllocatorPoolStats expected = new NativeAllocatorPoolStats(
            1024L,
            2048L,
            List.of(new NativeAllocatorPoolStats.PoolStats("flight", 100L, 200L, 2048L))
        );
        NodeService nodeService = createNodeService(() -> expected);

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
            false, // fileCacheDetailed
            false, // taskCancellation
            false, // searchPipelineStats
            false, // resourceUsageStats
            false, // segmentReplicationTrackerStats
            false, // repositoriesStats
            false, // admissionControl
            false, // cacheService
            false, // remoteStoreNodeStats
            true,  // nativeMemory
            false  // concurrencyLimiter
        );

        assertNotNull("nativeAllocatorStats should be present when supplier returns non-null", nodeStats.getNativeAllocatorStats());
        assertSame(expected, nodeStats.getNativeAllocatorStats());
    }

    /**
     * Tests that stats() with nativeMemory=true and no supplier
     * returns null for the nativeAllocatorStats field.
     */
    public void testStatsWithNativeMemoryTrueAndNoSupplier() {
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
            false, // fileCacheDetailed
            false, // taskCancellation
            false, // searchPipelineStats
            false, // resourceUsageStats
            false, // segmentReplicationTrackerStats
            false, // repositoriesStats
            false, // admissionControl
            false, // cacheService
            false, // remoteStoreNodeStats
            true,  // nativeMemory
            false  // concurrencyLimiter
        );

        assertNull("nativeAllocatorStats should be null when no supplier registered", nodeStats.getNativeAllocatorStats());
    }

    /**
     * Tests that stats() with nativeMemory=false returns null for the
     * nativeAllocatorStats field regardless of whether the supplier is present.
     */
    public void testStatsWithNativeMemoryFalse() {
        NativeAllocatorPoolStats expected = new NativeAllocatorPoolStats(
            4096L,
            8192L,
            List.of(new NativeAllocatorPoolStats.PoolStats("flight", 100L, 200L, 2048L))
        );
        NodeService nodeService = createNodeService(() -> expected);

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
            false, // fileCacheDetailed
            false, // taskCancellation
            false, // searchPipelineStats
            false, // resourceUsageStats
            false, // segmentReplicationTrackerStats
            false, // repositoriesStats
            false, // admissionControl
            false, // cacheService
            false, // remoteStoreNodeStats
            false, // nativeMemory
            false  // concurrencyLimiter
        );

        assertNull("nativeAllocatorStats should be null when nativeMemory=false", nodeStats.getNativeAllocatorStats());
    }
}
