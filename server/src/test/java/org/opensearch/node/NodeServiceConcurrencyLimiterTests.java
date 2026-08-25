/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.Version;
import org.opensearch.action.ActionConcurrencyLimiterStats;
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
 * Unit tests for NodeService concurrency limiter stats delegation logic.
 */
public class NodeServiceConcurrencyLimiterTests extends OpenSearchTestCase {

    private NodeService createNodeService(Supplier<ActionConcurrencyLimiterStats> concurrencyLimiterStatsSupplier) {
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
            null,
            mock(IngestService.class),
            mock(ClusterService.class),
            new SettingsFilter(Collections.emptyList()),
            null,
            mock(SearchTransportService.class),
            mock(IndexingPressureService.class),
            null,
            mock(SearchBackpressureService.class),
            mock(SearchPipelineService.class),
            null,
            mock(TaskCancellationMonitoringService.class),
            null,
            mock(SegmentReplicationStatsTracker.class),
            mock(RepositoriesService.class),
            mock(AdmissionControlService.class),
            null,
            null,
            concurrencyLimiterStatsSupplier
        );
    }

    private NodeStats callStats(NodeService nodeService, boolean concurrencyLimiter) {
        return nodeService.stats(
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
            concurrencyLimiter
        );
    }

    public void testStatsWithConcurrencyLimiterTrueAndSupplierPresent() {
        ActionConcurrencyLimiterStats expected = new ActionConcurrencyLimiterStats(
            List.of(
                new ActionConcurrencyLimiterStats.ActionLimiterSnapshot(
                    "search",
                    "indices:data/read/search",
                    "enforced",
                    "vegas",
                    20,
                    5,
                    0L,
                    10L,
                    8L
                )
            )
        );
        NodeService nodeService = createNodeService(() -> expected);
        NodeStats nodeStats = callStats(nodeService, true);

        assertNotNull(nodeStats.getConcurrencyLimiterStats());
        assertSame(expected, nodeStats.getConcurrencyLimiterStats());
    }

    public void testStatsWithConcurrencyLimiterTrueAndNoSupplier() {
        NodeService nodeService = createNodeService(null);
        NodeStats nodeStats = callStats(nodeService, true);

        assertNull(nodeStats.getConcurrencyLimiterStats());
    }

    public void testStatsWithConcurrencyLimiterFalse() {
        ActionConcurrencyLimiterStats stats = new ActionConcurrencyLimiterStats(
            List.of(
                new ActionConcurrencyLimiterStats.ActionLimiterSnapshot(
                    "search",
                    "indices:data/read/search",
                    "enforced",
                    "vegas",
                    20,
                    5,
                    0L,
                    -1L,
                    -1L
                )
            )
        );
        NodeService nodeService = createNodeService(() -> stats);
        NodeStats nodeStats = callStats(nodeService, false);

        assertNull("concurrencyLimiterStats should be null when flag=false", nodeStats.getConcurrencyLimiterStats());
    }
}
