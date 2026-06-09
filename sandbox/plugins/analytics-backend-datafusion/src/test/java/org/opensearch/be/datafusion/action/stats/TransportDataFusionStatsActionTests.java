/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.be.datafusion.DataFusionService;
import org.opensearch.be.datafusion.stats.DataFusionStats;
import org.opensearch.be.datafusion.stats.NativeExecutorsStats;
import org.opensearch.be.datafusion.stats.PartitionGateStats;
import org.opensearch.be.datafusion.stats.RuntimeMetrics;
import org.opensearch.be.datafusion.stats.TaskMonitorStats;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TransportDataFusionStatsAction}.
 *
 * <p>Tests the {@code nodeOperation()} method and the package-private
 * {@code filteredStats()} helper. Uses Mockito to mock {@link DataFusionService}
 * and the transport infrastructure.
 *
 * Validates: Requirements 5.1, 5.2, 5.3
 */
public class TransportDataFusionStatsActionTests extends OpenSearchTestCase {

    private DataFusionService mockDataFusionService;
    private ClusterService mockClusterService;
    private TransportDataFusionStatsAction action;
    private DiscoveryNode localNode;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockDataFusionService = mock(DataFusionService.class);
        mockClusterService = mock(ClusterService.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        TransportService mockTransportService = mock(TransportService.class);
        ActionFilters mockActionFilters = mock(ActionFilters.class);

        localNode = new DiscoveryNode("test-node", buildNewFakeTransportAddress(), Version.CURRENT);
        when(mockClusterService.localNode()).thenReturn(localNode);
        when(mockClusterService.getClusterName()).thenReturn(new ClusterName("test-cluster"));

        action = new TransportDataFusionStatsAction(
            mockThreadPool,
            mockClusterService,
            mockTransportService,
            mockActionFilters,
            mockDataFusionService
        );
    }

    // ---- Helper: build a full DataFusionStats with all sections populated ----

    private static DataFusionStats buildFullStats() {
        RuntimeMetrics io = new RuntimeMetrics(4, 1000, 500, 10, 5, 2, 20, 100, 8);
        RuntimeMetrics cpu = new RuntimeMetrics(8, 2000, 1000, 20, 10, 4, 40, 200, 16);
        Map<String, TaskMonitorStats> taskMonitors = new LinkedHashMap<>();
        taskMonitors.put("coordinator_reduce", new TaskMonitorStats(100, 200, 300));
        taskMonitors.put("query_execution", new TaskMonitorStats(400, 500, 600));
        taskMonitors.put("stream_next", new TaskMonitorStats(700, 800, 900));
        taskMonitors.put("plan_setup", new TaskMonitorStats(1000, 1100, 1200));
        NativeExecutorsStats nativeStats = new NativeExecutorsStats(io, cpu, taskMonitors);
        PartitionGateStats datanodeGate = new PartitionGateStats("datanode_gate", 64, 3, 150, 500, 0, 64);
        PartitionGateStats coordinatorGate = new PartitionGateStats("coordinator_gate", 32, 1, 75, 250, 0, 32);
        return new DataFusionStats(nativeStats, datanodeGate, coordinatorGate);
    }

    // ---- Test 1: nodeOperation calls dataFusionService.getStats() ----

    public void testNodeOperationCallsGetStats() {
        DataFusionStats stats = buildFullStats();
        when(mockDataFusionService.getStats()).thenReturn(stats);

        DataFusionStatsNodesRequest nodesRequest = new DataFusionStatsNodesRequest(new String[0], Collections.emptySet());
        DataFusionStatsNodeRequest nodeRequest = new DataFusionStatsNodeRequest(nodesRequest);

        action.nodeOperation(nodeRequest);

        verify(mockDataFusionService).getStats();
    }

    // ---- Test 2: nodeOperation returns correct response ----

    public void testNodeOperationReturnsCorrectResponse() {
        DataFusionStats stats = buildFullStats();
        when(mockDataFusionService.getStats()).thenReturn(stats);

        DataFusionStatsNodesRequest nodesRequest = new DataFusionStatsNodesRequest(new String[0], Collections.emptySet());
        DataFusionStatsNodeRequest nodeRequest = new DataFusionStatsNodeRequest(nodesRequest);

        DataFusionStatsNodeResponse response = action.nodeOperation(nodeRequest);

        assertNotNull(response);
        assertEquals(localNode, response.getNode());
        assertEquals(stats, response.getStats());
    }

    // ---- Test 3: filteredStats with null filter returns original stats ----

    public void testFilteredStatsWithNullFilter() {
        DataFusionStats stats = buildFullStats();

        DataFusionStats result = TransportDataFusionStatsAction.filteredStats(stats, null);

        assertSame(stats, result);
    }

    // ---- Test 4: filteredStats with empty filter returns original stats ----

    public void testFilteredStatsWithEmptyFilter() {
        DataFusionStats stats = buildFullStats();

        DataFusionStats result = TransportDataFusionStatsAction.filteredStats(stats, Collections.emptySet());

        assertSame(stats, result);
    }

    // ---- Test 5: filteredStats with single section (io_runtime) ----

    public void testFilteredStatsWithSingleSection() {
        DataFusionStats stats = buildFullStats();
        Set<String> filter = Set.of("io_runtime");

        DataFusionStats result = TransportDataFusionStatsAction.filteredStats(stats, filter);

        assertNotNull(result);
        // io_runtime should be present
        assertNotNull(result.getNativeExecutorsStats());
        assertNotNull(result.getNativeExecutorsStats().getIoRuntime());
        assertEquals(stats.getNativeExecutorsStats().getIoRuntime(), result.getNativeExecutorsStats().getIoRuntime());
        // cpu_runtime should be null
        assertNull(result.getNativeExecutorsStats().getCpuRuntime());
        // task monitors should be empty
        assertTrue(result.getNativeExecutorsStats().getTaskMonitors().isEmpty());
        // gate stats should be null
        assertNull(result.getDatanodeGateStats());
        assertNull(result.getCoordinatorGateStats());
    }

    // ---- Test 6: filteredStats with multiple sections ----

    public void testFilteredStatsWithMultipleSections() {
        DataFusionStats stats = buildFullStats();
        Set<String> filter = new HashSet<>();
        filter.add("cpu_runtime");
        filter.add("query_execution");
        filter.add("datanode_gate");

        DataFusionStats result = TransportDataFusionStatsAction.filteredStats(stats, filter);

        assertNotNull(result);
        // cpu_runtime should be present
        assertNotNull(result.getNativeExecutorsStats());
        assertNotNull(result.getNativeExecutorsStats().getCpuRuntime());
        assertEquals(stats.getNativeExecutorsStats().getCpuRuntime(), result.getNativeExecutorsStats().getCpuRuntime());
        // io_runtime should be null (not in filter)
        assertNull(result.getNativeExecutorsStats().getIoRuntime());
        // query_execution task monitor should be present
        Map<String, TaskMonitorStats> monitors = result.getNativeExecutorsStats().getTaskMonitors();
        assertEquals(1, monitors.size());
        assertTrue(monitors.containsKey("query_execution"));
        assertEquals(stats.getNativeExecutorsStats().getTaskMonitors().get("query_execution"), monitors.get("query_execution"));
        // datanode_gate should be present
        assertNotNull(result.getDatanodeGateStats());
        assertEquals(stats.getDatanodeGateStats(), result.getDatanodeGateStats());
        // coordinator_gate should be null (not in filter)
        assertNull(result.getCoordinatorGateStats());
    }

    // ---- Test 7: filteredStats with null stats input returns null ----

    public void testFilteredStatsWithNullStats() {
        DataFusionStats result = TransportDataFusionStatsAction.filteredStats(null, Set.of("io_runtime"));

        assertNull(result);
    }

    // ---- Test 8: nodeOperation handles service not started (IllegalStateException) ----

    public void testNodeOperationHandlesServiceNotStarted() {
        when(mockDataFusionService.getStats()).thenThrow(new IllegalStateException("DataFusionService has not been started"));

        DataFusionStatsNodesRequest nodesRequest = new DataFusionStatsNodesRequest(new String[0], Collections.emptySet());
        DataFusionStatsNodeRequest nodeRequest = new DataFusionStatsNodeRequest(nodesRequest);

        DataFusionStatsNodeResponse response = action.nodeOperation(nodeRequest);

        assertNotNull(response);
        assertEquals(localNode, response.getNode());
        assertNull(response.getStats());
    }
}
