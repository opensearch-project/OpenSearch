/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.be.datafusion.stats.DataFusionStats;
import org.opensearch.be.datafusion.stats.NativeExecutorsStats;
import org.opensearch.be.datafusion.stats.PartitionGateStats;
import org.opensearch.be.datafusion.stats.RuntimeMetrics;
import org.opensearch.be.datafusion.stats.TaskMonitorStats;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link DataFusionStatsNodesResponse#toXContent}.
 *
 * Validates: Requirements 1.4, 1.5, 2.1, 2.2, 2.3
 */
public class DataFusionStatsNodesResponseTests extends OpenSearchTestCase {

    // ---- Helper methods ----

    private static DiscoveryNode createNode(String id, String name, String host, int port) throws Exception {
        return new DiscoveryNode(
            name,
            id,
            new TransportAddress(InetAddress.getByName(host), port),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
    }

    private static DataFusionStats createSimpleStats() {
        RuntimeMetrics io = new RuntimeMetrics(4, 100, 500, 2, 10, 5, 8, 50, 0);
        Map<String, TaskMonitorStats> taskMonitors = new LinkedHashMap<>();
        taskMonitors.put("coordinator_reduce", new TaskMonitorStats(10, 20, 30));
        taskMonitors.put("query_execution", new TaskMonitorStats(40, 50, 60));
        taskMonitors.put("stream_next", new TaskMonitorStats(70, 80, 90));
        taskMonitors.put("plan_setup", new TaskMonitorStats(100, 110, 120));
        return new DataFusionStats(
            new NativeExecutorsStats(io, null, taskMonitors),
            new PartitionGateStats("datanode_gate", 12, 3, 100, 500, 0, 12),
            new PartitionGateStats("coordinator_gate", 8, 1, 50, 200, 0, 8),
            null
        );
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> renderToMap(DataFusionStatsNodesResponse response) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), builder.toString(), false);
    }

    // ---- Test 1: Single node JSON structure ----

    @SuppressWarnings("unchecked")
    public void testToXContentWithSingleNode() throws Exception {
        ClusterName clusterName = new ClusterName("test-cluster");
        DiscoveryNode node = createNode("node-1", "data-node-1", "10.0.0.1", 9300);
        DataFusionStats stats = createSimpleStats();
        DataFusionStatsNodeResponse nodeResponse = new DataFusionStatsNodeResponse(node, stats);

        DataFusionStatsNodesResponse response = new DataFusionStatsNodesResponse(
            clusterName,
            List.of(nodeResponse),
            Collections.emptyList()
        );

        Map<String, Object> map = renderToMap(response);

        // Verify top-level structure
        assertTrue(map.containsKey("_nodes"));
        assertTrue(map.containsKey("cluster_name"));
        assertTrue(map.containsKey("nodes"));

        // Verify _nodes counts
        Map<String, Object> nodesHeader = (Map<String, Object>) map.get("_nodes");
        assertEquals(1, nodesHeader.get("total"));
        assertEquals(1, nodesHeader.get("successful"));
        assertEquals(0, nodesHeader.get("failed"));

        // Verify cluster_name
        assertEquals("test-cluster", map.get("cluster_name"));

        // Verify nodes object has the node keyed by ID
        Map<String, Object> nodes = (Map<String, Object>) map.get("nodes");
        assertEquals(1, nodes.size());
        assertTrue(nodes.containsKey("node-1"));

        // Verify node entry has stats (no name/host/transport_address — matches KNN pattern)
        Map<String, Object> nodeEntry = (Map<String, Object>) nodes.get("node-1");
        assertFalse(nodeEntry.containsKey("name"));
        assertFalse(nodeEntry.containsKey("host"));
        assertFalse(nodeEntry.containsKey("transport_address"));
        assertTrue(nodeEntry.containsKey("io_runtime"));
        assertTrue(nodeEntry.containsKey("datanode_gate"));
        assertTrue(nodeEntry.containsKey("coordinator_gate"));
    }

    // ---- Test 2: Multiple nodes JSON structure ----

    @SuppressWarnings("unchecked")
    public void testToXContentWithMultipleNodes() throws Exception {
        ClusterName clusterName = new ClusterName("multi-cluster");
        DiscoveryNode node1 = createNode("node-1", "data-node-1", "10.0.0.1", 9300);
        DiscoveryNode node2 = createNode("node-2", "data-node-2", "10.0.0.2", 9300);
        DiscoveryNode node3 = createNode("node-3", "coordinator-node", "10.0.0.3", 9300);

        DataFusionStats stats = createSimpleStats();
        List<DataFusionStatsNodeResponse> nodeResponses = List.of(
            new DataFusionStatsNodeResponse(node1, stats),
            new DataFusionStatsNodeResponse(node2, stats),
            new DataFusionStatsNodeResponse(node3, stats)
        );

        DataFusionStatsNodesResponse response = new DataFusionStatsNodesResponse(clusterName, nodeResponses, Collections.emptyList());

        Map<String, Object> map = renderToMap(response);

        // Verify _nodes counts
        Map<String, Object> nodesHeader = (Map<String, Object>) map.get("_nodes");
        assertEquals(3, nodesHeader.get("total"));
        assertEquals(3, nodesHeader.get("successful"));
        assertEquals(0, nodesHeader.get("failed"));

        // Verify all nodes present
        Map<String, Object> nodes = (Map<String, Object>) map.get("nodes");
        assertEquals(3, nodes.size());
        assertTrue(nodes.containsKey("node-1"));
        assertTrue(nodes.containsKey("node-2"));
        assertTrue(nodes.containsKey("node-3"));
    }

    // ---- Test 3: Failures array rendering ----

    @SuppressWarnings("unchecked")
    public void testToXContentWithFailures() throws Exception {
        ClusterName clusterName = new ClusterName("failure-cluster");
        DiscoveryNode node1 = createNode("node-1", "data-node-1", "10.0.0.1", 9300);
        DataFusionStats stats = createSimpleStats();
        DataFusionStatsNodeResponse nodeResponse = new DataFusionStatsNodeResponse(node1, stats);

        List<FailedNodeException> failures = List.of(
            new FailedNodeException("node-2", "connection timeout", new RuntimeException("timeout")),
            new FailedNodeException("node-3", "node not available", new RuntimeException("unavailable"))
        );

        DataFusionStatsNodesResponse response = new DataFusionStatsNodesResponse(clusterName, List.of(nodeResponse), failures);

        Map<String, Object> map = renderToMap(response);

        // Verify _nodes counts include failures
        Map<String, Object> nodesHeader = (Map<String, Object>) map.get("_nodes");
        assertEquals(3, nodesHeader.get("total")); // 1 success + 2 failures
        assertEquals(1, nodesHeader.get("successful"));
        assertEquals(2, nodesHeader.get("failed"));

        // Verify failures array is present
        assertTrue(nodesHeader.containsKey("failures"));
        List<Object> failuresArray = (List<Object>) nodesHeader.get("failures");
        assertEquals(2, failuresArray.size());

        // Verify nodes only contains successful node
        Map<String, Object> nodes = (Map<String, Object>) map.get("nodes");
        assertEquals(1, nodes.size());
        assertTrue(nodes.containsKey("node-1"));
    }

    // ---- Test 4: Per-node metadata ----

    @SuppressWarnings("unchecked")
    public void testToXContentNodeMetadata() throws Exception {
        ClusterName clusterName = new ClusterName("metadata-cluster");
        DiscoveryNode node = createNode("node-abc-123", "my-data-node", "192.168.1.100", 9300);
        DataFusionStats stats = createSimpleStats();
        DataFusionStatsNodeResponse nodeResponse = new DataFusionStatsNodeResponse(node, stats);

        DataFusionStatsNodesResponse response = new DataFusionStatsNodesResponse(
            clusterName,
            List.of(nodeResponse),
            Collections.emptyList()
        );

        Map<String, Object> map = renderToMap(response);
        Map<String, Object> nodes = (Map<String, Object>) map.get("nodes");
        Map<String, Object> nodeEntry = (Map<String, Object>) nodes.get("node-abc-123");

        // Verify per-node metadata is NOT present (KNN pattern — no IP exposure)
        assertFalse(nodeEntry.containsKey("name"));
        assertFalse(nodeEntry.containsKey("host"));
        assertFalse(nodeEntry.containsKey("transport_address"));
    }

    // ---- Test 5: Cluster name field ----

    @SuppressWarnings("unchecked")
    public void testToXContentClusterName() throws Exception {
        String expectedClusterName = "production-us-east-1";
        ClusterName clusterName = new ClusterName(expectedClusterName);
        DiscoveryNode node = createNode("node-1", "node-1", "127.0.0.1", 9300);
        DataFusionStatsNodeResponse nodeResponse = new DataFusionStatsNodeResponse(node, createSimpleStats());

        DataFusionStatsNodesResponse response = new DataFusionStatsNodesResponse(
            clusterName,
            List.of(nodeResponse),
            Collections.emptyList()
        );

        Map<String, Object> map = renderToMap(response);
        assertEquals(expectedClusterName, map.get("cluster_name"));
    }

    // ---- Test 6: Nodes counts correctness ----

    @SuppressWarnings("unchecked")
    public void testToXContentNodesCountsCorrect() throws Exception {
        ClusterName clusterName = new ClusterName("count-cluster");

        // Create 3 successful nodes
        List<DataFusionStatsNodeResponse> nodeResponses = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            DiscoveryNode node = createNode("node-" + i, "node-" + i, "10.0.0." + i, 9300);
            nodeResponses.add(new DataFusionStatsNodeResponse(node, createSimpleStats()));
        }

        // Create 2 failures
        List<FailedNodeException> failures = List.of(
            new FailedNodeException("node-4", "failed", new RuntimeException()),
            new FailedNodeException("node-5", "failed", new RuntimeException())
        );

        DataFusionStatsNodesResponse response = new DataFusionStatsNodesResponse(clusterName, nodeResponses, failures);

        Map<String, Object> map = renderToMap(response);
        Map<String, Object> nodesHeader = (Map<String, Object>) map.get("_nodes");

        // total = successes + failures = 3 + 2 = 5
        assertEquals(5, nodesHeader.get("total"));
        // successful = number of successful responses = 3
        assertEquals(3, nodesHeader.get("successful"));
        // failed = number of failures = 2
        assertEquals(2, nodesHeader.get("failed"));
    }

    // ---- Test 7: Null stats renders without stats fields ----

    @SuppressWarnings("unchecked")
    public void testToXContentWithNullStats() throws Exception {
        ClusterName clusterName = new ClusterName("null-stats-cluster");
        DiscoveryNode node = createNode("node-1", "data-node-1", "10.0.0.1", 9300);
        // Node response with null stats (service not started)
        DataFusionStatsNodeResponse nodeResponse = new DataFusionStatsNodeResponse(node, null);

        DataFusionStatsNodesResponse response = new DataFusionStatsNodesResponse(
            clusterName,
            List.of(nodeResponse),
            Collections.emptyList()
        );

        Map<String, Object> map = renderToMap(response);
        Map<String, Object> nodes = (Map<String, Object>) map.get("nodes");
        Map<String, Object> nodeEntry = (Map<String, Object>) nodes.get("node-1");

        // Metadata should NOT be present (KNN pattern)
        assertFalse(nodeEntry.containsKey("name"));
        assertFalse(nodeEntry.containsKey("host"));
        assertFalse(nodeEntry.containsKey("transport_address"));

        // Stats fields should be absent
        assertFalse(nodeEntry.containsKey("io_runtime"));
        assertFalse(nodeEntry.containsKey("cpu_runtime"));
        assertFalse(nodeEntry.containsKey("coordinator_reduce"));
        assertFalse(nodeEntry.containsKey("query_execution"));
        assertFalse(nodeEntry.containsKey("stream_next"));
        assertFalse(nodeEntry.containsKey("plan_setup"));
        assertFalse(nodeEntry.containsKey("datanode_gate"));
        assertFalse(nodeEntry.containsKey("coordinator_gate"));
    }

    // ---- Test 8: Empty nodes list ----

    @SuppressWarnings("unchecked")
    public void testToXContentEmptyNodes() throws Exception {
        ClusterName clusterName = new ClusterName("empty-cluster");

        DataFusionStatsNodesResponse response = new DataFusionStatsNodesResponse(
            clusterName,
            Collections.emptyList(),
            Collections.emptyList()
        );

        Map<String, Object> map = renderToMap(response);

        // Verify _nodes counts are all zero
        Map<String, Object> nodesHeader = (Map<String, Object>) map.get("_nodes");
        assertEquals(0, nodesHeader.get("total"));
        assertEquals(0, nodesHeader.get("successful"));
        assertEquals(0, nodesHeader.get("failed"));

        // Verify cluster_name is present
        assertEquals("empty-cluster", map.get("cluster_name"));

        // Verify nodes object is empty
        Map<String, Object> nodes = (Map<String, Object>) map.get("nodes");
        assertTrue(nodes.isEmpty());
    }
}
