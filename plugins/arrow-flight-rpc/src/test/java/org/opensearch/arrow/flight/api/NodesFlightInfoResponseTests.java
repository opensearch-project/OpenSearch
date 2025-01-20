/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.api;

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.net.ConnectException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class NodesFlightInfoResponseTests extends OpenSearchTestCase {

    public void testNodesFlightInfoResponseSerialization() throws Exception {
        ClusterName clusterName = new ClusterName("test-cluster");
        List<NodeFlightInfo> nodes = new ArrayList<>();

        DiscoveryNode node1 = createTestNode("node1");
        DiscoveryNode node2 = createTestNode("node2");

        nodes.add(createNodeFlightInfo(node1, 47470));
        nodes.add(createNodeFlightInfo(node2, 47471));

        NodesFlightInfoResponse originalResponse = new NodesFlightInfoResponse(clusterName, nodes, List.of());

        BytesStreamOutput output = new BytesStreamOutput();
        originalResponse.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        NodesFlightInfoResponse deserializedResponse = new NodesFlightInfoResponse(input);
        assertEquals(originalResponse.getNodes().size(), deserializedResponse.getNodes().size());

        for (int i = 0; i < originalResponse.getNodes().size(); i++) {
            NodeFlightInfo originalNode = originalResponse.getNodes().get(i);
            NodeFlightInfo deserializedNode = deserializedResponse.getNodes().get(i);

            assertEquals(originalNode.getNode().getId(), deserializedNode.getNode().getId());
            assertEquals(originalNode.getNode().getName(), deserializedNode.getNode().getName());
            assertEquals(originalNode.getBoundAddress().publishAddress(), deserializedNode.getBoundAddress().publishAddress());
        }
        assertEquals(originalResponse.getClusterName(), deserializedResponse.getClusterName());
    }

    public void testNodesFlightInfoResponseEmpty() {
        ClusterName clusterName = new ClusterName("test-cluster");
        List<NodeFlightInfo> nodes = new ArrayList<>();

        NodesFlightInfoResponse response = new NodesFlightInfoResponse(clusterName, nodes, List.of());

        assertTrue(response.getNodes().isEmpty());
        assertEquals(clusterName, response.getClusterName());
    }

    public void testToXContentWithFailures() throws Exception {
        NodesFlightInfoResponse response = getNodesFlightInfoResponse();

        XContentBuilder builder = JsonXContent.contentBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            Map<String, Object> responseMap = parser.map();

            Map<String, Object> nodesStats = (Map<String, Object>) responseMap.get("_nodes");
            assertNotNull("_nodes object should exist", nodesStats);
            assertEquals(2, nodesStats.get("total"));
            assertEquals(2, nodesStats.get("successful"));
            assertEquals(2, nodesStats.get("failed"));

            assertEquals("test-cluster", responseMap.get("cluster_name"));

            Map<String, Object> nodes = (Map<String, Object>) responseMap.get("nodes");
            assertNotNull("nodes object should exist", nodes);
            assertEquals(2, nodes.size());

            Map<String, Object> firstNode = (Map<String, Object>) nodes.get("successful_node_1");
            assertNotNull(firstNode);
            Map<String, Object> firstNodeFlightServer = (Map<String, Object>) firstNode.get("flight_server");
            assertNotNull(firstNodeFlightServer);
            Map<String, Object> firstNodePublishAddress = (Map<String, Object>) firstNodeFlightServer.get("publish_address");
            assertEquals("localhost", firstNodePublishAddress.get("host"));
            assertEquals(47470, firstNodePublishAddress.get("port"));

            Map<String, Object> secondNode = (Map<String, Object>) nodes.get("successful_node_2");
            assertNotNull(secondNode);
            Map<String, Object> secondNodeFlightServer = (Map<String, Object>) secondNode.get("flight_server");
            assertNotNull(secondNodeFlightServer);
            Map<String, Object> secondNodePublishAddress = (Map<String, Object>) secondNodeFlightServer.get("publish_address");
            assertEquals("localhost", secondNodePublishAddress.get("host"));
            assertEquals(47471, secondNodePublishAddress.get("port"));

            List<Map<String, Object>> failuresList = (List<Map<String, Object>>) responseMap.get("failures");
            assertNotNull("failures array should exist", failuresList);
            assertEquals(2, failuresList.size());

            Map<String, Object> firstFailure = failuresList.get(0);
            assertEquals("failed_node_1", firstFailure.get("node_id"));
            assertEquals("Connection refused", firstFailure.get("reason"));

            Map<String, Object> secondFailure = failuresList.get(1);
            assertEquals("failed_node_2", secondFailure.get("node_id"));
            assertEquals("Node not found", secondFailure.get("reason"));
        }
    }

    private static NodesFlightInfoResponse getNodesFlightInfoResponse() {
        DiscoveryNode node1 = new DiscoveryNode(
            "successful_node_1",
            "successful_node_1",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );

        List<NodeFlightInfo> successfulNodes = getNodeFlightInfos(node1);

        return getNodesFlightInfoResponse(successfulNodes);
    }

    private static NodesFlightInfoResponse getNodesFlightInfoResponse(List<NodeFlightInfo> successfulNodes) {
        List<FailedNodeException> failures = Arrays.asList(
            new FailedNodeException("failed_node_1", "Connection refused", new ConnectException("Connection refused")),
            new FailedNodeException("failed_node_2", "Node not found", new Exception("Node not found"))
        );

        return new NodesFlightInfoResponse(new ClusterName("test-cluster"), successfulNodes, failures);
    }

    private static List<NodeFlightInfo> getNodeFlightInfos(DiscoveryNode node1) {
        DiscoveryNode node2 = new DiscoveryNode(
            "successful_node_2",
            "successful_node_2",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );

        TransportAddress address1 = new TransportAddress(InetAddress.getLoopbackAddress(), 47470);
        return getNodeFlightInfos(node1, address1, node2);
    }

    private static List<NodeFlightInfo> getNodeFlightInfos(DiscoveryNode node1, TransportAddress address1, DiscoveryNode node2) {
        BoundTransportAddress boundAddress1 = new BoundTransportAddress(new TransportAddress[] { address1 }, address1);

        TransportAddress address2 = new TransportAddress(InetAddress.getLoopbackAddress(), 47471);
        BoundTransportAddress boundAddress2 = new BoundTransportAddress(new TransportAddress[] { address2 }, address2);

        return Arrays.asList(new NodeFlightInfo(node1, boundAddress1), new NodeFlightInfo(node2, boundAddress2));
    }

    public void testToXContentWithNoFailures() throws Exception {
        NodesFlightInfoResponse response = getFlightInfoResponse();

        XContentBuilder builder = JsonXContent.contentBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            Map<String, Object> responseMap = parser.map();

            Map<String, Object> nodesStats = (Map<String, Object>) responseMap.get("_nodes");
            assertNotNull(nodesStats);
            assertEquals(1, nodesStats.get("total"));
            assertEquals(1, nodesStats.get("successful"));
            assertEquals(0, nodesStats.get("failed"));

            assertEquals("test-cluster", responseMap.get("cluster_name"));

            Map<String, Object> nodes = (Map<String, Object>) responseMap.get("nodes");
            assertNotNull(nodes);
            assertEquals(1, nodes.size());

            assertNull("failures array should not exist", responseMap.get("failures"));
        }
    }

    private static NodesFlightInfoResponse getFlightInfoResponse() {
        DiscoveryNode node = new DiscoveryNode(
            "successful_node",
            "successful_node",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );

        TransportAddress address = new TransportAddress(InetAddress.getLoopbackAddress(), 47470);
        BoundTransportAddress boundAddress = new BoundTransportAddress(new TransportAddress[] { address }, address);

        return new NodesFlightInfoResponse(
            new ClusterName("test-cluster"),
            Collections.singletonList(new NodeFlightInfo(node, boundAddress)),
            Collections.emptyList()
        );
    }

    private DiscoveryNode createTestNode(String nodeId) {
        return new DiscoveryNode(
            nodeId,
            nodeId,
            "host" + nodeId,
            "localhost",
            "127.0.0.1",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            new HashMap<>(),
            new HashSet<>(),
            Version.CURRENT
        );
    }

    private NodeFlightInfo createNodeFlightInfo(DiscoveryNode node, int port) {
        TransportAddress address = new TransportAddress(InetAddress.getLoopbackAddress(), port);
        BoundTransportAddress boundAddress = new BoundTransportAddress(new TransportAddress[] { address }, address);
        return new NodeFlightInfo(node, boundAddress);
    }
}
