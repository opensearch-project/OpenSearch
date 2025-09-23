/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.cache;

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link PruneCacheRequest} and {@link PruneCacheResponse} with enhanced multi-node architecture.
 * Covers node targeting and rich response aggregation.
 */
public class PruneCacheRequestResponseTests extends OpenSearchTestCase {

    private DiscoveryNode createRealNode(String nodeId, String nodeName) {
        return new DiscoveryNode(
            nodeName,
            nodeId,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
    }

    private PruneCacheResponse createMultiNodeResponse(long... prunedBytesPerNode) {
        ClusterName clusterName = new ClusterName("test-cluster");

        List<NodePruneCacheResponse> nodeResponses = Arrays.asList();
        if (prunedBytesPerNode.length > 0) {
            nodeResponses = new java.util.ArrayList<>();
            for (int i = 0; i < prunedBytesPerNode.length; i++) {
                DiscoveryNode node = createRealNode("node" + i, "test-node-" + i);
                nodeResponses.add(new NodePruneCacheResponse(node, prunedBytesPerNode[i], 10737418240L));
            }
        }

        List<FailedNodeException> failures = Collections.emptyList();

        return new PruneCacheResponse(clusterName, nodeResponses, failures);
    }

    public void testPruneCacheRequestSerialization() throws IOException {
        PruneCacheRequest originalRequest = new PruneCacheRequest("node1", "node2");

        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        PruneCacheRequest deserializedRequest = new PruneCacheRequest(in);

        assertArrayEquals(originalRequest.nodesIds(), deserializedRequest.nodesIds());
    }

    public void testPruneCacheRequestDefaultParameters() throws IOException {
        PruneCacheRequest originalRequest = new PruneCacheRequest();

        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        PruneCacheRequest deserializedRequest = new PruneCacheRequest(in);

        assertEquals(originalRequest.timeout(), deserializedRequest.timeout());
    }

    public void testPruneCacheResponseMultiNodeSerialization() throws IOException {
        PruneCacheResponse originalResponse = createMultiNodeResponse(1048576L, 2097152L);

        BytesStreamOutput out = new BytesStreamOutput();
        originalResponse.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        PruneCacheResponse deserializedResponse = new PruneCacheResponse(in);
        assertEquals("Node count should match", 2, deserializedResponse.getNodes().size());
        assertEquals("Total pruned bytes should match", 3145728L, deserializedResponse.getTotalPrunedBytes());
        assertEquals("getPrunedBytes should work", 3145728L, deserializedResponse.getPrunedBytes());
        assertTrue("Should be acknowledged", deserializedResponse.isAcknowledged());
        assertTrue("Should be completely successful", deserializedResponse.isCompletelySuccessful());
        assertFalse("Should not be partially successful", deserializedResponse.isPartiallySuccessful());
    }

    public void testPruneCacheResponseWithFailures() {
        ClusterName clusterName = new ClusterName("test-cluster");
        List<NodePruneCacheResponse> successfulNodes = Arrays.asList(
            new NodePruneCacheResponse(createRealNode("node1", "successful-node"), 1048576L, 10737418240L)
        );
        List<FailedNodeException> failures = Arrays.asList(
            new FailedNodeException("node2", "Cache operation failed", new RuntimeException("Test failure"))
        );

        PruneCacheResponse response = new PruneCacheResponse(clusterName, successfulNodes, failures);

        assertEquals("Should have one successful node", 1, response.getNodes().size());
        assertEquals("Should have one failure", 1, response.failures().size());
        assertTrue("Should be partially successful", response.isPartiallySuccessful());
        assertFalse("Should not be completely successful", response.isCompletelySuccessful());
    }

    public void testPruneCacheResponseEnhancedJSON() throws IOException {
        PruneCacheResponse response = createMultiNodeResponse(1048576L, 2097152L);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = builder.toString();
        assertTrue("Should contain acknowledged field", jsonString.contains("\"acknowledged\":true"));
        assertTrue("Should contain total_pruned_bytes", jsonString.contains("\"total_pruned_bytes\":3145728"));
        assertTrue("Should contain summary section", jsonString.contains("\"summary\""));
        assertTrue("Should contain nodes section", jsonString.contains("\"nodes\""));
        assertTrue("Should contain successful_nodes count", jsonString.contains("\"successful_nodes\":2"));
        assertTrue("Should contain failed_nodes count", jsonString.contains("\"failed_nodes\":0"));
    }

    public void testPruneCacheResponseJSONWithFailures() throws IOException {
        ClusterName clusterName = new ClusterName("test-cluster");
        List<NodePruneCacheResponse> successfulNodes = Arrays.asList(
            new NodePruneCacheResponse(createRealNode("node1", "good-node"), 1048576L, 10737418240L)
        );
        List<FailedNodeException> failures = Arrays.asList(
            new FailedNodeException("node2", "Test failure message", new RuntimeException("Root cause"))
        );

        PruneCacheResponse response = new PruneCacheResponse(clusterName, successfulNodes, failures);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = builder.toString();

        assertTrue("Should contain failures array", jsonString.contains("\"failures\""));
        assertTrue("Should contain failed node ID", jsonString.contains("\"node_id\":\"node2\""));
        assertTrue("Should contain failure reason", jsonString.contains("\"reason\""));
        assertTrue("Should contain caused_by", jsonString.contains("\"caused_by\":\"RuntimeException\""));
    }

    public void testPruneCacheResponseScenarios() {
        PruneCacheResponse zeroResponse = createMultiNodeResponse(0L, 0L);
        assertEquals("Zero bytes should sum correctly", 0L, zeroResponse.getTotalPrunedBytes());

        PruneCacheResponse mixedResponse = createMultiNodeResponse(1048576L, 0L, 2097152L);
        assertEquals("Mixed results should sum correctly", 3145728L, mixedResponse.getTotalPrunedBytes());

        PruneCacheResponse largeResponse = createMultiNodeResponse(Long.MAX_VALUE / 2, Long.MAX_VALUE / 2);
        assertEquals("Large values should sum correctly", Long.MAX_VALUE - 1, largeResponse.getTotalPrunedBytes());
    }

    public void testPruneCacheRequestValidation() {
        PruneCacheRequest request = new PruneCacheRequest();
        assertNull("Validation should be null", request.validate());

        PruneCacheRequest targetedRequest = new PruneCacheRequest("node1", "node2");
        assertNull("Targeted request validation should be null", targetedRequest.validate());
    }

    public void testPruneCacheRequestNodeTargeting() {
        PruneCacheRequest defaultRequest = new PruneCacheRequest();
        assertNull("Default request should have null nodeIds", defaultRequest.nodesIds());

        PruneCacheRequest singleNodeRequest = new PruneCacheRequest("node1");
        assertArrayEquals("Single node should be set", new String[] { "node1" }, singleNodeRequest.nodesIds());

        PruneCacheRequest multiNodeRequest = new PruneCacheRequest("node1", "node2", "node3");
        assertArrayEquals("Multiple nodes should be set", new String[] { "node1", "node2", "node3" }, multiNodeRequest.nodesIds());

        PruneCacheRequest emptyRequest = new PruneCacheRequest(new String[0]);
        assertArrayEquals("Empty array should be preserved", new String[0], emptyRequest.nodesIds());
    }

    public void testNodePruneCacheResponseSerialization() throws IOException {
        DiscoveryNode realNode = createRealNode("test-node", "test-node-name");
        NodePruneCacheResponse originalResponse = new NodePruneCacheResponse(realNode, 1048576L, 10737418240L);

        BytesStreamOutput out = new BytesStreamOutput();
        originalResponse.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        NodePruneCacheResponse deserializedResponse = new NodePruneCacheResponse(in);
        assertEquals("Pruned bytes should match", originalResponse.getPrunedBytes(), deserializedResponse.getPrunedBytes());
        assertEquals("Cache capacity should match", originalResponse.getCacheCapacity(), deserializedResponse.getCacheCapacity());
    }

    public void testResponseAggregationEdgeCases() {
        PruneCacheResponse emptyResponse = new PruneCacheResponse(
            new ClusterName("test"),
            Collections.emptyList(),
            Collections.emptyList()
        );
        assertEquals("Empty response total should be 0", 0L, emptyResponse.getTotalPrunedBytes());
        assertFalse("Empty response should not be completely successful (no nodes processed)", emptyResponse.isCompletelySuccessful());
        assertFalse("Empty response should not be partially successful (no nodes processed)", emptyResponse.isPartiallySuccessful());

        PruneCacheResponse allFailureResponse = new PruneCacheResponse(
            new ClusterName("test"),
            Collections.emptyList(),
            Arrays.asList(new FailedNodeException("node1", "Test failure", new RuntimeException()))
        );
        assertEquals("All-failure response total should be 0", 0L, allFailureResponse.getTotalPrunedBytes());
        assertFalse("All-failure response should not be successful", allFailureResponse.isCompletelySuccessful());
        assertFalse("All-failure response should not be partially successful", allFailureResponse.isPartiallySuccessful());
    }
}
