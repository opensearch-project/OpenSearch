/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.filecache;

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
 * Tests for {@link PruneFileCacheRequest} and {@link PruneFileCacheResponse} with enhanced multi-node architecture.
 * Covers node targeting and rich response aggregation.
 */
public class PruneFileCacheRequestResponseTests extends OpenSearchTestCase {

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

    private PruneFileCacheResponse createMultiNodeResponse(long... prunedBytesPerNode) {
        ClusterName clusterName = new ClusterName("test-cluster");

        List<NodePruneFileCacheResponse> nodeResponses = Arrays.asList();
        if (prunedBytesPerNode.length > 0) {
            nodeResponses = new java.util.ArrayList<>();
            for (int i = 0; i < prunedBytesPerNode.length; i++) {
                DiscoveryNode node = createRealNode("node" + i, "test-node-" + i);
                nodeResponses.add(new NodePruneFileCacheResponse(node, prunedBytesPerNode[i], 10737418240L));
            }
        }

        List<FailedNodeException> failures = Collections.emptyList();

        return new PruneFileCacheResponse(clusterName, nodeResponses, failures);
    }

    public void testPruneCacheRequestSerialization() throws IOException {
        PruneFileCacheRequest originalRequest = new PruneFileCacheRequest("node1", "node2");

        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        PruneFileCacheRequest deserializedRequest = new PruneFileCacheRequest(in);

        assertArrayEquals(originalRequest.nodesIds(), deserializedRequest.nodesIds());
    }

    public void testPruneCacheRequestDefaultParameters() throws IOException {
        PruneFileCacheRequest originalRequest = new PruneFileCacheRequest();

        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        PruneFileCacheRequest deserializedRequest = new PruneFileCacheRequest(in);

        assertEquals(originalRequest.timeout(), deserializedRequest.timeout());
    }

    public void testPruneCacheResponseMultiNodeSerialization() throws IOException {
        PruneFileCacheResponse originalResponse = createMultiNodeResponse(1048576L, 2097152L);

        BytesStreamOutput out = new BytesStreamOutput();
        originalResponse.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        PruneFileCacheResponse deserializedResponse = new PruneFileCacheResponse(in);
        assertEquals("Node count should match", 2, deserializedResponse.getNodes().size());
        assertEquals("Total pruned bytes should match", 3145728L, deserializedResponse.getTotalPrunedBytes());
        assertEquals("getPrunedBytes should work", 3145728L, deserializedResponse.getPrunedBytes());
        assertTrue("Should be acknowledged", deserializedResponse.isAcknowledged());
        assertTrue("Should be completely successful", deserializedResponse.isCompletelySuccessful());
        assertFalse("Should not be partially successful", deserializedResponse.isPartiallySuccessful());
    }

    public void testPruneCacheResponseWithFailures() {
        ClusterName clusterName = new ClusterName("test-cluster");
        List<NodePruneFileCacheResponse> successfulNodes = Arrays.asList(
            new NodePruneFileCacheResponse(createRealNode("node1", "successful-node"), 1048576L, 10737418240L)
        );
        List<FailedNodeException> failures = Arrays.asList(
            new FailedNodeException("node2", "Cache operation failed", new RuntimeException("Test failure"))
        );

        PruneFileCacheResponse response = new PruneFileCacheResponse(clusterName, successfulNodes, failures);

        assertEquals("Should have one successful node", 1, response.getNodes().size());
        assertEquals("Should have one failure", 1, response.failures().size());
        assertTrue("Should be partially successful", response.isPartiallySuccessful());
        assertFalse("Should not be completely successful", response.isCompletelySuccessful());
    }

    public void testPruneCacheResponseEnhancedJSON() throws IOException {
        PruneFileCacheResponse response = createMultiNodeResponse(1048576L, 2097152L);

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
        List<NodePruneFileCacheResponse> successfulNodes = Arrays.asList(
            new NodePruneFileCacheResponse(createRealNode("node1", "good-node"), 1048576L, 10737418240L)
        );
        List<FailedNodeException> failures = Arrays.asList(
            new FailedNodeException("node2", "Test failure message", new RuntimeException("Root cause"))
        );

        PruneFileCacheResponse response = new PruneFileCacheResponse(clusterName, successfulNodes, failures);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = builder.toString();

        assertTrue("Should contain failures array", jsonString.contains("\"failures\""));
        assertTrue("Should contain failed node ID", jsonString.contains("\"node_id\":\"node2\""));
        assertTrue("Should contain failure reason", jsonString.contains("\"reason\""));
        assertTrue("Should contain caused_by", jsonString.contains("\"caused_by\":\"RuntimeException\""));
    }

    public void testPruneCacheResponseScenarios() {
        PruneFileCacheResponse zeroResponse = createMultiNodeResponse(0L, 0L);
        assertEquals("Zero bytes should sum correctly", 0L, zeroResponse.getTotalPrunedBytes());

        PruneFileCacheResponse mixedResponse = createMultiNodeResponse(1048576L, 0L, 2097152L);
        assertEquals("Mixed results should sum correctly", 3145728L, mixedResponse.getTotalPrunedBytes());

        PruneFileCacheResponse largeResponse = createMultiNodeResponse(Long.MAX_VALUE / 2, Long.MAX_VALUE / 2);
        assertEquals("Large values should sum correctly", Long.MAX_VALUE - 1, largeResponse.getTotalPrunedBytes());
    }

    public void testPruneCacheRequestValidation() {
        PruneFileCacheRequest request = new PruneFileCacheRequest();
        assertNull("Validation should be null", request.validate());

        PruneFileCacheRequest targetedRequest = new PruneFileCacheRequest("node1", "node2");
        assertNull("Targeted request validation should be null", targetedRequest.validate());
    }

    public void testPruneCacheRequestNodeTargeting() {
        PruneFileCacheRequest defaultRequest = new PruneFileCacheRequest();
        assertNull("Default request should have null nodeIds", defaultRequest.nodesIds());

        PruneFileCacheRequest singleNodeRequest = new PruneFileCacheRequest("node1");
        assertArrayEquals("Single node should be set", new String[] { "node1" }, singleNodeRequest.nodesIds());

        PruneFileCacheRequest multiNodeRequest = new PruneFileCacheRequest("node1", "node2", "node3");
        assertArrayEquals("Multiple nodes should be set", new String[] { "node1", "node2", "node3" }, multiNodeRequest.nodesIds());

        PruneFileCacheRequest emptyRequest = new PruneFileCacheRequest(new String[0]);
        assertArrayEquals("Empty array should be preserved", new String[0], emptyRequest.nodesIds());
    }

    public void testNodePruneCacheResponseSerialization() throws IOException {
        DiscoveryNode realNode = createRealNode("test-node", "test-node-name");
        NodePruneFileCacheResponse originalResponse = new NodePruneFileCacheResponse(realNode, 1048576L, 10737418240L);

        BytesStreamOutput out = new BytesStreamOutput();
        originalResponse.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        NodePruneFileCacheResponse deserializedResponse = new NodePruneFileCacheResponse(in);
        assertEquals("Pruned bytes should match", originalResponse.getPrunedBytes(), deserializedResponse.getPrunedBytes());
        assertEquals("Cache capacity should match", originalResponse.getCacheCapacity(), deserializedResponse.getCacheCapacity());
    }

    public void testResponseAggregationEdgeCases() {
        PruneFileCacheResponse emptyResponse = new PruneFileCacheResponse(
            new ClusterName("test"),
            Collections.emptyList(),
            Collections.emptyList()
        );
        assertEquals("Empty response total should be 0", 0L, emptyResponse.getTotalPrunedBytes());
        assertFalse("Empty response should not be completely successful (no nodes processed)", emptyResponse.isCompletelySuccessful());
        assertFalse("Empty response should not be partially successful (no nodes processed)", emptyResponse.isPartiallySuccessful());

        PruneFileCacheResponse allFailureResponse = new PruneFileCacheResponse(
            new ClusterName("test"),
            Collections.emptyList(),
            Arrays.asList(new FailedNodeException("node1", "Test failure", new RuntimeException()))
        );
        assertEquals("All-failure response total should be 0", 0L, allFailureResponse.getTotalPrunedBytes());
        assertFalse("All-failure response should not be successful", allFailureResponse.isCompletelySuccessful());
        assertFalse("All-failure response should not be partially successful", allFailureResponse.isPartiallySuccessful());
    }
}
