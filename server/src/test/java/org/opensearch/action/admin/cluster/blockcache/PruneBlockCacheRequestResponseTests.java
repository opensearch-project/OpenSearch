/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.blockcache;

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Serialization and XContent tests for {@link PruneBlockCacheRequest}, {@link PruneBlockCacheResponse},
 * and {@link NodePruneBlockCacheResponse}.
 */
public class PruneBlockCacheRequestResponseTests extends OpenSearchTestCase {

    private DiscoveryNode createNode(String nodeId) {
        return new DiscoveryNode(
            nodeId,
            nodeId,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
    }

    // ── PruneBlockCacheRequest ────────────────────────────────────────────────

    public void testRequestSerializationNoNodes() throws IOException {
        PruneBlockCacheRequest original = new PruneBlockCacheRequest();
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        // just verify round-trip doesn't throw
        new PruneBlockCacheRequest(out.bytes().streamInput());
    }

    public void testRequestSerializationWithNodes() throws IOException {
        PruneBlockCacheRequest original = new PruneBlockCacheRequest("node1", "node2");
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        PruneBlockCacheRequest deserialized = new PruneBlockCacheRequest(out.bytes().streamInput());
        assertArrayEquals(original.nodesIds(), deserialized.nodesIds());
    }

    // ── NodePruneBlockCacheResponse ───────────────────────────────────────────

    public void testNodeResponseSerializationCleared() throws IOException {
        DiscoveryNode node = createNode("node1");
        NodePruneBlockCacheResponse original = new NodePruneBlockCacheResponse(node, true);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        NodePruneBlockCacheResponse deserialized = new NodePruneBlockCacheResponse(out.bytes().streamInput());
        assertEquals(original.isCleared(), deserialized.isCleared());
        assertEquals(original.getNode().getId(), deserialized.getNode().getId());
    }

    public void testNodeResponseSerializationNotCleared() throws IOException {
        DiscoveryNode node = createNode("node1");
        NodePruneBlockCacheResponse original = new NodePruneBlockCacheResponse(node, false);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        NodePruneBlockCacheResponse deserialized = new NodePruneBlockCacheResponse(out.bytes().streamInput());
        assertFalse(deserialized.isCleared());
    }

    public void testNodeResponseXContent() throws IOException {
        DiscoveryNode node = createNode("node1");
        NodePruneBlockCacheResponse response = new NodePruneBlockCacheResponse(node, true);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();
        assertTrue(json.contains("\"cleared\":true"));
        assertTrue(json.contains("node1"));
    }

    // ── PruneBlockCacheResponse ───────────────────────────────────────────────

    public void testResponseXContentSingleNode() throws IOException {
        DiscoveryNode node = createNode("node1");
        List<NodePruneBlockCacheResponse> nodes = List.of(new NodePruneBlockCacheResponse(node, true));
        PruneBlockCacheResponse response = new PruneBlockCacheResponse(new ClusterName("test"), nodes, Collections.emptyList());
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();
        assertTrue(json.contains("\"acknowledged\":true"));
        assertTrue(json.contains("\"cleared\":true"));
    }

    public void testResponseXContentWithFailure() throws IOException {
        DiscoveryNode node = createNode("node1");
        List<NodePruneBlockCacheResponse> nodes = List.of(new NodePruneBlockCacheResponse(node, true));
        List<FailedNodeException> failures = List.of(new FailedNodeException("node2", "simulated", new RuntimeException()));
        PruneBlockCacheResponse response = new PruneBlockCacheResponse(new ClusterName("test"), nodes, failures);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();
        assertTrue(json.contains("\"failed_nodes\":1"));
    }
}
