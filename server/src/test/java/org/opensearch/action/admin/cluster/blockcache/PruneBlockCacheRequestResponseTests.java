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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Wire-serialization and XContent tests for {@link PruneBlockCacheRequest}, {@link PruneBlockCacheResponse},
 * and {@link NodePruneBlockCacheResponse}.
 */
public class PruneBlockCacheRequestResponseTests extends AbstractWireSerializingTestCase<PruneBlockCacheRequest> {

    @Override
    protected PruneBlockCacheRequest createTestInstance() {
        if (randomBoolean()) {
            return new PruneBlockCacheRequest();
        }
        int count = randomIntBetween(1, 3);
        String[] nodes = new String[count];
        for (int i = 0; i < count; i++) {
            nodes[i] = "node" + i;
        }
        return new PruneBlockCacheRequest(nodes);
    }

    @Override
    protected Writeable.Reader<PruneBlockCacheRequest> instanceReader() {
        return PruneBlockCacheRequest::new;
    }

    @Override
    protected PruneBlockCacheRequest mutateInstance(PruneBlockCacheRequest instance) {
        // toggle between no-nodes and single-node to produce a different instance
        if (instance.nodesIds() == null || instance.nodesIds().length == 0) {
            return new PruneBlockCacheRequest("mutated-node");
        }
        return new PruneBlockCacheRequest();
    }

    // ── NodePruneBlockCacheResponse ───────────────────────────────────────────

    public void testNodeResponseXContent() throws IOException {
        DiscoveryNode node = new DiscoveryNode(
            "node1",
            "node1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
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
        DiscoveryNode node = new DiscoveryNode(
            "node1",
            "node1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
        List<NodePruneBlockCacheResponse> nodes = List.of(new NodePruneBlockCacheResponse(node, true));
        PruneBlockCacheResponse response = new PruneBlockCacheResponse(new ClusterName("test"), nodes, Collections.emptyList());
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();
        assertTrue(json.contains("\"acknowledged\":true"));
        assertTrue(json.contains("\"cleared\":true"));
    }

    public void testResponseXContentWithFailure() throws IOException {
        DiscoveryNode node = new DiscoveryNode(
            "node1",
            "node1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
        List<NodePruneBlockCacheResponse> nodes = List.of(new NodePruneBlockCacheResponse(node, true));
        List<FailedNodeException> failures = List.of(new FailedNodeException("node2", "simulated", new RuntimeException()));
        PruneBlockCacheResponse response = new PruneBlockCacheResponse(new ClusterName("test"), nodes, failures);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();
        assertTrue(json.contains("\"failed_nodes\":1"));
    }
}
