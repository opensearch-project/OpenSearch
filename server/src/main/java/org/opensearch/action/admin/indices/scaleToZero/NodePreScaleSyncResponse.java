/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scaleToZero;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;
import java.util.List;

public class NodePreScaleSyncResponse extends TransportResponse {
    private final DiscoveryNode node;
    private final List<ShardPreScaleSyncResponse> shardResponses;

    public NodePreScaleSyncResponse(DiscoveryNode node, List<ShardPreScaleSyncResponse> shardResponses) {
        this.node = node;
        this.shardResponses = shardResponses;
    }

    public NodePreScaleSyncResponse(StreamInput in) throws IOException {
        node = new DiscoveryNode(in);
        shardResponses = in.readList(ShardPreScaleSyncResponse::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        node.writeTo(out);
        out.writeList(shardResponses);
    }

    public DiscoveryNode getNode() {
        return node;
    }

    public List<ShardPreScaleSyncResponse> getShardResponses() {
        return shardResponses;
    }
}
