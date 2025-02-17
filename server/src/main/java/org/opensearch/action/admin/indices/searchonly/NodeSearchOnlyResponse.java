/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.searchonly;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;
import java.util.List;

public class NodeSearchOnlyResponse extends TransportResponse {
    private final DiscoveryNode node;
    private final List<ShardSearchOnlyResponse> shardResponses;

    public NodeSearchOnlyResponse(DiscoveryNode node, List<ShardSearchOnlyResponse> shardResponses) {
        this.node = node;
        this.shardResponses = shardResponses;
    }

    public NodeSearchOnlyResponse(StreamInput in) throws IOException {
        node = new DiscoveryNode(in);
        shardResponses = in.readList(ShardSearchOnlyResponse::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        node.writeTo(out);
        out.writeList(shardResponses);
    }

    public DiscoveryNode getNode() {
        return node;
    }

    public List<ShardSearchOnlyResponse> getShardResponses() {
        return shardResponses;
    }
}
