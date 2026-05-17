/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.blockcache;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Per-node response for a block cache prune operation.
 *
 * @opensearch.internal
 */
public class NodePruneBlockCacheResponse extends BaseNodeResponse implements ToXContentFragment {

    private final boolean cleared;

    public NodePruneBlockCacheResponse(StreamInput in) throws IOException {
        super(in);
        this.cleared = in.readBoolean();
    }

    public NodePruneBlockCacheResponse(DiscoveryNode node, boolean cleared) {
        super(node);
        this.cleared = cleared;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(cleared);
    }

    public boolean isCleared() {
        return cleared;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("name", getNode().getName());
        builder.field("transport_address", getNode().getAddress().toString());
        builder.field("host", getNode().getHostName());
        builder.field("ip", getNode().getHostAddress());
        builder.field("cleared", cleared);
        return builder;
    }
}
