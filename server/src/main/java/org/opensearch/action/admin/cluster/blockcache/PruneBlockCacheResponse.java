/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.blockcache;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Cluster-wide response for a block cache prune operation.
 *
 * @opensearch.internal
 */
public class PruneBlockCacheResponse extends BaseNodesResponse<NodePruneBlockCacheResponse> implements ToXContentObject {

    public PruneBlockCacheResponse(StreamInput in) throws IOException {
        super(in);
    }

    public PruneBlockCacheResponse(ClusterName clusterName, List<NodePruneBlockCacheResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodePruneBlockCacheResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodePruneBlockCacheResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodePruneBlockCacheResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("acknowledged", true);
        builder.startObject("summary");
        builder.field("total_nodes_targeted", getNodes().size() + failures().size());
        builder.field("successful_nodes", getNodes().size());
        builder.field("failed_nodes", failures().size());
        builder.endObject();
        builder.startObject("nodes");
        for (NodePruneBlockCacheResponse node : getNodes()) {
            if (node != null && node.getNode() != null) {
                builder.startObject(node.getNode().getId());
                node.toXContent(builder, params);
                builder.endObject();
            }
        }
        builder.endObject();
        if (failures().isEmpty() == false) {
            builder.startArray("failures");
            for (FailedNodeException failure : failures()) {
                builder.startObject();
                builder.field("node_id", failure.nodeId());
                builder.field("reason", failure.getDetailedMessage());
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }
}
