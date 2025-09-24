/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.action;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Response containing DataFusion information from multiple nodes
 */
public class NodesDataFusionInfoResponse extends BaseNodesResponse<NodeDataFusionInfo> implements ToXContentObject {

    /**
     * Constructor for NodesDataFusionInfoResponse.
     * @param clusterName The cluster name.
     * @param nodes The list of node DataFusion info.
     * @param failures The list of failed node exceptions.
     */
    public NodesDataFusionInfoResponse(
        ClusterName clusterName,
        List<NodeDataFusionInfo> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodeDataFusionInfo> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeDataFusionInfo::new);
    }

    /**
     * Constructor for NodesDataFusionInfoResponse from stream input.
     * @param in The stream input.
     * @throws IOException If an I/O error occurs.
     */
    public NodesDataFusionInfoResponse(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Writes the node response to stream output.
     * @param out The stream output.
     * @param nodes The list of nodes to write.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeDataFusionInfo> nodes) throws IOException {
        out.writeList(nodes);
    }

    /**
     * Converts the response to XContent.
     * @param builder The XContent builder.
     * @param params The parameters.
     * @return The XContent builder.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("nodes");
        for (NodeDataFusionInfo nodeInfo : getNodes()) {
            builder.field(nodeInfo.getNode().getId());
//            builder.field("name", nodeInfo.getNode().getName());
//            builder.field("transport_address", nodeInfo.getNode().getAddress().toString());
            nodeInfo.toXContent(builder, params);
        }
        builder.endObject();

        if (!failures().isEmpty()) {
            builder.startArray("failures");
            for (FailedNodeException failure : failures()) {
                builder.startObject();
                builder.field("node_id", failure.nodeId());
                builder.field("reason", failure.getMessage());
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }
}
