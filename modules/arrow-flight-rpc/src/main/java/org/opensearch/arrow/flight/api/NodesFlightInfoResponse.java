/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.api;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class NodesFlightInfoResponse extends BaseNodesResponse<NodeFlightInfo> implements ToXContentObject {
    public NodesFlightInfoResponse(StreamInput in) throws IOException {
        super(in);
    }

    public NodesFlightInfoResponse(ClusterName clusterName, List<NodeFlightInfo> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodeFlightInfo> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeFlightInfo::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeFlightInfo> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject();
        builder.startObject("_nodes");
        builder.field("total", getNodes().size());
        builder.field("successful", getNodes().size());
        builder.field("failed", failures().size());
        builder.endObject();

        builder.field("cluster_name", getClusterName().value());

        builder.startObject("nodes");
        for (NodeFlightInfo nodeInfo : getNodes()) {
            builder.field(nodeInfo.getNode().getId());
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
