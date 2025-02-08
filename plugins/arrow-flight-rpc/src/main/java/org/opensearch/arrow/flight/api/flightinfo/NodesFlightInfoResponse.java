/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.api.flightinfo;

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
 * Represents the response for nodes flight information.
 */
public class NodesFlightInfoResponse extends BaseNodesResponse<NodeFlightInfo> implements ToXContentObject {
    /**
     * Constructs a new NodesFlightInfoResponse instance.
     *
     * @param in The stream input to read from.
     * @throws IOException If an I/O error occurs.
     */
    public NodesFlightInfoResponse(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Constructs a new NodesFlightInfoResponse instance.
     *
     * @param clusterName The cluster name.
     * @param nodes       The list of node flight information.
     * @param failures    The list of failed node exceptions.
     */
    public NodesFlightInfoResponse(ClusterName clusterName, List<NodeFlightInfo> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    /**
     * Reads the nodes from the given stream input.
     *
     * @param in The stream input to read from.
     * @return The list of node flight information.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    protected List<NodeFlightInfo> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeFlightInfo::new);
    }

    /**
     * Writes the nodes to the given stream output.
     *
     * @param out   The stream output to write to.
     * @param nodes The list of node flight information.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeFlightInfo> nodes) throws IOException {
        out.writeList(nodes);
    }

    /**
     * Converts the nodes flight information response to XContent.
     * @param builder The XContent builder.
     * @param params  The parameters for the XContent conversion.
     * @return The XContent builder.
     * @throws IOException  If an I/O error occurs.
     */
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
