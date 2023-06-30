/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.node.stats;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.action.ProtobufFailedNodeException;
import org.opensearch.action.support.nodes.ProtobufBaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;

/**
 * Transport response for obtaining OpenSearch Node Stats
*
* @opensearch.internal
*/
public class ProtobufNodesStatsResponse extends ProtobufBaseNodesResponse<ProtobufNodeStats> implements ToXContentFragment {

    public ProtobufNodesStatsResponse(CodedInputStream in) throws IOException {
        super(in);
    }

    public ProtobufNodesStatsResponse(ClusterName clusterName, List<ProtobufNodeStats> nodes, List<ProtobufFailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<ProtobufNodeStats> readNodesFrom(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        return protobufStreamInput.readList(ProtobufNodeStats::new);
    }

    @Override
    protected void writeNodesTo(CodedOutputStream out, List<ProtobufNodeStats> nodes) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        protobufStreamOutput.writeCollection(nodes, (o, v) -> v.writeTo(o));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (ProtobufNodeStats nodeStats : getNodes()) {
            builder.startObject(nodeStats.getNode().getId());
            builder.field("timestamp", nodeStats.getTimestamp());
            nodeStats.toXContent(builder, params);

            builder.endObject();
        }
        builder.endObject();

        return builder;
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }
}
