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
import org.opensearch.server.proto.NodesStatsResponseProto;
import org.opensearch.server.proto.NodesStatsProto.NodesStats;
import org.opensearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transport response for obtaining OpenSearch Node Stats
*
* @opensearch.internal
*/
public class ProtobufNodesStatsResponse extends ProtobufBaseNodesResponse<ProtobufNodeStats> implements ToXContentFragment {

    private NodesStatsResponseProto.NodesStatsRes nodesStatsRes;
    private Map<String, NodesStats> nodesMap = new HashMap<>();

    public ProtobufNodesStatsResponse(CodedInputStream in) throws IOException {
        super(in);
    }

    public ProtobufNodesStatsResponse(ClusterName clusterName, List<ProtobufNodeStats> nodes, List<ProtobufFailedNodeException> failures) {
        super(clusterName, nodes, failures);
        System.out.println("ProtobufNodesStatsResponse constructor");
        System.out.println("clusterName: " + clusterName);
        System.out.println("nodes: " + nodes);
        List<NodesStats> nodesStats = new ArrayList<>();
        for (ProtobufNodeStats nodeStats : nodes) {
            nodesStats.add(nodeStats.response());
            this.nodesMap.put(nodeStats.response().getNodeId(), nodeStats.response());
        }
        this.nodesStatsRes = NodesStatsResponseProto.NodesStatsRes.newBuilder()
            .setClusterName(clusterName.value())
            .addAllNodesStats(nodesStats)
            .build();
        System.out.println("Proto nodes stats: " + this.nodesStatsRes);
        System.out.println("Nodes info map: " + this.nodesMap);
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

    public NodesStatsResponseProto.NodesStatsRes response() {
        return nodesStatsRes;
    }

    public Map<String, NodesStats> nodesMap() {
        return nodesMap;
    }
}
