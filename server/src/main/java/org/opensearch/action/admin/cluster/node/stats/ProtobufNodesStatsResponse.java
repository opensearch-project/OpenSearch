/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.ProtobufBaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.Strings;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.server.proto.NodesStatsResponseProto;
import org.opensearch.server.proto.NodesStatsProto.NodesStats;
import org.opensearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.io.OutputStream;
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

    private NodesStatsResponseProto.NodesStatsResponse nodesStatsRes;
    private Map<String, NodesStats> nodesMap = new HashMap<>();

    public ProtobufNodesStatsResponse(byte[] data) throws IOException {
        super(data);
        this.nodesStatsRes = NodesStatsResponseProto.NodesStatsResponse.parseFrom(data);
    }

    public ProtobufNodesStatsResponse(ClusterName clusterName, List<ProtobufNodeStats> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
        List<NodesStats> nodesStats = new ArrayList<>();
        for (ProtobufNodeStats nodeStats : nodes) {
            nodesStats.add(nodeStats.response());
            this.nodesMap.put(nodeStats.response().getNodeId(), nodeStats.response());
        }
        this.nodesStatsRes = NodesStatsResponseProto.NodesStatsResponse.newBuilder()
            .setClusterName(clusterName.value())
            .addAllNodesStats(nodesStats)
            .build();
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

    public NodesStatsResponseProto.NodesStatsResponse response() {
        return nodesStatsRes;
    }

    public Map<String, NodesStats> nodesMap() {
        return nodesMap;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        out.write(this.nodesStatsRes.toByteArray());
    }
}
