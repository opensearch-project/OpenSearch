/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.node.info;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.ProtobufBaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.server.proto.NodesInfoResponseProto;
import org.opensearch.server.proto.NodesInfoProto.NodesInfo;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transport response for OpenSearch Node Information
*
* @opensearch.internal
*/
public class ProtobufNodesInfoResponse extends ProtobufBaseNodesResponse<ProtobufNodeInfo> implements ToXContentFragment {

    private NodesInfoResponseProto.NodesInfoResponse nodesInfoRes;
    private Map<String, NodesInfo> nodesMap = new HashMap<>();

    public ProtobufNodesInfoResponse(ClusterName clusterName, List<ProtobufNodeInfo> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
        List<NodesInfo> nodesInfo = new ArrayList<>();
        for (ProtobufNodeInfo nodeInfo : nodes) {
            nodesInfo.add(nodeInfo.response());
            this.nodesMap.put(nodeInfo.response().getNodeId(), nodeInfo.response());
        }
        this.nodesInfoRes = NodesInfoResponseProto.NodesInfoResponse.newBuilder()
            .setClusterName(clusterName.value())
            .addAllNodesInfo(nodesInfo)
            .build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (ProtobufNodeInfo nodeInfo : getNodes()) {
            builder.startObject(nodeInfo.getNode().getId());

            builder.field("name", nodeInfo.getNode().getName());
            builder.field("transport_address", nodeInfo.getNode().getAddress().toString());
            builder.field("host", nodeInfo.getNode().getHostName());
            builder.field("ip", nodeInfo.getNode().getHostAddress());

            builder.field("version", nodeInfo.getVersion());
            builder.field("build_type", nodeInfo.getBuild().type().displayName());
            builder.field("build_hash", nodeInfo.getBuild().hash());
            if (nodeInfo.getTotalIndexingBuffer() != null) {
                builder.humanReadableField("total_indexing_buffer", "total_indexing_buffer_in_bytes", nodeInfo.getTotalIndexingBuffer());
            }

            builder.startArray("roles");
            for (DiscoveryNodeRole role : nodeInfo.getNode().getRoles()) {
                builder.value(role.roleName());
            }
            builder.endArray();

            if (!nodeInfo.getNode().getAttributes().isEmpty()) {
                builder.startObject("attributes");
                for (Map.Entry<String, String> entry : nodeInfo.getNode().getAttributes().entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
            }

            if (nodeInfo.getSettings() != null) {
                builder.startObject("settings");
                Settings settings = nodeInfo.getSettings();
                settings.toXContent(builder, params);
                builder.endObject();
            }

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

    public NodesInfoResponseProto.NodesInfoResponse response() {
        return nodesInfoRes;
    }

    public Map<String, NodesInfo> nodesMap() {
        return nodesMap;
    }

    public ProtobufNodesInfoResponse(byte[] data) throws IOException {
        super(data);
        this.nodesInfoRes = NodesInfoResponseProto.NodesInfoResponse.parseFrom(data);
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        out.write(this.nodesInfoRes.toByteArray());
    }
}
