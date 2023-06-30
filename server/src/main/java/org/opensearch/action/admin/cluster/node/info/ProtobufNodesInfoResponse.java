/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

/*
* Licensed to Elasticsearch under one or more contributor
* license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright
* ownership. Elasticsearch licenses this file to you under
* the Apache License, Version 2.0 (the "License"); you may
* not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.action.admin.cluster.node.info;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.action.ProtobufFailedNodeException;
import org.opensearch.action.support.nodes.ProtobufBaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent.Params;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.http.ProtobufHttpInfo;
import org.opensearch.ingest.ProtobufIngestInfo;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.ProtobufOsInfo;
import org.opensearch.monitor.process.ProtobufProcessInfo;
import org.opensearch.search.aggregations.support.ProtobufAggregationInfo;
import org.opensearch.search.pipeline.ProtobufSearchPipelineInfo;
import org.opensearch.threadpool.ProtobufThreadPoolInfo;
import org.opensearch.transport.ProtobufTransportInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Transport response for OpenSearch Node Information
*
* @opensearch.internal
*/
public class ProtobufNodesInfoResponse extends ProtobufBaseNodesResponse<ProtobufNodeInfo> implements ToXContentFragment {

    public ProtobufNodesInfoResponse(CodedInputStream in) throws IOException {
        super(in);
    }

    public ProtobufNodesInfoResponse(ClusterName clusterName, List<ProtobufNodeInfo> nodes, List<ProtobufFailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<ProtobufNodeInfo> readNodesFrom(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        return protobufStreamInput.readList(ProtobufNodeInfo::new);
    }

    @Override
    protected void writeNodesTo(CodedOutputStream out, List<ProtobufNodeInfo> nodes) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        protobufStreamOutput.writeCollection(nodes, (o, v) -> v.writeTo(o));
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

            if (nodeInfo.getInfo(ProtobufOsInfo.class) != null) {
                nodeInfo.getInfo(ProtobufOsInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(ProtobufProcessInfo.class) != null) {
                nodeInfo.getInfo(ProtobufProcessInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(JvmInfo.class) != null) {
                nodeInfo.getInfo(JvmInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(ProtobufThreadPoolInfo.class) != null) {
                nodeInfo.getInfo(ProtobufThreadPoolInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(ProtobufTransportInfo.class) != null) {
                nodeInfo.getInfo(ProtobufTransportInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(ProtobufHttpInfo.class) != null) {
                nodeInfo.getInfo(ProtobufHttpInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(ProtobufPluginsAndModules.class) != null) {
                nodeInfo.getInfo(ProtobufPluginsAndModules.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(ProtobufIngestInfo.class) != null) {
                nodeInfo.getInfo(ProtobufIngestInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(ProtobufAggregationInfo.class) != null) {
                nodeInfo.getInfo(ProtobufAggregationInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(ProtobufSearchPipelineInfo.class) != null) {
                nodeInfo.getInfo(ProtobufSearchPipelineInfo.class).toXContent(builder, params);
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
}
