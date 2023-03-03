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

package org.opensearch.action.admin.cluster.repositories.verify;

import org.opensearch.action.ActionResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.ParseField;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Verify repository response
 *
 * @opensearch.internal
 */
public class VerifyRepositoryResponse extends ActionResponse implements ToXContentObject {

    static final String NODES = "nodes";
    static final String NAME = "name";

    /**
     * Inner Node View
     *
     * @opensearch.internal
     */
    public static class NodeView implements Writeable, ToXContentObject {
        private static final ObjectParser.NamedObjectParser<NodeView, Void> PARSER;
        static {
            ObjectParser<NodeView, Void> internalParser = new ObjectParser<>(NODES, true, null);
            internalParser.declareString(NodeView::setName, new ParseField(NAME));
            PARSER = (p, v, name) -> internalParser.parse(p, new NodeView(name), null);
        }

        final String nodeId;
        String name;

        public NodeView(String nodeId) {
            this.nodeId = nodeId;
        }

        public NodeView(String nodeId, String name) {
            this(nodeId);
            this.name = name;
        }

        public NodeView(StreamInput in) throws IOException {
            this(in.readString(), in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeString(name);
        }

        void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public String getNodeId() {
            return nodeId;
        }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(nodeId);
            {
                builder.field(NAME, name);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            NodeView other = (NodeView) obj;
            return Objects.equals(nodeId, other.nodeId) && Objects.equals(name, other.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, name);
        }
    }

    private List<NodeView> nodes;

    private static final ObjectParser<VerifyRepositoryResponse, Void> PARSER = new ObjectParser<>(
        VerifyRepositoryResponse.class.getName(),
        true,
        VerifyRepositoryResponse::new
    );
    static {
        PARSER.declareNamedObjects(VerifyRepositoryResponse::setNodes, NodeView.PARSER, new ParseField("nodes"));
    }

    public VerifyRepositoryResponse() {}

    public VerifyRepositoryResponse(StreamInput in) throws IOException {
        super(in);
        this.nodes = in.readList(NodeView::new);
    }

    public VerifyRepositoryResponse(DiscoveryNode[] nodes) {
        this.nodes = Arrays.stream(nodes).map(dn -> new NodeView(dn.getId(), dn.getName())).collect(Collectors.toList());
    }

    public VerifyRepositoryResponse(List<NodeView> nodes) {
        this.nodes = nodes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(nodes);
    }

    public List<NodeView> getNodes() {
        return nodes;
    }

    protected void setNodes(List<NodeView> nodes) {
        this.nodes = nodes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject(NODES);
            {
                for (NodeView node : nodes) {
                    node.toXContent(builder, params);
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static VerifyRepositoryResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VerifyRepositoryResponse that = (VerifyRepositoryResponse) o;
        return Objects.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodes);
    }
}
