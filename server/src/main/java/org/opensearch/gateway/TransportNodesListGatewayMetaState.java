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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.gateway;

import org.opensearch.action.ActionType;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Lists gateway meta state
 *
 * @opensearch.internal
 */
public class TransportNodesListGatewayMetaState extends TransportNodesAction<
    TransportNodesListGatewayMetaState.Request,
    TransportNodesListGatewayMetaState.NodesGatewayMetaState,
    TransportNodesListGatewayMetaState.NodeRequest,
    TransportNodesListGatewayMetaState.NodeGatewayMetaState> {

    public static final String ACTION_NAME = "internal:gateway/local/meta_state";
    public static final ActionType<NodesGatewayMetaState> TYPE = new ActionType<>(ACTION_NAME, NodesGatewayMetaState::new);

    private final GatewayMetaState metaState;

    @Inject
    public TransportNodesListGatewayMetaState(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        GatewayMetaState metaState
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            NodeRequest::new,
            ThreadPool.Names.GENERIC,
            NodeGatewayMetaState.class
        );
        this.metaState = metaState;
    }

    public ActionFuture<NodesGatewayMetaState> list(String[] nodesIds, @Nullable TimeValue timeout) {
        PlainActionFuture<NodesGatewayMetaState> future = PlainActionFuture.newFuture();
        execute(new Request(nodesIds).timeout(timeout), future);
        return future;
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest();
    }

    @Override
    protected NodeGatewayMetaState newNodeResponse(StreamInput in) throws IOException {
        return new NodeGatewayMetaState(in);
    }

    @Override
    protected NodesGatewayMetaState newResponse(Request request, List<NodeGatewayMetaState> responses, List<FailedNodeException> failures) {
        return new NodesGatewayMetaState(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeGatewayMetaState nodeOperation(NodeRequest request) {
        return new NodeGatewayMetaState(clusterService.localNode(), metaState.getMetadata());
    }

    /**
     * The request.
     *
     * @opensearch.internal
     */
    public static class Request extends BaseNodesRequest<Request> {

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        public Request(String... nodesIds) {
            super(nodesIds);
        }
    }

    /**
     * The nodes gateway metastate.
     *
     * @opensearch.internal
     */
    public static class NodesGatewayMetaState extends BaseNodesResponse<NodeGatewayMetaState> {

        public NodesGatewayMetaState(StreamInput in) throws IOException {
            super(in);
        }

        public NodesGatewayMetaState(ClusterName clusterName, List<NodeGatewayMetaState> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeGatewayMetaState> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeGatewayMetaState::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeGatewayMetaState> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    /**
     * The node request.
     *
     * @opensearch.internal
     */
    public static class NodeRequest extends TransportRequest {
        NodeRequest() {}

        NodeRequest(StreamInput in) throws IOException {
            super(in);
        }
    }

    /**
     * The node gateway metastate.
     *
     * @opensearch.internal
     */
    public static class NodeGatewayMetaState extends BaseNodeResponse {

        private Metadata metadata;

        public NodeGatewayMetaState(StreamInput in) throws IOException {
            super(in);
            if (in.readBoolean()) {
                metadata = Metadata.readFrom(in);
            }
        }

        public NodeGatewayMetaState(DiscoveryNode node, Metadata metadata) {
            super(node);
            this.metadata = metadata;
        }

        public Metadata metadata() {
            return metadata;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (metadata == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                metadata.writeTo(out);
            }
        }
    }
}
