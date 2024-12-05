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

package org.opensearch.action.support.nodes;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.NodeShouldNotConnectException;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Base action class for transport nodes
 *
 * @opensearch.internal
 */
public abstract class TransportNodesAction<
    NodesRequest extends BaseNodesRequest<NodesRequest>,
    NodesResponse extends BaseNodesResponse,
    NodeRequest extends TransportRequest,
    NodeResponse extends BaseNodeResponse> extends HandledTransportAction<NodesRequest, NodesResponse> {

    protected final ThreadPool threadPool;
    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final Class<NodeResponse> nodeResponseClass;
    protected final String transportNodeAction;

    private final String finalExecutor;

    /**
     * @param actionName        action name
     * @param threadPool        thread-pool
     * @param clusterService    cluster service
     * @param transportService  transport service
     * @param actionFilters     action filters
     * @param request           node request writer
     * @param nodeRequest       node request reader
     * @param nodeExecutor      executor to execute node action on
     * @param finalExecutor     executor to execute final collection of all responses on
     * @param nodeResponseClass class of the node responses
     */
    protected TransportNodesAction(
        String actionName,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<NodesRequest> request,
        Writeable.Reader<NodeRequest> nodeRequest,
        String nodeExecutor,
        String finalExecutor,
        Class<NodeResponse> nodeResponseClass
    ) {
        super(actionName, transportService, actionFilters, request);
        this.threadPool = threadPool;
        this.clusterService = Objects.requireNonNull(clusterService);
        this.transportService = Objects.requireNonNull(transportService);
        this.nodeResponseClass = Objects.requireNonNull(nodeResponseClass);

        this.transportNodeAction = actionName + "[n]";
        this.finalExecutor = finalExecutor;
        transportService.registerRequestHandler(transportNodeAction, nodeExecutor, nodeRequest, new NodeTransportHandler());
    }

    /**
     * Same as {@link #TransportNodesAction(String, ThreadPool, ClusterService, TransportService, ActionFilters, Writeable.Reader,
     * Writeable.Reader, String, String, Class)} but executes final response collection on the transport thread except for when the final
     * node response is received from the local node, in which case {@code nodeExecutor} is used.
     * This constructor should only be used for actions for which the creation of the final response is fast enough to be safely executed
     * on a transport thread.
     */
    protected TransportNodesAction(
        String actionName,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<NodesRequest> request,
        Writeable.Reader<NodeRequest> nodeRequest,
        String nodeExecutor,
        Class<NodeResponse> nodeResponseClass
    ) {
        this(
            actionName,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            request,
            nodeRequest,
            nodeExecutor,
            ThreadPool.Names.SAME,
            nodeResponseClass
        );
    }

    @Override
    protected void doExecute(Task task, NodesRequest request, ActionListener<NodesResponse> listener) {
        new AsyncAction(task, request, listener).start();
    }

    /**
     * Map the responses into {@code nodeResponseClass} responses and {@link FailedNodeException}s.
     *
     * @param request The associated request.
     * @param nodesResponses All node-level responses
     * @return Never {@code null}.
     * @throws NullPointerException if {@code nodesResponses} is {@code null}
     * @see #newResponse(BaseNodesRequest, List, List)
     */
    protected NodesResponse newResponse(NodesRequest request, AtomicReferenceArray<?> nodesResponses) {
        final List<NodeResponse> responses = new ArrayList<>();
        final List<FailedNodeException> failures = new ArrayList<>();

        for (int i = 0; i < nodesResponses.length(); ++i) {
            Object response = nodesResponses.get(i);

            if (response instanceof FailedNodeException) {
                failures.add((FailedNodeException) response);
            } else {
                responses.add(nodeResponseClass.cast(response));
            }
        }

        return newResponse(request, responses, failures);
    }

    /**
     * Create a new {@link NodesResponse} (multi-node response).
     *
     * @param request The associated request.
     * @param responses All successful node-level responses.
     * @param failures All node-level failures.
     * @return Never {@code null}.
     * @throws NullPointerException if any parameter is {@code null}.
     */
    protected abstract NodesResponse newResponse(NodesRequest request, List<NodeResponse> responses, List<FailedNodeException> failures);

    protected abstract NodeRequest newNodeRequest(NodesRequest request);

    protected abstract NodeResponse newNodeResponse(StreamInput in) throws IOException;

    protected abstract NodeResponse nodeOperation(NodeRequest request);

    protected NodeResponse nodeOperation(NodeRequest request, Task task) {
        return nodeOperation(request);
    }

    /**
     * resolve node ids to concrete nodes of the incoming request
     **/
    protected void resolveRequest(NodesRequest request, ClusterState clusterState) {
        assert request.concreteNodes() == null : "request concreteNodes shouldn't be set";
        String[] nodesIds = clusterState.nodes().resolveNodes(request.nodesIds());
        request.setConcreteNodes(Arrays.stream(nodesIds).map(clusterState.nodes()::get).toArray(DiscoveryNode[]::new));
    }

    /**
     * Get a backwards compatible transport action name
     */
    protected String getTransportNodeAction(DiscoveryNode node) {
        return transportNodeAction;
    }

    /**
     * Asynchronous action
     *
     * @opensearch.internal
     */
    class AsyncAction {

        private final NodesRequest request;
        private final ActionListener<NodesResponse> listener;
        private final AtomicReferenceArray<Object> responses;
        private final DiscoveryNode[] concreteNodes;
        private final AtomicInteger counter = new AtomicInteger();
        private final Task task;

        AsyncAction(Task task, NodesRequest request, ActionListener<NodesResponse> listener) {
            this.task = task;
            this.request = request;
            this.listener = listener;
            if (request.concreteNodes() == null) {
                resolveRequest(request, clusterService.state());
                assert request.concreteNodes() != null;
            }
            this.responses = new AtomicReferenceArray<>(request.concreteNodes().length);
            this.concreteNodes = request.concreteNodes();
            if (request.getIncludeDiscoveryNodes() == false) {
                // As we transfer the ownership of discovery nodes to route the request to into the AsyncAction class,
                // we remove the list of DiscoveryNodes from the request. This reduces the payload of the request and improves
                // the number of concrete nodes in the memory.
                request.setConcreteNodes(null);
            }
        }

        void start() {
            if (this.concreteNodes.length == 0) {
                // nothing to notify
                threadPool.generic().execute(() -> listener.onResponse(newResponse(request, responses)));
                return;
            }
            TransportRequestOptions.Builder builder = TransportRequestOptions.builder();
            if (request.timeout() != null) {
                builder.withTimeout(request.timeout());
            }
            for (int i = 0; i < this.concreteNodes.length; i++) {
                final int idx = i;
                final DiscoveryNode node = this.concreteNodes[i];
                final String nodeId = node.getId();
                try {
                    TransportRequest nodeRequest = newNodeRequest(request);
                    if (task != null) {
                        nodeRequest.setParentTask(clusterService.localNode().getId(), task.getId());
                    }
                    transportService.sendRequest(
                        node,
                        getTransportNodeAction(node),
                        nodeRequest,
                        builder.build(),
                        new TransportResponseHandler<NodeResponse>() {
                            @Override
                            public NodeResponse read(StreamInput in) throws IOException {
                                return newNodeResponse(in);
                            }

                            @Override
                            public void handleResponse(NodeResponse response) {
                                onOperation(idx, response);
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                onFailure(idx, node.getId(), exp);
                            }

                            @Override
                            public String executor() {
                                return ThreadPool.Names.SAME;
                            }
                        }
                    );
                } catch (Exception e) {
                    onFailure(idx, nodeId, e);
                }
            }
        }

        private void onOperation(int idx, NodeResponse nodeResponse) {
            responses.set(idx, nodeResponse);
            if (counter.incrementAndGet() == responses.length()) {
                finishHim();
            }
        }

        private void onFailure(int idx, String nodeId, Throwable t) {
            if (logger.isDebugEnabled() && !(t instanceof NodeShouldNotConnectException)) {
                logger.debug(new ParameterizedMessage("failed to execute on node [{}]", nodeId), t);
            }
            responses.set(idx, new FailedNodeException(nodeId, "Failed node [" + nodeId + "]", t));
            if (counter.incrementAndGet() == responses.length()) {
                finishHim();
            }
        }

        private void finishHim() {
            threadPool.executor(finalExecutor).execute(ActionRunnable.supply(listener, () -> newResponse(request, responses)));
        }
    }

    /**
     * A node transport handler
     *
     * @opensearch.internal
     */
    class NodeTransportHandler implements TransportRequestHandler<NodeRequest> {

        @Override
        public void messageReceived(NodeRequest request, TransportChannel channel, Task task) throws Exception {
            channel.sendResponse(nodeOperation(request, task));
        }
    }

}
