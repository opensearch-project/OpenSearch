/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.nodes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledStreamTransportAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for streaming transport nodes actions
 * <p>
 * This class provides a framework for executing streaming operations across multiple nodes.
 * Subclasses must implement {@link #nodeStreamOperation} to handle the streaming logic on each node.
 * <p>
 * This class handles both coordinator-level (fan-out) and node-level (streaming) operations.
 * The coordinator-level handler fans out requests to nodes and aggregates responses.
 * The node-level handler performs the actual streaming operation on each node.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class StreamTransportNodesAction<
    NodesRequest extends BaseNodesRequest<NodesRequest>,
    NodesResponse extends BaseNodesResponse,
    NodeRequest extends TransportRequest,
    NodeResponse extends BaseNodeResponse> extends HandledStreamTransportAction<NodesRequest, NodesResponse> {

    private static final Logger logger = LogManager.getLogger(StreamTransportNodesAction.class);

    protected final ThreadPool threadPool;
    protected final ClusterService clusterService;
    protected final String transportNodeAction;
    private final String nodeExecutor;

    protected StreamTransportNodesAction(
        String actionName,
        ThreadPool threadPool,
        ClusterService clusterService,
        StreamTransportService streamTransportService,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<NodesRequest> request,
        Writeable.Reader<NodeRequest> nodeRequest,
        String nodeExecutor
    ) {
        super(actionName, transportService, streamTransportService, actionFilters, request);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.nodeExecutor = nodeExecutor;
        this.transportNodeAction = actionName + "[n]";

        // Register streaming handler for node-level requests
        streamTransportService.registerRequestHandler(transportNodeAction, nodeExecutor, nodeRequest, this::handleNodeRequest);
    }

    @Override
    protected void executeStream(Task task, NodesRequest request, TransportChannel channel) throws IOException {
        // This is the coordinator-level handler that fans out to nodes
        handleCoordinatorRequest(request, channel, task);
    }

    /**
     * Handle coordinator-level streaming request - fans out to nodes and aggregates responses
     */
    private void handleCoordinatorRequest(NodesRequest request, TransportChannel channel, Task task) throws IOException {
        if (request.concreteNodes() == null) {
            resolveRequest(request, clusterService.state());
            assert request.concreteNodes() != null;
        }

        DiscoveryNode[] nodes = request.concreteNodes();
        request.setConcreteNodes(null);

        if (nodes.length == 0) {
            try {
                channel.completeStream();
            } catch (Exception e) {
                channel.sendResponse(e);
            }
            return;
        }

        // Track completion of all node requests
        final AtomicInteger pendingNodes = new AtomicInteger(nodes.length);
        final AtomicBoolean errorSent = new AtomicBoolean(false);

        // Fan out to all nodes and stream back aggregated responses
        for (DiscoveryNode node : nodes) {
            try {
                NodeRequest nodeRequest = newNodeRequest(request);
                streamTransportService.sendRequest(
                    node,
                    transportNodeAction,
                    nodeRequest,
                    TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
                    new StreamTransportResponseHandler<NodeResponse>() {
                        @Override
                        public void handleStreamResponse(StreamTransportResponse<NodeResponse> streamResponse) {
                            try {
                                NodeResponse response;
                                while ((response = streamResponse.nextResponse()) != null) {
                                    // Aggregate into NodesResponse and send back
                                    NodesResponse nodesResponse = newResponse(request, List.of(response), List.of());
                                    channel.sendResponseBatch(nodesResponse);
                                }
                                streamResponse.close();

                                // Check if all nodes have completed
                                if (pendingNodes.decrementAndGet() == 0) {
                                    try {
                                        channel.completeStream();
                                    } catch (Exception e) {
                                        if (errorSent.compareAndSet(false, true)) {
                                            try {
                                                channel.sendResponse(e);
                                            } catch (IOException ioException) {
                                                logger.error("Failed to send error response", ioException);
                                            }
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                streamResponse.cancel("Coordinator error", e);
                                if (errorSent.compareAndSet(false, true)) {
                                    try {
                                        channel.sendResponse(e);
                                    } catch (IOException ioException) {
                                        logger.error("Failed to send error response", ioException);
                                    }
                                }
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            if (errorSent.compareAndSet(false, true)) {
                                try {
                                    channel.sendResponse(exp);
                                } catch (IOException e) {
                                    logger.error("Failed to send error response", e);
                                }
                            }
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                        @Override
                        public NodeResponse read(StreamInput in) throws IOException {
                            return newNodeResponse(in);
                        }
                    }
                );
            } catch (Exception e) {
                if (errorSent.compareAndSet(false, true)) {
                    channel.sendResponse(e);
                }
                return;
            }
        }
    }

    /**
     * Handle streaming request on individual node
     */
    private void handleNodeRequest(NodeRequest request, TransportChannel channel, Task task) throws IOException {
        try {
            nodeStreamOperation(request, channel, task);
        } catch (Exception e) {
            channel.sendResponse(e);
        }
    }

    /**
     * Implement this to handle streaming on each node.
     * <p>
     * Use {@link TransportChannel#sendResponseBatch} to send batched responses
     * and {@link TransportChannel#completeStream} to signal completion.
     *
     * @param request the node request
     * @param channel the transport channel for sending responses
     * @param task    the task
     * @throws IOException if an I/O error occurs
     */
    protected abstract void nodeStreamOperation(NodeRequest request, TransportChannel channel, Task task) throws IOException;

    /**
     * Create node request from nodes request
     */
    protected abstract NodeRequest newNodeRequest(NodesRequest request);

    /**
     * Read node response from stream
     */
    protected abstract NodeResponse newNodeResponse(StreamInput in) throws IOException;

    /**
     * Create aggregated response from node responses
     */
    protected abstract NodesResponse newResponse(NodesRequest request, List<NodeResponse> responses, List<FailedNodeException> failures);

    protected void resolveRequest(NodesRequest request, ClusterState clusterState) {
        assert request.concreteNodes() == null : "request concreteNodes shouldn't be set";
        String[] nodesIds = clusterState.nodes().resolveNodes(request.nodesIds());
        request.setConcreteNodes(Arrays.stream(nodesIds).map(clusterState.nodes()::get).toArray(DiscoveryNode[]::new));
    }
}
