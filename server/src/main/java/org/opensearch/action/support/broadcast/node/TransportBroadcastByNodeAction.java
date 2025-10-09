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

package org.opensearch.action.support.broadcast.node;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.TransportActions;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.transport.TransportResponse;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Abstraction for transporting aggregated shard-level operations in a single request (NodeRequest) per-node
 * and executing the shard-level operations serially on the receiving node. Each shard-level operation can produce a
 * result (ShardOperationResult), these per-node shard-level results are aggregated into a single result
 * (BroadcastByNodeResponse) to the coordinating node. These per-node results are aggregated into a single result (Result)
 * to the client.
 *
 * @param <Request>              the underlying client request
 * @param <Response>             the response to the client request
 * @param <ShardOperationResult> per-shard operation results
 *
 * @opensearch.internal
 */
public abstract class TransportBroadcastByNodeAction<
    Request extends BroadcastRequest<Request>,
    Response extends BroadcastResponse,
    ShardOperationResult extends Writeable> extends HandledTransportAction<Request, Response> {

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    final String transportNodeBroadcastAction;

    public TransportBroadcastByNodeAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Writeable.Reader<Request> request,
        String executor
    ) {
        this(actionName, clusterService, transportService, actionFilters, indexNameExpressionResolver, request, executor, true);
    }

    public TransportBroadcastByNodeAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Writeable.Reader<Request> request,
        String executor,
        boolean canTripCircuitBreaker
    ) {
        super(actionName, canTripCircuitBreaker, transportService, actionFilters, request);

        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;

        transportNodeBroadcastAction = actionName + "[n]";

        transportService.registerRequestHandler(
            transportNodeBroadcastAction,
            executor,
            false,
            canTripCircuitBreaker,
            NodeRequest::new,
            new BroadcastByNodeTransportRequestHandler()
        );
    }

    private Response newResponse(
        Request request,
        AtomicReferenceArray responses,
        List<NoShardAvailableActionException> unavailableShardExceptions,
        Map<String, List<ShardRouting>> nodes,
        ClusterState clusterState
    ) {
        int totalShards = 0;
        int successfulShards = 0;
        List<ShardOperationResult> broadcastByNodeResponses = new ArrayList<>();
        List<DefaultShardOperationFailedException> exceptions = new ArrayList<>();
        for (int i = 0; i < responses.length(); i++) {
            if (responses.get(i) instanceof FailedNodeException) {
                FailedNodeException exception = (FailedNodeException) responses.get(i);
                totalShards += nodes.get(exception.nodeId()).size();
                for (ShardRouting shard : nodes.get(exception.nodeId())) {
                    exceptions.add(new DefaultShardOperationFailedException(shard.getIndexName(), shard.getId(), exception));
                }
            } else {
                NodeResponse response = (NodeResponse) responses.get(i);
                broadcastByNodeResponses.addAll(response.results);
                totalShards += response.getTotalShards();
                successfulShards += response.getSuccessfulShards();
                for (BroadcastShardOperationFailedException throwable : response.getExceptions()) {
                    if (!TransportActions.isShardNotAvailableException(throwable)) {
                        exceptions.add(
                            new DefaultShardOperationFailedException(
                                throwable.getShardId().getIndexName(),
                                throwable.getShardId().getId(),
                                throwable
                            )
                        );
                    }
                }
            }
        }
        totalShards += unavailableShardExceptions.size();
        int failedShards = exceptions.size();
        return newResponse(request, totalShards, successfulShards, failedShards, broadcastByNodeResponses, exceptions, clusterState);
    }

    /**
     * Deserialize a shard-level result from an input stream
     *
     * @param in input stream
     * @return a deserialized shard-level result
     */
    protected abstract ShardOperationResult readShardResult(StreamInput in) throws IOException;

    /**
     * Creates a new response to the underlying request.
     *
     * @param request          the underlying request
     * @param totalShards      the total number of shards considered for execution of the operation
     * @param successfulShards the total number of shards for which execution of the operation was successful
     * @param failedShards     the total number of shards for which execution of the operation failed
     * @param results          the per-node aggregated shard-level results
     * @param shardFailures    the exceptions corresponding to shard operation failures
     * @param clusterState     the cluster state
     * @return the response
     */
    protected abstract Response newResponse(
        Request request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<ShardOperationResult> results,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    );

    /**
     * Deserialize a request from an input stream
     *
     * @param in input stream
     * @return a de-serialized request
     */
    protected abstract Request readRequestFrom(StreamInput in) throws IOException;

    /**
     * Executes the shard-level operation. This method is called once per shard serially on the receiving node.
     *
     * @param request      the node-level request
     * @param shardRouting the shard on which to execute the operation
     * @return the result of the shard-level operation for the shard
     */
    protected abstract ShardOperationResult shardOperation(Request request, ShardRouting shardRouting) throws IOException;

    /**
     * Determines the shards on which this operation will be executed on. The operation is executed once per shard.
     *
     * @param clusterState    the cluster state
     * @param request         the underlying request
     * @param concreteIndices the concrete indices on which to execute the operation
     * @return the shards on which to execute the operation
     */
    protected abstract ShardsIterator shards(ClusterState clusterState, Request request, String[] concreteIndices);

    /**
     * Executes a node-level operation. This method is called one time per node, after all shard-level operations have completed.
     * @param results List of results from the completed shard-level operations.
     * @param accumulatedExceptions List of any exceptions thrown by the shard-level operations.
     */
    protected void nodeOperation(List<ShardOperationResult> results, List<BroadcastShardOperationFailedException> accumulatedExceptions) {}

    /**
     * Executes a global block check before polling the cluster state.
     *
     * @param state   the cluster state
     * @param request the underlying request
     * @return a non-null exception if the operation is blocked
     */
    protected abstract ClusterBlockException checkGlobalBlock(ClusterState state, Request request);

    /**
     * Executes a global request-level check before polling the cluster state.
     *
     * @param state           the cluster state
     * @param request         the underlying request
     * @param concreteIndices the concrete indices on which to execute the operation
     * @return a non-null exception if the operation if blocked
     */
    protected abstract ClusterBlockException checkRequestBlock(ClusterState state, Request request, String[] concreteIndices);

    /**
     * Resolves a list of concrete index names. Override this if index names should be resolved differently than normal.
     *
     * @param clusterState the cluster state
     * @param request the underlying request
     * @return a list of concrete index names that this action should operate on
     */
    protected String[] resolveConcreteIndexNames(ClusterState clusterState, Request request) {
        return indexNameExpressionResolver.concreteIndexNames(clusterState, request);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        new AsyncAction(task, request, listener).start();
    }

    /**
     * Asynchronous action
     *
     * @opensearch.internal
     */
    protected class AsyncAction {
        private final Task task;
        private final Request request;
        private final ActionListener<Response> listener;
        private final ClusterState clusterState;
        private final DiscoveryNodes nodes;
        private final Map<String, List<ShardRouting>> nodeIds;
        private final AtomicReferenceArray<Object> responses;
        private final AtomicInteger counter = new AtomicInteger();
        private List<NoShardAvailableActionException> unavailableShardExceptions = new ArrayList<>();

        protected AsyncAction(Task task, Request request, ActionListener<Response> listener) {
            this.task = task;
            this.request = request;
            this.listener = listener;

            clusterState = clusterService.state();
            nodes = clusterState.nodes();

            ClusterBlockException globalBlockException = checkGlobalBlock(clusterState, request);
            if (globalBlockException != null) {
                throw globalBlockException;
            }

            String[] concreteIndices = resolveConcreteIndexNames(clusterState, request);
            ClusterBlockException requestBlockException = checkRequestBlock(clusterState, request, concreteIndices);
            if (requestBlockException != null) {
                throw requestBlockException;
            }

            if (logger.isTraceEnabled()) {
                logger.trace("resolving shards for [{}] based on cluster state version [{}]", actionName, clusterState.version());
            }
            ShardsIterator shardIt = shards(clusterState, request, concreteIndices);
            nodeIds = new HashMap<>();

            for (ShardRouting shard : shardIt) {
                // send a request to the shard only if it is assigned to a node that is in the local node's cluster state
                // a scenario in which a shard can be assigned but to a node that is not in the local node's cluster state
                // is when the shard is assigned to the cluster-manager node, the local node has detected the cluster-manager as failed
                // and a new cluster-manager has not yet been elected; in this situation the local node will have removed the
                // cluster-manager node from the local cluster state, but the shards assigned to the cluster-manager will still be in the
                // routing table as such
                if (shard.assignedToNode() && nodes.get(shard.currentNodeId()) != null) {
                    String nodeId = shard.currentNodeId();
                    if (!nodeIds.containsKey(nodeId)) {
                        nodeIds.put(nodeId, new ArrayList<>());
                    }
                    nodeIds.get(nodeId).add(shard);
                } else {
                    unavailableShardExceptions.add(
                        new NoShardAvailableActionException(
                            shard.shardId(),
                            " no shards available for shard " + shard.toString() + " while executing " + actionName
                        )
                    );
                }
            }

            responses = new AtomicReferenceArray<>(nodeIds.size());
        }

        public void start() {
            if (nodeIds.size() == 0) {
                try {
                    onCompletion();
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            } else {
                int nodeIndex = -1;
                for (Map.Entry<String, List<ShardRouting>> entry : nodeIds.entrySet()) {
                    nodeIndex++;
                    DiscoveryNode node = nodes.get(entry.getKey());
                    sendNodeRequest(node, entry.getValue(), nodeIndex);
                }
            }
        }

        private void sendNodeRequest(final DiscoveryNode node, List<ShardRouting> shards, final int nodeIndex) {
            try {
                NodeRequest nodeRequest = new NodeRequest(node.getId(), request, shards);
                if (task != null) {
                    nodeRequest.setParentTask(clusterService.localNode().getId(), task.getId());
                }
                TransportRequestOptions transportRequestOptions = TransportRequestOptions.EMPTY;
                if (request != null && request.timeout() != null) {
                    transportRequestOptions = TransportRequestOptions.builder().withTimeout(request.timeout()).build();
                }
                transportService.sendRequest(
                    node,
                    transportNodeBroadcastAction,
                    nodeRequest,
                    transportRequestOptions,
                    new TransportResponseHandler<NodeResponse>() {
                        @Override
                        public NodeResponse read(StreamInput in) throws IOException {
                            return new NodeResponse(in);
                        }

                        @Override
                        public void handleResponse(NodeResponse response) {
                            onNodeResponse(node, nodeIndex, response);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            onNodeFailure(node, nodeIndex, exp);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }
                    }
                );
            } catch (Exception e) {
                onNodeFailure(node, nodeIndex, e);
            }
        }

        protected void onNodeResponse(DiscoveryNode node, int nodeIndex, NodeResponse response) {
            if (logger.isTraceEnabled()) {
                logger.trace("received response for [{}] from node [{}]", actionName, node.getId());
            }

            // this is defensive to protect against the possibility of double invocation
            // the current implementation of TransportService#sendRequest guards against this
            // but concurrency is hard, safety is important, and the small performance loss here does not matter
            if (responses.compareAndSet(nodeIndex, null, response)) {
                if (counter.incrementAndGet() == responses.length()) {
                    onCompletion();
                }
            }
        }

        protected void onNodeFailure(DiscoveryNode node, int nodeIndex, Throwable t) {
            String nodeId = node.getId();
            if (logger.isDebugEnabled() && !(t instanceof NodeShouldNotConnectException)) {
                logger.debug(new ParameterizedMessage("failed to execute [{}] on node [{}]", actionName, nodeId), t);
            }

            // this is defensive to protect against the possibility of double invocation
            // the current implementation of TransportService#sendRequest guards against this
            // but concurrency is hard, safety is important, and the small performance loss here does not matter
            if (responses.compareAndSet(nodeIndex, null, new FailedNodeException(nodeId, "Failed node [" + nodeId + "]", t))) {
                if (counter.incrementAndGet() == responses.length()) {
                    onCompletion();
                }
            }
        }

        protected void onCompletion() {
            Response response = null;
            try {
                response = newResponse(request, responses, unavailableShardExceptions, nodeIds, clusterState);
            } catch (Exception e) {
                logger.debug("failed to combine responses from nodes", e);
                listener.onFailure(e);
            }
            if (response != null) {
                try {
                    listener.onResponse(response);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        }
    }

    /**
     * Broadcast by a node's transport request handler
     *
     * @opensearch.internal
     */
    class BroadcastByNodeTransportRequestHandler implements TransportRequestHandler<NodeRequest> {
        @Override
        public void messageReceived(final NodeRequest request, TransportChannel channel, Task task) throws Exception {
            List<ShardRouting> shards = request.getShards();
            final int totalShards = shards.size();
            if (logger.isTraceEnabled()) {
                logger.trace("[{}] executing operation on [{}] shards", actionName, totalShards);
            }
            final Object[] shardResultOrExceptions = new Object[totalShards];

            int shardIndex = -1;
            for (final ShardRouting shardRouting : shards) {
                shardIndex++;
                onShardOperation(request, shardResultOrExceptions, shardIndex, shardRouting);
            }

            List<BroadcastShardOperationFailedException> accumulatedExceptions = new ArrayList<>();
            List<ShardOperationResult> results = new ArrayList<>();
            for (int i = 0; i < totalShards; i++) {
                if (shardResultOrExceptions[i] instanceof BroadcastShardOperationFailedException) {
                    accumulatedExceptions.add((BroadcastShardOperationFailedException) shardResultOrExceptions[i]);
                } else {
                    results.add((ShardOperationResult) shardResultOrExceptions[i]);
                }
            }

            nodeOperation(results, accumulatedExceptions);

            channel.sendResponse(new NodeResponse(request.getNodeId(), totalShards, results, accumulatedExceptions));
        }

        private void onShardOperation(
            final NodeRequest request,
            final Object[] shardResults,
            final int shardIndex,
            final ShardRouting shardRouting
        ) {
            try {
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}]  executing operation for shard [{}]", actionName, shardRouting.shortSummary());
                }
                ShardOperationResult result = shardOperation(request.indicesLevelRequest, shardRouting);
                shardResults[shardIndex] = result;
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}]  completed operation for shard [{}]", actionName, shardRouting.shortSummary());
                }
            } catch (Exception e) {
                BroadcastShardOperationFailedException failure = new BroadcastShardOperationFailedException(
                    shardRouting.shardId(),
                    "operation " + actionName + " failed",
                    e
                );
                failure.setShard(shardRouting.shardId());
                shardResults[shardIndex] = failure;
                if (TransportActions.isShardNotAvailableException(e)) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            new ParameterizedMessage(
                                "[{}] failed to execute operation for shard [{}]",
                                actionName,
                                shardRouting.shortSummary()
                            ),
                            e
                        );
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            new ParameterizedMessage(
                                "[{}] failed to execute operation for shard [{}]",
                                actionName,
                                shardRouting.shortSummary()
                            ),
                            e
                        );
                    }
                }
            }
        }
    }

    /**
     * This method reads ShardRouting from input stream
     */
    public List<ShardRouting> getShardRoutingsFromInputStream(StreamInput in) throws IOException {
        return in.readList(ShardRouting::new);
    }

    /**
     * A node request
     *
     * @opensearch.internal
     */
    public class NodeRequest extends TransportRequest implements IndicesRequest {
        private String nodeId;

        private List<ShardRouting> shards;

        protected Request indicesLevelRequest;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            indicesLevelRequest = readRequestFrom(in);
            shards = getShardRoutingsFromInputStream(in);
            nodeId = in.readString();
        }

        public NodeRequest(String nodeId, Request request, List<ShardRouting> shards) {
            this.indicesLevelRequest = request;
            this.shards = shards;
            this.nodeId = nodeId;
        }

        public List<ShardRouting> getShards() {
            return shards;
        }

        public String getNodeId() {
            return nodeId;
        }

        @Override
        public String[] indices() {
            return indicesLevelRequest.indices();
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesLevelRequest.indicesOptions();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            indicesLevelRequest.writeTo(out);
            out.writeList(shards);
            out.writeString(nodeId);
        }
    }

    /**
     * A node response
     *
     * @opensearch.internal
     */
    class NodeResponse extends TransportResponse {
        protected String nodeId;
        protected int totalShards;
        protected List<BroadcastShardOperationFailedException> exceptions;
        protected List<ShardOperationResult> results;

        NodeResponse(StreamInput in) throws IOException {
            super(in);
            nodeId = in.readString();
            totalShards = in.readVInt();
            results = in.readList((stream) -> stream.readBoolean() ? readShardResult(stream) : null);
            if (in.readBoolean()) {
                exceptions = in.readList(BroadcastShardOperationFailedException::new);
            } else {
                exceptions = null;
            }
        }

        NodeResponse(
            String nodeId,
            int totalShards,
            List<ShardOperationResult> results,
            List<BroadcastShardOperationFailedException> exceptions
        ) {
            this.nodeId = nodeId;
            this.totalShards = totalShards;
            this.results = results;
            this.exceptions = exceptions;
        }

        public String getNodeId() {
            return nodeId;
        }

        public int getTotalShards() {
            return totalShards;
        }

        public int getSuccessfulShards() {
            return results.size();
        }

        public List<BroadcastShardOperationFailedException> getExceptions() {
            return exceptions;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeVInt(totalShards);
            out.writeVInt(results.size());
            for (ShardOperationResult result : results) {
                out.writeOptionalWriteable(result);
            }
            out.writeBoolean(exceptions != null);
            if (exceptions != null) {
                out.writeList(exceptions);
            }
        }
    }

    /**
     * Can be used for implementations of {@link #shardOperation(BroadcastRequest, ShardRouting) shardOperation} for
     * which there is no shard-level return value.
     *
     * @opensearch.internal
     */
    public static final class EmptyResult implements Writeable {
        public static EmptyResult INSTANCE = new EmptyResult();

        private EmptyResult() {}

        private EmptyResult(StreamInput in) {}

        @Override
        public void writeTo(StreamOutput out) {}

        public static EmptyResult readEmptyResultFrom(StreamInput in) {
            return INSTANCE;
        }
    }
}
