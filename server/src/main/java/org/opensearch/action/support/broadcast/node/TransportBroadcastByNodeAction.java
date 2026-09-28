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
import org.opensearch.action.support.TransportIndicesResolvingAction;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.OptionallyResolvedIndices;
import org.opensearch.cluster.metadata.ResolvedIndices;
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
import java.util.concurrent.RejectedExecutionException;
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
    ShardOperationResult extends Writeable> extends HandledTransportAction<Request, Response>
    implements
        TransportIndicesResolvingAction<Request> {

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ThreadPool threadPool;

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
        this.threadPool = transportService.getThreadPool();

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
     * Async shard operation with ActionListener callback.
     * Subclasses that opt into async execution (via {@link #isAsyncShardOperation()}) MUST
     * override this method with their non-blocking implementation.
     * The default throws {@link UnsupportedOperationException} — async subclasses should
     * never fall through to the sync path.
     *
     * @param request      the indices-level request
     * @param shardRouting the shard on which to execute the operation
     * @param listener     callback to invoke with the result or failure
     */
    protected void shardOperationAsync(Request request, ShardRouting shardRouting, ActionListener<ShardOperationResult> listener) {
        throw new UnsupportedOperationException(
            "shardOperationAsync must be overridden by subclasses that return isAsyncShardOperation()=true"
        );
    }

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
     * Returns true if shard operations should execute asynchronously on the data node.
     * When true, each shard operation is submitted to the thread pool in parallel and the
     * transport response is sent only after all shards complete (via CountDownLatch).
     * This prevents long-running shard operations (e.g., flush + remote sync) from blocking
     * the thread pool thread that received the transport request.
     * <p>
     * Default is false (synchronous, sequential execution) for backwards compatibility.
     *
     * @return true if shard operations should be dispatched asynchronously
     */
    protected boolean isAsyncShardOperation() {
        return false;
    }

    /**
     * Returns the thread pool name to use when dispatching async shard operations on the data node.
     * Only consulted when {@link #isAsyncShardOperation()} is true.
     * Default is {@link ThreadPool.Names#GENERIC} — a scaling pool, so per-shard tasks do not queue
     * behind each other on a bounded executor. Override to redirect to a dedicated pool if needed.
     *
     * @return the thread pool name for async per-shard execution
     */
    protected String asyncShardOperationThreadPool() {
        return ThreadPool.Names.GENERIC;
    }

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
     * @return a list of concrete indices that this action should operate on
     */
    protected ResolvedIndices.Local.Concrete resolveConcreteIndices(ClusterState clusterState, Request request) {
        return indexNameExpressionResolver.concreteResolvedIndices(clusterState, request);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        new AsyncAction(task, request, listener).start();
    }

    @Override
    public OptionallyResolvedIndices resolveIndices(Request request) {
        return ResolvedIndices.of(resolveConcreteIndices(clusterService.state(), request));
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

            String[] concreteIndices = resolveConcreteIndices(clusterState, request).namesOfConcreteIndicesAsArray();
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

            if (isAsyncShardOperation()) {
                logger.trace("[{}] executing async operation on [{}] shards", actionName, totalShards);
                onAsyncShardOperation(request, channel, shards, totalShards, shardResultOrExceptions);
            } else {
                // Sync mode: original sequential execution (backwards compatible)
                int shardIndex = -1;
                for (final ShardRouting shardRouting : shards) {
                    shardIndex++;
                    onShardOperation(request, shardResultOrExceptions, shardIndex, shardRouting);
                }
                sendNodeResponse(request, channel, totalShards, shardResultOrExceptions);
            }
        }

        /**
         * Dispatches each shard's {@link #shardOperationAsync} as its own task on
         * {@link #asyncShardOperationThreadPool()} so a slow/blocking shard prefix (e.g. a permit wait)
         * does not serialize the others on the single node-handler thread. The listener is wrapped in
         * {@link ActionListener#notifyOnce} so a shard implementation that completes more than once
         * (e.g. a racing drain-callback and timeout) still counts as exactly one shard.
         * If the pool rejects a per-shard dispatch, that shard is recorded as a failure via the same
         * listener so {@code remaining} still reaches zero and the node response is sent.
         */
        private void onAsyncShardOperation(
            final NodeRequest request,
            TransportChannel channel,
            List<ShardRouting> shards,
            int totalShards,
            Object[] shardResultOrExceptions
        ) {
            final AtomicInteger remaining = new AtomicInteger(totalShards);
            final String asyncPool = asyncShardOperationThreadPool();
            int idx = 0;
            for (final ShardRouting shardRouting : shards) {
                final int shardIndex = idx++;
                ActionListener<ShardOperationResult> shardListener = ActionListener.notifyOnce(new ActionListener<ShardOperationResult>() {
                    @Override
                    public void onResponse(ShardOperationResult result) {
                        shardResultOrExceptions[shardIndex] = result;
                        onShardOperationComplete(request, channel, totalShards, shardResultOrExceptions, remaining);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        BroadcastShardOperationFailedException failure = new BroadcastShardOperationFailedException(
                            shardRouting.shardId(),
                            "operation " + actionName + " failed",
                            e
                        );
                        failure.setShard(shardRouting.shardId());
                        shardResultOrExceptions[shardIndex] = failure;
                        onShardOperationComplete(request, channel, totalShards, shardResultOrExceptions, remaining);
                    }
                });
                try {
                    threadPool.executor(asyncPool).execute(() -> {
                        try {
                            shardOperationAsync(request.indicesLevelRequest, shardRouting, shardListener);
                        } catch (Exception inner) {
                            // shardOperationAsync threw synchronously on the pool worker (e.g. an
                            // IndexNotFoundException raced index deletion). Without this, the throw
                            // would escape onto the pool's uncaught-exception handler — the listener
                            // would never fire, `remaining` would never decrement, and the coordinator
                            // would only get a response after its transport timeout. notifyOnce keeps
                            // this safe even if the impl partially completed the listener before throwing.
                            shardListener.onFailure(inner);
                        }
                    });
                } catch (Exception e) {
                    // Pool rejected at dispatch time (shutdown / saturated) — record as a shard failure
                    // on the same (notifyOnce-wrapped) listener so the remaining counter still reaches
                    // zero and the node response is sent.
                    RejectedExecutionException rejection = new RejectedExecutionException(
                        "rejected by thread pool [" + asyncPool + "] for shard " + shardRouting.shardId(),
                        e
                    );
                    shardListener.onFailure(rejection);
                }
            }
        }

        private void onShardOperationComplete(
            NodeRequest request,
            TransportChannel channel,
            int totalShards,
            Object[] shardResultOrExceptions,
            AtomicInteger remaining
        ) {
            if (remaining.decrementAndGet() == 0) {
                try {
                    sendNodeResponse(request, channel, totalShards, shardResultOrExceptions);
                } catch (IOException e) {
                    logger.warn("[{}] failed to send response after async shard operations", actionName);
                }
            }
        }

        private void sendNodeResponse(
            final NodeRequest request,
            TransportChannel channel,
            int totalShards,
            Object[] shardResultOrExceptions
        ) throws IOException {
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
