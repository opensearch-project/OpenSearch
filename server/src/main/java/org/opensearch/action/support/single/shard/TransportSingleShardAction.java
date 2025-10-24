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

package org.opensearch.action.support.single.shard;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ChannelActionListener;
import org.opensearch.action.support.TransportAction;
import org.opensearch.action.support.TransportActions;
import org.opensearch.action.support.TransportIndicesResolvingAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.FailAwareWeightedRouting;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.logging.LoggerMessageFormat;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static org.opensearch.action.support.TransportActions.isShardNotAvailableException;

/**
 * A base class for operations that need to perform a read operation on a single shard copy. If the operation fails,
 * the read operation can be performed on other shard copies. Concrete implementations can provide their own list
 * of candidate shards to try the read operation on.
 *
 * @opensearch.internal
 */
public abstract class TransportSingleShardAction<Request extends SingleShardRequest<Request>, Response extends ActionResponse> extends
    TransportAction<Request, Response>
    implements
        TransportIndicesResolvingAction<Request> {

    protected final ThreadPool threadPool;
    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final IndexNameExpressionResolver indexNameExpressionResolver;

    private final String transportShardAction;
    private final String executor;

    protected TransportSingleShardAction(
        String actionName,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Writeable.Reader<Request> request,
        String executor
    ) {
        super(actionName, actionFilters, transportService.getTaskManager());
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;

        this.transportShardAction = actionName + "[s]";
        this.executor = executor;

        if (!isSubAction()) {
            transportService.registerRequestHandler(actionName, ThreadPool.Names.SAME, request, new TransportHandler());
        }
        transportService.registerRequestHandler(transportShardAction, ThreadPool.Names.SAME, request, new ShardTransportHandler());
    }

    /**
     * Tells whether the action is a main one or a subaction. Used to decide whether we need to register
     * the main transport handler. In fact if the action is a subaction, its execute method
     * will be called locally to its parent action.
     */
    protected boolean isSubAction() {
        return false;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(request, listener).start();
    }

    protected abstract Response shardOperation(Request request, ShardId shardId) throws IOException;

    protected void asyncShardOperation(Request request, ShardId shardId, ActionListener<Response> listener) throws IOException {
        threadPool.executor(getExecutor(request, shardId)).execute(ActionRunnable.supply(listener, () -> shardOperation(request, shardId)));
    }

    protected abstract Writeable.Reader<Response> getResponseReader();

    protected abstract boolean resolveIndex(Request request);

    protected ClusterBlockException checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, request.concreteIndex());
    }

    protected void resolveRequest(ClusterState state, InternalRequest request) {

    }

    /**
     * Returns the candidate shards to execute the operation on or <code>null</code> the execute
     * the operation locally (the node that received the request)
     */
    @Nullable
    protected abstract ShardsIterator shards(ClusterState state, InternalRequest request);

    @Override
    public ResolvedIndices resolveIndices(Request request) {
        return ResolvedIndices.ofNonNull(resolveToConcreteSingleIndex(request, clusterService.state()));
    }

    private String resolveToConcreteSingleIndex(Request request, ClusterState clusterState) {
        if (resolveIndex(request)) {
            return indexNameExpressionResolver.concreteSingleIndex(clusterState, request).getName();
        } else {
            return request.index();
        }
    }

    /**
     * Asynchronous single action
     *
     * @opensearch.internal
     */
    class AsyncSingleAction {

        private final ActionListener<Response> listener;
        private final ShardsIterator shardIt;
        private final InternalRequest internalRequest;
        private final DiscoveryNodes nodes;
        private volatile Exception lastFailure;

        private AsyncSingleAction(Request request, ActionListener<Response> listener) {
            this.listener = listener;

            ClusterState clusterState = clusterService.state();
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}] based on cluster state version [{}]", request, clusterState.version());
            }
            nodes = clusterState.nodes();
            ClusterBlockException blockException = checkGlobalBlock(clusterState);
            if (blockException != null) {
                throw blockException;
            }

            String concreteSingleIndex = resolveToConcreteSingleIndex(request, clusterState);
            this.internalRequest = new InternalRequest(request, concreteSingleIndex);
            resolveRequest(clusterState, internalRequest);

            blockException = checkRequestBlock(clusterState, internalRequest);
            if (blockException != null) {
                throw blockException;
            }

            this.shardIt = shards(clusterState, internalRequest);
        }

        public void start() {
            if (shardIt == null) {
                // just execute it on the local node
                final Writeable.Reader<Response> reader = getResponseReader();
                transportService.sendRequest(
                    clusterService.localNode(),
                    transportShardAction,
                    internalRequest.request(),
                    new TransportResponseHandler<Response>() {
                        @Override
                        public Response read(StreamInput in) throws IOException {
                            return reader.read(in);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                        @Override
                        public void handleResponse(final Response response) {
                            listener.onResponse(response);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            listener.onFailure(exp);
                        }
                    }
                );
            } else {
                perform(null);
            }
        }

        private void onFailure(ShardRouting shardRouting, Exception e) {
            if (e != null) {
                logger.trace(() -> new ParameterizedMessage("{}: failed to execute [{}]", shardRouting, internalRequest.request()), e);
            }
            perform(e);
        }

        private void perform(@Nullable final Exception currentFailure) {
            Exception lastFailure = this.lastFailure;
            if (lastFailure == null || TransportActions.isReadOverrideException(currentFailure)) {
                lastFailure = currentFailure;
                this.lastFailure = currentFailure;
            }
            ShardRouting shardRouting = FailAwareWeightedRouting.getInstance()
                .findNext(shardIt, clusterService.state(), currentFailure, () -> {});

            if (shardRouting == null) {
                Exception failure = lastFailure;
                if (failure == null || isShardNotAvailableException(failure)) {
                    failure = new NoShardAvailableActionException(
                        null,
                        LoggerMessageFormat.format("No shard available for [{}]", internalRequest.request()),
                        failure
                    );
                } else {
                    logger.debug(() -> new ParameterizedMessage("{}: failed to execute [{}]", null, internalRequest.request()), failure);
                }
                listener.onFailure(failure);
                return;
            }
            DiscoveryNode node = nodes.get(shardRouting.currentNodeId());
            if (node == null) {
                onFailure(shardRouting, new NoShardAvailableActionException(shardRouting.shardId()));
            } else {
                internalRequest.request().internalShardId = shardRouting.shardId();
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "sending request [{}] to shard [{}] on node [{}]",
                        internalRequest.request(),
                        internalRequest.request().internalShardId,
                        node
                    );
                }
                final Writeable.Reader<Response> reader = getResponseReader();
                ShardRouting finalShardRouting = shardRouting;
                transportService.sendRequest(
                    node,
                    transportShardAction,
                    internalRequest.request(),
                    new TransportResponseHandler<Response>() {

                        @Override
                        public Response read(StreamInput in) throws IOException {
                            return reader.read(in);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                        @Override
                        public void handleResponse(final Response response) {
                            listener.onResponse(response);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            onFailure(finalShardRouting, exp);
                        }
                    }
                );
            }
        }
    }

    /**
     * Internal transport handler
     *
     * @opensearch.internal
     */
    private class TransportHandler implements TransportRequestHandler<Request> {

        @Override
        public void messageReceived(Request request, final TransportChannel channel, Task task) throws Exception {
            // if we have a local operation, execute it on a thread since we don't spawn
            execute(request, new ChannelActionListener<>(channel, actionName, request));
        }
    }

    /**
     * Shard level transport handler
     *
     * @opensearch.internal
     */
    private class ShardTransportHandler implements TransportRequestHandler<Request> {

        @Override
        public void messageReceived(final Request request, final TransportChannel channel, Task task) throws Exception {
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}] on shard [{}]", request, request.internalShardId);
            }
            asyncShardOperation(request, request.internalShardId, new ChannelActionListener<>(channel, transportShardAction, request));
        }
    }

    /**
     * Internal request class that gets built on each node. Holds the original request plus additional info.
     *
     * @opensearch.internal
     */
    protected class InternalRequest {
        final Request request;
        final String concreteIndex;

        InternalRequest(Request request, String concreteIndex) {
            this.request = request;
            this.concreteIndex = concreteIndex;
        }

        public Request request() {
            return request;
        }

        public String concreteIndex() {
            return concreteIndex;
        }
    }

    protected String getExecutor(Request request, ShardId shardId) {
        return executor;
    }
}
