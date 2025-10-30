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

package org.opensearch.action.support.single.instance;

import org.opensearch.action.UnavailableShardsException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.TransportIndicesResolvingAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.node.NodeClosedException;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPool.Names;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static org.opensearch.cluster.metadata.IndexNameExpressionResolver.EXCLUDED_DATA_STREAMS_KEY;

/**
 * Base class for a single operation action
 *
 * @opensearch.internal
 */
public abstract class TransportInstanceSingleOperationAction<
    Request extends InstanceShardOperationRequest<Request>,
    Response extends ActionResponse> extends HandledTransportAction<Request, Response> implements TransportIndicesResolvingAction<Request> {

    protected final ThreadPool threadPool;
    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final IndexNameExpressionResolver indexNameExpressionResolver;

    final String shardActionName;

    protected TransportInstanceSingleOperationAction(
        String actionName,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Writeable.Reader<Request> request
    ) {
        super(actionName, transportService, actionFilters, request);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.shardActionName = actionName + "[s]";
        transportService.registerRequestHandler(shardActionName, Names.SAME, request, new ShardTransportHandler());
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(request, listener).start();
    }

    @Override
    public ResolvedIndices resolveIndices(Request request) {
        if (request.concreteIndex() != null) {
            return ResolvedIndices.of(request.concreteIndex());
        } else {
            try {
                return ResolvedIndices.of(indexNameExpressionResolver.concreteWriteIndex(clusterService.state(), request).getName());
            } catch (IndexNotFoundException e) {
                // We just return the original unresolved expression. The error we encountered here will
                // be encountered again in the doStart() method
                return ResolvedIndices.of(request.index());
            }
        }
    }

    protected abstract String executor(ShardId shardId);

    protected abstract void shardOperation(Request request, ActionListener<Response> listener);

    protected abstract Response newResponse(StreamInput in) throws IOException;

    protected ClusterBlockException checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    protected ClusterBlockException checkRequestBlock(ClusterState state, Request request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.concreteIndex());
    }

    /**
     * Resolves the request. Throws an exception if the request cannot be resolved.
     */
    protected abstract void resolveRequest(ClusterState state, Request request);

    protected boolean retryOnFailure(Exception e) {
        return false;
    }

    protected TransportRequestOptions transportOptions() {
        return TransportRequestOptions.EMPTY;
    }

    /**
     * Should return an iterator with a single shard!
     */
    protected abstract ShardIterator shards(ClusterState clusterState, Request request);

    /**
     * Asynchronous single action
     *
     * @opensearch.internal
     */
    class AsyncSingleAction {

        private final ActionListener<Response> listener;
        private final Request request;
        private volatile ClusterStateObserver observer;
        private ShardIterator shardIt;

        AsyncSingleAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;
        }

        public void start() {
            ClusterState state = clusterService.state();
            this.observer = new ClusterStateObserver(state, clusterService, request.timeout(), logger, threadPool.getThreadContext());
            doStart(state);
        }

        protected void doStart(ClusterState clusterState) {
            try {
                ClusterBlockException blockException = checkGlobalBlock(clusterState);
                if (blockException != null) {
                    if (blockException.retryable()) {
                        retry(blockException);
                        return;
                    } else {
                        throw blockException;
                    }
                }
                try {
                    request.concreteIndex(indexNameExpressionResolver.concreteWriteIndex(clusterState, request).getName());
                } catch (IndexNotFoundException e) {
                    if (request.includeDataStreams() == false && e.getMetadataKeys().contains(EXCLUDED_DATA_STREAMS_KEY)) {
                        throw new IllegalArgumentException("only write ops with an op_type of create are allowed in data streams");
                    } else {
                        throw e;
                    }
                }
                resolveRequest(clusterState, request);
                blockException = checkRequestBlock(clusterState, request);
                if (blockException != null) {
                    if (blockException.retryable()) {
                        retry(blockException);
                        return;
                    } else {
                        throw blockException;
                    }
                }
                shardIt = shards(clusterState, request);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }

            // no shardIt, might be in the case between index gateway recovery and shardIt initialization
            if (shardIt.size() == 0) {
                retry(null);
                return;
            }

            // this transport only make sense with an iterator that returns a single shard routing (like primary)
            assert shardIt.size() == 1;

            ShardRouting shard = shardIt.nextOrNull();
            assert shard != null;

            if (!shard.active()) {
                retry(null);
                return;
            }

            request.shardId = shardIt.shardId();
            DiscoveryNode node = clusterState.nodes().get(shard.currentNodeId());
            transportService.sendRequest(node, shardActionName, request, transportOptions(), new TransportResponseHandler<Response>() {

                @Override
                public Response read(StreamInput in) throws IOException {
                    return newResponse(in);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public void handleResponse(Response response) {
                    listener.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    final Throwable cause = exp.unwrapCause();
                    // if we got disconnected from the node, or the node / shard is not in the right state (being closed)
                    if (cause instanceof ConnectTransportException || cause instanceof NodeClosedException || retryOnFailure(exp)) {
                        retry((Exception) cause);
                    } else {
                        listener.onFailure(exp);
                    }
                }
            });
        }

        void retry(@Nullable final Exception failure) {
            if (observer.isTimedOut()) {
                // we running as a last attempt after a timeout has happened. don't retry
                Exception listenFailure = failure;
                if (listenFailure == null) {
                    if (shardIt == null) {
                        listenFailure = new UnavailableShardsException(
                            request.concreteIndex(),
                            -1,
                            "Timeout waiting for [{}], request: {}",
                            request.timeout(),
                            actionName
                        );
                    } else {
                        listenFailure = new UnavailableShardsException(
                            shardIt.shardId(),
                            "[{}] shardIt, [{}] active : Timeout waiting for [{}], request: {}",
                            shardIt.size(),
                            shardIt.sizeActive(),
                            request.timeout(),
                            actionName
                        );
                    }
                }
                listener.onFailure(listenFailure);
                return;
            }

            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    doStart(state);
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    // just to be on the safe side, see if we can start it now?
                    doStart(observer.setAndGetObservedState());
                }
            }, request.timeout());
        }
    }

    /**
     * Transport handler per shard
     *
     * @opensearch.internal
     */
    private class ShardTransportHandler implements TransportRequestHandler<Request> {

        @Override
        public void messageReceived(final Request request, final TransportChannel channel, Task task) throws Exception {
            threadPool.executor(executor(request.shardId)).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.warn("failed to send response for " + shardActionName, inner);
                    }
                }

                @Override
                protected void doRun() {
                    shardOperation(request, ActionListener.wrap(channel::sendResponse, this::onFailure));
                }
            });
        }
    }
}
