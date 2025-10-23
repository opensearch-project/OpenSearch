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

package org.opensearch.action.support.broadcast;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.TransportActions;
import org.opensearch.action.support.TransportIndicesResolvingAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.FailAwareWeightedRouting;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Base transport broadcast action class
 *
 * @opensearch.internal
 */
public abstract class TransportBroadcastAction<
    Request extends BroadcastRequest<Request>,
    Response extends BroadcastResponse,
    ShardRequest extends BroadcastShardRequest,
    ShardResponse extends BroadcastShardResponse> extends HandledTransportAction<Request, Response>
    implements
        TransportIndicesResolvingAction<Request> {

    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final IndexNameExpressionResolver indexNameExpressionResolver;

    final String transportShardAction;
    private final String shardExecutor;

    protected TransportBroadcastAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Writeable.Reader<Request> request,
        Writeable.Reader<ShardRequest> shardRequest,
        String shardExecutor
    ) {
        super(actionName, transportService, actionFilters, request);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.transportShardAction = actionName + "[s]";
        this.shardExecutor = shardExecutor;

        transportService.registerRequestHandler(transportShardAction, ThreadPool.Names.SAME, shardRequest, new ShardTransportHandler());
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        new AsyncBroadcastAction(task, request, listener).start();
    }

    @Override
    public ResolvedIndices resolveIndices(Request request) {
        return ResolvedIndices.of(resolveIndices(request, clusterService.state()));
    }

    protected ResolvedIndices.Local.Concrete resolveIndices(Request request, ClusterState clusterState) {
        return indexNameExpressionResolver.concreteResolvedIndices(clusterState, request);
    }

    protected abstract Response newResponse(Request request, AtomicReferenceArray shardsResponses, ClusterState clusterState);

    protected abstract ShardRequest newShardRequest(int numShards, ShardRouting shard, Request request);

    protected abstract ShardResponse readShardResponse(StreamInput in) throws IOException;

    protected abstract ShardResponse shardOperation(ShardRequest request, Task task) throws IOException;

    /**
     * Determines the shards this operation will be executed on. The operation is executed once per shard iterator, typically
     * on the first shard in it. If the operation fails, it will be retried on the next shard in the iterator.
     */
    protected abstract GroupShardsIterator<ShardIterator> shards(ClusterState clusterState, Request request, String[] concreteIndices);

    protected abstract ClusterBlockException checkGlobalBlock(ClusterState state, Request request);

    protected abstract ClusterBlockException checkRequestBlock(ClusterState state, Request request, String[] concreteIndices);

    /**
     * Asynchronous broadcast action
     *
     * @opensearch.internal
     */
    protected class AsyncBroadcastAction {

        private final Task task;
        private final Request request;
        private final ActionListener<Response> listener;
        private final ClusterState clusterState;
        private final DiscoveryNodes nodes;
        private final GroupShardsIterator<ShardIterator> shardsIts;
        private final int expectedOps;
        private final AtomicInteger counterOps = new AtomicInteger();
        private final AtomicReferenceArray shardsResponses;

        protected AsyncBroadcastAction(Task task, Request request, ActionListener<Response> listener) {
            this.task = task;
            this.request = request;
            this.listener = listener;

            clusterState = clusterService.state();

            ClusterBlockException blockException = checkGlobalBlock(clusterState, request);
            if (blockException != null) {
                throw blockException;
            }
            // update to concrete indices
            String[] concreteIndices = resolveIndices(request, clusterState).namesOfConcreteIndicesAsArray();
            blockException = checkRequestBlock(clusterState, request, concreteIndices);
            if (blockException != null) {
                throw blockException;
            }

            nodes = clusterState.nodes();
            logger.trace("resolving shards based on cluster state version [{}]", clusterState.version());
            shardsIts = shards(clusterState, request, concreteIndices);
            expectedOps = shardsIts.size();

            shardsResponses = new AtomicReferenceArray<>(expectedOps);
        }

        public void start() {
            if (shardsIts.size() == 0) {
                // no shards
                try {
                    listener.onResponse(newResponse(request, new AtomicReferenceArray(0), clusterState));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
                return;
            }
            // count the local operations, and perform the non local ones
            int shardIndex = -1;
            for (final ShardIterator shardIt : shardsIts) {
                shardIndex++;
                final ShardRouting shard = shardIt.nextOrNull();
                if (shard != null) {
                    performOperation(shardIt, shard, shardIndex);
                } else {
                    // really, no shards active in this group
                    onOperation(null, shardIt, shardIndex, new NoShardAvailableActionException(shardIt.shardId()));
                }
            }
        }

        protected void performOperation(final ShardIterator shardIt, final ShardRouting shard, final int shardIndex) {
            if (shard == null) {
                // no more active shards... (we should not really get here, just safety)
                onOperation(null, shardIt, shardIndex, new NoShardAvailableActionException(shardIt.shardId()));
            } else {
                try {
                    final ShardRequest shardRequest = newShardRequest(shardIt.size(), shard, request);
                    shardRequest.setParentTask(clusterService.localNode().getId(), task.getId());
                    DiscoveryNode node = nodes.get(shard.currentNodeId());
                    if (node == null) {
                        // no node connected, act as failure
                        onOperation(shard, shardIt, shardIndex, new NoShardAvailableActionException(shardIt.shardId()));
                    } else {
                        transportService.sendRequest(
                            node,
                            transportShardAction,
                            shardRequest,
                            new TransportResponseHandler<ShardResponse>() {
                                @Override
                                public ShardResponse read(StreamInput in) throws IOException {
                                    return readShardResponse(in);
                                }

                                @Override
                                public String executor() {
                                    return ThreadPool.Names.SAME;
                                }

                                @Override
                                public void handleResponse(ShardResponse response) {
                                    onOperation(shard, shardIndex, response);
                                }

                                @Override
                                public void handleException(TransportException e) {
                                    onOperation(shard, shardIt, shardIndex, e);
                                }
                            }
                        );
                    }
                } catch (Exception e) {
                    onOperation(shard, shardIt, shardIndex, e);
                }
            }
        }

        @SuppressWarnings({ "unchecked" })
        protected void onOperation(ShardRouting shard, int shardIndex, ShardResponse response) {
            logger.trace("received response for {}", shard);
            shardsResponses.set(shardIndex, response);
            if (expectedOps == counterOps.incrementAndGet()) {
                finishHim();
            }
        }

        void onOperation(@Nullable ShardRouting shard, final ShardIterator shardIt, int shardIndex, Exception e) {
            // we set the shard failure always, even if its the first in the replication group, and the next one
            // will work (it will just override it...)
            setFailure(shardIt, shardIndex, e);
            ShardRouting nextShard = FailAwareWeightedRouting.getInstance()
                .findNext(shardIt, clusterService.state(), e, () -> counterOps.incrementAndGet());

            if (nextShard != null) {
                if (e != null) {
                    if (logger.isTraceEnabled()) {
                        if (!TransportActions.isShardNotAvailableException(e)) {
                            logger.trace(
                                new ParameterizedMessage(
                                    "{}: failed to execute [{}]",
                                    shard != null ? shard.shortSummary() : shardIt.shardId(),
                                    request
                                ),
                                e
                            );
                        }
                    }
                }
                performOperation(shardIt, nextShard, shardIndex);
            } else {
                if (logger.isDebugEnabled()) {
                    if (e != null) {
                        if (!TransportActions.isShardNotAvailableException(e)) {
                            logger.debug(
                                new ParameterizedMessage(
                                    "{}: failed to execute [{}]",
                                    shard != null ? shard.shortSummary() : shardIt.shardId(),
                                    request
                                ),
                                e
                            );
                        }
                    }
                }
                if (expectedOps == counterOps.incrementAndGet()) {
                    finishHim();
                }
            }
        }

        protected void finishHim() {
            try {
                listener.onResponse(newResponse(request, shardsResponses, clusterState));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        void setFailure(ShardIterator shardIt, int shardIndex, Exception e) {
            // we don't aggregate shard failures on non active shards (but do keep the header counts right)
            if (TransportActions.isShardNotAvailableException(e)) {
                return;
            }

            if (!(e instanceof BroadcastShardOperationFailedException)) {
                e = new BroadcastShardOperationFailedException(shardIt.shardId(), e);
            }

            Object response = shardsResponses.get(shardIndex);
            if (response == null) {
                // just override it and return
                shardsResponses.set(shardIndex, e);
            }

            if (!(response instanceof Throwable)) {
                // we should never really get here...
                return;
            }

            // the failure is already present, try and not override it with an exception that is less meaningless
            // for example, getting illegal shard state
            if (TransportActions.isReadOverrideException(e)) {
                shardsResponses.set(shardIndex, e);
            }
        }
    }

    /**
     * A shard transport handler
     *
     * @opensearch.internal
     */
    class ShardTransportHandler implements TransportRequestHandler<ShardRequest> {

        @Override
        public void messageReceived(ShardRequest request, TransportChannel channel, Task task) throws Exception {
            asyncShardOperation(request, task, ActionListener.wrap(channel::sendResponse, e -> {
                try {
                    channel.sendResponse(e);
                } catch (Exception e1) {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "Failed to send error response for action [{}] and request [{}]",
                            actionName,
                            request
                        ),
                        e1
                    );
                }
            }));
        }
    }

    private void asyncShardOperation(ShardRequest request, Task task, ActionListener<ShardResponse> listener) {
        transportService.getThreadPool()
            .executor(shardExecutor)
            .execute(ActionRunnable.supply(listener, () -> shardOperation(request, task)));
    }
}
