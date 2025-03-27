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

package org.opensearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.support.ChannelActionListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * The source recovery accepts recovery requests from other peer shards and start the recovery process from this
 * source shard to the target shard.
 *
 * @opensearch.internal
 */
public class PeerRecoverySourceService extends AbstractLifecycleComponent implements IndexEventListener, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(PeerRecoverySourceService.class);

    /**
     * The internal actions
     *
     * @opensearch.internal
     */
    public static class Actions {
        public static final String START_RECOVERY = "internal:index/shard/recovery/start_recovery";
        public static final String REESTABLISH_RECOVERY = "internal:index/shard/recovery/reestablish_recovery";
    }

    private final TransportService transportService;
    private final IndicesService indicesService;
    private final RecoverySettings recoverySettings;

    final OngoingRecoveries ongoingRecoveries = new OngoingRecoveries();

    @Inject
    public PeerRecoverySourceService(TransportService transportService, IndicesService indicesService, RecoverySettings recoverySettings) {
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.recoverySettings = recoverySettings;
        // When the target node wants to start a peer recovery it sends a START_RECOVERY request to the source
        // node. Upon receiving START_RECOVERY, the source node will initiate the peer recovery.
        transportService.registerRequestHandler(
            Actions.START_RECOVERY,
            ThreadPool.Names.GENERIC,
            StartRecoveryRequest::new,
            new StartRecoveryTransportRequestHandler()
        );
        // When the target node's START_RECOVERY request has failed due to a network disconnection, it will
        // send a REESTABLISH_RECOVERY. This attempts to reconnect to an existing recovery process taking
        // place on the source node. If the recovery process no longer exists, then the REESTABLISH_RECOVERY
        // action will fail and the target node will send a new START_RECOVERY request.
        transportService.registerRequestHandler(
            Actions.REESTABLISH_RECOVERY,
            ThreadPool.Names.GENERIC,
            ReestablishRecoveryRequest::new,
            new ReestablishRecoveryTransportRequestHandler()
        );
    }

    @Override
    protected void doStart() {
        final ClusterService clusterService = indicesService.clusterService();
        if (DiscoveryNode.canContainData(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
    }

    @Override
    protected void doStop() {
        final ClusterService clusterService = indicesService.clusterService();
        if (DiscoveryNode.canContainData(clusterService.getSettings())) {
            ongoingRecoveries.awaitEmpty();
            indicesService.clusterService().removeListener(this);
        }
    }

    @Override
    protected void doClose() {}

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            ongoingRecoveries.cancel(indexShard, "shard is closed");
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                ongoingRecoveries.cancelOnNodeLeft(removedNode);
            }
        }
    }

    private void recover(StartRecoveryRequest request, ActionListener<RecoveryResponse> listener) {
        final IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        final IndexShard shard = indexService.getShard(request.shardId().id());

        final ShardRouting routingEntry = shard.routingEntry();

        if (routingEntry.primary() == false || routingEntry.active() == false) {
            throw new DelayRecoveryException("source shard [" + routingEntry + "] is not an active primary");
        }

        if (request.isPrimaryRelocation()
            && (routingEntry.relocating() == false || routingEntry.relocatingNodeId().equals(request.targetNode().getId()) == false)) {
            logger.debug(
                "delaying recovery of {} as source shard is not marked yet as relocating to {}",
                request.shardId(),
                request.targetNode()
            );
            throw new DelayRecoveryException("source shard is not marked yet as relocating to [" + request.targetNode() + "]");
        }

        RecoverySourceHandler handler = ongoingRecoveries.addNewRecovery(request, shard);
        logger.trace(
            "[{}][{}] starting recovery to {}",
            request.shardId().getIndex().getName(),
            request.shardId().id(),
            request.targetNode()
        );
        handler.recoverToTarget(ActionListener.runAfter(listener, () -> ongoingRecoveries.remove(shard, handler)));
    }

    private void reestablish(ReestablishRecoveryRequest request, ActionListener<RecoveryResponse> listener) {
        final IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        final IndexShard shard = indexService.getShard(request.shardId().id());

        logger.trace(
            "[{}][{}] reestablishing recovery {}",
            request.shardId().getIndex().getName(),
            request.shardId().id(),
            request.recoveryId()
        );
        ongoingRecoveries.reestablishRecovery(request, shard, listener);
    }

    class StartRecoveryTransportRequestHandler implements TransportRequestHandler<StartRecoveryRequest> {
        @Override
        public void messageReceived(final StartRecoveryRequest request, final TransportChannel channel, Task task) throws Exception {
            recover(request, new ChannelActionListener<>(channel, Actions.START_RECOVERY, request));
        }
    }

    class ReestablishRecoveryTransportRequestHandler implements TransportRequestHandler<ReestablishRecoveryRequest> {
        @Override
        public void messageReceived(final ReestablishRecoveryRequest request, final TransportChannel channel, Task task) throws Exception {
            reestablish(request, new ChannelActionListener<>(channel, Actions.REESTABLISH_RECOVERY, request));
        }
    }

    // exposed for testing
    final int numberOfOngoingRecoveries() {
        return ongoingRecoveries.ongoingRecoveries.size();
    }

    final class OngoingRecoveries {

        private final Map<IndexShard, ShardRecoveryContext> ongoingRecoveries = new HashMap<>();

        private final Map<DiscoveryNode, Collection<RemoteRecoveryTargetHandler>> nodeToHandlers = new HashMap<>();

        @Nullable
        private List<ActionListener<Void>> emptyListeners;

        synchronized RecoverySourceHandler addNewRecovery(StartRecoveryRequest request, IndexShard shard) {
            assert lifecycle.started();
            final ShardRecoveryContext shardContext = ongoingRecoveries.computeIfAbsent(shard, s -> new ShardRecoveryContext());
            final Tuple<RecoverySourceHandler, RemoteRecoveryTargetHandler> handlers = shardContext.addNewRecovery(request, shard);
            final RemoteRecoveryTargetHandler recoveryTargetHandler = handlers.v2();
            nodeToHandlers.computeIfAbsent(recoveryTargetHandler.targetNode(), k -> new HashSet<>()).add(recoveryTargetHandler);
            shard.recoveryStats().incCurrentAsSource();
            return handlers.v1();
        }

        synchronized void cancelOnNodeLeft(DiscoveryNode node) {
            final Collection<RemoteRecoveryTargetHandler> handlers = nodeToHandlers.get(node);
            if (handlers != null) {
                for (RemoteRecoveryTargetHandler handler : handlers) {
                    handler.cancel();
                }
            }
        }

        synchronized void reestablishRecovery(
            ReestablishRecoveryRequest request,
            IndexShard shard,
            ActionListener<RecoveryResponse> listener
        ) {
            assert lifecycle.started();
            final ShardRecoveryContext shardContext = ongoingRecoveries.get(shard);
            if (shardContext == null) {
                throw new PeerRecoveryNotFound(request.recoveryId(), request.shardId(), request.targetAllocationId());
            }
            shardContext.reestablishRecovery(request, listener);
        }

        synchronized void remove(IndexShard shard, RecoverySourceHandler handler) {
            final ShardRecoveryContext shardRecoveryContext = ongoingRecoveries.get(shard);
            assert shardRecoveryContext != null : "Shard was not registered [" + shard + "]";
            final RemoteRecoveryTargetHandler removed = shardRecoveryContext.recoveryHandlers.remove(handler);
            assert removed != null : "Handler was not registered [" + handler + "]";
            if (removed != null) {
                shard.recoveryStats().decCurrentAsSource();
                removed.cancel();
                assert nodeToHandlers.getOrDefault(removed.targetNode(), Collections.emptySet()).contains(removed)
                    : "Remote recovery was not properly tracked [" + removed + "]";
                nodeToHandlers.computeIfPresent(removed.targetNode(), (k, handlersForNode) -> {
                    handlersForNode.remove(removed);
                    if (handlersForNode.isEmpty()) {
                        return null;
                    }
                    return handlersForNode;
                });
            }
            if (shardRecoveryContext.recoveryHandlers.isEmpty()) {
                ongoingRecoveries.remove(shard);
            }
            if (ongoingRecoveries.isEmpty()) {
                if (emptyListeners != null) {
                    final List<ActionListener<Void>> onEmptyListeners = emptyListeners;
                    emptyListeners = null;
                    ActionListener.onResponse(onEmptyListeners, null);
                }
            }
        }

        synchronized void cancel(IndexShard shard, String reason) {
            final ShardRecoveryContext shardRecoveryContext = ongoingRecoveries.get(shard);
            if (shardRecoveryContext != null) {
                final List<Exception> failures = new ArrayList<>();
                for (RecoverySourceHandler handlers : shardRecoveryContext.recoveryHandlers.keySet()) {
                    try {
                        handlers.cancel(reason);
                    } catch (Exception ex) {
                        failures.add(ex);
                    } finally {
                        shard.recoveryStats().decCurrentAsSource();
                    }
                }
                ExceptionsHelper.maybeThrowRuntimeAndSuppress(failures);
            }
        }

        void awaitEmpty() {
            assert lifecycle.stoppedOrClosed();
            final PlainActionFuture<Void> future;
            synchronized (this) {
                if (ongoingRecoveries.isEmpty()) {
                    return;
                }
                future = new PlainActionFuture<>();
                if (emptyListeners == null) {
                    emptyListeners = new ArrayList<>();
                }
                emptyListeners.add(future);
            }
            FutureUtils.get(future);
        }

        private final class ShardRecoveryContext {
            final Map<RecoverySourceHandler, RemoteRecoveryTargetHandler> recoveryHandlers = new HashMap<>();

            /**
             * Adds recovery source handler.
             */
            synchronized Tuple<RecoverySourceHandler, RemoteRecoveryTargetHandler> addNewRecovery(
                StartRecoveryRequest request,
                IndexShard shard
            ) {
                for (RecoverySourceHandler existingHandler : recoveryHandlers.keySet()) {
                    if (existingHandler.getRequest().targetAllocationId().equals(request.targetAllocationId())) {
                        throw new DelayRecoveryException(
                            "recovery with same target already registered, waiting for "
                                + "previous recovery attempt to be cancelled or completed"
                        );
                    }
                }
                final Tuple<RecoverySourceHandler, RemoteRecoveryTargetHandler> handlers = createRecoverySourceHandler(request, shard);
                recoveryHandlers.put(handlers.v1(), handlers.v2());
                return handlers;
            }

            /**
             * Adds recovery source handler.
             */
            synchronized void reestablishRecovery(ReestablishRecoveryRequest request, ActionListener<RecoveryResponse> listener) {
                RecoverySourceHandler handler = null;
                for (RecoverySourceHandler existingHandler : recoveryHandlers.keySet()) {
                    if (existingHandler.getRequest().recoveryId() == request.recoveryId()
                        && existingHandler.getRequest().targetAllocationId().equals(request.targetAllocationId())) {
                        handler = existingHandler;
                        break;
                    }
                }
                if (handler == null) {
                    throw new ResourceNotFoundException(
                        "Cannot reestablish recovery, recovery id [" + request.recoveryId() + "] not found."
                    );
                }
                handler.addListener(listener);
            }

            private Tuple<RecoverySourceHandler, RemoteRecoveryTargetHandler> createRecoverySourceHandler(
                StartRecoveryRequest request,
                IndexShard shard
            ) {
                RecoverySourceHandler handler;
                final RemoteRecoveryTargetHandler recoveryTarget = new RemoteRecoveryTargetHandler(
                    request.recoveryId(),
                    request.shardId(),
                    transportService,
                    request.targetNode(),
                    recoverySettings,
                    throttleTime -> shard.recoveryStats().addThrottleTime(throttleTime),
                    shard.isRemoteTranslogEnabled() || request.targetNode().isRemoteStoreNode()
                );
                handler = RecoverySourceHandlerFactory.create(shard, recoveryTarget, request, recoverySettings);
                return Tuple.tuple(handler, recoveryTarget);
            }
        }
    }
}
