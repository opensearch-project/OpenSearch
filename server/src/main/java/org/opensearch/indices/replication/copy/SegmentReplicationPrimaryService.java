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

package org.opensearch.indices.replication.copy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.ChannelActionListener;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.index.IndexService;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.DelayRecoveryException;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RemoteRecoveryTargetHandler;
import org.opensearch.indices.replication.checkpoint.TransportCheckpointInfoResponse;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.Transports;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Request handlers for the primary shard during segment copy.
 */
public class SegmentReplicationPrimaryService {

    private static final Logger logger = LogManager.getLogger(SegmentReplicationPrimaryService.class);

    public static class Actions {
        public static final String GET_CHECKPOINT_INFO = "internal:index/shard/segrep/checkpoint_info";
        public static final String GET_FILES = "internal:index/shard/segrep/get_files";
        public static final String TRACK_SHARD = "internal:index/shard/segrep/track_shard";
    }

    private final TransportService transportService;
    private final IndicesService indicesService;
    private final RecoverySettings recoverySettings;

    final CopyStateCache commitCache = new CopyStateCache();

    @Inject
    public SegmentReplicationPrimaryService(
        TransportService transportService,
        IndicesService indicesService,
        RecoverySettings recoverySettings
    ) {
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.recoverySettings = recoverySettings;
        // When the target node wants to start a peer recovery it sends a START_RECOVERY request to the source
        // node. Upon receiving START_RECOVERY, the source node will initiate the peer recovery.
        transportService.registerRequestHandler(
            Actions.GET_CHECKPOINT_INFO,
            ThreadPool.Names.GENERIC,
            StartReplicationRequest::new,
            new StartReplicationRequestHandler()
        );

        transportService.registerRequestHandler(
            Actions.GET_FILES,
            ThreadPool.Names.GENERIC,
            GetFilesRequest::new,
            new GetFilesRequestHandler()
        );

        transportService.registerRequestHandler(
            Actions.TRACK_SHARD,
            ThreadPool.Names.GENERIC,
            TrackShardRequest::new,
            new TrackShardRequestHandler()
        );
    }

    private static final class CopyStateCache {
        private final Map<ReplicationCheckpoint, CopyState> checkpointCopyState = Collections.synchronizedMap(new HashMap<>());

        public void addCopyState(CopyState copyState) {
            checkpointCopyState.putIfAbsent(copyState.getCheckpoint(), copyState);
        }

        public CopyState getCopyStateForCheckpoint(ReplicationCheckpoint checkpoint) {
            return checkpointCopyState.get(checkpoint);
        }

        public boolean hasCheckpoint(ReplicationCheckpoint checkpoint) {
            return checkpointCopyState.containsKey(checkpoint);
        }

        public synchronized void removeCopyState(ReplicationCheckpoint checkpoint) {
            final Optional<CopyState> nrtCopyState = Optional.ofNullable(checkpointCopyState.get(checkpoint));
            nrtCopyState.ifPresent((state) -> {
                if (state.decRef()) {
                    // decRef() returns true if there are no longer any references, if so remove it from our cache.
                    checkpointCopyState.remove(checkpoint);
                }
            });
        }
    }

    private synchronized CopyState getCopyState(ReplicationCheckpoint checkpoint) throws IOException {
        if (commitCache.hasCheckpoint(checkpoint)) {
            final CopyState copyState = commitCache.getCopyStateForCheckpoint(checkpoint);
            copyState.incRef();
            return copyState;
        }
        final CopyState copyState = buildCopyState(checkpoint.getShardId());
        commitCache.addCopyState(copyState);
        return copyState;
    }

    private CopyState buildCopyState(ShardId shardId) throws IOException {
        final IndexService indexService = indicesService.indexService(shardId.getIndex());
        final IndexShard shard = indexService.getShard(shardId.id());
        return new CopyState(shard);
    }

    private class StartReplicationRequestHandler implements TransportRequestHandler<StartReplicationRequest> {
        @Override
        public void messageReceived(StartReplicationRequest request, TransportChannel channel, Task task) throws Exception {
            final ReplicationCheckpoint checkpoint = request.getCheckpoint();
            logger.trace("Received request for checkpoint {}", checkpoint);
            final CopyState copyState = getCopyState(checkpoint);
            channel.sendResponse(
                new TransportCheckpointInfoResponse(
                    copyState.getCheckpoint(),
                    copyState.getMetadataSnapshot(),
                    copyState.getInfosBytes(),
                    copyState.getPendingDeleteFiles()
                )
            );
        }
    }

    class GetFilesRequestHandler implements TransportRequestHandler<GetFilesRequest> {
        @Override
        public void messageReceived(GetFilesRequest request, TransportChannel channel, Task task) throws Exception {
            if (commitCache.hasCheckpoint(request.getCheckpoint())) {
                sendFiles(request, new ChannelActionListener<>(channel, Actions.GET_FILES, request));
            } else {
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        }
    }

    private void sendFiles(GetFilesRequest request, ActionListener<GetFilesResponse> listener) {
        final ShardId shardId = request.getCheckpoint().getShardId();
        logger.trace("Requested file copy for checkpoint {}", request.getCheckpoint());

        final CopyState copyState = commitCache.getCopyStateForCheckpoint(request.getCheckpoint());

        final IndexService indexService = indicesService.indexService(shardId.getIndex());
        final IndexShard shard = indexService.getShard(shardId.id());
        final RemoteRecoveryTargetHandler remoteRecoveryTargetHandler = new RemoteRecoveryTargetHandler(request.getReplicationId(), shardId, transportService, request.getTargetNode(), recoverySettings, throttleTime -> shard.recoveryStats().addThrottleTime(throttleTime));
        PrimaryShardReplicationHandler handler = new PrimaryShardReplicationHandler(
            request.getReplicationId(),
            shard,
            request.getTargetNode(),
            request.getTargetAllocationId(),
            remoteRecoveryTargetHandler,
            shard.getThreadPool(),
            request,
            Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
            recoverySettings.getMaxConcurrentFileChunks(),
            recoverySettings.getMaxConcurrentOperations()
        );
        logger.debug("[{}][{}] fetching files for {}", shardId.getIndex().getName(), shardId.id(), request.getTargetNode());
        // TODO: The calling shard could die between requests without finishing.
        handler.sendFiles(copyState, ActionListener.runAfter(listener, () -> commitCache.removeCopyState(request.getCheckpoint())));
    }

    class TrackShardRequestHandler implements TransportRequestHandler<TrackShardRequest> {
        @Override
        public void messageReceived(TrackShardRequest request, TransportChannel channel, Task task) throws Exception {
            final ShardId shardId = request.getShardId();
            final String targetAllocationId = request.getTargetAllocationId();

            final IndexService indexService = indicesService.indexService(shardId.getIndex());
            final IndexShard shard = indexService.getShard(shardId.id());

            final IndexShardRoutingTable routingTable = shard.getReplicationGroup().getRoutingTable();

            if (routingTable.getByAllocationId(targetAllocationId) == null) {
                throw new DelayRecoveryException("source node does not have the shard listed in its state as allocated on the node");
            }
            final StepListener<ReplicationResponse> addRetentionLeaseStep = new StepListener<>();
            final Consumer<Exception> onFailure = e -> {
                assert Transports.assertNotTransportThread(this + "[onFailure]");
                logger.error(
                    new ParameterizedMessage(
                        "Error marking shard {} as tracked for allocation ID {}",
                        shardId,
                        request.getTargetAllocationId()
                    ),
                    e
                );
                try {
                    channel.sendResponse(e);
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    logger.warn("failed to send back failure on track shard request", inner);
                }
            };
            PrimaryShardReplicationHandler.runUnderPrimaryPermit(
                () -> shard.cloneLocalPeerRecoveryRetentionLease(
                    request.getTargetNode().getId(),
                    new ThreadedActionListener<>(logger, shard.getThreadPool(), ThreadPool.Names.GENERIC, addRetentionLeaseStep, false)
                ),
                "Add retention lease step",
                shard,
                new CancellableThreads(),
                logger
            );
            addRetentionLeaseStep.whenComplete(r -> {
                PrimaryShardReplicationHandler.runUnderPrimaryPermit(
                    () -> shard.initiateTracking(targetAllocationId),
                    shardId + " initiating tracking of " + targetAllocationId,
                    shard,
                    new CancellableThreads(),
                    logger
                );
                PrimaryShardReplicationHandler.runUnderPrimaryPermit(
                    () -> shard.updateLocalCheckpointForShard(targetAllocationId, SequenceNumbers.NO_OPS_PERFORMED),
                    shardId + " marking " + targetAllocationId + " as in sync",
                    shard,
                    new CancellableThreads(),
                    logger
                );
                channel.sendResponse(new TrackShardResponse());
            }, onFailure);
        }
    }
}
