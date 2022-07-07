/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexCommit;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.ChannelActionListener;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RunUnderPrimaryPermit;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RetryableTransportClient;
import org.opensearch.indices.replication.common.CopyState;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.index.seqno.ReplicationTracker.getPeerRecoveryRetentionLeaseId;
import static org.opensearch.index.translog.Translog.TRANSLOG_UUID_KEY;

/**
 * Service class that handles segment replication requests from replica shards.
 * Typically, the "source" is a primary shard. This code executes on the source node.
 *
 * @opensearch.internal
 */
public final class SegmentReplicationSourceService extends AbstractLifecycleComponent implements ClusterStateListener, IndexEventListener {

    private static final Logger logger = LogManager.getLogger(SegmentReplicationSourceService.class);
    private final RecoverySettings recoverySettings;
    private final TransportService transportService;
    private final IndicesService indicesService;

    /**
     * Internal actions used by the segment replication source service on the primary shard
     *
     * @opensearch.internal
     */
    public static class Actions {

        public static final String PREPARE_SHARD = "internal:index/shard/replication/prepare_shard";
        public static final String GET_CHECKPOINT_INFO = "internal:index/shard/replication/get_checkpoint_info";
        public static final String GET_SEGMENT_FILES = "internal:index/shard/replication/get_segment_files";
    }

    private final OngoingSegmentReplications ongoingSegmentReplications;

    public SegmentReplicationSourceService(
        IndicesService indicesService,
        TransportService transportService,
        RecoverySettings recoverySettings
    ) {
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.recoverySettings = recoverySettings;
        transportService.registerRequestHandler(
            Actions.PREPARE_SHARD,
            ThreadPool.Names.GENERIC,
            PrepareShardRequest::new,
            (request, channel, task) -> {
                final ShardId shardId = request.getShardId();
                final IndexService indexService = indicesService.indexService(shardId.getIndex());
                final IndexShard indexShard = indexService.getShard(shardId.id());
                try (final GatedCloseable<IndexCommit> safeIndexCommit = indexShard.acquireSafeIndexCommit()) {
                    final String translogUUID = safeIndexCommit.get().getUserData().get(TRANSLOG_UUID_KEY);
                    channel.sendResponse(new PrepareShardResponse(translogUUID));
                }
            }
        );
        transportService.registerRequestHandler(
            Actions.GET_CHECKPOINT_INFO,
            ThreadPool.Names.GENERIC,
            CheckpointInfoRequest::new,
            new CheckpointInfoRequestHandler()
        );
        transportService.registerRequestHandler(
            Actions.GET_SEGMENT_FILES,
            ThreadPool.Names.GENERIC,
            GetSegmentFilesRequest::new,
            new GetSegmentFilesRequestHandler()
        );
        this.ongoingSegmentReplications = new OngoingSegmentReplications(indicesService, recoverySettings);
    }

    private class CheckpointInfoRequestHandler implements TransportRequestHandler<CheckpointInfoRequest> {
        @Override
        public void messageReceived(CheckpointInfoRequest request, TransportChannel channel, Task task) throws Exception {
            final RemoteSegmentFileChunkWriter segmentSegmentFileChunkWriter = new RemoteSegmentFileChunkWriter(
                request.getReplicationId(),
                recoverySettings,
                new RetryableTransportClient(
                    transportService,
                    request.getTargetNode(),
                    recoverySettings.internalActionRetryTimeout(),
                    logger
                ),
                request.getCheckpoint().getShardId(),
                SegmentReplicationTargetService.Actions.FILE_CHUNK,
                new AtomicLong(0),
                (throttleTime) -> {}
            );
            ShardId shardId = request.getCheckpoint().getShardId();
            final IndexService indexService = indicesService.indexService(shardId.getIndex());
            final IndexShard indexShard = indexService.getShard(shardId.id());
            final StepListener<ReplicationResponse> addRetentionLeaseStep = new StepListener<>();
            if (indexShard.getRetentionLeases().contains(getPeerRecoveryRetentionLeaseId(request.getTargetNode().getId())) == false) {
                RunUnderPrimaryPermit.run(
                    () -> indexShard.cloneLocalPeerRecoveryRetentionLease(
                        request.getTargetNode().getId(),
                        new ThreadedActionListener<>(
                            logger,
                            indexShard.getThreadPool(),
                            ThreadPool.Names.GENERIC,
                            addRetentionLeaseStep,
                            false
                        )
                    ),
                    "Add retention lease step",
                    indexShard,
                    new CancellableThreads(),
                    logger
                );
            } else {
                addRetentionLeaseStep.onResponse(new ReplicationResponse());
            }
            addRetentionLeaseStep.whenComplete(r -> {
                RunUnderPrimaryPermit.run(
                    () -> indexShard.initiateTracking(request.getTargetAllocationId()),
                    shardId + " initiating tracking of " + request.getTargetAllocationId(),
                    indexShard,
                    new CancellableThreads(),
                    logger
                );
                indexShard.refresh("Recovering new shard");
                final CopyState copyState = ongoingSegmentReplications.prepareForReplication(request, segmentSegmentFileChunkWriter);
                channel.sendResponse(
                    new CheckpointInfoResponse(
                        copyState.getCheckpoint(),
                        copyState.getMetadataSnapshot(),
                        copyState.getInfosBytes(),
                        copyState.getPendingDeleteFiles()
                    )
                );
            }, (e) -> {
                try {
                    channel.sendResponse(e);
                } catch (IOException ex) {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "Failed to send error response for action [{}] and request [{}]",
                            Actions.GET_CHECKPOINT_INFO,
                            request
                        ),
                        ex
                    );
                }
            });

        }
    }

    private class GetSegmentFilesRequestHandler implements TransportRequestHandler<GetSegmentFilesRequest> {
        @Override
        public void messageReceived(GetSegmentFilesRequest request, TransportChannel channel, Task task) throws Exception {
            ongoingSegmentReplications.startSegmentCopy(request, new ChannelActionListener<>(channel, Actions.GET_SEGMENT_FILES, request));
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                ongoingSegmentReplications.cancelReplication(removedNode);
            }
        }
    }

    @Override
    protected void doStart() {
        final ClusterService clusterService = indicesService.clusterService();
        if (DiscoveryNode.isDataNode(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
    }

    @Override
    protected void doStop() {
        final ClusterService clusterService = indicesService.clusterService();
        if (DiscoveryNode.isDataNode(clusterService.getSettings())) {
            indicesService.clusterService().removeListener(this);
        }
    }

    @Override
    protected void doClose() throws IOException {

    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            ongoingSegmentReplications.cancel(indexShard, "shard is closed");
        }
    }
}
