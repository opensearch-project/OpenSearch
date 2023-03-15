/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.support.replication.ReplicationTask;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardClosedException;
import org.opensearch.index.shard.ShardNotInPrimaryModeException;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.common.ReplicationTimer;
import org.opensearch.node.NodeClosedException;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

import org.opensearch.action.support.replication.ReplicationMode;

/**
 * Replication action responsible for publishing checkpoint to a replica shard.
 *
 * @opensearch.internal
 */

public class PublishCheckpointAction extends TransportReplicationAction<
    PublishCheckpointRequest,
    PublishCheckpointRequest,
    ReplicationResponse> {

    public static final String ACTION_NAME = "indices:admin/publishCheckpoint";
    protected static Logger logger = LogManager.getLogger(PublishCheckpointAction.class);

    private final SegmentReplicationTargetService replicationService;

    @Inject
    public PublishCheckpointAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        SegmentReplicationTargetService targetService
    ) {
        super(
            settings,
            ACTION_NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            PublishCheckpointRequest::new,
            PublishCheckpointRequest::new,
            ThreadPool.Names.REFRESH
        );
        this.replicationService = targetService;
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected void doExecute(Task task, PublishCheckpointRequest request, ActionListener<ReplicationResponse> listener) {
        assert false : "use PublishCheckpointAction#publish";
    }

    @Override
    public ReplicationMode getReplicationMode(IndexShard indexShard) {
        if (indexShard.isRemoteTranslogEnabled()) {
            return ReplicationMode.FULL_REPLICATION;
        }
        return super.getReplicationMode(indexShard);
    }

    /**
     * Publish checkpoint request to shard
     */
    final void publish(IndexShard indexShard, ReplicationCheckpoint checkpoint) {
        String primaryAllocationId = indexShard.routingEntry().allocationId().getId();
        long primaryTerm = indexShard.getPendingPrimaryTerm();
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            // we have to execute under the system context so that if security is enabled the sync is authorized
            threadContext.markAsSystemContext();
            PublishCheckpointRequest request = new PublishCheckpointRequest(checkpoint);
            final ReplicationTask task = (ReplicationTask) taskManager.register("transport", "segrep_publish_checkpoint", request);
            final ReplicationTimer timer = new ReplicationTimer();
            timer.start();
            transportService.sendChildRequest(
                indexShard.recoveryState().getTargetNode(),
                transportPrimaryAction,
                new ConcreteShardRequest<>(request, primaryAllocationId, primaryTerm),
                task,
                transportOptions,
                new TransportResponseHandler<ReplicationResponse>() {
                    @Override
                    public ReplicationResponse read(StreamInput in) throws IOException {
                        return newResponseInstance(in);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public void handleResponse(ReplicationResponse response) {
                        timer.stop();
                        logger.trace(
                            () -> new ParameterizedMessage(
                                "[shardId {}] Completed publishing checkpoint [{}], timing: {}",
                                indexShard.shardId().getId(),
                                checkpoint,
                                timer.time()
                            )
                        );
                        task.setPhase("finished");
                        taskManager.unregister(task);
                    }

                    @Override
                    public void handleException(TransportException e) {
                        timer.stop();
                        logger.trace("[shardId {}] Failed to publish checkpoint, timing: {}", indexShard.shardId().getId(), timer.time());
                        task.setPhase("finished");
                        taskManager.unregister(task);
                        if (ExceptionsHelper.unwrap(
                            e,
                            NodeClosedException.class,
                            IndexNotFoundException.class,
                            AlreadyClosedException.class,
                            IndexShardClosedException.class,
                            ShardNotInPrimaryModeException.class
                        ) != null) {
                            // Node is shutting down or the index was deleted or the shard is closed
                            return;
                        }
                        logger.warn(
                            new ParameterizedMessage("{} segment replication checkpoint publishing failed", indexShard.shardId()),
                            e
                        );
                    }
                }
            );
            logger.trace(
                () -> new ParameterizedMessage(
                    "[shardId {}] Publishing replication checkpoint [{}]",
                    checkpoint.getShardId().getId(),
                    checkpoint
                )
            );
        }
    }

    @Override
    protected void shardOperationOnPrimary(
        PublishCheckpointRequest request,
        IndexShard primary,
        ActionListener<PrimaryResult<PublishCheckpointRequest, ReplicationResponse>> listener
    ) {
        ActionListener.completeWith(listener, () -> new PrimaryResult<>(request, new ReplicationResponse()));
    }

    @Override
    protected void shardOperationOnReplica(PublishCheckpointRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        Objects.requireNonNull(request);
        Objects.requireNonNull(replica);
        ActionListener.completeWith(listener, () -> {
            logger.trace(() -> new ParameterizedMessage("Checkpoint {} received on replica {}", request, replica.shardId()));
            if (request.getCheckpoint().getShardId().equals(replica.shardId())) {
                replicationService.onNewCheckpoint(request.getCheckpoint(), replica);
            }
            return new ReplicaResult();
        });
    }
}
