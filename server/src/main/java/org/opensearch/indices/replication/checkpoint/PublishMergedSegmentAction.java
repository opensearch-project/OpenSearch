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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.replication.ReplicationMode;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.support.replication.ReplicationTask;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.ThreadContextAccess;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Replication action responsible for publishing merged segment to a replica shard.
 *
 * @opensearch.api
 */
@ExperimentalApi
public class PublishMergedSegmentAction extends TransportReplicationAction<
    PublishMergedSegmentRequest,
    PublishMergedSegmentRequest,
    ReplicationResponse> {

    public static final String ACTION_NAME = "indices:admin/publish_merged_segment";
    protected static Logger logger = LogManager.getLogger(PublishMergedSegmentAction.class);

    private final SegmentReplicationTargetService replicationService;

    @Inject
    public PublishMergedSegmentAction(
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
            PublishMergedSegmentRequest::new,
            PublishMergedSegmentRequest::new,
            ThreadPool.Names.GENERIC
        );
        this.replicationService = targetService;
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected void doExecute(Task task, PublishMergedSegmentRequest request, ActionListener<ReplicationResponse> listener) {
        assert false : "use PublishMergedSegmentAction#publish";
    }

    @Override
    public ReplicationMode getReplicationMode(IndexShard indexShard) {
        if (indexShard.indexSettings().isAssignedOnRemoteNode()) {
            return ReplicationMode.FULL_REPLICATION;
        }
        return super.getReplicationMode(indexShard);
    }

    /**
     * Publish merged segment request to shard
     */
    final void publish(IndexShard indexShard, MergeSegmentCheckpoint checkpoint) {
        String primaryAllocationId = indexShard.routingEntry().allocationId().getId();
        long primaryTerm = indexShard.getPendingPrimaryTerm();
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            // we have to execute under the system context so that if security is enabled the sync is authorized
            ThreadContextAccess.doPrivilegedVoid(threadContext::markAsSystemContext);
            PublishMergedSegmentRequest request = new PublishMergedSegmentRequest(checkpoint);
            final ReplicationTask task = (ReplicationTask) taskManager.register("transport", "segrep_publish_merged_segment", request);
            final ReplicationTimer timer = new ReplicationTimer();
            timer.start();
            CountDownLatch latch = new CountDownLatch(1);
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
                        try {
                            timer.stop();
                            logger.debug(
                                () -> new ParameterizedMessage(
                                    "[shardId {}] Completed publishing merged segment [{}], timing: {}",
                                    indexShard.shardId().getId(),
                                    checkpoint,
                                    timer.time()
                                )
                            );
                            task.setPhase("finished");
                            taskManager.unregister(task);
                        } finally {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void handleException(TransportException e) {
                        try {
                            timer.stop();
                            logger.debug(
                                "[shardId {}] Failed to publish merged segment [{}], timing: {}",
                                indexShard.shardId().getId(),
                                checkpoint,
                                timer.time()
                            );
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
                                new ParameterizedMessage("{} merged segment [{}] publishing failed", indexShard.shardId(), checkpoint),
                                e
                            );
                        } finally {
                            latch.countDown();
                        }
                    }
                }
            );
            logger.trace(
                () -> new ParameterizedMessage("[shardId {}] Publishing merged segment [{}]", checkpoint.getShardId().getId(), checkpoint)
            );
            try {
                latch.await(indexShard.getRecoverySettings().getMergedSegmentReplicationTimeout().seconds(), TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.warn(() -> new ParameterizedMessage("Interrupted while waiting for pre copy merged segment [{}]", checkpoint), e);
            }
        }
    }

    @Override
    protected void shardOperationOnPrimary(
        PublishMergedSegmentRequest request,
        IndexShard primary,
        ActionListener<PrimaryResult<PublishMergedSegmentRequest, ReplicationResponse>> listener
    ) {
        ActionListener.completeWith(listener, () -> new PrimaryResult<>(request, new ReplicationResponse()));
    }

    @Override
    protected void shardOperationOnReplica(
        PublishMergedSegmentRequest request,
        IndexShard replica,
        ActionListener<ReplicaResult> listener
    ) {
        Objects.requireNonNull(request);
        Objects.requireNonNull(replica);
        ActionListener.completeWith(listener, () -> {
            logger.trace(() -> new ParameterizedMessage("Merged segment {} received on replica {}", request, replica.shardId()));
            // Condition for ensuring that we ignore Segrep checkpoints received on Docrep shard copies.
            // This case will hit iff the replica hosting node is not remote enabled and replication type != SEGMENT
            if (replica.indexSettings().isAssignedOnRemoteNode() == false && replica.indexSettings().isSegRepLocalEnabled() == false) {
                logger.trace("Received merged segment on a docrep shard copy during an ongoing remote migration. NoOp.");
                return new ReplicaResult();
            }
            if (request.getMergedSegment().getShardId().equals(replica.shardId())) {
                replicationService.onNewMergedSegmentCheckpoint(request.getMergedSegment(), replica);
            }
            return new ReplicaResult();
        });
    }
}
