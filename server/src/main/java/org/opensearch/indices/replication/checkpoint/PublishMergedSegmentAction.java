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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.checkpoint.MergedSegmentPublisher.PublishAction;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Replication action responsible for publishing merged segment to a replica shard.
 *
 * @opensearch.api
 */
@ExperimentalApi
public class PublishMergedSegmentAction extends AbstractPublishCheckpointAction<PublishMergedSegmentRequest, PublishMergedSegmentRequest>
    implements
        PublishAction {
    private static final String TASK_ACTION_NAME = "segrep_publish_merged_segment";
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
            ThreadPool.Names.GENERIC,
            logger
        );
        this.replicationService = targetService;
    }

    @Override
    protected void doExecute(Task task, PublishMergedSegmentRequest request, ActionListener<ReplicationResponse> listener) {
        assert false : "use PublishMergedSegmentAction#publish";
    }

    /**
     * Publish merged segment request to shard
     */
    final public void publish(IndexShard indexShard, MergedSegmentCheckpoint checkpoint) {
        doPublish(
            indexShard,
            checkpoint,
            new PublishMergedSegmentRequest(checkpoint),
            TASK_ACTION_NAME,
            true,
            indexShard.getRecoverySettings().getMergedSegmentReplicationTimeout(),
            ActionListener.noOp()
        );
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
    protected void doReplicaOperation(PublishMergedSegmentRequest request, IndexShard replica) {
        if (request.getMergedSegment().getShardId().equals(replica.shardId())) {
            replicationService.onNewMergedSegmentCheckpoint(request.getMergedSegment(), replica);
        }
    }
}
