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
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Replication action responsible for publishing referenced segments to a replica shard.
 *
 * @opensearch.api
 */
@ExperimentalApi
public class PublishReferencedSegmentsAction extends AbstractPublishCheckpointAction<
    PublishReferencedSegmentsRequest,
    PublishReferencedSegmentsRequest> {

    public static final String ACTION_NAME = "indices:admin/publish_referenced_segments";
    private static final String TASK_ACTION_NAME = "segrep_publish_referenced_segments";
    protected static Logger logger = LogManager.getLogger(PublishReferencedSegmentsAction.class);

    @Inject
    public PublishReferencedSegmentsAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters
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
            PublishReferencedSegmentsRequest::new,
            PublishReferencedSegmentsRequest::new,
            ThreadPool.Names.GENERIC,
            logger
        );
    }

    @Override
    protected void doExecute(Task task, PublishReferencedSegmentsRequest request, ActionListener<ReplicationResponse> listener) {
        assert false : "use PublishReferencedSegmentsAction#publish";
    }

    /**
     * Publish referenced segment request to shard
     */
    final void publish(IndexShard indexShard, ReferencedSegmentsCheckpoint checkpoint) {
        doPublish(
            indexShard,
            checkpoint,
            new PublishReferencedSegmentsRequest(checkpoint),
            TASK_ACTION_NAME,
            false,
            indexShard.getRecoverySettings().getMergedSegmentReplicationTimeout(),
            ActionListener.noOp()
        );
    }

    @Override
    protected void shardOperationOnPrimary(
        PublishReferencedSegmentsRequest request,
        IndexShard primary,
        ActionListener<PrimaryResult<PublishReferencedSegmentsRequest, ReplicationResponse>> listener
    ) {
        ActionListener.completeWith(listener, () -> new PrimaryResult<>(request, new ReplicationResponse()));
    }

    @Override
    protected void doReplicaOperation(PublishReferencedSegmentsRequest request, IndexShard replica) {
        if (request.getReferencedSegmentsCheckpoint().getShardId().equals(replica.shardId())) {
            replica.cleanupRedundantPendingMergeSegment(request.getReferencedSegmentsCheckpoint());
        }
    }
}
