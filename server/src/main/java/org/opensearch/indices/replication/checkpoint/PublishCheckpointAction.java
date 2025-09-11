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
import org.opensearch.action.support.replication.ReplicationMode;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Replication action responsible for publishing checkpoint to a replica shard.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.2.0")
public class PublishCheckpointAction extends AbstractPublishCheckpointAction<PublishCheckpointRequest, PublishCheckpointRequest> {
    private static final String TASK_ACTION_NAME = "segrep_publish_checkpoint";
    public static final String ACTION_NAME = "indices:admin/publishCheckpoint";
    protected static Logger logger = LogManager.getLogger(PublishCheckpointAction.class);

    private final SegmentReplicationTargetService replicationService;

    /**
     * The timeout for retrying publish checkpoint requests.
     */
    public static final Setting<TimeValue> PUBLISH_CHECK_POINT_RETRY_TIMEOUT = Setting.timeSetting(
        "indices.publish_check_point.retry_timeout",
        TimeValue.timeValueMinutes(5),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

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
            ThreadPool.Names.REFRESH,
            logger
        );
        this.replicationService = targetService;
    }

    @Override
    protected Setting<TimeValue> getRetryTimeoutSetting() {
        return PUBLISH_CHECK_POINT_RETRY_TIMEOUT;
    }

    @Override
    protected void doExecute(Task task, PublishCheckpointRequest request, ActionListener<ReplicationResponse> listener) {
        assert false : "use PublishCheckpointAction#publish";
    }

    @Override
    public ReplicationMode getReplicationMode(IndexShard indexShard) {
        return super.getReplicationMode(indexShard);
    }

    /**
     * Publish checkpoint request to shard
     */
    final void publish(IndexShard indexShard, ReplicationCheckpoint checkpoint) {
        doPublish(indexShard, checkpoint, new PublishCheckpointRequest(checkpoint), TASK_ACTION_NAME, false, null, ActionListener.noOp());
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
    protected void doReplicaOperation(PublishCheckpointRequest request, IndexShard replica) {
        if (request.getCheckpoint().getShardId().equals(replica.shardId())) {
            replicationService.onNewCheckpoint(request.getCheckpoint(), replica);
        }
    }
}
