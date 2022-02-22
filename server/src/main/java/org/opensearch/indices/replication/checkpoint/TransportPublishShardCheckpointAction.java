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
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.SegmentReplicationReplicaService;
import org.opensearch.indices.replication.copy.PrimaryShardReplicationSource;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

public class TransportPublishShardCheckpointAction extends TransportReplicationAction<
    ShardPublishCheckpointRequest,
    ShardPublishCheckpointRequest,
    ReplicationResponse> {

    protected static Logger logger = LogManager.getLogger(TransportPublishShardCheckpointAction.class);

    public static final String ACTION_NAME = PublishCheckpointAction.NAME + "[s]";

    private final SegmentReplicationReplicaService replicationService;
    private final PrimaryShardReplicationSource source;

    @Inject
    public TransportPublishShardCheckpointAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        SegmentReplicationReplicaService segmentCopyService,
        PrimaryShardReplicationSource source) {
        super(
            settings,
            ACTION_NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            ShardPublishCheckpointRequest::new,
            ShardPublishCheckpointRequest::new,
            ThreadPool.Names.SNAPSHOT
        );
        this.replicationService = segmentCopyService;
        this.source = source;
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected void shardOperationOnPrimary(ShardPublishCheckpointRequest shardRequest, IndexShard primary, ActionListener<PrimaryResult<ShardPublishCheckpointRequest, ReplicationResponse>> listener) {
        ActionListener.completeWith(listener, () -> new PrimaryResult<>(shardRequest, new ReplicationResponse()));
    }

    @Override
    protected void shardOperationOnReplica(ShardPublishCheckpointRequest shardRequest, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            PublishCheckpointRequest request = shardRequest.getRequest();
            logger.trace("Checkpoint received on replica {}", request);
            if (request.getCheckpoint().getShardId().equals(replica.shardId())) {
                replica.onNewCheckpoint(request, source, replicationService);
            }
            // TODO: Segrep - These requests are getting routed to all shards across all indices.
            //  We should only publish to replicas of the updated index.
            return new ReplicaResult();
        });
    }
}
