/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.support.replication.TransportBroadcastReplicationAction;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.index.shard.ShardId;
import org.opensearch.transport.TransportService;

import java.util.List;

public class TransportPublishCheckpointAction extends TransportBroadcastReplicationAction<
    PublishCheckpointRequest,
    RefreshResponse,
    ShardPublishCheckpointRequest,
    ReplicationResponse> {

    @Inject
    public TransportPublishCheckpointAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        TransportPublishShardCheckpointAction shardCheckpointAction
    ) {
        super(
            PublishCheckpointAction.NAME,
            PublishCheckpointRequest::new,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            shardCheckpointAction
        );
    }

    @Override
    protected ReplicationResponse newShardResponse() {
        return new ReplicationResponse();
    }

    @Override
    protected ShardPublishCheckpointRequest newShardRequest(PublishCheckpointRequest request, ShardId shardId) {
        return new ShardPublishCheckpointRequest(request, shardId);
    }

    @Override
    protected RefreshResponse newResponse(
        int successfulShards,
        int failedShards,
        int totalNumCopies,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        return new RefreshResponse(totalNumCopies, successfulShards, failedShards, shardFailures);
    }
}
