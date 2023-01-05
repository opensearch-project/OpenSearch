/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.segment_replication;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.SegmentReplicationStatsState;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportSegmentReplicationAction extends TransportBroadcastByNodeAction<SegmentReplicationRequest, SegmentReplicationResponse, SegmentReplicationStatsState>  {

    private final IndicesService indicesService;

    @Inject
    public TransportSegmentReplicationAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            SegmentReplicationAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            SegmentReplicationRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
    }

    @Override
    protected SegmentReplicationStatsState readShardResult(StreamInput in) throws IOException {
        return new SegmentReplicationStatsState(in);
    }

    @Override
    protected SegmentReplicationResponse newResponse(
        SegmentReplicationRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<SegmentReplicationStatsState> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        Map<String, List<SegmentReplicationStatsState>> shardResponses = new HashMap<>();
        for (SegmentReplicationStatsState segmentReplicationStatsState : responses) {
            if (segmentReplicationStatsState == null) {
                continue;
            }
            String indexName = segmentReplicationStatsState.getShardRouting().getIndexName();
            if (!shardResponses.containsKey(indexName)) {
                shardResponses.put(indexName, new ArrayList<>());
            }
            if (request.activeOnly()) {
                shardResponses.get(indexName).add(segmentReplicationStatsState);
            } else {
                shardResponses.get(indexName).add(segmentReplicationStatsState);
            }
        }
        return new SegmentReplicationResponse(totalShards, successfulShards, failedShards, shardResponses, shardFailures);
    }

    @Override
    protected SegmentReplicationRequest readRequestFrom(StreamInput in) throws IOException {
        return new SegmentReplicationRequest(in);
    }

    @Override
    protected SegmentReplicationStatsState shardOperation(SegmentReplicationRequest request, ShardRouting shardRouting) {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
        return indexShard.getSegmentReplicationStatsState();
    }

    @Override
    protected ShardsIterator shards(ClusterState state, SegmentReplicationRequest request, String[] concreteIndices) {
        return state.routingTable().allShardsIncludingRelocationTargets(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, SegmentReplicationRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, SegmentReplicationRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }
}
