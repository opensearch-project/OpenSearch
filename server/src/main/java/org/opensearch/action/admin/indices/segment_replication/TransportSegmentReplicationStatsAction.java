/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.segment_replication;

import org.opensearch.OpenSearchStatusException;
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
import org.opensearch.indices.replication.SegmentReplicationState;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.rest.RestStatus;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

/**
 * Transport action for shard segment replication operation. This transport action does not actually
 * perform segment replication, it only reports on metrics of segment replication event (both active and complete).
 *
 * @opensearch.internal
 */
public class TransportSegmentReplicationStatsAction extends TransportBroadcastByNodeAction<
    SegmentReplicationStatsRequest,
    SegmentReplicationStatsResponse,
    SegmentReplicationState> {

    private final SegmentReplicationTargetService targetService;
    private final IndicesService indicesService;
    private String singleIndexWithSegmentReplicationDisabled = null;

    @Inject
    public TransportSegmentReplicationStatsAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        SegmentReplicationTargetService targetService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            SegmentReplicationStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            SegmentReplicationStatsRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
        this.targetService = targetService;
    }

    @Override
    protected SegmentReplicationState readShardResult(StreamInput in) throws IOException {
        return new SegmentReplicationState(in);
    }

    @Override
    protected SegmentReplicationStatsResponse newResponse(
        SegmentReplicationStatsRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<SegmentReplicationState> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        // throw exception if API call is made on single index with segment replication disabled.
        if (singleIndexWithSegmentReplicationDisabled != null) {
            String index = singleIndexWithSegmentReplicationDisabled;
            singleIndexWithSegmentReplicationDisabled = null;
            throw new OpenSearchStatusException("Segment Replication is not enabled on Index: " + index, RestStatus.BAD_REQUEST);
        }
        String[] shards = request.shards();
        Set<String> set = new HashSet<>();
        if (shards.length > 0) {
            for (String shard : shards) {
                set.add(shard);
            }
        }
        Map<String, List<SegmentReplicationState>> shardResponses = new HashMap<>();
        for (SegmentReplicationState segmentReplicationState : responses) {
            if (segmentReplicationState == null) {
                continue;
            }

            // Limit responses to only specific shard id's passed in query paramter shards.
            int shardId = segmentReplicationState.getShardRouting().shardId().id();
            if (shards.length > 0 && set.contains(Integer.toString(shardId)) == false) {
                continue;
            }
            String indexName = segmentReplicationState.getShardRouting().getIndexName();
            if (!shardResponses.containsKey(indexName)) {
                shardResponses.put(indexName, new ArrayList<>());
            }
            shardResponses.get(indexName).add(segmentReplicationState);
        }
        return new SegmentReplicationStatsResponse(totalShards, successfulShards, failedShards, shardResponses, shardFailures);
    }

    @Override
    protected SegmentReplicationStatsRequest readRequestFrom(StreamInput in) throws IOException {
        return new SegmentReplicationStatsRequest(in);
    }

    @Override
    protected SegmentReplicationState shardOperation(SegmentReplicationStatsRequest request, ShardRouting shardRouting) {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());

        // check if API call is made on single index with segment replication disabled.
        if (request.indices().length == 1 && indexShard.indexSettings().isSegRepEnabled() == false) {
            singleIndexWithSegmentReplicationDisabled = shardRouting.getIndexName();
            return null;
        }
        if (indexShard.indexSettings().isSegRepEnabled() == false) {
            return null;
        }

        // return information about only on-going segment replication events.
        if (request.activeOnly()) {
            return targetService.getOngoingEventSegmentReplicationState(shardRouting);
        }

        // return information about only latest completed segment replication events.
        if (request.completedOnly()) {
            return targetService.getlatestCompletedEventSegmentReplicationState(shardRouting);
        }
        return targetService.getSegmentReplicationState(shardRouting);
    }

    @Override
    protected ShardsIterator shards(ClusterState state, SegmentReplicationStatsRequest request, String[] concreteIndices) {
        return state.routingTable().allShardsIncludingRelocationTargets(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, SegmentReplicationStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(
        ClusterState state,
        SegmentReplicationStatsRequest request,
        String[] concreteIndices
    ) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }
}
