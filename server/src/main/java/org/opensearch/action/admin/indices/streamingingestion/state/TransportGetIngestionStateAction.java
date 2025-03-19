/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.state;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Transport action for retrieving ingestion state.
 *
 * @opensearch.experimental
 */
public class TransportGetIngestionStateAction extends TransportBroadcastByNodeAction<
    GetIngestionStateRequest,
    GetIngestionStateResponse,
    ShardIngestionState> {

    private final IndicesService indicesService;

    @Inject
    public TransportGetIngestionStateAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetIngestionStateAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            GetIngestionStateRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
    }

    /**
     * Indicates the shards to consider.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, GetIngestionStateRequest request, String[] concreteIndices) {
        Set<Integer> shardSet = Arrays.stream(request.getShards()).boxed().collect(Collectors.toSet());

        Predicate<ShardRouting> shardFilter = ShardRouting::primary;
        if (shardSet.isEmpty() == false) {
            shardFilter = shardFilter.and(shardRouting -> shardSet.contains(shardRouting.shardId().getId()));
        }

        return clusterState.routingTable().allShardsSatisfyingPredicate(new String[] { request.getIndex() }, shardFilter);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, GetIngestionStateRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, GetIngestionStateRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected ShardIngestionState readShardResult(StreamInput in) throws IOException {
        return new ShardIngestionState(in);
    }

    @Override
    protected GetIngestionStateResponse newResponse(
        GetIngestionStateRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<ShardIngestionState> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new GetIngestionStateResponse(
            request.getIndex(),
            responses.toArray(new ShardIngestionState[0]),
            totalShards,
            successfulShards,
            failedShards,
            shardFailures
        );
    }

    @Override
    protected GetIngestionStateRequest readRequestFrom(StreamInput in) throws IOException {
        return new GetIngestionStateRequest(in);
    }

    @Override
    protected ShardIngestionState shardOperation(GetIngestionStateRequest request, ShardRouting shardRouting) {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
        if (indexShard.routingEntry() == null) {
            throw new ShardNotFoundException(indexShard.shardId());
        }

        try {
            return indexShard.getIngestionState();
        } catch (final AlreadyClosedException e) {
            throw new ShardNotFoundException(indexShard.shardId());
        }
    }
}
