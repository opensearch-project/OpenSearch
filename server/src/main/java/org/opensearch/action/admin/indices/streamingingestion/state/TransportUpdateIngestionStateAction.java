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
import org.opensearch.cluster.metadata.IngestionStatus;
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
 * Transport action for updating ingestion state on provided shards. Shard level failures are provided if there are
 * errors during updating shard state.
 *
 * <p>This is for internal use and will not be exposed to the user directly. </p>
 *
 * @opensearch.experimental
 */
public class TransportUpdateIngestionStateAction extends TransportBroadcastByNodeAction<
    UpdateIngestionStateRequest,
    UpdateIngestionStateResponse,
    ShardIngestionState> {

    private final IndicesService indicesService;

    @Inject
    public TransportUpdateIngestionStateAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            UpdateIngestionStateAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            UpdateIngestionStateRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
    }

    /**
     * Indicates the shards to consider.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, UpdateIngestionStateRequest request, String[] concreteIndices) {
        Set<Integer> shardSet = Arrays.stream(request.getShards()).boxed().collect(Collectors.toSet());

        Predicate<ShardRouting> shardFilter = ShardRouting::primary;
        if (shardSet.isEmpty() == false) {
            shardFilter = shardFilter.and(shardRouting -> shardSet.contains(shardRouting.shardId().getId()));
        }

        return clusterState.routingTable().allShardsSatisfyingPredicate(request.getIndex(), shardFilter);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, UpdateIngestionStateRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, UpdateIngestionStateRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, request.indices());
    }

    @Override
    protected ShardIngestionState readShardResult(StreamInput in) throws IOException {
        return new ShardIngestionState(in);
    }

    @Override
    protected UpdateIngestionStateResponse newResponse(
        UpdateIngestionStateRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<ShardIngestionState> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new UpdateIngestionStateResponse(true, totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected UpdateIngestionStateRequest readRequestFrom(StreamInput in) throws IOException {
        return new UpdateIngestionStateRequest(in);
    }

    /**
     * Updates shard ingestion states depending on the requested changes.
     */
    @Override
    protected ShardIngestionState shardOperation(UpdateIngestionStateRequest request, ShardRouting shardRouting) {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
        if (indexShard.routingEntry() == null) {
            throw new ShardNotFoundException(indexShard.shardId());
        }

        try {
            if (request.getIngestionPaused() != null) {
                // update pause/resume state
                indexShard.updateShardIngestionState(new IngestionStatus(request.getIngestionPaused()));
            }

            return indexShard.getIngestionState();
        } catch (final AlreadyClosedException e) {
            throw new ShardNotFoundException(indexShard.shardId());
        }
    }
}
