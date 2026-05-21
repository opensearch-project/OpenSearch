/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.PlainShardsIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.composite.CompositeIndexingExecutionEngine;
import org.opensearch.composite.stats.CompositeStatsRegistry;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Transport action that collects dataformat stats using broadcast-by-node routing.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TransportDataFormatStatsAction extends TransportBroadcastByNodeAction<
    DataFormatStatsRequest,
    DataFormatStatsResponse,
    DataFormatStatsShardResult> {

    private final IndicesService indicesService;

    @Inject
    public TransportDataFormatStatsAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            DataFormatStatsActionType.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            DataFormatStatsRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
    }

    @Override
    protected DataFormatStatsShardResult readShardResult(StreamInput in) throws IOException {
        return new DataFormatStatsShardResult(in);
    }

    @Override
    protected DataFormatStatsResponse newResponse(
        DataFormatStatsRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DataFormatStatsShardResult> results,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new DataFormatStatsResponse(results, request.isShardLevel(), totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected DataFormatStatsRequest readRequestFrom(StreamInput in) throws IOException {
        return new DataFormatStatsRequest(in);
    }

    @Override
    protected DataFormatStatsShardResult shardOperation(DataFormatStatsRequest request, ShardRouting shardRouting) throws IOException {
        ShardId shardId = shardRouting.shardId();
        CompositeIndexingExecutionEngine engine = CompositeStatsRegistry.getInstance().getEngines().get(shardId);
        if (engine == null) {
            return new DataFormatStatsShardResult(shardRouting, new org.opensearch.composite.stats.CompositeShardStats());
        }
        return new DataFormatStatsShardResult(shardRouting, engine.getStats());
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, DataFormatStatsRequest request, String[] concreteIndices) {
        List<ShardRouting> shards = clusterState.routingTable().allShards(concreteIndices).getShardRoutings();
        String nodeFilter = request.getNodeFilter();
        Integer shardFilter = request.getShardFilter();

        // Validate shard filter against actual shard count
        if (shardFilter != null) {
            for (String index : concreteIndices) {
                int numShards = clusterState.routingTable().index(index).getShards().size();
                if (shardFilter < 0 || shardFilter >= numShards) {
                    throw new ShardNotFoundException(new ShardId(clusterState.metadata().index(index).getIndex(), shardFilter));
                }
            }
        }

        // Resolve _local to actual node ID and validate node existence
        String resolvedNodeFilter = null;
        if (nodeFilter != null) {
            if ("_local".equals(nodeFilter)) {
                resolvedNodeFilter = clusterState.getNodes().getLocalNodeId();
            } else {
                if (clusterState.getNodes().get(nodeFilter) == null) {
                    throw new IllegalArgumentException("node [" + nodeFilter + "] not found in cluster");
                }
                resolvedNodeFilter = nodeFilter;
            }
        }

        final String finalNodeFilter = resolvedNodeFilter;
        List<ShardRouting> filtered = shards.stream().filter(sr -> {
            // Shard filter
            if (shardFilter != null && sr.shardId().id() != shardFilter) {
                return false;
            }
            // Node filter
            if (finalNodeFilter != null && !finalNodeFilter.equals(sr.currentNodeId())) {
                return false;
            }
            // Default: primary only; shard-level or shard filter: all copies
            if (shardFilter != null || request.isShardLevel()) {
                return true;
            }
            return sr.primary();
        }).collect(Collectors.toList());

        return new PlainShardsIterator(filtered);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, DataFormatStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, DataFormatStatsRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }
}
