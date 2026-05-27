/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action.format.lucene;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.be.lucene.LuceneDataFormat;
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
import org.opensearch.composite.action.format.StatsReflectionUtil;
import org.opensearch.composite.stats.CompositeStatsRegistry;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Transport action that collects Lucene shard stats using broadcast-by-node routing.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TransportLuceneStatsAction extends TransportBroadcastByNodeAction<
    LuceneStatsRequest,
    LuceneStatsResponse,
    LuceneStatsShardResult> {

    private final ClusterService clusterService;

    @Inject
    public TransportLuceneStatsAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            LuceneStatsActionType.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            LuceneStatsRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.clusterService = clusterService;
    }

    @Override
    protected LuceneStatsShardResult readShardResult(StreamInput in) throws IOException {
        return new LuceneStatsShardResult(in);
    }

    @Override
    protected LuceneStatsResponse newResponse(
        LuceneStatsRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<LuceneStatsShardResult> results,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new LuceneStatsResponse(results, request.isShardLevel(), totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected LuceneStatsRequest readRequestFrom(StreamInput in) throws IOException {
        return new LuceneStatsRequest(in);
    }

    @Override
    protected LuceneStatsShardResult shardOperation(LuceneStatsRequest request, ShardRouting shardRouting) throws IOException {
        ShardId shardId = shardRouting.shardId();
        CompositeIndexingExecutionEngine engine = CompositeStatsRegistry.getInstance().getEngines().get(shardId);
        if (engine == null) {
            return new LuceneStatsShardResult(shardRouting, StatsReflectionUtil.EMPTY_STATS);
        }
        for (IndexingExecutionEngine<?, ?> secondary : engine.getSecondaryDelegates()) {
            if (secondary.getDataFormat() != null && LuceneDataFormat.LUCENE_FORMAT_NAME.equals(secondary.getDataFormat().name())) {
                org.opensearch.core.xcontent.ToXContentFragment stats = StatsReflectionUtil.invokeGetStats(secondary);
                if (stats != null) {
                    return new LuceneStatsShardResult(shardRouting, stats);
                }
            }
        }
        return new LuceneStatsShardResult(shardRouting, StatsReflectionUtil.EMPTY_STATS);
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, LuceneStatsRequest request, String[] concreteIndices) {
        List<ShardRouting> shards = clusterState.routingTable().allShards(concreteIndices).getShardRoutings();
        String nodeFilter = request.getNodeFilter();
        Integer shardFilter = request.getShardFilter();

        // Validate shard filter
        if (shardFilter != null) {
            for (String index : concreteIndices) {
                int numShards = clusterState.routingTable().index(index).getShards().size();
                if (shardFilter < 0 || shardFilter >= numShards) {
                    throw new IllegalArgumentException(
                        "shard " + shardFilter + " is out of range; index [" + index + "] has only " + numShards + " shards"
                    );
                }
            }
        }

        // Resolve _local to actual node ID
        String resolvedNodeFilter = null;
        if (nodeFilter != null) {
            if ("_local".equals(nodeFilter)) {
                resolvedNodeFilter = clusterService.localNode().getId();
            } else {
                if (clusterState.getNodes().get(nodeFilter) == null) {
                    throw new IllegalArgumentException("node [" + nodeFilter + "] not found in cluster");
                }
                resolvedNodeFilter = nodeFilter;
            }
        }

        final String finalNodeFilter = resolvedNodeFilter;
        List<ShardRouting> filtered = shards.stream().filter(sr -> {
            if (shardFilter != null && sr.shardId().id() != shardFilter) {
                return false;
            }
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
    protected ClusterBlockException checkGlobalBlock(ClusterState state, LuceneStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, LuceneStatsRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }
}
