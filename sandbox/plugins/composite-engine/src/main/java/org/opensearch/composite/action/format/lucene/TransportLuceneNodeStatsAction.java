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
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Transport action that collects Lucene stats from all shards, grouped by node.
 * Uses {@link CompositeStatsRegistry} to access engine instances via reflection.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TransportLuceneNodeStatsAction extends TransportBroadcastByNodeAction<
    LuceneNodeStatsRequest,
    LuceneNodeStatsResponse,
    LuceneNodeStatsShardResult> {


    @Inject
    public TransportLuceneNodeStatsAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            LuceneNodeStatsActionType.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            LuceneNodeStatsRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
    }

    @Override
    protected LuceneNodeStatsShardResult readShardResult(StreamInput in) throws IOException {
        return new LuceneNodeStatsShardResult(in);
    }

    @Override
    protected LuceneNodeStatsResponse newResponse(
        LuceneNodeStatsRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<LuceneNodeStatsShardResult> results,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new LuceneNodeStatsResponse(
            results,
            request.isShardLevel(),
            totalShards,
            successfulShards,
            failedShards,
            shardFailures,
            clusterState
        );
    }

    @Override
    protected LuceneNodeStatsRequest readRequestFrom(StreamInput in) throws IOException {
        return new LuceneNodeStatsRequest(in);
    }

    @Override
    protected LuceneNodeStatsShardResult shardOperation(LuceneNodeStatsRequest request, ShardRouting shardRouting) throws IOException {
        ShardId shardId = shardRouting.shardId();
        Map<ShardId, CompositeIndexingExecutionEngine> engines = CompositeStatsRegistry.getInstance().getEngines();
        CompositeIndexingExecutionEngine compositeEngine = engines.get(shardId);

        if (compositeEngine != null) {
            for (IndexingExecutionEngine<?, ?> secondary : compositeEngine.getSecondaryDelegates()) {
                if (secondary.getDataFormat() != null && LuceneDataFormat.LUCENE_FORMAT_NAME.equals(secondary.getDataFormat().name())) {
                    org.opensearch.core.xcontent.ToXContentFragment stats = StatsReflectionUtil.invokeGetStats(secondary);
                    if (stats != null) {
                        return new LuceneNodeStatsShardResult(shardRouting, stats);
                    }
                }
            }
        }

        return new LuceneNodeStatsShardResult(shardRouting, StatsReflectionUtil.EMPTY_STATS);
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, LuceneNodeStatsRequest request, String[] concreteIndices) {
        List<ShardRouting> allShards = clusterState.routingTable().allShards(concreteIndices).getShardRoutings();
        String nodeFilter = request.getNodeFilter();

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
        List<ShardRouting> filtered = allShards.stream().filter(sr -> {
            if (finalNodeFilter != null && !finalNodeFilter.equals(sr.currentNodeId())) {
                return false;
            }
            // Default: primary only; if shardLevel: all copies (primary + replicas)
            return request.isShardLevel() ? true : sr.primary();
        }).collect(Collectors.toList());

        return new PlainShardsIterator(filtered);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, LuceneNodeStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, LuceneNodeStatsRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }
}
