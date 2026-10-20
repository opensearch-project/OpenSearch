/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats.transport;

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
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.plugin.stats.DataFormatShardStats;
import org.opensearch.plugin.stats.DataFormatStatsProvider;
import org.opensearch.plugin.stats.DataFormatStatsProviderRegistry;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract transport action that collects per-format stats using broadcast-by-node routing.
 * Each data-format plugin subclasses this to register its own action.
 *
 * @param <T> concrete shard-stats type
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class BaseTransportFormatStatsAction<T extends DataFormatShardStats<T>> extends TransportBroadcastByNodeAction<
    FormatStatsRequest,
    FormatStatsResponse<T>,
    FormatStatsShardResult<T>> {

    private final String formatName;
    private final Writeable.Reader<T> reader;

    protected BaseTransportFormatStatsAction(
        String actionName,
        String formatName,
        Writeable.Reader<T> reader,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            actionName,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            FormatStatsRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.formatName = formatName;
        this.reader = reader;
    }

    @Override
    protected FormatStatsShardResult<T> readShardResult(StreamInput in) throws IOException {
        return new FormatStatsShardResult<>(in, reader);
    }

    @Override
    protected FormatStatsResponse<T> newResponse(
        FormatStatsRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<FormatStatsShardResult<T>> results,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new FormatStatsResponse<>(
            request.formatName(),
            request.isShardLevel(),
            results,
            totalShards,
            successfulShards,
            failedShards,
            shardFailures
        );
    }

    @Override
    protected FormatStatsRequest readRequestFrom(StreamInput in) throws IOException {
        return new FormatStatsRequest(in);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected FormatStatsShardResult<T> shardOperation(FormatStatsRequest request, ShardRouting shardRouting) {
        DataFormatStatsProvider<T> provider = (DataFormatStatsProvider<T>) DataFormatStatsProviderRegistry.INSTANCE.get(formatName);
        if (provider == null) return new FormatStatsShardResult<>(shardRouting, null);
        return new FormatStatsShardResult<>(shardRouting, provider.shardStats(shardRouting.shardId()).orElse(null));
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, FormatStatsRequest request, String[] concreteIndices) {
        List<ShardRouting> shards = clusterState.routingTable().allShards(concreteIndices).getShardRoutings();
        int[] shardFilter = request.getShardFilter();
        String[] nodeFilter = request.getNodeFilter();

        if (shardFilter != null && shardFilter.length > 0) {
            for (String index : concreteIndices) {
                int numShards = clusterState.routingTable().index(index).getShards().size();
                for (int sid : shardFilter) {
                    if (sid < 0 || sid >= numShards) {
                        throw new IllegalArgumentException(
                            "shard [" + sid + "] is out of range for index [" + index + "] which has [" + numShards + "] shard(s)"
                        );
                    }
                }
            }
        }

        final Set<String> resolvedNodeFilter = new HashSet<>();
        if (nodeFilter != null) {
            for (String nf : nodeFilter) {
                if ("_local".equals(nf)) {
                    resolvedNodeFilter.add(clusterState.getNodes().getLocalNodeId());
                } else {
                    if (clusterState.getNodes().get(nf) == null) {
                        throw new IllegalArgumentException("node [" + nf + "] not found in cluster");
                    }
                    resolvedNodeFilter.add(nf);
                }
            }
        }
        final boolean nodeFilterActive = nodeFilter != null && nodeFilter.length > 0;

        final Set<Integer> shardFilterSet = new HashSet<>();
        if (shardFilter != null) {
            for (int sid : shardFilter) {
                shardFilterSet.add(sid);
            }
        }
        final boolean shardFilterActive = shardFilter != null && shardFilter.length > 0;

        List<ShardRouting> filtered = shards.stream().filter(sr -> {
            // DFA replicas use segment replication from remote store and don't run an indexing
            // engine, so they have no per-format tracker registered. Always restrict to primaries
            // — the broadcast then has exactly one copy per logical shard, which makes
            // `_shards.total` match the user's `index.number_of_shards` setting and prevents
            // empty `{ primary: false }` noise in `level=shards` output.
            if (sr.primary() == false) {
                return false;
            }
            if (shardFilterActive && !shardFilterSet.contains(sr.shardId().id())) {
                return false;
            }
            if (nodeFilterActive && !resolvedNodeFilter.contains(sr.currentNodeId())) {
                return false;
            }
            return true;
        }).collect(Collectors.toList());

        return new PlainShardsIterator(filtered);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, FormatStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, FormatStatsRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }
}
