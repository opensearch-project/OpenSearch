/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats.transport;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.plugin.stats.DataFormatShardStats;
import org.opensearch.plugin.stats.DataFormatStatsProvider;
import org.opensearch.plugin.stats.DataFormatStatsProviderRegistry;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Abstract transport action that collects per-format aggregate node stats.
 * Each data-format plugin subclasses this to register its own node-stats action.
 *
 * @param <T> concrete shard-stats type
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class BaseTransportFormatNodeStatsAction<T extends DataFormatShardStats<T>> extends TransportNodesAction<
    FormatNodeStatsRequest,
    FormatNodeStatsResponse<T>,
    FormatNodeStatsRequest,
    NodeFormatStats<T>> {

    private final String formatName;
    private final Writeable.Reader<T> reader;

    @SuppressWarnings("unchecked")
    protected BaseTransportFormatNodeStatsAction(
        String actionName,
        String formatName,
        Writeable.Reader<T> reader,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(
            actionName,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            FormatNodeStatsRequest::new,
            FormatNodeStatsRequest::new,
            ThreadPool.Names.MANAGEMENT,
            (Class<NodeFormatStats<T>>) (Class<?>) NodeFormatStats.class
        );
        this.formatName = formatName;
        this.reader = reader;
    }

    @Override
    protected FormatNodeStatsResponse<T> newResponse(
        FormatNodeStatsRequest request,
        List<NodeFormatStats<T>> nodeStats,
        List<FailedNodeException> failures
    ) {
        return new FormatNodeStatsResponse<>(clusterService.getClusterName(), nodeStats, failures);
    }

    @Override
    protected FormatNodeStatsRequest newNodeRequest(FormatNodeStatsRequest request) {
        return request;
    }

    @Override
    protected NodeFormatStats<T> newNodeResponse(StreamInput in) throws IOException {
        return new NodeFormatStats<>(in, reader);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected NodeFormatStats<T> nodeOperation(FormatNodeStatsRequest request) {
        DataFormatStatsProvider<T> provider = (DataFormatStatsProvider<T>) DataFormatStatsProviderRegistry.INSTANCE.get(formatName);
        if (provider == null) {
            throw new IllegalArgumentException("unknown format: " + formatName);
        }
        T stats = provider.aggregateNodeStats().orElse(null);
        return new NodeFormatStats<>(transportService.getLocalNode(), formatName, stats);
    }
}
