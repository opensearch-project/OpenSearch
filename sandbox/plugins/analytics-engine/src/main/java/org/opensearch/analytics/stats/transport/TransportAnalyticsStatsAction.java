/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.stats.transport;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.analytics.stats.AnalyticsStats;
import org.opensearch.analytics.stats.AnalyticsStatsCollector;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Coordinator-level fan-out for {@link AnalyticsStatsAction}. Each contacted
 * node runs {@link #nodeOperation} which returns a fresh snapshot of the
 * local {@link AnalyticsStatsCollector}; the coordinator collects all the
 * responses into an {@link AnalyticsStatsResponse} for rendering.
 *
 * <p>The node-level work is a single {@code snapshot()} call — cheap,
 * lock-free, and bounded — so it runs on the {@code MANAGEMENT} threadpool
 * mirroring {@code TransportNodesStatsAction}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TransportAnalyticsStatsAction extends TransportNodesAction<
    AnalyticsStatsRequest,
    AnalyticsStatsResponse,
    AnalyticsStatsNodeRequest,
    AnalyticsStatsNodeResponse> {

    private final AnalyticsStatsCollector collector;

    @Inject
    public TransportAnalyticsStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        AnalyticsStatsCollector collector
    ) {
        super(
            AnalyticsStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            AnalyticsStatsRequest::new,
            AnalyticsStatsNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            AnalyticsStatsNodeResponse.class
        );
        this.collector = collector;
    }

    @Override
    protected AnalyticsStatsResponse newResponse(
        AnalyticsStatsRequest request,
        List<AnalyticsStatsNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new AnalyticsStatsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected AnalyticsStatsNodeRequest newNodeRequest(AnalyticsStatsRequest request) {
        return new AnalyticsStatsNodeRequest();
    }

    @Override
    protected AnalyticsStatsNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new AnalyticsStatsNodeResponse(in);
    }

    @Override
    protected AnalyticsStatsNodeResponse nodeOperation(AnalyticsStatsNodeRequest request) {
        AnalyticsStats stats = collector.snapshot();
        return new AnalyticsStatsNodeResponse(clusterService.localNode(), stats);
    }
}
