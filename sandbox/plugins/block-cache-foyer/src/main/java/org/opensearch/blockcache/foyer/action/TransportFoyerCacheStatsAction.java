/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer.action;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.blockcache.foyer.FoyerBlockCache;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Transport action for {@code GET /_nodes/foyer_cache/stats}.
 *
 * <p>On each targeted node: calls {@link FoyerBlockCache#foyerStats()} to
 * snapshot the native Foyer stats, and returns a
 * {@link FoyerCacheNodeResponse}. If no Foyer cache is registered on the node,
 * the stats field is {@code null} and the REST handler skips that node.
 *
 * @opensearch.internal
 */
public class TransportFoyerCacheStatsAction extends TransportNodesAction<
    FoyerCacheStatsRequest,
    FoyerCacheStatsResponse,
    FoyerCacheStatsRequest,         // node-level request reuses the same type
    FoyerCacheNodeResponse> {

    public static final String ACTION_NAME = "cluster:monitor/foyer_cache/stats";

    /** The local Foyer cache, or {@code null} if this node has none. */
    private final FoyerBlockCache cache;

    @Inject
    public TransportFoyerCacheStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        FoyerBlockCache cache           // injected by the plugin's createComponents()
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            FoyerCacheStatsRequest::new,
            FoyerCacheStatsRequest::new,
            ThreadPool.Names.MANAGEMENT,
            FoyerCacheNodeResponse.class
        );
        this.cache = cache;
    }

    // ─── TransportNodesAction callbacks ──────────────────────────────────────

    @Override
    protected FoyerCacheStatsResponse newResponse(
        FoyerCacheStatsRequest request,
        List<FoyerCacheNodeResponse> nodes,
        List<FailedNodeException> failures
    ) {
        return new FoyerCacheStatsResponse(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected FoyerCacheStatsRequest newNodeRequest(FoyerCacheStatsRequest request) {
        return request;  // request carries no extra per-node payload; reuse as-is
    }

    @Override
    protected FoyerCacheNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new FoyerCacheNodeResponse(in);
    }

    /**
     * Called on each targeted node to build the local response.
     * Snapshots the Foyer cache stats via a single native FFM call.
     */
    @Override
    protected FoyerCacheNodeResponse nodeOperation(FoyerCacheStatsRequest request) {
        if (cache == null) {
            return new FoyerCacheNodeResponse(clusterService.localNode(), null);
        }
        return new FoyerCacheNodeResponse(clusterService.localNode(), cache.foyerStats());
    }
}
