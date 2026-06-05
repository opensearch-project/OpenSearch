/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.stats;

import org.opensearch.analytics.stats.transport.AnalyticsStatsAction;
import org.opensearch.analytics.stats.transport.AnalyticsStatsRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions.NodesResponseRestListener;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

/**
 * REST handler for {@code GET _plugins/_analytics/stats} (and the per-node
 * variant {@code GET _plugins/_analytics/{nodeId}/stats}). Fans out to the
 * matching nodes via {@link AnalyticsStatsAction} and renders one
 * {@link AnalyticsStats} block per node, mirroring {@code _nodes/stats}'s
 * path scoping.
 *
 * <p>Marked {@link ExperimentalApi} — route, response shape, and aggregation
 * scope may evolve.
 */
@ExperimentalApi
public class RestAnalyticsStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "analytics_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "_plugins/_analytics/stats"),
            new Route(RestRequest.Method.GET, "_plugins/_analytics/{nodeId}/stats")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        // Empty array means "all nodes" — BaseNodesRequest's contract. nodeId can be a
        // single id, a comma-separated list, or a node selector like "master:true" / "_local"
        // (same vocabulary as /_nodes/stats path scoping).
        String[] nodeIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        AnalyticsStatsRequest req = new AnalyticsStatsRequest(nodeIds);
        return channel -> client.execute(AnalyticsStatsAction.INSTANCE, req, new NodesResponseRestListener<>(channel));
    }
}
