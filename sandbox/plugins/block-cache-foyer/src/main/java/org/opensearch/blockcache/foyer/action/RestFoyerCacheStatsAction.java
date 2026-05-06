/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer.action;

import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * REST handler for {@code GET /_nodes/foyer_cache/stats} and
 * {@code GET /_nodes/{nodeId}/foyer_cache/stats}.
 *
 * <p>Returns per-node Foyer block cache statistics in the standard OpenSearch
 * {@code _nodes} response shape:
 * <pre>{@code
 * {
 *   "_nodes":       { "total": 3, "successful": 3, "failed": 0 },
 *   "cluster_name": "my-cluster",
 *   "nodes": {
 *     "<nodeId>": {
 *       "name": "node-1",
 *       "foyer_block_cache": {
 *         "overall":     { "hit_count": ..., "hit_bytes": "256mb", ... },
 *         "block_level": { ... }
 *       }
 *     }
 *   }
 * }
 * }</pre>
 *
 * <p>Nodes without an active Foyer cache are omitted from the {@code "nodes"}
 * map (they return {@code null} stats and are filtered by
 * {@link FoyerCacheStatsResponse#toXContent}).
 *
 * @opensearch.internal
 */
public class RestFoyerCacheStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "foyer_cache_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_nodes/foyer_cache/stats"),
            new Route(GET, "/_nodes/{nodeId}/foyer_cache/stats")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Parse optional node filter from the URI path.
        String[] nodeIds = request.hasParam("nodeId")
            ? request.param("nodeId").split(",")
            : new String[0];

        FoyerCacheStatsRequest statsRequest = nodeIds.length == 0
            ? new FoyerCacheStatsRequest()
            : new FoyerCacheStatsRequest(nodeIds);

        return channel -> client.execute(
            FoyerCacheStatsAction.INSTANCE,
            statsRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
