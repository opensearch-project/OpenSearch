/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions.NodesResponseRestListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Clears DataFusion caches across ALL nodes in the cluster.
 *
 * <pre>
 * POST /_plugins/_analytics_backend_datafusion/cache/_clear           → clears all caches
 * POST /_plugins/_analytics_backend_datafusion/cache/_clear?footer=true  → footer metadata only
 * POST /_plugins/_analytics_backend_datafusion/cache/_clear?column=true  → column index only
 * POST /_plugins/_analytics_backend_datafusion/cache/_clear?offset=true  → offset index only
 * POST /_plugins/_analytics_backend_datafusion/cache/_clear?statistics=true  → statistics only
 * </pre>
 *
 * <p>Multiple params may be combined. When no param is set, all caches are cleared.
 * Mirrors the {@code /_cache/clear?query=true} pattern used by OpenSearch index caches.
 *
 * @opensearch.internal
 */
public class RestClearCacheAction extends BaseRestHandler {

    private static final String ROUTE = "/_plugins/_analytics_backend_datafusion/cache/_clear";

    @Override
    public String getName() {
        return "datafusion_clear_cache_action";
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, ROUTE));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ClearCacheNodesRequest nodesRequest = new ClearCacheNodesRequest();
        nodesRequest.setFooter(request.paramAsBoolean("footer", false));
        nodesRequest.setColumn(request.paramAsBoolean("column", false));
        nodesRequest.setOffset(request.paramAsBoolean("offset", false));
        nodesRequest.setStatistics(request.paramAsBoolean("statistics", false));
        return channel -> client.execute(ClearCacheActionType.INSTANCE, nodesRequest, new NodesResponseRestListener<>(channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
