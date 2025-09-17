/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.cache.PruneCacheAction;
import org.opensearch.action.admin.cluster.cache.PruneCacheRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * REST action to manually trigger FileCache prune operation.
 * This endpoint allows administrators to clear out non-referenced cache entries on demand.
 *
 * @opensearch.api
 */
public class RestPruneCacheAction extends BaseRestHandler {

    /**
     * Default constructor
     */
    public RestPruneCacheAction() {}

    @Override
    public List<Route> routes() {
        return singletonList(new Route(Method.POST, "/_cache/remote/prune"));
    }

    @Override
    public String getName() {
        return "prune_cache_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        PruneCacheRequest pruneCacheRequest = new PruneCacheRequest();

        // Handle timeout parameter
        pruneCacheRequest.clusterManagerNodeTimeout(request.paramAsTime("timeout", pruneCacheRequest.clusterManagerNodeTimeout()));

        // Delegate to Transport Action with standard response handling
        return channel -> client.execute(PruneCacheAction.INSTANCE, pruneCacheRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
