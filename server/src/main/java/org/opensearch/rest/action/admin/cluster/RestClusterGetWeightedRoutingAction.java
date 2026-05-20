/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.Requests;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Fetch Weighted Round Robin based shard routing weights
 *
 * @opensearch.api
 *
 */
public class RestClusterGetWeightedRoutingAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestClusterGetWeightedRoutingAction.class);

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cluster/routing/awareness/{attribute}/weights"));
    }

    @Override
    public String getName() {
        return "get_weighted_routing_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ClusterGetWeightedRoutingRequest getWeightedRoutingRequest = Requests.getWeightedRoutingRequest(request.param("attribute"));
        getWeightedRoutingRequest.local(request.paramAsBoolean("local", getWeightedRoutingRequest.local()));
        return channel -> client.admin().cluster().getWeightedRouting(getWeightedRoutingRequest, new RestToXContentListener<>(channel));
    }
}
