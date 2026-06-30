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
import org.opensearch.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.Requests;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.DELETE;

/**
 * Delete Weighted Round Robin based shard routing weights
 *
 * @opensearch.api
 *
 */
public class RestClusterDeleteWeightedRoutingAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestClusterDeleteWeightedRoutingAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(DELETE, "/_cluster/routing/awareness/weights"),
                new Route(DELETE, "/_cluster/routing/awareness/{attribute}/weights")
            )
        );
    }

    @Override
    public String getName() {
        return "delete_weighted_routing_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ClusterDeleteWeightedRoutingRequest clusterDeleteWeightedRoutingRequest = createRequest(request);
        return channel -> client.admin()
            .cluster()
            .deleteWeightedRouting(clusterDeleteWeightedRoutingRequest, new RestToXContentListener<>(channel));
    }

    public static ClusterDeleteWeightedRoutingRequest createRequest(RestRequest request) throws IOException {
        ClusterDeleteWeightedRoutingRequest deleteWeightedRoutingRequest = Requests.deleteWeightedRoutingRequest(
            request.param("attribute")
        );
        request.applyContentParser(p -> deleteWeightedRoutingRequest.source(p.mapStrings()));
        return deleteWeightedRoutingRequest;
    }
}
