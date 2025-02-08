/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.arrow.flight.api;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * It handles GET requests for retrieving Flight server information.
 */
public class FlightServerInfoAction extends BaseRestHandler {

    /**
     * Constructor for FlightServerInfoAction.
     */
    public FlightServerInfoAction() {}

    /**
     * Returns the name of the action.
     * @return The name of the action.
     */
    @Override
    public String getName() {
        return "flight_server_info_action";
    }

    /**
     * Returns the list of routes for the action.
     * @return The list of routes for the action.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_flight/info"), new Route(GET, "/_flight/info/{nodeId}"));
    }

    /**
     * Prepares the request for the action.
     * @param request The REST request.
     * @param client The node client.
     * @return The rest channel consumer.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String nodeId = request.param("nodeId");
        if (nodeId != null) {
            // Query specific node
            NodesFlightInfoRequest nodesRequest = new NodesFlightInfoRequest(nodeId);
            return channel -> client.execute(NodesFlightInfoAction.INSTANCE, nodesRequest, new RestToXContentListener<>(channel));
        } else {
            NodesFlightInfoRequest nodesRequest = new NodesFlightInfoRequest();
            return channel -> client.execute(NodesFlightInfoAction.INSTANCE, nodesRequest, new RestToXContentListener<>(channel));
        }
    }
}
