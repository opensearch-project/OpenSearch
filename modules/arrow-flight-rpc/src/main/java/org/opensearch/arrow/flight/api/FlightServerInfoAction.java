/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.arrow.flight.api;

import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

public class FlightServerInfoAction extends BaseRestHandler {

    public FlightServerInfoAction() {}

    @Override
    public String getName() {
        return "flight_server_info_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_flight/info"), new Route(GET, "/_flight/info/{nodeId}"));
    }

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
