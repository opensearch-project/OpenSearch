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
import org.opensearch.action.admin.cluster.decommission.awareness.get.GetDecommissionRequest;
import org.opensearch.client.Requests;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Returns decommissioned attribute information
 *
 * @opensearch.api
 */
public class RestGetDecommissionAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cluster/decommission/awareness/_status"));
    }

    @Override
    public String getName() {
        return "get_decommission_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        GetDecommissionRequest getDecommissionRequest = Requests.getDecommissionRequest();
        getDecommissionRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", getDecommissionRequest.clusterManagerNodeTimeout())
        );
        return channel -> client.admin()
            .cluster()
            .getDecommission(getDecommissionRequest, new RestToXContentListener<>(channel));
    }
}
