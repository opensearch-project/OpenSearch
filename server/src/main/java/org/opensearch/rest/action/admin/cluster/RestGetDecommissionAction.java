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
import org.opensearch.action.admin.cluster.decommission.get.GetDecommissionRequest;
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

    // TODO - Use debug logs
    private static final Logger logger = LogManager.getLogger(RestPutDecommissionAction.class);

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "_cluster/management/decommission"));
    }

    @Override
    public String getName() {
        return "get_decommission_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        return channel -> client.admin()
            .cluster()
            .getDecommission(Requests.getDecommissionRequest(), new RestToXContentListener<>(channel));
    }
}
