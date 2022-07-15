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
import org.opensearch.action.admin.cluster.management.decommission.PutDecommissionRequest;
import org.opensearch.client.Requests;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Transport action to decommission nodes
 *
 * @opensearch.api
 */
public class RestPutDecommissionAction extends BaseRestHandler {

    // TODO - Use debug logs
    private static final Logger logger = LogManager.getLogger(RestPutDecommissionAction.class);

    @Override
    public List<Route> routes() {
        return singletonList(new Route(PUT, "_cluster/management/decommission"));
    }

    @Override
    public String getName() {
        return "cluster_management_decommission_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        PutDecommissionRequest putDecommissionRequest = createRequest(request);
        return channel -> client.admin()
            .cluster()
            .decommission(putDecommissionRequest, new RestToXContentListener<>(channel));
    }

    public static PutDecommissionRequest createRequest(RestRequest request) throws IOException {
        PutDecommissionRequest putDecommissionRequest = Requests.putDecommissionRequest();
        request.applyContentParser(p -> putDecommissionRequest.source(p.mapOrdered()));
        return putDecommissionRequest;
    }
}
