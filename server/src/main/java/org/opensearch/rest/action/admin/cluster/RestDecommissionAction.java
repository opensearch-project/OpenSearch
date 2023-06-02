/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.ActionScopes;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionRequest;
import org.opensearch.client.Requests;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.identity.Scope;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Registers decommission action
 *
 * @opensearch.api
 */
public class RestDecommissionAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(PUT, "/_cluster/decommission/awareness/{awareness_attribute_name}/{awareness_attribute_value}"));
    }

    @Override
    public String getName() {
        return "decommission_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        DecommissionRequest decommissionRequest = createRequest(request);
        return channel -> client.admin().cluster().decommission(decommissionRequest, new RestToXContentListener<>(channel));
    }

    DecommissionRequest createRequest(RestRequest request) throws IOException {
        DecommissionRequest decommissionRequest = Requests.decommissionRequest();
        String attributeName = request.param("awareness_attribute_name");
        String attributeValue = request.param("awareness_attribute_value");
        // Check if we have no delay set.
        boolean noDelay = request.paramAsBoolean("no_delay", false);
        decommissionRequest.setNoDelay(noDelay);

        if (request.hasParam("delay_timeout")) {
            TimeValue delayTimeout = request.paramAsTime("delay_timeout", DecommissionRequest.DEFAULT_NODE_DRAINING_TIMEOUT);
            decommissionRequest.setDelayTimeout(delayTimeout);
        }
        return decommissionRequest.setDecommissionAttribute(new DecommissionAttribute(attributeName, attributeValue));
    }

    @Override
    public List<Scope> allowedScopes() {
        return List.of(ActionScopes.Cluster_ALL);
    }
}
