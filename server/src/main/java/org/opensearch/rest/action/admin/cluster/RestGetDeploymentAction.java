/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.deployment.GetDeploymentAction;
import org.opensearch.action.admin.cluster.deployment.GetDeploymentRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * REST action for getting deployment status.
 *
 * @opensearch.internal
 */
public class RestGetDeploymentAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Arrays.asList(new Route(GET, "/_cluster/deployment/{deployment_id}"), new Route(GET, "/_cluster/deployment"));
    }

    @Override
    public String getName() {
        return "get_deployment_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        GetDeploymentRequest getRequest = new GetDeploymentRequest();
        String deploymentId = request.param("deployment_id");
        if (deploymentId != null) {
            getRequest.deploymentId(deploymentId);
        }
        return channel -> client.execute(GetDeploymentAction.INSTANCE, getRequest, new RestToXContentListener<>(channel));
    }
}
