/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.awarenesshealth.ClusterAwarenessHealthRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.opensearch.client.Requests.clusterAwarenessHealthRequest;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Transport action to get cluster awareness health
 *
 * @opensearch.api
 */

public class RestClusterAwarenessHealthAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cluster/awareness/{awareness_attribute_name}/health"));
    }

    @Override
    public String getName() {
        return "cluster_awareness_attribute_health_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final ClusterAwarenessHealthRequest clusterAwarenessHealthRequest = clusterAwarenessHealthRequest();
        String awarenessAttributeName = request.param("awareness_attribute_name");
        clusterAwarenessHealthRequest.setAwarenessAttributeName(awarenessAttributeName);
        return channel -> client.admin()
            .cluster()
            .awarenessHealth(clusterAwarenessHealthRequest, new RestStatusToXContentListener<>(channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
