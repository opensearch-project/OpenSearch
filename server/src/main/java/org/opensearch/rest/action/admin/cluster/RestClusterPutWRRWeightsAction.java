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
import org.opensearch.action.admin.cluster.shards.routing.wrr.put.ClusterPutWRRWeightsRequest;
import org.opensearch.client.Requests;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Update Weighted Round Robin based shard routing weights
 *
 * @opensearch.api
 *
 */
public class RestClusterPutWRRWeightsAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestClusterPutWRRWeightsAction.class);

    @Override
    public List<Route> routes() {
        return singletonList(new Route(PUT, "/_cluster/routing/awareness/{attribute}/weights"));
    }

    @Override
    public String getName() {
        return "put_wrr_weights_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ClusterPutWRRWeightsRequest putWRRWeightsRequest = createRequest(request);
        return channel -> client.admin().cluster().putWRRWeights(putWRRWeightsRequest, new RestToXContentListener<>(channel));
    }

    public static ClusterPutWRRWeightsRequest createRequest(RestRequest request) throws IOException {
        ClusterPutWRRWeightsRequest putWRRWeightsRequest = Requests.putWRRWeightsRequest();
        String attributeName = null;
        if (request.hasParam("attribute")) {
            attributeName = request.param("attribute");
        }
        putWRRWeightsRequest.attributeName(attributeName);
        request.applyContentParser(p -> putWRRWeightsRequest.source(p.mapStrings()));
        return putWRRWeightsRequest;
    }

}
