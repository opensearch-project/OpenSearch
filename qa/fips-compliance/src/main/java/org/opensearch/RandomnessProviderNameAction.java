/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch;

import org.opensearch.common.Randomness;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.rest.RestRequest.Method.GET;

public class RandomnessProviderNameAction extends BaseRestHandler {

    RandomnessProviderNameAction() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_randomness/provider_name"));
    }

    @Override
    public String getName() {
        return "randomness_get_provider_name_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> {
            String providerName = Randomness.createSecure().getProvider().getName();
            XContentBuilder builder = channel.newBuilder();
            builder.startObject().field("name", providerName).endObject();
            channel.sendResponse(new BytesRestResponse(OK, builder));
        };
    }
}
