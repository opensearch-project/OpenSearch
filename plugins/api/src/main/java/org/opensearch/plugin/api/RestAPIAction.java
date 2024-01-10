/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.api;

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.api.action.APIAction;
import org.opensearch.plugin.api.action.APIRequest;
import org.opensearch.plugin.api.action.APIResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * A REST handler.
 */
public class RestAPIAction extends BaseRestHandler {
    RestController restController;

    RestAPIAction(RestController restController) {
        this.restController = restController;
    }

    /**
     *
     * @return
     */
    @Override
    public String getName() {
        return "api";
    }

    /**
     *
     * @return
     */
    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_plugins/api")));
    }

    /**
     *
     * @param request the request to execute
     * @param client  client for executing actions on the local node
     * @return
     * @throws IOException
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return channel -> client.execute(APIAction.INSTANCE, new APIRequest(), new RestBuilderListener<APIResponse>(channel) {
            @Override
            public RestResponse buildResponse(APIResponse response, XContentBuilder builder) throws Exception {
                response.setRestController(restController);
                response.toXContent(builder, request);
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
}
