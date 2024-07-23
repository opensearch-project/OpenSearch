/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rest;

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.wlm.action.GetQueryGroupAction;
import org.opensearch.plugin.wlm.action.GetQueryGroupRequest;
import org.opensearch.plugin.wlm.action.GetQueryGroupResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Rest action to get a QueryGroup0
 *
 * @opensearch.experimental
 */
public class RestGetQueryGroupAction extends BaseRestHandler {

    /**
     * Constructor for RestGetQueryGroupAction
     */
    public RestGetQueryGroupAction() {}

    @Override
    public String getName() {
        return "get_query_group";
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "_wlm/query_group/{name}"), new Route(GET, "_wlm/query_group/"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String name = request.param("name");
        GetQueryGroupRequest getQueryGroupRequest = new GetQueryGroupRequest(name);
        return channel -> client.execute(GetQueryGroupAction.INSTANCE, getQueryGroupRequest, getQueryGroupResponse(channel));
    }

    private RestResponseListener<GetQueryGroupResponse> getQueryGroupResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final GetQueryGroupResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
