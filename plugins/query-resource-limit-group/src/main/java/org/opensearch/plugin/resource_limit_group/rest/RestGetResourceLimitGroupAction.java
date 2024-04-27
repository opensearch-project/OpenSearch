/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group.rest;

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.resource_limit_group.GetResourceLimitGroupAction;
import org.opensearch.plugin.resource_limit_group.GetResourceLimitGroupRequest;
import org.opensearch.plugin.resource_limit_group.GetResourceLimitGroupResponse;
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
 * Rest action to get a resource limit group
 *
 * @opensearch.api
 */
public class RestGetResourceLimitGroupAction extends BaseRestHandler {

    /**
     * Constructor for RestGetResourceLimitGroupAction
     */
    public RestGetResourceLimitGroupAction() {}

    @Override
    public String getName() {
        return "get_resource_limit_group";
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "_resource_limit_group/{name}"), new Route(GET, "_resource_limit_group/"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String name = request.param("name");
        GetResourceLimitGroupRequest getResourceLimitGroupRequest = new GetResourceLimitGroupRequest(name);
        return channel -> client.execute(
            GetResourceLimitGroupAction.INSTANCE,
            getResourceLimitGroupRequest,
            getResourceLimitGroupResponse(channel)
        );
    }

    private RestResponseListener<GetResourceLimitGroupResponse> getResourceLimitGroupResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final GetResourceLimitGroupResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
