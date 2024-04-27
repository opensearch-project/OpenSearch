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
import org.opensearch.plugin.resource_limit_group.DeleteResourceLimitGroupAction;
import org.opensearch.plugin.resource_limit_group.DeleteResourceLimitGroupRequest;
import org.opensearch.plugin.resource_limit_group.DeleteResourceLimitGroupResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.DELETE;

/**
 * Rest action to delete a resource limit group
 *
 * @opensearch.api
 */
public class RestDeleteResourceLimitGroupAction extends BaseRestHandler {

    /**
     * Constructor for RestDeleteResourceLimitGroupAction
     */
    public RestDeleteResourceLimitGroupAction() {}

    @Override
    public String getName() {
        return "delete_resource_limit_group";
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "_resource_limit_group/{name}"), new Route(DELETE, "_resource_limit_group/"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String name = request.param("name");
        DeleteResourceLimitGroupRequest deleteResourceLimitGroupRequest = new DeleteResourceLimitGroupRequest(name);
        return channel -> client.execute(
            DeleteResourceLimitGroupAction.INSTANCE,
            deleteResourceLimitGroupRequest,
            deleteResourceLimitGroupResponse(channel)
        );
    }

    private RestResponseListener<DeleteResourceLimitGroupResponse> deleteResourceLimitGroupResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final DeleteResourceLimitGroupResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
