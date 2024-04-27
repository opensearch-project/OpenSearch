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
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.resource_limit_group.UpdateResourceLimitGroupAction;
import org.opensearch.plugin.resource_limit_group.UpdateResourceLimitGroupRequest;
import org.opensearch.plugin.resource_limit_group.UpdateResourceLimitGroupResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Rest action to update a resource limit group
 *
 * @opensearch.api
 */
public class RestUpdateResourceLimitGroupAction extends BaseRestHandler {

    /**
     * Constructor for RestUpdateResourceLimitGroupAction
     */
    public RestUpdateResourceLimitGroupAction() {}

    @Override
    public String getName() {
        return "update_resource_limit_group";
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "_resource_limit_group/{name}"), new Route(PUT, "_resource_limit_group/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String name = request.param("name");
        UpdateResourceLimitGroupRequest updateResourceLimitGroupRequest = new UpdateResourceLimitGroupRequest(name);
        request.applyContentParser((parser) -> parseRestRequest(updateResourceLimitGroupRequest, parser));
        return channel -> client.execute(
            UpdateResourceLimitGroupAction.INSTANCE,
            updateResourceLimitGroupRequest,
            updateResourceLimitGroupResponse(channel)
        );
    }

    private void parseRestRequest(UpdateResourceLimitGroupRequest request, XContentParser parser) throws IOException {
        final UpdateResourceLimitGroupRequest updateResourceLimitGroupRequest = UpdateResourceLimitGroupRequest.fromXContent(parser);
        request.setResourceLimits(updateResourceLimitGroupRequest.getResourceLimits());
        request.setEnforcement(updateResourceLimitGroupRequest.getEnforcement());
        request.setUpdatedAt(updateResourceLimitGroupRequest.getUpdatedAt());
    }

    private RestResponseListener<UpdateResourceLimitGroupResponse> updateResourceLimitGroupResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final UpdateResourceLimitGroupResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
