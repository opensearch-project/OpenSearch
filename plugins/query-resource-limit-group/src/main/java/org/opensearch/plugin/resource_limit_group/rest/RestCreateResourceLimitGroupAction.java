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
import org.opensearch.plugin.resource_limit_group.CreateResourceLimitGroupAction;
import org.opensearch.plugin.resource_limit_group.CreateResourceLimitGroupRequest;
import org.opensearch.plugin.resource_limit_group.CreateResourceLimitGroupResponse;
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
 * Rest action to create a resource limit group
 *
 * @opensearch.api
 */
public class RestCreateResourceLimitGroupAction extends BaseRestHandler {

    /**
     * Constructor for RestCreateResourceLimitGroupAction
     */
    public RestCreateResourceLimitGroupAction() {}

    @Override
    public String getName() {
        return "create_resource_limit_group";
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "_resource_limit_group/"), new Route(PUT, "_resource_limit_group/"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        CreateResourceLimitGroupRequest createResourceLimitGroupRequest = new CreateResourceLimitGroupRequest();
        request.applyContentParser((parser) -> parseRestRequest(createResourceLimitGroupRequest, parser));
        return channel -> client.execute(
            CreateResourceLimitGroupAction.INSTANCE,
            createResourceLimitGroupRequest,
            createResourceLimitGroupResponse(channel)
        );
    }

    private void parseRestRequest(CreateResourceLimitGroupRequest request, XContentParser parser) throws IOException {
        final CreateResourceLimitGroupRequest createResourceLimitGroupRequest = CreateResourceLimitGroupRequest.fromXContent(parser);
        request.setName(createResourceLimitGroupRequest.getName());
        request.setUUID(createResourceLimitGroupRequest.getUUID());
        request.setResourceLimits(createResourceLimitGroupRequest.getResourceLimits());
        request.setEnforcement(createResourceLimitGroupRequest.getEnforcement());
        request.setCreatedAt(createResourceLimitGroupRequest.getCreatedAt());
        request.setUpdatedAt(createResourceLimitGroupRequest.getUpdatedAt());
    }

    private RestResponseListener<CreateResourceLimitGroupResponse> createResourceLimitGroupResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final CreateResourceLimitGroupResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
