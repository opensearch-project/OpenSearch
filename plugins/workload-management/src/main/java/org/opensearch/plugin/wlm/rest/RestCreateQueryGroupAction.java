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
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.wlm.CreateQueryGroupAction;
import org.opensearch.plugin.wlm.CreateQueryGroupRequest;
import org.opensearch.plugin.wlm.CreateQueryGroupResponse;
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
 * Rest action to create a QueryGroup
 *
 * @opensearch.api
 */
public class RestCreateQueryGroupAction extends BaseRestHandler {

    /**
     * Constructor for RestCreateQueryGroupAction
     */
    public RestCreateQueryGroupAction() {}

    @Override
    public String getName() {
        return "create_query_group";
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "_wlm/_query_group/"), new Route(PUT, "_wlm/_query_group/"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        CreateQueryGroupRequest createQueryGroupRequest = new CreateQueryGroupRequest();
        request.applyContentParser((parser) -> parseRestRequest(createQueryGroupRequest, parser));
        return channel -> client.execute(CreateQueryGroupAction.INSTANCE, createQueryGroupRequest, createQueryGroupResponse(channel));
    }

    private void parseRestRequest(CreateQueryGroupRequest request, XContentParser parser) throws IOException {
        final CreateQueryGroupRequest createQueryGroupRequest = CreateQueryGroupRequest.fromXContent(parser);
        request.setName(createQueryGroupRequest.getName());
        request.set_id(createQueryGroupRequest.get_id());
        request.setResiliencyMode(createQueryGroupRequest.getResiliencyMode());
        request.setResourceLimits(createQueryGroupRequest.getResourceLimits());
        request.setUpdatedAtInMillis(createQueryGroupRequest.getUpdatedAtInMillis());
    }

    private RestResponseListener<CreateQueryGroupResponse> createQueryGroupResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final CreateQueryGroupResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
