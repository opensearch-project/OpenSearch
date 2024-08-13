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
import org.opensearch.plugin.wlm.action.DeleteQueryGroupAction;
import org.opensearch.plugin.wlm.action.DeleteQueryGroupRequest;
import org.opensearch.plugin.wlm.action.DeleteQueryGroupResponse;
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
 * Rest action to delete a QueryGroup
 *
 * @opensearch.experimental
 */
public class RestDeleteQueryGroupAction extends BaseRestHandler {

    /**
     * Constructor for RestDeleteQueryGroupAction
     */
    public RestDeleteQueryGroupAction() {}

    @Override
    public String getName() {
        return "delete_query_group";
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "_wlm/query_group/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        DeleteQueryGroupRequest deleteQueryGroupRequest = new DeleteQueryGroupRequest(request.param("name"));
        return channel -> client.execute(DeleteQueryGroupAction.INSTANCE, deleteQueryGroupRequest, deleteQueryGroupResponse(channel));
    }

    private RestResponseListener<DeleteQueryGroupResponse> deleteQueryGroupResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final DeleteQueryGroupResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
