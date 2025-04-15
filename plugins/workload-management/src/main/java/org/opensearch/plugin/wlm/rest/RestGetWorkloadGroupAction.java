/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.wlm.action.GetWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.GetWorkloadGroupRequest;
import org.opensearch.plugin.wlm.action.GetWorkloadGroupResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Rest action to get a WorkloadGroup
 *
 * @opensearch.experimental
 */
public class RestGetWorkloadGroupAction extends BaseRestHandler {

    /**
     * Constructor for RestGetWorkloadGroupAction
     */
    public RestGetWorkloadGroupAction() {}

    @Override
    public String getName() {
        return "get_workload_group";
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "_wlm/workload_group/{name}"), new Route(GET, "_wlm/workload_group/"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final GetWorkloadGroupRequest getWorkloadGroupRequest = new GetWorkloadGroupRequest(request.param("name"));
        return channel -> client.execute(GetWorkloadGroupAction.INSTANCE, getWorkloadGroupRequest, getWorkloadGroupResponse(channel));
    }

    private RestResponseListener<GetWorkloadGroupResponse> getWorkloadGroupResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final GetWorkloadGroupResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
