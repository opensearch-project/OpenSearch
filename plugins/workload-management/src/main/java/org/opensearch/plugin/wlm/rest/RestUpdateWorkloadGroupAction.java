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
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.wlm.action.UpdateWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.UpdateWorkloadGroupRequest;
import org.opensearch.plugin.wlm.action.UpdateWorkloadGroupResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.wlm.WlmMode;
import org.opensearch.wlm.WorkloadManagementSettings;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Rest action to update a WorkloadGroup
 *
 * @opensearch.experimental
 */
public class RestUpdateWorkloadGroupAction extends BaseRestHandler {

    private final WorkloadManagementSettings workloadManagementSettings;

    /**
     * Constructor for RestUpdateWorkloadGroupAction
     * @param workloadManagementSettings the WorkloadManagementSettings instance to access the current WLM mode
     */
    public RestUpdateWorkloadGroupAction(WorkloadManagementSettings workloadManagementSettings) {
        this.workloadManagementSettings = workloadManagementSettings;
    }

    @Override
    public String getName() {
        return "update_workload_group";
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "_wlm/workload_group/{name}"), new Route(PUT, "_wlm/workload_group/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (workloadManagementSettings.getWlmMode() == WlmMode.DISABLED) {
            throw new IllegalStateException("Workload management mode is DISABLED. Cannot update workload group.");
        }
        try (XContentParser parser = request.contentParser()) {
            UpdateWorkloadGroupRequest updateWorkloadGroupRequest = UpdateWorkloadGroupRequest.fromXContent(parser, request.param("name"));
            return channel -> client.execute(
                UpdateWorkloadGroupAction.INSTANCE,
                updateWorkloadGroupRequest,
                updateWorkloadGroupResponse(channel)
            );
        }
    }

    private RestResponseListener<UpdateWorkloadGroupResponse> updateWorkloadGroupResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final UpdateWorkloadGroupResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
