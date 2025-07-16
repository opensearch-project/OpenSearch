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
import org.opensearch.plugin.wlm.WlmClusterSettingValuesProvider;
import org.opensearch.plugin.wlm.action.CreateWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.CreateWorkloadGroupRequest;
import org.opensearch.plugin.wlm.action.CreateWorkloadGroupResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Rest action to create a WorkloadGroup
 *
 * @opensearch.experimental
 */
public class RestCreateWorkloadGroupAction extends BaseRestHandler {

    private final WlmClusterSettingValuesProvider nonPluginSettingValuesProvider;

    /**
     * Constructor for RestCreateWorkloadGroupAction
     * @param nonPluginSettingValuesProvider the settings provider to access the current WLM mode
     */
    public RestCreateWorkloadGroupAction(WlmClusterSettingValuesProvider nonPluginSettingValuesProvider) {
        this.nonPluginSettingValuesProvider = nonPluginSettingValuesProvider;
    }

    @Override
    public String getName() {
        return "create_workload_group";
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "_wlm/workload_group/"), new Route(PUT, "_wlm/workload_group/"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        nonPluginSettingValuesProvider.ensureWlmEnabled(getName());
        try (XContentParser parser = request.contentParser()) {
            CreateWorkloadGroupRequest createWorkloadGroupRequest = CreateWorkloadGroupRequest.fromXContent(parser);
            return channel -> client.execute(
                CreateWorkloadGroupAction.INSTANCE,
                createWorkloadGroupRequest,
                createWorkloadGroupResponse(channel)
            );
        }
    }

    private RestResponseListener<CreateWorkloadGroupResponse> createWorkloadGroupResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final CreateWorkloadGroupResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
