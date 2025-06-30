/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rest;

import org.opensearch.plugin.wlm.action.DeleteWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.DeleteWorkloadGroupRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.wlm.WlmMode;
import org.opensearch.wlm.WorkloadManagementSettings;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.DELETE;

/**
 * Rest action to delete a WorkloadGroup
 *
 * @opensearch.experimental
 */
public class RestDeleteWorkloadGroupAction extends BaseRestHandler {

    private final WorkloadManagementSettings workloadManagementSettings;

    /**
     * Constructor for RestDeleteWorkloadGroupAction
     * @param workloadManagementSettings the WorkloadManagementSettings instance to access the current WLM mode
     */
    public RestDeleteWorkloadGroupAction(WorkloadManagementSettings workloadManagementSettings) {
        this.workloadManagementSettings = workloadManagementSettings;
    }

    @Override
    public String getName() {
        return "delete_workload_group";
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "_wlm/workload_group/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (workloadManagementSettings.getWlmMode() == WlmMode.DISABLED) {
            throw new IllegalStateException("Workload management mode is DISABLED. Cannot delete workload group.");
        }

        DeleteWorkloadGroupRequest deleteWorkloadGroupRequest = new DeleteWorkloadGroupRequest(request.param("name"));
        deleteWorkloadGroupRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", deleteWorkloadGroupRequest.clusterManagerNodeTimeout())
        );
        deleteWorkloadGroupRequest.timeout(request.paramAsTime("timeout", deleteWorkloadGroupRequest.timeout()));
        return channel -> client.execute(
            DeleteWorkloadGroupAction.INSTANCE,
            deleteWorkloadGroupRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
