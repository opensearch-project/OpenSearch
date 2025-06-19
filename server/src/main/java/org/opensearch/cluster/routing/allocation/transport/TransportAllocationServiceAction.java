/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;

public class TransportAllocationServiceAction extends TransportAction<RerouteActionRequest, RerouteActionResponse> {
    private final AllocationService allocationService;

    @Inject
    protected TransportAllocationServiceAction(
        String actionName, ActionFilters actionFilters, TaskManager taskManager, AllocationService service
    ) {
        super(actionName, actionFilters, taskManager);
        this.allocationService = service;
    }

    @Override
    protected void doExecute(Task task, RerouteActionRequest request, ActionListener<RerouteActionResponse> listener) {
        ClusterState updatedState = allocationService.reroute(request.state, request.reason);
        listener.onResponse(new RerouteActionResponse(updatedState));
    }
}
