/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.tasks.create;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.*;
import org.opensearch.transport.TransportService;

/**
 * Transport action for creating new task
 *
 * @opensearch.internal
 */
public class TransportCreateTaskAction extends HandledTransportAction<CreateTaskRequest, CreateTaskResponse> {

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final NodeClient client;

    @Inject
    public TransportCreateTaskAction(
        NodeClient client,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService
    ) {
        super(CreateTaskAction.NAME, transportService, actionFilters, CreateTaskRequest::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, CreateTaskRequest request, ActionListener<CreateTaskResponse> listener) {
        Task new_task =  transportService.getTaskManager().register("transport", CreateTaskAction.NAME, request);
        CreateTaskResponse response = new CreateTaskResponse(new_task);
        listener.onResponse(response);

    }
}
