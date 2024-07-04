/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.tasks.unregister;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportUnregisterTaskAction extends HandledTransportAction<UnregisterTaskRequest, UnregisterTaskResponse> {
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final NodeClient client;

    @Inject
    public TransportUnregisterTaskAction(
        NodeClient client,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService
    ) {
        super(UnregisterTaskAction.NAME, transportService, actionFilters, UnregisterTaskRequest::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, UnregisterTaskRequest request, ActionListener<UnregisterTaskResponse> listener) {
        try {
            transportService.getTaskManager().unregister(request.getTask());
        } finally {
            UnregisterTaskResponse unregisterTaskResponse = new UnregisterTaskResponse();
            listener.onResponse(unregisterTaskResponse);
        }
    }
}
