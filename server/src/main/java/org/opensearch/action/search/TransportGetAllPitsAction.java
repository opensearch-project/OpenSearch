/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action to get all active PIT contexts across the cluster
 */
public class TransportGetAllPitsAction extends HandledTransportAction<GetAllPitNodesRequest, GetAllPitNodesResponse> {
    private final PitService pitService;

    @Inject
    public TransportGetAllPitsAction(ActionFilters actionFilters, TransportService transportService, PitService pitService) {
        super(GetAllPitsAction.NAME, transportService, actionFilters, in -> new GetAllPitNodesRequest(in));
        this.pitService = pitService;
    }

    protected void doExecute(Task task, GetAllPitNodesRequest request, ActionListener<GetAllPitNodesResponse> listener) {
        // If security plugin intercepts the request, it'll replace all PIT IDs with permitted PIT IDs
        if (request.getGetAllPitNodesResponse() != null) {
            listener.onResponse(request.getGetAllPitNodesResponse());
        } else {
            pitService.getAllPits(listener);
        }
    }
}
