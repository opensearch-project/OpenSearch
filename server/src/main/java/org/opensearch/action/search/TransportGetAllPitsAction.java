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
    public TransportGetAllPitsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        SearchService searchService
    ) {
        super(
            GetAllPitsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            GetAllPitNodesRequest::new,
            GetAllPitNodeRequest::new,
            ThreadPool.Names.SAME,
            GetAllPitNodeResponse.class
        );
        this.searchService = searchService;
    }

    @Override
    protected GetAllPitNodesResponse newResponse(
        GetAllPitNodesRequest request,
        List<GetAllPitNodeResponse> getAllPitNodeResponses,
        List<FailedNodeException> failures
    ) {
        return new GetAllPitNodesResponse(clusterService.getClusterName(), getAllPitNodeResponses, failures);
    }

    @Override
    protected GetAllPitNodeRequest newNodeRequest(GetAllPitNodesRequest request) {
        return new GetAllPitNodeRequest();
    }

    @Override
    protected GetAllPitNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new GetAllPitNodeResponse(in);
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
