/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards;

import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.TimeoutTaskCancellationUtility;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Perform cat shards action
 *
 * @opensearch.internal
 */
public class TransportCatShardsAction extends HandledTransportAction<CatShardsRequest, CatShardsResponse> {

    private final NodeClient client;

    @Inject
    public TransportCatShardsAction(
        NodeClient client,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(CatShardsAction.NAME, transportService, actionFilters, CatShardsRequest::new);
        this.client = client;
    }

    @Override
    public void doExecute(Task parenTask, CatShardsRequest shardsRequest, ActionListener<CatShardsResponse> listener) {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(shardsRequest.getLocal());
        clusterStateRequest.clusterManagerNodeTimeout(shardsRequest.getClusterManagerNodeTimeout());
        clusterStateRequest.clear().nodes(true).routingTable(true).indices(shardsRequest.getIndices());

        clusterStateRequest.setParentTask(client.getLocalNodeId(), parenTask.getId());
        listener = TimeoutTaskCancellationUtility.wrapAndHandleWithCancellationListener(
            client,
            (CancellableTask) parenTask,
            ((CancellableTask) parenTask).getCancellationTimeout(),
            listener
        );
        CatShardsResponse catShardsResponse = new CatShardsResponse();
        ActionListener<CatShardsResponse> finalListener = listener;
        try {
            client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
                @Override
                public void onResponse(ClusterStateResponse clusterStateResponse) {
                    catShardsResponse.setClusterStateResponse(clusterStateResponse);
                    IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
                    indicesStatsRequest.all();
                    indicesStatsRequest.indices(shardsRequest.getIndices());
                    indicesStatsRequest.setParentTask(client.getLocalNodeId(), parenTask.getId());
                    try {
                        client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                            @Override
                            public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                                catShardsResponse.setIndicesStatsResponse(indicesStatsResponse);
                                if (!((CancellableTask) parenTask).isCancelled()) finalListener.onResponse(catShardsResponse);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                if (!((CancellableTask) parenTask).isCancelled()) finalListener.onFailure(e);
                            }
                        });
                    } catch (Exception e) {
                        if (!((CancellableTask) parenTask).isCancelled()) finalListener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (!((CancellableTask) parenTask).isCancelled()) finalListener.onFailure(e);
                }
            });
        } catch (Exception e) {
            if (!((CancellableTask) parenTask).isCancelled()) finalListener.onFailure(e);
        }

    }
}
