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

public class TransportCatShardsAction extends HandledTransportAction<CatShardsRequest, CatShardsResponse> {

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final NodeClient client;

    @Inject
    public TransportCatShardsAction(
        NodeClient client,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService
    ) {
        super(CatShardsAction.NAME, transportService, actionFilters, CatShardsRequest::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.client = client;
    }

    @Override
    public void doExecute(Task task, CatShardsRequest shardsRequest, ActionListener<CatShardsResponse> listener) {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(shardsRequest.getLocal());
        clusterStateRequest.clusterManagerNodeTimeout(shardsRequest.getClusterManagerNodeTimeout());
        clusterStateRequest.clear().nodes(true).routingTable(true).indices(shardsRequest.getIndices());

        if (task != null) {
            clusterStateRequest.setParentTask(client.getLocalNodeId(), task.getId());
            TimeoutTaskCancellationUtility.wrapWithCancellationListener(
                client,
                (CancellableTask) task,
                ((CancellableTask) task).getCancellationTimeout(),
                ActionListener.wrap(r -> {}, e -> {})
            );
        }
        CatShardsResponse catShardsResponse = new CatShardsResponse();

        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
            @Override
            public void onResponse(ClusterStateResponse clusterStateResponse) {
                IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
                indicesStatsRequest.all();
                indicesStatsRequest.indices(shardsRequest.getIndices());
                catShardsResponse.setClusterStateResponse(clusterStateResponse);
                indicesStatsRequest.setParentTask(client.getLocalNodeId(), task.getId());
                client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                    @Override
                    public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                        catShardsResponse.setIndicesStatsResponse(indicesStatsResponse);
                        listener.onResponse(catShardsResponse);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
