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
import org.opensearch.action.pagination.PageParams;
import org.opensearch.action.pagination.ShardPaginationStrategy;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.TimeoutTaskCancellationUtility;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.breaker.ResponseLimitBreachedException;
import org.opensearch.common.breaker.ResponseLimitSettings;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.NotifyOnceListener;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.Objects;

import static org.opensearch.common.breaker.ResponseLimitSettings.LimitEntity.SHARDS;

/**
 * Perform cat shards action
 *
 * @opensearch.internal
 */
public class TransportCatShardsAction extends HandledTransportAction<CatShardsRequest, CatShardsResponse> {

    private final NodeClient client;
    private final ResponseLimitSettings responseLimitSettings;

    @Inject
    public TransportCatShardsAction(
        NodeClient client,
        TransportService transportService,
        ActionFilters actionFilters,
        ResponseLimitSettings responseLimitSettings
    ) {
        super(CatShardsAction.NAME, transportService, actionFilters, CatShardsRequest::new);
        this.client = client;
        this.responseLimitSettings = responseLimitSettings;
    }

    @Override
    public void doExecute(Task parentTask, CatShardsRequest shardsRequest, ActionListener<CatShardsResponse> listener) {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.setShouldCancelOnTimeout(true);
        clusterStateRequest.local(shardsRequest.local());
        clusterStateRequest.clusterManagerNodeTimeout(shardsRequest.clusterManagerNodeTimeout());
        if (Objects.isNull(shardsRequest.getPageParams())) {
            clusterStateRequest.clear().nodes(true).routingTable(true).indices(shardsRequest.getIndices());
        } else {
            clusterStateRequest.clear().nodes(true).routingTable(true).indices(shardsRequest.getIndices()).metadata(true);
        }
        assert parentTask instanceof CancellableTask;
        clusterStateRequest.setParentTask(client.getLocalNodeId(), parentTask.getId());

        ActionListener<CatShardsResponse> originalListener = new NotifyOnceListener<CatShardsResponse>() {
            @Override
            protected void innerOnResponse(CatShardsResponse catShardsResponse) {
                listener.onResponse(catShardsResponse);
            }

            @Override
            protected void innerOnFailure(Exception e) {
                listener.onFailure(e);
            }
        };
        ActionListener<CatShardsResponse> cancellableListener = TimeoutTaskCancellationUtility.wrapWithCancellationListener(
            client,
            (CancellableTask) parentTask,
            ((CancellableTask) parentTask).getCancellationTimeout(),
            originalListener,
            e -> {
                originalListener.onFailure(e);
            }
        );
        CatShardsResponse catShardsResponse = new CatShardsResponse();
        try {
            client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
                @Override
                public void onResponse(ClusterStateResponse clusterStateResponse) {
                    validateRequestLimit(shardsRequest, clusterStateResponse, cancellableListener);
                    try {
                        ShardPaginationStrategy paginationStrategy = getPaginationStrategy(
                            shardsRequest.getPageParams(),
                            clusterStateResponse
                        );
                        String[] indices = Objects.isNull(paginationStrategy)
                            ? shardsRequest.getIndices()
                            : paginationStrategy.getRequestedIndices().toArray(new String[0]);
                        catShardsResponse.setNodes(clusterStateResponse.getState().getNodes());
                        catShardsResponse.setResponseShards(
                            Objects.isNull(paginationStrategy)
                                ? clusterStateResponse.getState().routingTable().allShards()
                                : paginationStrategy.getRequestedEntities()
                        );
                        catShardsResponse.setPageToken(Objects.isNull(paginationStrategy) ? null : paginationStrategy.getResponseToken());
                        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
                        indicesStatsRequest.setShouldCancelOnTimeout(true);
                        indicesStatsRequest.all();
                        indicesStatsRequest.indices(indices);
                        indicesStatsRequest.setParentTask(client.getLocalNodeId(), parentTask.getId());
                        client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                            @Override
                            public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                                catShardsResponse.setIndicesStatsResponse(indicesStatsResponse);
                                cancellableListener.onResponse(catShardsResponse);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                cancellableListener.onFailure(e);
                            }
                        });
                    } catch (Exception e) {
                        cancellableListener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    cancellableListener.onFailure(e);
                }
            });
        } catch (Exception e) {
            cancellableListener.onFailure(e);
        }

    }

    private ShardPaginationStrategy getPaginationStrategy(PageParams pageParams, ClusterStateResponse clusterStateResponse) {
        return Objects.isNull(pageParams) ? null : new ShardPaginationStrategy(pageParams, clusterStateResponse.getState());
    }

    private void validateRequestLimit(
        final CatShardsRequest shardsRequest,
        final ClusterStateResponse clusterStateResponse,
        final ActionListener<CatShardsResponse> listener
    ) {
        if (shardsRequest.isRequestLimitCheckSupported()
            && Objects.nonNull(clusterStateResponse)
            && Objects.nonNull(clusterStateResponse.getState())) {
            int limit = responseLimitSettings.getCatShardsResponseLimit();
            if (ResponseLimitSettings.isResponseLimitBreached(clusterStateResponse.getState().getRoutingTable(), SHARDS, limit)) {
                listener.onFailure(new ResponseLimitBreachedException("Too many shards requested.", limit, SHARDS));
            }
        }
    }
}
