/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.weighted.put;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.WeightedRoutingService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for updating weights for weighted round-robin search routing policy
 *
 * @opensearch.internal
 */
public class TransportPutWeightedRoutingAction extends TransportClusterManagerNodeAction<
    ClusterPutWeightedRoutingRequest,
    ClusterPutWeightedRoutingResponse> {

    private final WeightedRoutingService weightedRoutingService;

    @Inject
    public TransportPutWeightedRoutingAction(
        TransportService transportService,
        ClusterService clusterService,
        WeightedRoutingService weightedRoutingService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterPutWeightedRoutingAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterPutWeightedRoutingRequest::new,
            indexNameExpressionResolver
        );
        this.weightedRoutingService = weightedRoutingService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterPutWeightedRoutingResponse read(StreamInput in) throws IOException {
        return new ClusterPutWeightedRoutingResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        ClusterPutWeightedRoutingRequest request,
        ClusterState state,
        ActionListener<ClusterPutWeightedRoutingResponse> listener
    ) throws Exception {
        weightedRoutingService.verifyAwarenessAttribute(request.getWeightedRouting().attributeName());
        weightedRoutingService.registerWeightedRoutingMetadata(
            request,
            ActionListener.delegateFailure(
                listener,
                (delegatedListener, response) -> {
                    delegatedListener.onResponse(new ClusterPutWeightedRoutingResponse(response.isAcknowledged()));
                }
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterPutWeightedRoutingRequest request, ClusterState state) {
        return null;
    }

}
