/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.weighted.get;

import org.opensearch.action.ActionListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;

import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.routing.WeightedRouting;
import org.opensearch.cluster.routing.WeightedRoutingService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;

import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for getting weights for weighted round-robin search routing policy
 *
 * @opensearch.internal
 */
public class TransportGetWeightedRoutingAction extends TransportClusterManagerNodeReadAction<
    ClusterGetWeightedRoutingRequest,
    ClusterGetWeightedRoutingResponse> {
    private static final Logger logger = LogManager.getLogger(TransportGetWeightedRoutingAction.class);
    private final WeightedRoutingService weightedRoutingService;

    @Inject
    public TransportGetWeightedRoutingAction(
        TransportService transportService,
        ClusterService clusterService,
        WeightedRoutingService weightedRoutingService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterGetWeightedRoutingAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterGetWeightedRoutingRequest::new,
            indexNameExpressionResolver
        );
        this.weightedRoutingService = weightedRoutingService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterGetWeightedRoutingResponse read(StreamInput in) throws IOException {
        return new ClusterGetWeightedRoutingResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterGetWeightedRoutingRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void clusterManagerOperation(
        final ClusterGetWeightedRoutingRequest request,
        ClusterState state,
        final ActionListener<ClusterGetWeightedRoutingResponse> listener
    ) {
        try {
            weightedRoutingService.verifyAwarenessAttribute(request.getAwarenessAttribute());
            WeightedRoutingMetadata weightedRoutingMetadata = state.metadata().custom(WeightedRoutingMetadata.TYPE);
            ClusterGetWeightedRoutingResponse clusterGetWeightedRoutingResponse = new ClusterGetWeightedRoutingResponse();
            if (weightedRoutingMetadata != null && weightedRoutingMetadata.getWeightedRouting() != null) {
                WeightedRouting weightedRouting = weightedRoutingMetadata.getWeightedRouting();
                clusterGetWeightedRoutingResponse = new ClusterGetWeightedRoutingResponse(
                    weightedRouting,
                    state.nodes().getClusterManagerNodeId() != null,
                    weightedRoutingMetadata.getVersion()
                );
            }
            listener.onResponse(clusterGetWeightedRoutingResponse);
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }
}
