/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.weighted.delete;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.WeightedRoutingService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for deleting weights for weighted round-robin search routing policy
 *
 * @opensearch.internal
 */
public class TransportDeleteWeightedRoutingAction extends TransportClusterManagerNodeAction<
    ClusterDeleteWeightedRoutingRequest,
    ClusterDeleteWeightedRoutingResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteWeightedRoutingAction.class);

    private final WeightedRoutingService weightedRoutingService;

    @Inject
    public TransportDeleteWeightedRoutingAction(
        TransportService transportService,
        ClusterService clusterService,
        WeightedRoutingService weightedRoutingService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterDeleteWeightedRoutingAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterDeleteWeightedRoutingRequest::new,
            indexNameExpressionResolver
        );
        this.weightedRoutingService = weightedRoutingService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterDeleteWeightedRoutingResponse read(StreamInput in) throws IOException {
        return new ClusterDeleteWeightedRoutingResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterDeleteWeightedRoutingRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(
        ClusterDeleteWeightedRoutingRequest request,
        ClusterState state,
        ActionListener<ClusterDeleteWeightedRoutingResponse> listener
    ) throws Exception {
        weightedRoutingService.deleteWeightedRoutingMetadata(request, listener);
    }
}
