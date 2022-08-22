/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.put;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.WRRShardRoutingService;
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
public class TransportPutWRRWeightsAction extends TransportClusterManagerNodeAction<
    ClusterPutWRRWeightsRequest,
    ClusterPutWRRWeightsResponse> {

    private final WRRShardRoutingService wrrShardRoutingService;

    @Inject
    public TransportPutWRRWeightsAction(
        TransportService transportService,
        ClusterService clusterService,
        WRRShardRoutingService wrrShardRoutingService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterPutWRRWeightsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterPutWRRWeightsRequest::new,
            indexNameExpressionResolver
        );
        this.wrrShardRoutingService = wrrShardRoutingService;
    }

    @Override
    protected String executor() {
        // TODO: Check threadpool to use??
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected ClusterPutWRRWeightsResponse read(StreamInput in) throws IOException {
        return new ClusterPutWRRWeightsResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        ClusterPutWRRWeightsRequest request,
        ClusterState state,
        ActionListener<ClusterPutWRRWeightsResponse> listener
    ) throws Exception {

        wrrShardRoutingService.registerWRRWeightsMetadata(
            request,
            ActionListener.delegateFailure(
                listener,
                (delegatedListener, response) -> {
                    delegatedListener.onResponse(new ClusterPutWRRWeightsResponse(response.isAcknowledged()));
                }
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterPutWRRWeightsRequest request, ClusterState state) {
        return null;
    }

}
