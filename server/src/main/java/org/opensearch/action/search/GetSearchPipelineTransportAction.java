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
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Perform the action of getting a search pipeline
 *
 * @opensearch.internal
 */
public class GetSearchPipelineTransportAction extends TransportClusterManagerNodeReadAction<
    GetSearchPipelineRequest,
    GetSearchPipelineResponse> {

    @Inject
    public GetSearchPipelineTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetSearchPipelineAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetSearchPipelineRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetSearchPipelineResponse read(StreamInput in) throws IOException {
        return new GetSearchPipelineResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        GetSearchPipelineRequest request,
        ClusterState state,
        ActionListener<GetSearchPipelineResponse> listener
    ) throws Exception {
        listener.onResponse(new GetSearchPipelineResponse(SearchPipelineService.getPipelines(state, request.getIds())));
    }

    @Override
    protected ClusterBlockException checkBlock(GetSearchPipelineRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
