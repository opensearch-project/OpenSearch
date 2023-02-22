/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.get;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.decommission.DecommissionAttributeMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for getting decommission status
 *
 * @opensearch.internal
 */
public class TransportGetDecommissionStateAction extends TransportClusterManagerNodeReadAction<
    GetDecommissionStateRequest,
    GetDecommissionStateResponse> {

    @Inject
    public TransportGetDecommissionStateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetDecommissionStateAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetDecommissionStateRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetDecommissionStateResponse read(StreamInput in) throws IOException {
        return new GetDecommissionStateResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        GetDecommissionStateRequest request,
        ClusterState state,
        ActionListener<GetDecommissionStateResponse> listener
    ) throws Exception {
        DecommissionAttributeMetadata decommissionAttributeMetadata = state.metadata().decommissionAttributeMetadata();
        if (decommissionAttributeMetadata != null
            && request.attributeName().equals(decommissionAttributeMetadata.decommissionAttribute().attributeName())) {
            listener.onResponse(
                new GetDecommissionStateResponse(
                    decommissionAttributeMetadata.decommissionAttribute().attributeValue(),
                    decommissionAttributeMetadata.status()
                )
            );
        } else {
            listener.onResponse(new GetDecommissionStateResponse());
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetDecommissionStateRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
