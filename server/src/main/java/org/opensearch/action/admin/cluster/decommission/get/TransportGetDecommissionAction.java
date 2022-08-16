/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.get;

import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.DecommissionedAttributesMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

public class TransportGetDecommissionAction extends TransportClusterManagerNodeReadAction<
    GetDecommissionRequest,
    GetDecommissionResponse> {

    @Inject
    public TransportGetDecommissionAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetDecommissionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetDecommissionRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetDecommissionResponse read(StreamInput in) throws IOException {
        return new GetDecommissionResponse(in);
    }

    @Override
    protected void masterOperation(
        GetDecommissionRequest request,
        ClusterState state,
        ActionListener<GetDecommissionResponse> listener
    ) throws Exception {
        Metadata metadata = state.metadata();
        DecommissionedAttributesMetadata decommissionedAttributes = metadata.custom(DecommissionedAttributesMetadata.TYPE);
        listener.onResponse(
            new GetDecommissionResponse(
                Objects.requireNonNullElseGet(decommissionedAttributes,
                    () -> new DecommissionedAttributesMetadata(Collections.emptyList())
                )
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetDecommissionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
