/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.catalog;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.catalog.CatalogPublishesInProgress;
import org.opensearch.catalog.PublishEntry;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * @opensearch.experimental
 */
public class TransportGetPublishStatusAction extends TransportClusterManagerNodeReadAction<
    GetPublishStatusRequest,
    GetPublishStatusResponse> {

    @Inject
    public TransportGetPublishStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetPublishStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetPublishStatusRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetPublishStatusResponse read(StreamInput in) throws IOException {
        return new GetPublishStatusResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(GetPublishStatusRequest request, ClusterState state) {
        return null;
    }

    @Override
    protected void clusterManagerOperation(
        GetPublishStatusRequest request,
        ClusterState state,
        ActionListener<GetPublishStatusResponse> listener
    ) {
        CatalogPublishesInProgress custom = state.metadata().custom(CatalogPublishesInProgress.TYPE);
        if (custom == null) custom = CatalogPublishesInProgress.EMPTY;

        PublishEntry entry;
        if (request.publishId() != null && !request.publishId().isEmpty()) {
            entry = custom.entry(request.publishId());
        } else {
            entry = custom.entryForIndex(request.indexName());
        }
        listener.onResponse(new GetPublishStatusResponse(entry));
    }
}
