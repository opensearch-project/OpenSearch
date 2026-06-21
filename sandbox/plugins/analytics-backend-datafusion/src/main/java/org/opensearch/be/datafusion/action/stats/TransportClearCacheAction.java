/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Broadcast transport action that clears the process-global scoped page-index
 * caches (ColumnIndex + OffsetIndex) on every target node.
 *
 * @opensearch.internal
 */
public class TransportClearCacheAction extends TransportNodesAction<
    ClearCacheNodesRequest,
    ClearCacheNodesResponse,
    ClearCacheNodeRequest,
    ClearCacheNodeResponse> {

    @Inject
    public TransportClearCacheAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(
            ClearCacheActionType.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ClearCacheNodesRequest::new,
            ClearCacheNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ClearCacheNodeResponse.class
        );
    }

    @Override
    protected ClearCacheNodesResponse newResponse(
        ClearCacheNodesRequest request,
        List<ClearCacheNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new ClearCacheNodesResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ClearCacheNodeRequest newNodeRequest(ClearCacheNodesRequest request) {
        return new ClearCacheNodeRequest(request.isFooter(), request.isColumn(), request.isOffset());
    }

    @Override
    protected ClearCacheNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ClearCacheNodeResponse(in);
    }

    @Override
    protected ClearCacheNodeResponse nodeOperation(ClearCacheNodeRequest request) {
        if (request.isClearAll()) {
            NativeBridge.clearColumnIndexCache();
            NativeBridge.clearOffsetIndexCache();
            NativeBridge.clearFooterCache();
        } else {
            if (request.isColumn()) NativeBridge.clearColumnIndexCache();
            if (request.isOffset()) NativeBridge.clearOffsetIndexCache();
            if (request.isFooter()) NativeBridge.clearFooterCache();
        }
        return new ClearCacheNodeResponse(clusterService.localNode());
    }
}
