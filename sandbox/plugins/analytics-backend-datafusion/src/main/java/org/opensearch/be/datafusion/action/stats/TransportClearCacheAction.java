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
import org.opensearch.be.datafusion.DataFusionService;
import org.opensearch.be.datafusion.cache.CacheManager;
import org.opensearch.be.datafusion.cache.CacheUtils;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Broadcast transport action that clears the DataFusion parquet caches on every
 * target node. Clearing is routed through the node-local {@link CacheManager},
 * which holds the native runtime handle: the metadata (footer) and statistics
 * caches are runtime-scoped and can only be cleared through that handle, while
 * the ColumnIndex/OffsetIndex scoped caches are cleared alongside them.
 *
 * @opensearch.internal
 */
public class TransportClearCacheAction extends TransportNodesAction<
    ClearCacheNodesRequest,
    ClearCacheNodesResponse,
    ClearCacheNodeRequest,
    ClearCacheNodeResponse> {

    private final DataFusionService dataFusionService;

    @Inject
    public TransportClearCacheAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        DataFusionService dataFusionService
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
        this.dataFusionService = dataFusionService;
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
        return new ClearCacheNodeRequest(request.isFooter(), request.isColumn(), request.isOffset(), request.isStatistics());
    }

    @Override
    protected ClearCacheNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ClearCacheNodeResponse(in);
    }

    @Override
    protected ClearCacheNodeResponse nodeOperation(ClearCacheNodeRequest request) {
        CacheManager cacheManager = dataFusionService.getCacheManager();
        // Caching may not be configured on this node (DataFusion not started or
        // caches disabled). Nothing to clear in that case.
        if (cacheManager != null) {
            if (request.isClearAll()) {
                cacheManager.clearAllCache();
            } else {
                if (request.isFooter()) cacheManager.clearCacheForCacheType(CacheUtils.CacheType.METADATA);
                if (request.isColumn()) cacheManager.clearCacheForCacheType(CacheUtils.CacheType.COLUMN_INDEX);
                if (request.isOffset()) cacheManager.clearCacheForCacheType(CacheUtils.CacheType.OFFSET_INDEX);
                if (request.isStatistics()) cacheManager.clearCacheForCacheType(CacheUtils.CacheType.STATISTICS);
            }
        }
        return new ClearCacheNodeResponse(clusterService.localNode());
    }
}
