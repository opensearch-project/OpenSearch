/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.cache;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for pruning remote file cache
 *
 * @opensearch.internal
 */
public class TransportPruneCacheAction extends TransportClusterManagerNodeAction<PruneCacheRequest, PruneCacheResponse> {

    private final FileCache fileCache;

    @Inject
    public TransportPruneCacheAction(
        ThreadPool threadPool,
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        @Nullable FileCache fileCache
    ) {
        super(
            PruneCacheAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PruneCacheRequest::new,
            indexNameExpressionResolver
        );
        this.fileCache = fileCache;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PruneCacheResponse read(StreamInput in) throws IOException {
        return new PruneCacheResponse(in);
    }

    @Override
    protected void clusterManagerOperation(PruneCacheRequest request, ClusterState state, ActionListener<PruneCacheResponse> listener)
        throws Exception {
        try {
            if (fileCache == null) {
                listener.onResponse(new PruneCacheResponse(true, 0));
                return;
            }

            long prunedBytes = fileCache.prune();
            listener.onResponse(new PruneCacheResponse(true, prunedBytes));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(PruneCacheRequest request, ClusterState state) {
        // Prune cache operation doesn't require any cluster blocks
        return null;
    }
}
