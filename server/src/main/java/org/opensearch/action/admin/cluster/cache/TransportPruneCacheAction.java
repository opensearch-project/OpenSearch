/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.cache;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport action for pruning file cache.
 * Uses HandledTransportAction for direct node execution instead of cluster manager routing.
 *
 * @opensearch.internal
 */
public class TransportPruneCacheAction extends HandledTransportAction<PruneCacheRequest, PruneCacheResponse> {

    private final FileCache fileCache;

    @Inject
    public TransportPruneCacheAction(TransportService transportService, ActionFilters actionFilters, @Nullable FileCache fileCache) {
        super(PruneCacheAction.NAME, transportService, actionFilters, PruneCacheRequest::new, ThreadPool.Names.GENERIC);
        this.fileCache = fileCache;
    }

    @Override
    protected void doExecute(Task task, PruneCacheRequest request, ActionListener<PruneCacheResponse> listener) {
        try {
            if (fileCache == null) {
                listener.onResponse(new PruneCacheResponse(true, 0L));
                return;
            }

            long prunedBytes = fileCache.prune();
            listener.onResponse(new PruneCacheResponse(true, prunedBytes));
        } catch (RuntimeException e) {
            // Catch runtime exceptions from FileCache operations for better error diagnostics
            listener.onFailure(e);
        } catch (Exception e) {
            // Fallback for any other unexpected exceptions
            listener.onFailure(e);
        }
    }
}
