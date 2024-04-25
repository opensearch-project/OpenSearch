/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * Hook for running code that needs to be executed before the upload of index metadata. Here we have introduced a hook
 * for index creation (also triggerred after enabling the remote cluster statement for the first time). The listener
 * is intended to be run in parallel and async with the index metadata upload.
 *
 * @opensearch.internal
 */
public abstract class IndexMetadataUploadListener {

    private final ExecutorService executorService;

    public IndexMetadataUploadListener(ThreadPool threadPool, String threadPoolName) {
        Objects.requireNonNull(threadPool);
        Objects.requireNonNull(threadPoolName);
        assert ThreadPool.THREAD_POOL_TYPES.containsKey(threadPoolName) && ThreadPool.Names.SAME.equals(threadPoolName) == false;
        this.executorService = threadPool.executor(threadPoolName);
    }

    /**
     * Runs before the new index upload of index metadata (or first time upload). The caller is expected to trigger
     * onSuccess or onFailure of the {@code ActionListener}.
     *
     * @param indexMetadataList list of index metadata of new indexes (or first time index metadata upload).
     * @param actionListener    listener to be invoked on success or failure.
     */
    public final void onNewIndexUpload(List<IndexMetadata> indexMetadataList, ActionListener<Void> actionListener) {
        executorService.execute(() -> doOnNewIndexUpload(indexMetadataList, actionListener));
    }

    protected abstract void doOnNewIndexUpload(List<IndexMetadata> indexMetadataList, ActionListener<Void> actionListener);
}
