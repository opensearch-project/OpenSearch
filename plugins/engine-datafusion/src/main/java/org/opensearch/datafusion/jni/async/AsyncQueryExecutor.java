/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.jni.async;

import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.datafusion.jni.handle.ReaderHandle;
import org.opensearch.datafusion.jni.handle.StreamHandle;

import java.util.concurrent.CompletableFuture;

/**
 * Async wrapper for query execution operations.
 * Provides non-blocking API for DataFusion queries.
 */
public final class AsyncQueryExecutor {

    private final AsyncExecutor asyncExecutor;

    public AsyncQueryExecutor(AsyncExecutor asyncExecutor) {
        this.asyncExecutor = asyncExecutor;
    }

    /**
     * Executes query phase asynchronously.
     */
    public CompletableFuture<StreamHandle> executeQueryPhase(
        long cachePtr,
        String tableName,
        byte[] substraitPlan
    ) {
        return asyncExecutor.submit(tokioPtr -> {
            long streamPtr = NativeBridge.executeQueryPhase(cachePtr, tableName, substraitPlan, tokioPtr);
            return new StreamHandle(streamPtr);
        });
    }

    /**
     * Executes fetch phase asynchronously.
     */
    public CompletableFuture<StreamHandle> executeFetchPhase(
        long cachePtr,
        long[] rowIds,
        String[] projections
    ) {
        return asyncExecutor.submit(tokioPtr -> {
            long streamPtr = NativeBridge.executeFetchPhase(cachePtr, rowIds, projections, tokioPtr);
            return new StreamHandle(streamPtr);
        });
    }

    /**
     * Creates DataFusion reader asynchronously.
     */
    public CompletableFuture<ReaderHandle> createReader(String path, String[] files) {
        return asyncExecutor.submit(tokioPtr -> {
            long readerPtr = NativeBridge.createDatafusionReader(path, files);
            return new ReaderHandle(readerPtr);
        });
    }
}
