/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.be.datafusion.nativelib.SessionContextHandle;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.exec.EngineSearcher;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * DataFusion searcher — executes substrait query plans via a pre-configured SessionContext.
 * <p>
 * Requires a {@link SessionContextHandle} on the context (set by instruction handlers).
 * The native side classifies the substrait plan and dispatches to the appropriate
 * execution path (vanilla parquet or indexed).
 * <p>
 * After {@link #search}, the result stream handle is available on the context
 * via {@link DatafusionContext#getStreamHandle()}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionSearcher implements EngineSearcher<DatafusionContext> {

    private final ReaderHandle readerHandle;

    /**
     * Creates a searcher
     * @param readerHandle the native reader handle
     */
    public DatafusionSearcher(ReaderHandle readerHandle) {
        this.readerHandle = readerHandle;
    }

    @Override
    public void search(DatafusionContext context) throws IOException {
        SessionContextHandle sessionCtx = context.getSessionContextHandle();
        if (sessionCtx == null) {
            throw new IllegalStateException("SessionContextHandle must be set before search");
        }
        DatafusionQuery query = context.getDatafusionQuery();
        NativeRuntimeHandle runtimeHandle = context.getNativeRuntime();
        CompletableFuture<Long> future = new CompletableFuture<>();
        NativeBridge.executeWithContextAsync(sessionCtx, query.getSubstraitBytes(), new ActionListener<>() {
            @Override
            public void onResponse(Long streamPtr) {
                future.complete(streamPtr);
            }

            @Override
            public void onFailure(Exception exception) {
                future.completeExceptionally(exception);
            }
        });
        long streamPtr;
        try {
            streamPtr = future.join();
        } catch (Exception exception) {
            throw new IOException("Query execution with session context failed", exception);
        }
        context.setStreamHandle(new StreamHandle(streamPtr, runtimeHandle));
    }

    /**
     * Returns the type-safe handle to the native reader.
     */
    public ReaderHandle getReaderHandle() {
        return readerHandle;
    }

    @Override
    public void close() {
        // ReaderHandle lifecycle is owned by DatafusionReader / EngineReaderManager,
        // not by the searcher. Do not close it here.
    }
}
