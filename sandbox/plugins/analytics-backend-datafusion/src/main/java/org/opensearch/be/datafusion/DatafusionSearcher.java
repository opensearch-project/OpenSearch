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
 * DataFusion searcher — executes substrait query plans against a native DataFusion reader.
 * <p>
 * A single entry point: {@link NativeBridge#executeQueryAsync} handles both vanilla
 * parquet and indexed (index_filter-bearing) plans. The native side classifies the
 * substrait plan and dispatches internally; Java is oblivious to which path runs.
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
        if (sessionCtx != null) {
            searchWithSessionContext(context, sessionCtx);
        } else {
            searchVanilla(context);
        }
    }

    private void searchWithSessionContext(DatafusionContext context, SessionContextHandle sessionCtx) throws IOException {
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
        // NativeBridge#executeWithContextAsync has already marked the handle consumed (which
        // closes the Java wrapper) on both success and native-error paths; no explicit close
        // is needed here. The owning DatafusionContext#close() closes it as a safety net for
        // paths that never reach this method (e.g. aborted search).
        context.setStreamHandle(new StreamHandle(streamPtr, runtimeHandle));
    }

    // TODO: Remove searchVanilla once all execution paths go through instruction handlers.
    // Deprecated — retained only for tests that bypass AnalyticsSearchService.
    private void searchVanilla(DatafusionContext context) throws IOException {
        DatafusionQuery query = context.getDatafusionQuery();
        if (query == null) {
            throw new IllegalStateException("DatafusionQuery must be set before search");
        }
        NativeRuntimeHandle runtimeHandle = context.getNativeRuntime();
        CompletableFuture<Long> future = new CompletableFuture<>();
        NativeBridge.executeQueryAsync(
            readerHandle.getPointer(),
            query.getIndexName(),
            query.getSubstraitBytes(),
            runtimeHandle.get(),
            query.getContextId(),
            0L,
            new ActionListener<>() {
                @Override
                public void onResponse(Long streamPtr) {
                    future.complete(streamPtr);
                }

                @Override
                public void onFailure(Exception e) {
                    future.completeExceptionally(e);
                }
            }
        );
        long streamPtr;
        try {
            streamPtr = future.join();
        } catch (Exception e) {
            throw new IOException("Query execution failed", e);
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
