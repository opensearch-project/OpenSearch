/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.BigIntVector;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.backend.ShardScanExecutionContext;

/**
 * Holds the per-fragment resources (reader context, engine, result stream) kept alive for
 * the duration of a streaming fragment execution, and releases them in reverse order on close.
 *
 * <p>The reader is owned by {@link ReaderContextStore}, not by this class — close releases
 * (does not free) the context, so the reader stays alive across the QTF query→fetch
 * boundary. The store's reaper closes the underlying reader after keepAlive elapses.
 *
 * @opensearch.internal
 */
public final class FragmentResources implements AutoCloseable {

    private final ReaderContextStore readerContextStore;
    private final ReaderContext readerContext;
    private final SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine;
    private final EngineResultStream stream;
    private final Runnable onClose;
    /**
     * Off-heap rowId buffer kept alive across the fetch stream's lifetime. Non-null only
     * for the QTF fetch path, where the native side reads rowIds directly via the
     * BigIntVector's data-buffer address — closing it before the stream drains would
     * pull memory out from under the FFM call.
     */
    private final BigIntVector rowIdVector;

    public FragmentResources(
        ReaderContextStore readerContextStore,
        ReaderContext readerContext,
        SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine,
        EngineResultStream stream,
        Runnable onClose
    ) {
        this(readerContextStore, readerContext, engine, stream, onClose, null);
    }

    public FragmentResources(
        ReaderContextStore readerContextStore,
        ReaderContext readerContext,
        SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine,
        EngineResultStream stream,
        Runnable onClose,
        BigIntVector rowIdVector
    ) {
        assert assertCtorInvariants(readerContextStore, readerContext);
        this.readerContextStore = readerContextStore;
        this.readerContext = readerContext;
        this.engine = engine;
        this.stream = stream;
        this.onClose = onClose;
        this.rowIdVector = rowIdVector;
    }

    private static boolean assertCtorInvariants(ReaderContextStore store, ReaderContext ctx) {
        if (store == null) throw new AssertionError("readerContextStore is required for FragmentResources");
        if (ctx == null) throw new AssertionError("readerContext is required for FragmentResources");
        return true;
    }

    public EngineResultStream stream() {
        return stream;
    }

    @Override
    public void close() throws Exception {
        Exception first = null;
        if (onClose != null) {
            try {
                onClose.run();
            } catch (Exception e) {
                first = e;
            }
        }
        first = closeQuietly(stream, first);
        first = closeQuietly(engine, first);
        first = closeQuietly(rowIdVector, first);
        // Release (not close) — the store's reaper closes after keepAlive, and the QTF
        // fetch phase may still need this reader before then.
        if (readerContext != null) {
            try {
                readerContextStore.releaseContext(readerContext.getQueryId());
            } catch (Exception e) {
                if (first == null) first = e;
                else first.addSuppressed(e);
            }
        }
        if (first != null) throw first;
    }

    private static Exception closeQuietly(AutoCloseable resource, Exception prior) {
        if (resource == null) return prior;
        try {
            resource.close();
        } catch (Exception e) {
            if (prior == null) return e;
            prior.addSuppressed(e);
        }
        return prior;
    }
}
