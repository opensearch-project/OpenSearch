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
 * <p>The reader is owned by {@link ReaderContextStore}, not by this class. For a multi-session
 * reader (QTF query phase, whose fetch phase reuses the reader), close only releases the context
 * so the reader stays alive across the query→fetch boundary, and the store's reaper closes it
 * after keepAlive. For a single-session reader (non-QTF query, or the terminal fetch phase
 * itself), close frees the context immediately so the reader is not pinned in the store until the
 * reaper sweeps it.
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
    /** True when a fetch phase will reuse this reader (QTF query phase); close then keeps it for the fetch. */
    private final boolean fetchFollows;

    /**
     * Whether this reader serves a single request rather than living across many (scroll/PIT style).
     * Always true today — analytics-engine has no scroll/PIT-equivalent that outlives one request.
     * TODO: the coordinator (which knows the query shape) should set this per request rather than it
     *  defaulting to true; a future multi-session reader would not be freed on close here.
     */
    private final boolean singleSession = true;

    public FragmentResources(
        ReaderContextStore readerContextStore,
        ReaderContext readerContext,
        SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine,
        EngineResultStream stream,
        Runnable onClose,
        boolean fetchFollows
    ) {
        this(readerContextStore, readerContext, engine, stream, onClose, null, fetchFollows);
    }

    public FragmentResources(
        ReaderContextStore readerContextStore,
        ReaderContext readerContext,
        SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine,
        EngineResultStream stream,
        Runnable onClose,
        BigIntVector rowIdVector,
        boolean fetchFollows
    ) {
        assert assertCtorInvariants(readerContextStore, readerContext);
        this.readerContextStore = readerContextStore;
        this.readerContext = readerContext;
        this.engine = engine;
        this.stream = stream;
        this.onClose = onClose;
        this.rowIdVector = rowIdVector;
        this.fetchFollows = fetchFollows;
    }

    private static boolean assertCtorInvariants(ReaderContextStore store, ReaderContext ctx) {
        if (store == null) throw new AssertionError("readerContextStore is required for FragmentResources");
        if (ctx == null) throw new AssertionError("readerContext is required for FragmentResources");
        return true;
    }

    public EngineResultStream stream() {
        return stream;
    }

    /**
     * Extracts execution metrics from the underlying engine stream (if supported).
     * Must be called after the stream is exhausted but before close().
     * Returns null if the stream doesn't support metrics extraction.
     */
    public byte[] getExecutionMetrics() {
        if (stream instanceof MetricsCapable mc) {
            return mc.getMetricsJson();
        }
        return null;
    }

    /** Marker interface for streams that can provide execution metrics. */
    public interface MetricsCapable {
        byte[] getMetricsJson();
    }

    @Override
    public void close() throws Exception {
        // Close the stream and engine first so any in-flight release upcalls from native
        // code (e.g. ProviderHandle::drop -> releaseProvider) can still find their
        // per-query binding in FilterTreeCallbacks. Running onClose first would unregister
        // the binding while release upcalls are still pending, causing the eager release
        // of Lucene resources to be skipped.
        Exception first = closeQuietly(stream, null);
        first = closeQuietly(engine, first);
        first = closeQuietly(rowIdVector, first);
        if (onClose != null) {
            try {
                onClose.run();
            } catch (Exception e) {
                if (first == null) first = e;
                else first.addSuppressed(e);
            }
        }
        // Drop this phase's use-reference. If a fetch phase will reuse this reader (QTF query
        // phase), only release it so it stays alive for the fetch and the reaper closes after
        // keepAlive. Otherwise free it now: release this phase's reference, then free the store's
        // base reference so a single-session reader is closed immediately instead of being pinned
        // until the reaper sweeps it.
        if (readerContext != null) {
            try {
                readerContextStore.releaseContext(readerContext.getQueryId(), readerContext.getShardId());
                if (singleSession && !fetchFollows) {
                    readerContextStore.freeContext(readerContext.getQueryId(), readerContext.getShardId());
                }
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
