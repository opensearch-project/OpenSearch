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
import org.opensearch.analytics.spi.ExchangeSink;

/**
 * Holds the per-fragment resources (reader context, engine, result stream) kept alive for
 * the duration of a streaming fragment execution, and releases them in reverse order on close.
 *
 * <p>The reader is owned by {@link ReaderContextStore}, not by this class. When a fetch phase will
 * reuse the reader (QTF query phase), close only releases the context so it stays alive across the
 * query→fetch boundary, and the store's reaper closes it after keepAlive. Otherwise (non-QTF query,
 * or the terminal fetch phase itself) close frees the context immediately so the reader is not
 * pinned in the store until the reaper sweeps it.
 *
 * @opensearch.internal
 */
public final class FragmentResources implements AutoCloseable {

    private final ReaderContextStore readerContextStore;
    private final ReaderContext readerContext;
    private final SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine;
    private final EngineResultStream stream;
    private final Runnable onClose;
    private final ExchangeSink partitionedSink;
    private final ShardScanExecutionContext executionContext;
    /**
     * Off-heap rowId buffer kept alive across the fetch stream's lifetime. Non-null only
     * for the QTF fetch path, where the native side reads rowIds directly via the
     * BigIntVector's data-buffer address — closing it before the stream drains would
     * pull memory out from under the FFM call.
     */
    private final BigIntVector rowIdVector;
    /**
     * True when this query requested top-N docs (row-ids), so a fetch phase will reuse this reader
     * (QTF query phase); close then keeps the reader in the store for that fetch.
     */
    private final boolean requiresTopDocs;

    public FragmentResources(
        ReaderContextStore readerContextStore,
        ReaderContext readerContext,
        SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine,
        EngineResultStream stream,
        Runnable onClose,
        boolean requiresTopDocs
    ) {
        this(readerContextStore, readerContext, engine, stream, onClose, null, requiresTopDocs, null, null);
    }

    public FragmentResources(
        ReaderContextStore readerContextStore,
        ReaderContext readerContext,
        SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine,
        EngineResultStream stream,
        Runnable onClose,
        BigIntVector rowIdVector,
        boolean requiresTopDocs
    ) {
        this(readerContextStore, readerContext, engine, stream, onClose, rowIdVector, requiresTopDocs, null, null);
    }

    /**
     * Convenience overload for the hash-shuffle producer path: no top-docs fetch (producers stream
     * their output into the partitioned sink, never reuse the reader for a fetch phase).
     *
     * @param partitionedSink  non-null when the fragment's instruction chain produced a
     *                         {@code ShuffleProducerOutputState}: the engine's output is to be
     *                         drained into this sink instead of through the streaming response.
     *                         The caller owns the sink's lifecycle (close after draining).
     * @param executionContext the {@link ShardScanExecutionContext} the engine is running on.
     *                         Captured here so the caller can pass it back into the partitioned
     *                         sink's flow if needed (allocator etc.).
     */
    public FragmentResources(
        ReaderContextStore readerContextStore,
        ReaderContext readerContext,
        SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine,
        EngineResultStream stream,
        Runnable onClose,
        BigIntVector rowIdVector,
        ExchangeSink partitionedSink,
        ShardScanExecutionContext executionContext
    ) {
        this(readerContextStore, readerContext, engine, stream, onClose, rowIdVector, false, partitionedSink, executionContext);
    }

    /**
     * Full constructor. Upstream's {@code requiresTopDocs} is kept ahead of our MPP-shuffle params
     * ({@code partitionedSink}, {@code executionContext}) per the "our params to the END of
     * upstream-owned signatures" convention.
     */
    public FragmentResources(
        ReaderContextStore readerContextStore,
        ReaderContext readerContext,
        SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine,
        EngineResultStream stream,
        Runnable onClose,
        BigIntVector rowIdVector,
        boolean requiresTopDocs,
        ExchangeSink partitionedSink,
        ShardScanExecutionContext executionContext
    ) {
        assert assertCtorInvariants(readerContextStore, readerContext);
        this.readerContextStore = readerContextStore;
        this.readerContext = readerContext;
        this.engine = engine;
        this.stream = stream;
        this.onClose = onClose;
        this.rowIdVector = rowIdVector;
        this.requiresTopDocs = requiresTopDocs;
        this.partitionedSink = partitionedSink;
        this.executionContext = executionContext;
    }

    private static boolean assertCtorInvariants(ReaderContextStore store, ReaderContext ctx) {
        if (store == null) throw new AssertionError("readerContextStore is required for FragmentResources");
        if (ctx == null) throw new AssertionError("readerContext is required for FragmentResources");
        return true;
    }

    public EngineResultStream stream() {
        return stream;
    }

    /** Non-null iff this fragment is a hash-shuffle producer; the stream's batches must be fed
     *  into the sink instead of being returned to the originating coordinator. */
    public ExchangeSink partitionedSink() {
        return partitionedSink;
    }

    public ShardScanExecutionContext executionContext() {
        return executionContext;
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
        // partitionedSink is closed by the routing flow in AnalyticsSearchService BEFORE this
        // close() runs, so the sink's isLast markers are guaranteed to ship before the engine /
        // reader are torn down. We don't double-close here — close() is idempotent on the sink
        // but we keep ownership clear: routing closes when draining is done.
        //
        // Reader context: if this query requested top-N docs, a fetch phase will reuse this reader,
        // so only release this phase's use-reference (releaseContext) — it stays alive for the fetch
        // and the store's reaper closes it after keepAlive. Otherwise releaseAndFree now so the reader
        // is closed immediately instead of being pinned until the reaper sweeps it.
        if (readerContext != null) {
            try {
                if (requiresTopDocs) {
                    readerContextStore.releaseContext(readerContext.getQueryId(), readerContext.getShardId());
                } else {
                    readerContextStore.releaseAndFree(readerContext.getQueryId(), readerContext.getShardId());
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
