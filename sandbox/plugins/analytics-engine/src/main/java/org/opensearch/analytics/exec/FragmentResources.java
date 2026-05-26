/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;

/**
 * Holds the per-fragment resources (reader, engine, result stream) kept alive for the
 * duration of a streaming fragment execution, and releases them in reverse order on close.
 *
 * @opensearch.internal
 */
public final class FragmentResources implements AutoCloseable {

    private final GatedCloseable<Reader> gatedReader;
    private final SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine;
    private final EngineResultStream stream;
    private final Runnable onClose;
    private final org.opensearch.analytics.spi.ExchangeSink partitionedSink;
    private final ShardScanExecutionContext executionContext;

    public FragmentResources(
        GatedCloseable<Reader> gatedReader,
        SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine,
        EngineResultStream stream
    ) {
        this(gatedReader, engine, stream, null, null, null);
    }

    public FragmentResources(
        GatedCloseable<Reader> gatedReader,
        SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine,
        EngineResultStream stream,
        Runnable onClose
    ) {
        this(gatedReader, engine, stream, onClose, null, null);
    }

    /**
     * @param partitionedSink  non-null when the fragment's instruction chain produced a
     *                         {@code ShuffleProducerOutputState}: the engine's output is to be
     *                         drained into this sink instead of through the streaming response.
     *                         The caller owns the sink's lifecycle (close after draining).
     * @param executionContext the {@link ShardScanExecutionContext} the engine is running on.
     *                         Captured here so the caller can pass it back into the partitioned
     *                         sink's flow if needed (allocator etc.).
     */
    public FragmentResources(
        GatedCloseable<Reader> gatedReader,
        SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine,
        EngineResultStream stream,
        Runnable onClose,
        org.opensearch.analytics.spi.ExchangeSink partitionedSink,
        ShardScanExecutionContext executionContext
    ) {
        this.gatedReader = gatedReader;
        this.engine = engine;
        this.stream = stream;
        this.onClose = onClose;
        this.partitionedSink = partitionedSink;
        this.executionContext = executionContext;
    }

    public EngineResultStream stream() {
        return stream;
    }

    /** Non-null iff this fragment is a hash-shuffle producer; the stream's batches must be fed
     *  into the sink instead of being returned to the originating coordinator. */
    public org.opensearch.analytics.spi.ExchangeSink partitionedSink() {
        return partitionedSink;
    }

    public ShardScanExecutionContext executionContext() {
        return executionContext;
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
        first = closeQuietly(gatedReader, first);
        // partitionedSink is closed by the routing flow in AnalyticsSearchService BEFORE this
        // close() runs, so the sink's isLast markers are guaranteed to ship before the engine /
        // reader are torn down. We don't double-close here — close() is idempotent on the sink
        // but we keep ownership clear: routing closes when draining is done.
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
