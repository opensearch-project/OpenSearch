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

    public FragmentResources(
        GatedCloseable<Reader> gatedReader,
        SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine,
        EngineResultStream stream
    ) {
        this(gatedReader, engine, stream, null);
    }

    public FragmentResources(
        GatedCloseable<Reader> gatedReader,
        SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine,
        EngineResultStream stream,
        Runnable onClose
    ) {
        this.gatedReader = gatedReader;
        this.engine = engine;
        this.stream = stream;
        this.onClose = onClose;
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
        first = closeQuietly(gatedReader, first);
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
