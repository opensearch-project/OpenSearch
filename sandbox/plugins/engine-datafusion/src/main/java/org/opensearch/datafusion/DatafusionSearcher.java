/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.EngineSearcher;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * DataFusion searcher — executes substrait query plans against a native DataFusion reader.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionSearcher implements EngineSearcher<DatafusionContext> {

    private final String source;
    private final long readerPtr;
    private final Closeable onClose;

    public DatafusionSearcher(String source, long readerPtr, Closeable onClose) {
        this.source = source;
        this.readerPtr = readerPtr;
        this.onClose = onClose;
    }

    @Override
    public String source() {
        return source;
    }

    @Override
    public void search(DatafusionContext context) throws IOException {
        // TODO: wire NativeBridge — execute substrait plan, consume stream, populate context
        throw new UnsupportedOperationException("DataFusion native bridge not yet wired");
    }

    public long getReaderPtr() {
        return readerPtr;
    }

    @Override
    public void close() {
        try {
            if (onClose != null) {
                onClose.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to close DatafusionSearcher", e);
        }
    }
}
