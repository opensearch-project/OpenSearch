/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.index.engine.exec.read.EngineSearcher;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Datafusion searcher to query via substrait plan
 */
public class DatafusionSearcher implements EngineSearcher<DatafusionQuery> {
    private final String source;
    private org.opensearch.datafusion.search.DatafusionReader reader;
    private Closeable closeable;

    public DatafusionSearcher(String source, org.opensearch.datafusion.search.DatafusionReader reader, Closeable close) {
        this.source = source;
        this.reader = reader;
    }

    @Override
    public String source() {
        return source;
    }

    @Override
    public void search(DatafusionQuery datafusionQuery) {
        // TODO : implementation for search
        // return DataFusionQueryJNI.executeSubstraitQuery(reader.getReaderPtr(), datafusionQuery.getSubstraitBytes(), contextPtr);
    }

    public org.opensearch.datafusion.search.DatafusionReader getReader() {
        return reader;
    }

    @Override
    public void close() {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to close", e);
        } catch (AlreadyClosedException e) {
            // This means there's a bug somewhere: don't suppress it
            throw new AssertionError(e);
        }

    }
}
