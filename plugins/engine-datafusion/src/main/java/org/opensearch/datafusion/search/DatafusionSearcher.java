/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.datafusion.DataFusionQueryJNI;
import org.opensearch.datafusion.DataFusionService;
import org.opensearch.datafusion.core.DefaultRecordBatchStream;
import org.opensearch.index.engine.EngineSearcher;
import org.opensearch.search.aggregations.SearchResultsCollector;
import org.opensearch.vectorized.execution.search.spi.RecordBatchStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DatafusionSearcher implements EngineSearcher<DatafusionQuery, RecordBatchStream> {
    private final String source;
    private DatafusionReader reader;
    private Closeable closeable;
    public DatafusionSearcher(String source, DatafusionReader reader, Closeable close) {
        this.source = source;
        this.reader = reader;
    }

    @Override
    public String source() {
        return source;
    }

    @Override
    public void search(DatafusionQuery datafusionQuery, List<SearchResultsCollector<RecordBatchStream>> collectors) throws IOException {
        // TODO : call search here to native
        // TODO : change RunTimePtr
        long nativeStreamPtr = DataFusionQueryJNI.executeSubstraitQuery(reader.getCachePtr(), datafusionQuery.getSubstraitBytes(), 0);
        RecordBatchStream stream = new DefaultRecordBatchStream(nativeStreamPtr);
        while(stream.hasNext()) {
            for(SearchResultsCollector<RecordBatchStream> collector : collectors) {
                collector.collect(stream);
            }
        }
    }

    @Override
    public long search(DatafusionQuery datafusionQuery, Long contextPtr) {
        return DataFusionQueryJNI.executeSubstraitQuery(reader.getCachePtr(), datafusionQuery.getSubstraitBytes(), contextPtr);
    }

    public DatafusionReader getReader() {
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
