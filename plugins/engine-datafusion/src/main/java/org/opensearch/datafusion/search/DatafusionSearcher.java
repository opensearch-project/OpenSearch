/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.index.engine.EngineSearcher;
import org.opensearch.vectorized.execution.search.spi.RecordBatchStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class DatafusionSearcher implements EngineSearcher<DatafusionQuery, RecordBatchStream> {
    private final String source;
    private DatafusionReader reader;
    private Closeable closeable;

    public DatafusionSearcher(String source, DatafusionReader reader, Closeable close) {
        this.source = source;
        this.reader = reader;
        this.closeable = close;
    }

    @Override
    public String source() {
        return source;
    }


    @Override
    public long search(DatafusionQuery datafusionQuery, Long runtimePtr) {
        if (datafusionQuery.isFetchPhase()) {
            long[] row_ids = datafusionQuery.getQueryPhaseRowIds()
                .stream()
                .mapToLong(Long::longValue)
                .toArray();
            String[] includeFields = Objects.isNull(datafusionQuery.getIncludeFields()) ? new String[]{} : datafusionQuery.getIncludeFields().toArray(String[]::new);
            String[] excludeFields = Objects.isNull(datafusionQuery.getExcludeFields()) ? new String[]{} : datafusionQuery.getExcludeFields().toArray(String[]::new);

            return NativeBridge.executeFetchPhase(reader.getReaderPtr(), row_ids, includeFields, excludeFields, runtimePtr);
        }
        throw new RuntimeException("Can be only called for fetch phase");
    }

    @Override
    public CompletableFuture<Long> searchAsync(DatafusionQuery datafusionQuery, Long runtimePtr) {
        CompletableFuture<Long> result = new CompletableFuture<>();
        NativeBridge.executeQueryPhaseAsync(reader.getReaderPtr(), datafusionQuery.getIndexName(), datafusionQuery.getSubstraitBytes(), datafusionQuery.getQueryPlanExplainEnabled(), runtimePtr, new ActionListener<Long>() {
            @Override
            public void onResponse(Long streamPointer) {
                if (streamPointer == 0) {
                    result.complete(0L);
                } else {
                    result.complete(streamPointer);
                }
            }

            @Override
            public void onFailure(Exception e) {
                result.completeExceptionally(e);
            }
        });
        return result;
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
