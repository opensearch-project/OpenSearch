/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.analytics.backend.EngineResultBatchIterator;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.datafusion.search.RecordBatchIterator;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * DataFusion implementation of {@link EngineResultStream} that wraps a
 * {@link RecordBatchStream} and exposes record batches through the
 * engine-agnostic abstraction.
 *
 * <p>The iterator is lazily created on the first call to {@link #iterator()}
 * and cached for subsequent calls — the stream is single-pass.
 *
 * <p>{@link #close()} delegates to {@link RecordBatchStream#close()} to
 * release native Arrow memory and JNI handles.
 *
 * @opensearch.internal
 */
public class DataFusionResultStream implements EngineResultStream {

    private final RecordBatchStream stream;
    private DataFusionResultBatchIterator iteratorInstance;

    public DataFusionResultStream(long streamPointer, long runtimePointer, BufferAllocator allocator) {
        this.stream = new RecordBatchStream(streamPointer, runtimePointer, allocator);
    }

    public DataFusionResultStream(long streamPointer, long runtimePointer, long taskId, BufferAllocator allocator) {
        this.stream = new RecordBatchStream(streamPointer, runtimePointer, taskId, allocator);
    }

    @Override
    public EngineResultBatchIterator iterator() {
        if (iteratorInstance == null) {
            iteratorInstance = new DataFusionResultBatchIterator(new RecordBatchIterator(stream));
        }
        return iteratorInstance;
    }

    @Override
    public void close() {
        try {
            stream.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
