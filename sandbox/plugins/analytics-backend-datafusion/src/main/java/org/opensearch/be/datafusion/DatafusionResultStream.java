/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultBatchIterator;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.be.datafusion.jni.StreamHandle;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.NoSuchElementException;

/**
 * {@link EngineResultStream} backed by a native DataFusion record batch stream.
 * <p>
 * Reads Arrow record batches from the native stream via JNI and exposes them
 * as {@link EngineResultBatch} instances. The stream is single-pass; calling
 * {@link #iterator()} multiple times returns the same iterator.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionResultStream implements EngineResultStream {

    private final StreamHandle streamHandle;
    private volatile BatchIterator iteratorInstance;

    /**
     * Creates a result stream
     * @param streamHandle the native stream handle
     */
    public DatafusionResultStream(StreamHandle streamHandle) {
        this.streamHandle = streamHandle;
    }

    @Override
    public EngineResultBatchIterator iterator() {
        if (iteratorInstance == null) {
            iteratorInstance = new BatchIterator(streamHandle);
        }
        return iteratorInstance;
    }

    @Override
    public void close() {
        streamHandle.close();
    }

    /**
     * Iterator that pulls Arrow record batches from the native stream via JNI.
     * Each call to {@link #next()} returns a batch wrapping the current Arrow data.
     */
    static class BatchIterator implements EngineResultBatchIterator {

        private final StreamHandle streamHandle;
        private Boolean hasNext;

        BatchIterator(StreamHandle streamHandle) {
            this.streamHandle = streamHandle;
        }

        @Override
        public boolean hasNext() {
            if (hasNext == null) {
                long arrowArrayAddr = NativeBridge.streamNext(streamHandle.getStreamPtr(), streamHandle.getPointer());
                hasNext = arrowArrayAddr != 0;
                // TODO: if hasNext, import ArrowArray into VectorSchemaRoot and cache for next()
            }
            return hasNext;
        }

        @Override
        public EngineResultBatch next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            hasNext = null;
            // TODO: return batch wrapping the imported VectorSchemaRoot
            throw new UnsupportedOperationException("Arrow C Data import not yet wired");
        }
    }
}
