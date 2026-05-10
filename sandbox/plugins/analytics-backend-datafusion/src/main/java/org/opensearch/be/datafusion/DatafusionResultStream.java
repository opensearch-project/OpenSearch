/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.exec.ArrowValues;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

import static org.apache.arrow.c.Data.importField;

/**
 * {@link EngineResultStream} backed by a native DataFusion record batch stream.
 * <p>
 * Reads Arrow record batches from the native stream via async JNI using the
 * Arrow C Data Interface and exposes them as {@link EngineResultBatch} instances.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionResultStream implements EngineResultStream {

    private final StreamHandle streamHandle;
    private final BufferAllocator allocator;
    private final CDataDictionaryProvider dictionaryProvider;
    private volatile BatchIterator iteratorInstance;

    // Allocator is caller-owned; this stream imports into it but never closes it.
    public DatafusionResultStream(StreamHandle streamHandle, BufferAllocator allocator) {
        this.streamHandle = streamHandle;
        this.allocator = allocator;
        this.dictionaryProvider = new CDataDictionaryProvider();
    }

    @Override
    public Iterator<EngineResultBatch> iterator() {
        if (iteratorInstance == null) {
            iteratorInstance = new BatchIterator(streamHandle, allocator, dictionaryProvider);
        }
        return iteratorInstance;
    }

    @Override
    public void close() {
        try {
            if (iteratorInstance != null) {
                iteratorInstance.closeLastBatch();
            }
        } finally {
            try {
                streamHandle.close();
            } finally {
                dictionaryProvider.close();
            }
        }
    }

    // Fresh VSR per batch so each can be handed off independently
    // Close-on-advance releases the previous VSR (no-op if transport already transferred it).
    static class BatchIterator implements Iterator<EngineResultBatch> {

        private final StreamHandle streamHandle;
        private final BufferAllocator allocator;
        private final CDataDictionaryProvider dictionaryProvider;
        private Schema schema;
        private VectorSchemaRoot nextBatch;
        private Boolean nextAvailable;
        // True once we've returned at least one batch (real or synthesized). Prevents the
        // "zero batches produced" rescue below from re-firing on subsequent polls.
        private boolean batchEmitted;
        // True once the native stream has yielded EOS (arrayAddr == 0). Combined with
        // batchEmitted, it lets us synthesize exactly one schema-carrying empty batch when
        // the native side produced nothing — so downstream Arrow Flight (or any other
        // transport that relies on the first data frame to deliver the schema) still sees
        // the column layout.
        private boolean nativeStreamExhausted;

        BatchIterator(StreamHandle streamHandle, BufferAllocator allocator, CDataDictionaryProvider dictionaryProvider) {
            this.streamHandle = streamHandle;
            this.allocator = allocator;
            this.dictionaryProvider = dictionaryProvider;
        }

        private void ensureSchema() {
            if (schema != null) return;
            long schemaAddr = callNativeFn(listener -> NativeBridge.streamGetSchema(streamHandle.getPointer(), listener));
            try (ArrowSchema arrowSchema = ArrowSchema.wrap(schemaAddr)) {
                Field structField = importField(allocator, arrowSchema, dictionaryProvider);
                if (structField.getType().getTypeID() != ArrowType.ArrowTypeID.Struct) {
                    throw new IllegalStateException("ArrowSchema describes non-struct type");
                }
                schema = new Schema(structField.getChildren(), structField.getMetadata());
            }
        }

        private boolean loadNextBatch() {
            ensureSchema();
            // Once the native stream has reported EOS, never poll again and never
            // re-synthesize an empty batch. See the synthesize branch below.
            if (nativeStreamExhausted) return false;

            long arrayAddr = callNativeFn(
                listener -> NativeBridge.streamNext(streamHandle.getRuntimeHandle().get(), streamHandle.getPointer(), listener)
            );

            if (arrayAddr == 0) {
                nativeStreamExhausted = true;
                // Native source had zero batches — synthesize one zero-row batch so the
                // schema rides on the first (and only) Flight data frame. Without this the
                // wire stream is schema-less and any downstream that relies on the schema
                // (coordinator StreamingTableExec, row-path schema introspection) fails
                // with cryptic errors such as "project index 0 out of bounds, max field 0".
                if (!batchEmitted) {
                    nextBatch = VectorSchemaRoot.create(schema, allocator);
                    nextBatch.setRowCount(0);
                    batchEmitted = true;
                    return true;
                }
                return false;
            }

            VectorSchemaRoot freshRoot = VectorSchemaRoot.create(schema, allocator);
            try (ArrowArray arrowArray = ArrowArray.wrap(arrayAddr)) {
                Data.importIntoVectorSchemaRoot(allocator, arrowArray, freshRoot, dictionaryProvider);
            }
            nextBatch = freshRoot;
            batchEmitted = true;
            return true;
        }

        @Override
        public boolean hasNext() {
            if (nextAvailable == null) {
                nextAvailable = loadNextBatch();
            }
            return nextAvailable;
        }

        @Override
        public EngineResultBatch next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            nextAvailable = null;
            VectorSchemaRoot batch = nextBatch;
            nextBatch = null;
            // Caller owns the returned VSR's lifecycle. Streaming handler transfers it to Flight
            // (Flight closes after wire write); row-path collector closes after reading.
            return new ArrowResultBatch(batch);
        }

        void closeLastBatch() {
            // Only close batches that were loaded but never handed to the caller. Caller
            // owns any batch returned by next(); closing it here would double-close after
            // Flight's transferTo or after row-path reads.
            if (nextBatch != null) {
                nextBatch.close();
                nextBatch = null;
            }
        }

        private static long callNativeFn(java.util.function.Consumer<ActionListener<Long>> fn) {
            CompletableFuture<Long> future = new CompletableFuture<>();
            fn.accept(new ActionListener<>() {
                @Override
                public void onResponse(Long v) {
                    future.complete(v);
                }

                @Override
                public void onFailure(Exception e) {
                    future.completeExceptionally(e);
                }
            });
            return future.join();
        }
    }

    static class ArrowResultBatch implements EngineResultBatch {

        private final VectorSchemaRoot root;
        private final List<String> fieldNames;

        ArrowResultBatch(VectorSchemaRoot root) {
            this.root = root;
            this.fieldNames = root.getSchema().getFields().stream().map(Field::getName).toList();
        }

        @Override
        public VectorSchemaRoot getArrowRoot() {
            return root;
        }

        @Override
        public List<String> getFieldNames() {
            return fieldNames;
        }

        @Override
        public int getRowCount() {
            return root.getRowCount();
        }

        @Override
        public Object getFieldValue(String fieldName, int rowIndex) {
            FieldVector vector = root.getVector(fieldName);
            if (vector == null) {
                throw new IllegalArgumentException("Unknown field: " + fieldName);
            }
            return ArrowValues.toJavaValue(vector, rowIndex);
        }
    }
}
