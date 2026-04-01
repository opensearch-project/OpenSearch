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
import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.be.datafusion.jni.StreamHandle;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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

    /**
     * Creates a result stream.
     * @param streamHandle the native stream handle
     * @param allocator the Arrow buffer allocator for this stream (caller transfers ownership)
     */
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
            if (iteratorInstance != null && iteratorInstance.vectorSchemaRoot != null) {
                iteratorInstance.vectorSchemaRoot.close();
            }
        } finally {
            try {
                streamHandle.close();
            } finally {
                try {
                    dictionaryProvider.close();
                } finally {
                    allocator.close();
                }
            }
        }
    }

    /**
     * Iterator that pulls Arrow record batches from the native stream via async JNI.
     * Uses one-ahead buffering: the next batch is pre-loaded so hasNext() is side-effect-free.
     */
    static class BatchIterator implements Iterator<EngineResultBatch> {

        private final StreamHandle streamHandle;
        private final BufferAllocator allocator;
        private final CDataDictionaryProvider dictionaryProvider;
        VectorSchemaRoot vectorSchemaRoot;
        private Boolean nextAvailable;
        /** Incremented each time {@link #next()} is called. Used by {@link ArrowResultBatch} to detect stale access. */
        long generation;

        BatchIterator(StreamHandle streamHandle, BufferAllocator allocator, CDataDictionaryProvider dictionaryProvider) {
            this.streamHandle = streamHandle;
            this.allocator = allocator;
            this.dictionaryProvider = dictionaryProvider;
        }

        private void ensureSchema() {
            if (vectorSchemaRoot != null) return;
            long schemaAddr = callNativeFn(listener -> NativeBridge.streamGetSchema(streamHandle.getPointer(), listener));
            try (ArrowSchema arrowSchema = ArrowSchema.wrap(schemaAddr)) {
                Field structField = importField(allocator, arrowSchema, dictionaryProvider);
                if (structField.getType().getTypeID() != ArrowType.ArrowTypeID.Struct) {
                    throw new IllegalStateException("ArrowSchema describes non-struct type");
                }
                Schema schema = new Schema(structField.getChildren(), structField.getMetadata());
                vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
            }
        }

        private boolean loadNextBatch() {
            ensureSchema();
            long arrayAddr = callNativeFn(
                listener -> NativeBridge.streamNext(streamHandle.getRuntimeHandle().get(), streamHandle.getPointer(), listener)
            );
            if (arrayAddr == 0) return false;
            try (ArrowArray arrowArray = ArrowArray.wrap(arrayAddr)) {
                Data.importIntoVectorSchemaRoot(allocator, arrowArray, vectorSchemaRoot, dictionaryProvider);
            }
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
            generation++;
            return new ArrowResultBatch(vectorSchemaRoot, generation, this);
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

    /**
     * Adapts an Arrow {@link VectorSchemaRoot} to the engine-agnostic {@link EngineResultBatch}.
     * <p>
     * Because the underlying {@code VectorSchemaRoot} is reused across batches,
     * this view is only valid until the next call to {@link Iterator#next()} on
     * the parent iterator. A generation counter detects stale access at runtime.
     */
    static class ArrowResultBatch implements EngineResultBatch {

        private final VectorSchemaRoot root;
        private final List<String> fieldNames;
        private final long createdAtGeneration;
        private final BatchIterator owner;

        ArrowResultBatch(VectorSchemaRoot root, long generation, BatchIterator owner) {
            this.root = root;
            this.fieldNames = root.getSchema().getFields().stream().map(Field::getName).collect(Collectors.toUnmodifiableList());
            this.createdAtGeneration = generation;
            this.owner = owner;
        }

        private void checkValid() {
            if (owner.generation != createdAtGeneration) {
                throw new IllegalStateException(
                    "Batch is no longer valid — the iterator has advanced past this batch. "
                        + "Extract all needed values before calling next()."
                );
            }
        }

        @Override
        public List<String> getFieldNames() {
            checkValid();
            return fieldNames;
        }

        @Override
        public int getRowCount() {
            checkValid();
            return root.getRowCount();
        }

        @Override
        public Object getFieldValue(String fieldName, int rowIndex) {
            checkValid();
            FieldVector vector = root.getVector(fieldName);
            if (vector == null) {
                throw new IllegalArgumentException("Unknown field: " + fieldName);
            }
            return vector.getObject(rowIndex);
        }
    }
}
