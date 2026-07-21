/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

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
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.arrow.c.Data.importField;

/**
 * Lucene-side {@link EngineResultStream}. Mirrors {@code DatafusionResultStream}: same
 * {@link BatchIterator} pump, same {@link ArrowResultBatch} wrapper, same
 * {@link Data#importIntoVectorSchemaRoot} call to materialise each batch into a fresh
 * {@link VectorSchemaRoot}. The only difference is the source of the {@link ArrowArray}:
 * DataFusion gets it from a native record-batch stream (Rust → JNI), Lucene exports a
 * scratch VSR through the C-Data interface so the resulting buffer layout matches the
 * foreign-allocation-managed shape that survives Flight's {@code VectorTransfer.transferRoot}.
 *
 * <p>Today's only producer is the count fast path (one batch per shard), but the class
 * itself is operation-agnostic — any future Lucene-driver result that fits a single
 * pre-built {@code ArrowArray} reuses this stream.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneResultStream implements EngineResultStream {

    /** C-Data array carrying the populated batch. Owned by this stream until {@link #close}. */
    private final ArrowArray arrowArray;
    /** C-Data schema describing {@link #arrowArray}. */
    private final ArrowSchema arrowSchema;
    private final BufferAllocator allocator;
    private final CDataDictionaryProvider dictionaryProvider;
    private volatile BatchIterator iteratorInstance;

    /**
     * Caller hands over ownership of {@code arrowArray} and {@code arrowSchema}; this stream
     * closes them in {@link #close}.
     */
    public LuceneResultStream(ArrowArray arrowArray, ArrowSchema arrowSchema, BufferAllocator allocator) {
        this.arrowArray = arrowArray;
        this.arrowSchema = arrowSchema;
        this.allocator = allocator;
        this.dictionaryProvider = new CDataDictionaryProvider();
    }

    @Override
    public Iterator<EngineResultBatch> iterator() {
        if (iteratorInstance == null) {
            iteratorInstance = new BatchIterator(arrowArray, arrowSchema, allocator, dictionaryProvider);
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
                arrowArray.close();
            } finally {
                try {
                    arrowSchema.close();
                } finally {
                    dictionaryProvider.close();
                }
            }
        }
    }

    /**
     * Single-batch iterator. Mirrors
     * {@code DatafusionResultStream.BatchIterator#loadNextBatch} — same lazy schema import,
     * same {@link Data#importIntoVectorSchemaRoot} call to populate a fresh
     * {@link VectorSchemaRoot}, same emit-then-exhaust contract.
     */
    static class BatchIterator implements Iterator<EngineResultBatch> {

        private final ArrowArray arrowArray;
        private final ArrowSchema arrowSchema;
        private final BufferAllocator allocator;
        private final CDataDictionaryProvider dictionaryProvider;
        private Schema schema;
        private VectorSchemaRoot nextBatch;
        private Boolean nextAvailable;
        private boolean batchEmitted;
        private boolean exhausted;

        BatchIterator(
            ArrowArray arrowArray,
            ArrowSchema arrowSchema,
            BufferAllocator allocator,
            CDataDictionaryProvider dictionaryProvider
        ) {
            this.arrowArray = arrowArray;
            this.arrowSchema = arrowSchema;
            this.allocator = allocator;
            this.dictionaryProvider = dictionaryProvider;
        }

        private void ensureSchema() {
            if (schema != null) return;
            Field structField = importField(allocator, arrowSchema, dictionaryProvider);
            if (structField.getType().getTypeID() != ArrowType.ArrowTypeID.Struct) {
                throw new IllegalStateException("ArrowSchema describes non-struct type");
            }
            schema = new Schema(structField.getChildren(), structField.getMetadata());
        }

        private boolean loadNextBatch() {
            ensureSchema();
            if (exhausted) return false;
            VectorSchemaRoot freshRoot = VectorSchemaRoot.create(schema, allocator);
            Data.importIntoVectorSchemaRoot(allocator, arrowArray, freshRoot, dictionaryProvider);
            nextBatch = freshRoot;
            batchEmitted = true;
            exhausted = true;
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
            batchEmitted = true;
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
