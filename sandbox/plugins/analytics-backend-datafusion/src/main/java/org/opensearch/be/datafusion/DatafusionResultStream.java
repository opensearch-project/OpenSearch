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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.exec.ArrowValues;
import org.opensearch.analytics.exec.FragmentResources;
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
public class DatafusionResultStream implements EngineResultStream, FragmentResources.MetricsCapable {

    private static final Logger LOGGER = LogManager.getLogger(DatafusionResultStream.class);

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
    public byte[] getMetricsJson() {
        return NativeBridge.streamGetMetrics(streamHandle.getPointer());
    }

    @Override
    public void close() {
        try {
            if (iteratorInstance != null) {
                iteratorInstance.closeLastBatch();
                iteratorInstance.closeStagingAllocator();
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
        private boolean batchEmitted;
        private boolean nativeStreamExhausted;
        /**
         * ONE staging allocator for the whole stream, created lazily on first import (see
         * {@link #importBatch}). Deliberately NOT per-batch: the Flight transport builds its reused stream
         * root on {@code fieldVectors.getFirst().getAllocator()}, i.e. the FIRST batch's staging allocator
         * ({@code FlightServerChannel#transferIntoStreamRoot}, whose comment states "The producer's
         * allocator must be long-lived (not closed per-request)"), and then charges that same allocator for
         * every later batch via {@code BaseFixedWidthVector#transferTo}. Closing a staging allocator
         * per-batch therefore closed an allocator the transport was still using, and
         * {@code BaseAllocator#close} threw {@code IllegalStateException: Memory was leaked by query}
         * whenever a transfer landed in the window between the drained-check and the close.
         *
         * <p>Unboundedness is the only property the original per-batch design actually needed (so an import
         * cannot OOM part-way through a C Data array — see {@link #importBatch}); a single unbounded child
         * preserves that while removing the check-then-close race entirely.
         */
        private BufferAllocator stagingAllocator;

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
            if (nativeStreamExhausted) return false;
            long arrayAddr = callNativeFn(
                listener -> NativeBridge.streamNext(streamHandle.getRuntimeHandle().get(), streamHandle.getPointer(), listener)
            );
            if (arrayAddr == 0) {
                nativeStreamExhausted = true;
                // Streaming Flight requires ≥1 schema-bearing frame before completeStream;
                // synthesise a zero-row batch carrying the schema for empty native streams.
                if (!batchEmitted) {
                    nextBatch = VectorSchemaRoot.create(schema, allocator);
                    nextBatch.setRowCount(0);
                    batchEmitted = true;
                    return true;
                }
                return false;
            }
            try (ArrowArray arrowArray = ArrowArray.wrap(arrayAddr)) {
                nextBatch = importBatch(arrowArray);
            }
            batchEmitted = true;
            return true;
        }

        /**
         * Imports one native batch across the Arrow C Data Interface into a per-batch staging allocator
         * (an unbounded child of the root) rather than directly into {@code allocator}.
         *
         * <p>{@link Data#importIntoVectorSchemaRoot} charges each buffer against the target allocator as it
         * walks the array. Against a bounded target that fills part-way through a wide batch the import
         * throws, and arrow-java's {@code ReferenceCountedArrowArray#unsafeAssociateAllocation} retains the
         * imported array <em>before</em> the throwing {@code wrapForeignAllocation} without rolling back, so
         * the C Data release callback never fires and the whole native batch leaks in the producer's native
         * allocator — invisible to the JVM heap and the Java Arrow allocator (arrow-java &le; 18.1.0). An
         * unbounded staging child can't OOM mid-array, so the release callback always fires.
         *
         * <p>The batch is returned as-is (zero-copy); its buffers are released by the existing consumer close
         * paths, which drives the C Data reference count to zero. The staging allocator is stream-scoped and
         * outlives every batch (see {@link #stagingAllocator}), so nothing is closed per-batch; on import
         * failure the partially-imported root is closed by {@link #importOntoStaging} but the allocator
         * itself stays open for subsequent batches.
         */
        private VectorSchemaRoot importBatch(ArrowArray arrowArray) {
            if (stagingAllocator == null) {
                stagingAllocator = allocator.getRoot().newChildAllocator("datafusion-import-staging", 0, Long.MAX_VALUE);
            }
            return importOntoStaging(stagingAllocator, schema, arrowArray, dictionaryProvider);
        }

        /**
         * Closes the stream-scoped staging allocator, if it can be closed. Called only from
         * {@link DatafusionResultStream#close()} — never per-batch.
         *
         * <p>A non-zero balance here means the transport still holds buffers charged to this allocator: the
         * Flight channel's reused stream root is freed in its own {@code close()}, which may run after ours.
         * Closing anyway would throw {@code IllegalStateException} from {@code BaseAllocator#close} AND — worse
         * — leave the allocator permanently half-closed, because {@code close()} sets {@code isClosed = true}
         * BEFORE its leak check, so its bytes would never be returned to the parent and a later retry would
         * early-return as a no-op. Leaving it open hands ownership to the root allocator, which is the same
         * outcome the previous per-batch code produced for any still-in-flight batch.
         *
         * <p>The deferral is logged at DEBUG, not WARN: the transport frees its stream root asynchronously
         * (posted to the flight executor by {@code FlightServerChannel#close}), so a non-zero balance here is
         * the expected outcome of every streaming query, not a signal of a leak.
         */
        void closeStagingAllocator() {
            if (stagingAllocator == null) {
                return;
            }
            if (stagingAllocator.getAllocatedMemory() == 0) {
                stagingAllocator.close();
                stagingAllocator = null;
            } else {
                LOGGER.debug(
                    "Deferring close of staging allocator [{}] with {} bytes outstanding; the transport still "
                        + "holds them and frees them with its stream root",
                    stagingAllocator.getName(),
                    stagingAllocator.getAllocatedMemory()
                );
            }
        }

        /** The stream-scoped staging allocator, or null before the first import. For tests. */
        BufferAllocator stagingAllocator() {
            return stagingAllocator;
        }

        /**
         * Drives the exact production {@link #importBatch} path with an explicitly supplied schema, so the
         * staging-allocator lifetime regression test can import several batches without a native stream.
         * {@code schema} is normally set by {@link #ensureSchema()} from the native handle.
         */
        VectorSchemaRoot importBatchForTest(Schema batchSchema, ArrowArray arrowArray) {
            this.schema = batchSchema;
            return importBatch(arrowArray);
        }

        /**
         * Imports {@code arrowArray} into a fresh {@link VectorSchemaRoot} on {@code staging}, which MUST be
         * an unbounded child of the root so the import cannot OOM part-way through the array. On failure the
         * returned root is closed (firing the native release for the whole batch) and the exception rethrown;
         * the caller owns {@code staging}. Package-private so the leak regression test can drive the exact
         * production import path.
         */
        static VectorSchemaRoot importOntoStaging(
            BufferAllocator staging,
            Schema schema,
            ArrowArray arrowArray,
            CDataDictionaryProvider dictionaryProvider
        ) {
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, staging);
            try {
                Data.importIntoVectorSchemaRoot(staging, arrowArray, root, dictionaryProvider);
            } catch (RuntimeException e) {
                // Releasing a partially-imported root can itself throw (VectorSchemaRoot#close rethrows any
                // RuntimeException from the vectors' release). Attach it rather than let it mask the import
                // failure that is the real diagnosis.
                try {
                    root.close();
                } catch (RuntimeException releaseFailure) {
                    e.addSuppressed(releaseFailure);
                }
                throw e;
            }
            return root;
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
