/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.bridge.ParquetSortConfig;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.stats.ParquetShardStatsTracker;
import org.opensearch.parquet.writer.FieldValuePair;
import org.opensearch.parquet.writer.MismatchedInputException;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Top-level orchestrator for the Arrow batching → Parquet file generation pipeline.
 *
 * <p>Combines {@link VSRPool} (Arrow batch management) with {@link NativeParquetWriter}
 * (native Rust Parquet writer) to provide a single entry point for document ingestion.
 * Handles the complete flow:
 * <ol>
 *   <li>{@link #addDocument(ParquetDocumentInput)} — transfers document fields into the active
 *       VSR's Arrow vectors, rotating the VSR if the row threshold is reached.</li>
 *   <li>{@link #flush()} — freezes the active VSR, exports it to the native writer,
 *       finalizes the Parquet file, and returns file metadata.</li>
 * </ol>
 *
 * <p>Field values are resolved to their Arrow vector types via {@link ArrowFieldRegistry}
 * during document ingestion.
 *
 * <p>This class is NOT Thread-Safe. External synchronization is required
 * if instances are shared across threads.
 */
public class VSRManager implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(VSRManager.class);

    private final AtomicReference<ManagedVSR> managedVSR = new AtomicReference<>();
    private final String fileName;
    private final IndexSettings indexSettings;
    private final VSRPool vsrPool;
    private final ThreadPool threadPool;
    private final String vsrRotationThread;
    private final long writerGeneration;
    private final ParquetShardStatsTracker stats;
    private volatile Future<?> pendingWrite;
    private final NativeParquetWriter writer;
    private final int ROTATION_TIMEOUT = 120;
    private LongAdder rowCount = new LongAdder();
    private long acceptedRows = 0L;

    /**
     * Creates a new VSRManager with asynchronous background writes (production default).
     */
    public VSRManager(
        String fileName,
        IndexSettings indexSettings,
        Schema schema,
        ArrowBufferPool bufferPool,
        int maxRowsPerVSR,
        ThreadPool threadPool,
        long writerGeneration,
        ParquetShardStatsTracker stats
    ) {
        this(fileName, indexSettings, schema, bufferPool, maxRowsPerVSR, threadPool, true, writerGeneration, stats);
    }

    /**
     * Creates a new VSRManager with asynchronous background writes and no stats collection.
     */
    public VSRManager(
        String fileName,
        IndexSettings indexSettings,
        Schema schema,
        ArrowBufferPool bufferPool,
        int maxRowsPerVSR,
        ThreadPool threadPool,
        long writerGeneration
    ) {
        this(
            fileName,
            indexSettings,
            schema,
            bufferPool,
            maxRowsPerVSR,
            threadPool,
            true,
            writerGeneration,
            new ParquetShardStatsTracker()
        );
    }

    /**
     * Creates a new VSRManager without stats collection.
     */
    public VSRManager(
        String fileName,
        IndexSettings indexSettings,
        Schema schema,
        ArrowBufferPool bufferPool,
        int maxRowsPerVSR,
        ThreadPool threadPool,
        boolean runAsync,
        long writerGeneration
    ) {
        this(
            fileName,
            indexSettings,
            schema,
            bufferPool,
            maxRowsPerVSR,
            threadPool,
            runAsync,
            writerGeneration,
            new ParquetShardStatsTracker()
        );
    }

    /**
     * Creates a new VSRManager.
     *
     * @param fileName output Parquet file path
     * @param indexSettings the index settings (sort config is read from here)
     * @param schema Arrow schema for vector creation
     * @param bufferPool shared Arrow buffer pool
     * @param maxRowsPerVSR row threshold triggering VSR rotation
     * @param threadPool the thread pool for background native writes
     * @param runAsync if true, frozen VSR writes run on the background thread pool;
     *                 if false, they run on the calling thread (for benchmarks/tests)
     * @param writerGeneration the writer generation to store in file metadata
     * @param stats shard-level stats tracker
     */
    public VSRManager(
        String fileName,
        IndexSettings indexSettings,
        Schema schema,
        ArrowBufferPool bufferPool,
        int maxRowsPerVSR,
        ThreadPool threadPool,
        boolean runAsync,
        long writerGeneration,
        ParquetShardStatsTracker stats
    ) {
        this.fileName = fileName;
        this.indexSettings = indexSettings;
        this.writerGeneration = writerGeneration;
        this.stats = stats;
        this.vsrPool = new VSRPool("pool-" + fileName, schema, bufferPool, maxRowsPerVSR);
        this.threadPool = threadPool;
        this.vsrRotationThread = runAsync ? ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME : ThreadPool.Names.SAME;
        this.managedVSR.set(vsrPool.getActiveVSR());
        this.writer = new NativeParquetWriter(fileName, stats);
    }

    /**
     * Adds a document to the active VSR, rotating if necessary.
     * Transfers collected fields from the document input into the active VSR
     * using the ArrowFieldRegistry to resolve typed vector writes.
     * <p>
     * Single-value semantics are enforced at the {@link ParquetDocumentInput} layer:
     * if an array field produces multiple values for the same field type, only the
     * last value is retained (last-value-wins).
     *
     * @param doc the document input containing field-value pairs
     */
    public void addDocument(ParquetDocumentInput doc) throws IOException {
        if (pendingWrite != null && pendingWrite.isDone()) {
            Future.State state = pendingWrite.state();
            if (state == Future.State.FAILED) {
                stats.incBackgroundWriteFailures();
                throw new IllegalStateException(pendingWrite.exceptionNow());
            } else if (state == Future.State.CANCELLED) {
                throw new IllegalStateException("Background write was cancelled");
            }
        }
        maybeRotateActiveVSR();
        // Re-check the rowId invariant so a single-format Parquet path is protected too.
        if (doc.getRowId() != acceptedRows) {
            throw new IllegalStateException(
                "rowId [" + doc.getRowId() + "] does not match accepted row count [" + acceptedRows + "] for " + fileName
            );
        }
        ManagedVSR activeVSR = managedVSR.get();
        final int rowIndex = activeVSR.getRowCount();
        final List<FieldVector> writtenVectors = new ArrayList<>();
        try {
            for (FieldValuePair pair : doc.getFinalInput()) {
                MappedFieldType fieldType = pair.getFieldType();
                ParquetField parquetField = ArrowFieldRegistry.getParquetField(fieldType.typeName());
                if (parquetField == null) {
                    // Defense-in-depth: schema reconciliation is supposed to happen in
                    // ParquetWriter.updateMappingVersion before any addDocument with a new
                    // field type. If we still see an unmapped type here, the writer is
                    // out of sync with the mapping — surface as a recoverable failure.
                    // TODO:: we can remove this post the validation on mapping update
                    throw new MismatchedInputException(
                        "No ParquetField mapping for field [" + fieldType.name() + "] of type [" + fieldType.typeName() + "]"
                    );
                }
                FieldVector vector = activeVSR.getVector(fieldType.name());
                if (vector == null) {
                    logger.error(
                        "[Gen: {}] VSR schema mismatch: field [{}] not in active VSR. VSR schema fields: {}",
                        writerGeneration,
                        fieldType.name(),
                        activeVSR.getSchema().getFields().stream().map(f -> f.getName()).collect(java.util.stream.Collectors.joining(", "))
                    );
                    throw new MismatchedInputException(
                        "Active VSR has no vector for field ["
                            + fieldType.name()
                            + "] — schema reconciliation must run via updateMappingVersion before addDocument"
                    );
                }
                parquetField.createField(fieldType, activeVSR, pair.getValue());
                writtenVectors.add(vector);
            }
            BigIntVector rowIdVector = (BigIntVector) activeVSR.getVector(DocumentInput.ROW_ID_FIELD);
            if (rowIdVector != null) {
                rowIdVector.setSafe(rowIndex, doc.getRowId());
                writtenVectors.add(rowIdVector);
            }
            activeVSR.setRowCount(rowIndex + 1);
            acceptedRows++;
        } catch (Exception e) {
            // Any failure between the first field write and acceptance leaves an uncounted partial
            // row. Scrub it (best-effort, never throws) so no stale value can leak into the next doc
            // that reuses this row index, then rethrow the original failure unchanged. Precise
            // rethrow keeps addDocument's throws clause unchanged.
            scrubPartialRow(writtenVectors, rowIndex);
            throw e;
        }
    }

    /**
     * Scrubs a partially-written, uncounted row after a mid-document failure so no stale value
     * survives to leak into the next document that reuses this row index (issue #22417). Only the
     * vectors already written for this row are reset — their slot at {@code rowIndex} is already
     * allocated, so {@link #setNull} never triggers a (re)allocation and is safe to run under the
     * memory pressure that may have caused the failure.
     *
     * <p>Best-effort and strictly non-throwing: a failure clearing any single vector is logged and
     * the remaining vectors are still scrubbed, so the original write failure (which the caller
     * rethrows) is never masked.
     *
     * @param writtenVectors the vectors written for the failed row, in write order
     * @param rowIndex       the uncommitted row index to clear
     */
    private void scrubPartialRow(List<FieldVector> writtenVectors, int rowIndex) {
        for (FieldVector vector : writtenVectors) {
            try {
                setNull(vector, rowIndex);
            } catch (RuntimeException | Error scrubFailure) {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "[Gen: {}] Failed to scrub partial row {} for vector [{}] in {}; column may retain a stale value",
                        writerGeneration,
                        rowIndex,
                        vector.getName(),
                        fileName
                    ),
                    scrubFailure
                );
            }
        }
    }

    /**
     * Clears the value at {@code index} by unsetting its validity bit. For variable-width vectors
     * this also resets the offset buffer and {@code lastSet} bookkeeping via the vector's own
     * {@code setNull}. Every vector type the Parquet field registry produces is either fixed- or
     * variable-width; the final branch is a non-throwing fallback for any other vector type (e.g.,
     * a future nested/view field) that clears only the top-level validity bit.
     */
    private static void setNull(FieldVector vector, int index) {
        switch (vector) {
            case BaseFixedWidthVector fixed -> fixed.setNull(index);
            case BaseVariableWidthVector variable -> variable.setNull(index);
            case BaseLargeVariableWidthVector large -> large.setNull(index);
            default -> BitVectorHelper.unsetBit(vector.getValidityBuffer(), index);
        }
    }

    public long getAcceptedRows() {
        return acceptedRows;
    }

    /**
     * Reconciles the active VSR with the given schema by adding vectors for any fields
     * present in {@code newSchema} but not yet in the active VSR. Also updates the pool
     * schema so subsequently rotated VSRs include the new fields.
     * <p>
     * Called from {@link org.opensearch.parquet.writer.ParquetWriter#updateMappingVersion}
     * when the mapping version advances. No-op if every field in {@code newSchema} is
     * already present in the active VSR.
     *
     * @param newSchema the schema to reconcile against
     */
    public boolean reconcileSchema(Schema newSchema) {
        ManagedVSR activeVSR = managedVSR.get();
        boolean changed = false;
        for (Field schemaField : newSchema.getFields()) {
            if (activeVSR.getVector(schemaField.getName()) == null) {
                Field field = new Field(schemaField.getName(), schemaField.getFieldType(), null);
                activeVSR.addFieldVector(field);
                changed = true;
            }
        }
        if (changed) {
            vsrPool.updateSchema(activeVSR.getSchema());
        } else {
            logger.debug("no changes in schema despite change in mapping version");
        }
        return changed;
    }

    /**
     * Checks if VSR rotation is needed before accepting the next document.
     * If the active VSR has reached the row threshold and the frozen slot is empty,
     * freezes the active VSR, submits it for background native write, and creates
     * a new active VSR. If the frozen slot is occupied, rotation is skipped.
     */
    public void maybeRotateActiveVSR() throws IOException {
        boolean rotated = vsrPool.maybeRotateActiveVSR();
        if (rotated == false) {
            return;
        }
        stats.incVsrRotations();
        logger.debug("VSR rotation occurred for {}", fileName);
        ManagedVSR frozenVSR = vsrPool.getFrozenVSR();
        if (frozenVSR != null) {
            logger.debug("Writing frozen VSR {} ({} rows) for {}", frozenVSR.getId(), frozenVSR.getRowCount(), fileName);
            maybeInitializeWriter(frozenVSR);
            Runnable writeTask = () -> {
                try {
                    try (ArrowExport export = frozenVSR.exportToArrow()) {
                        rowCount.add(frozenVSR.getRowCount());
                        writer.write(export.getArrayAddress(), export.getSchemaAddress());
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                vsrPool.completeVSR(frozenVSR);
                vsrPool.unsetFrozenVSR();
            };
            try {
                pendingWrite = threadPool.executor(vsrRotationThread).submit(writeTask);
            } catch (OpenSearchRejectedExecutionException e) {
                // Pool saturated — count the rejection and re-throw (surfaces as HTTP 429).
                stats.incNativeWriteRejections();
                throw e;
            }
        }
        ManagedVSR newVSR = vsrPool.getActiveVSR();
        if (newVSR == null) {
            throw new IOException("No active VSR available after rotation");
        }
        managedVSR.set(newVSR);
        logger.debug("VSR rotation completed for {}, new active VSR: {}", fileName, newVSR.getId());
    }

    /**
     * Flushes the current VSR data to a Parquet file via native bridge.
     *
     * @return metadata about the written Parquet file, or null if no data to flush
     */
    public ParquetFileMetadata flush() throws IOException {
        awaitPendingWrite(ROTATION_TIMEOUT, false);
        ManagedVSR currentVSR = managedVSR.get();
        if (currentVSR != null && currentVSR.getRowCount() > 0) {
            logger.info("Flushing {} rows for {}", currentVSR.getRowCount(), fileName);
            currentVSR.moveToFrozen();
            maybeInitializeWriter(currentVSR);
            try (ArrowExport export = currentVSR.exportToArrow()) {
                rowCount.add(currentVSR.getRowCount());
                writer.write(export.getArrayAddress(), export.getSchemaAddress());
            }
            vsrPool.completeVSR(currentVSR);
            managedVSR.set(null);
        }
        ParquetFileMetadata metadata = writer.flush();
        assert metadata == null || metadata.numRows() == rowCount.sum() : "Row count mismatch between Java managed VSR and Rust writer";
        logger.debug("Flush completed for {} with metadata: {}", fileName, metadata);
        return metadata;
    }

    /**
     * Syncs the Parquet file to disk. Must be called after {@link #flush()}.
     */

    @Override
    public void close() {
        // vsrPool.close() MUST run even if awaitPendingWrite / writer.flush() throws: a failed or
        // timed-out background write (IOException from awaitPendingWrite) previously skipped it,
        // stranding the pool's per-VSR child allocators (their off-heap Arrow buffers leaked onto
        // the ingest pool for the node's lifetime — "Memory was leaked by query"). Release the pool
        // in a finally so the buffers are reclaimed regardless of the drain/flush outcome.
        try {
            awaitPendingWrite(ROTATION_TIMEOUT, true);
            if (writer != null) {
                writer.flush();
            }
        } catch (Exception e) {
            logger.error("Error during close for {}: {}", fileName, e.getMessage());
            throw new RuntimeException("Failed to close VSRManager: " + e.getMessage(), e);
        } finally {
            try {
                vsrPool.close();
            } catch (Exception e) {
                logger.error("Error releasing VSR pool during close for {}: {}", fileName, e.getMessage());
            }
            managedVSR.set(null);
        }
    }

    /**
     * Initializes the native writer on first use, using the schema from the given VSR.
     */
    private void maybeInitializeWriter(ManagedVSR vsr) throws IOException {
        if (writer.isInitialized() == false) {
            String indexName = indexSettings.getIndex().getName();
            ParquetSortConfig sortConfig = new ParquetSortConfig(indexSettings);
            try (ArrowSchema schema = vsr.exportSchema()) {
                writer.initialize(indexName, schema.memoryAddress(), sortConfig, writerGeneration);
            }
        }
    }

    /**
     * Waits for any in-flight background write to complete with an optional timeout.
     *
     * @param timeoutSeconds timeout in seconds (0 means wait indefinitely)
     * @param ignoreTimeout if true, log a warning on timeout instead of throwing
     */
    private void awaitPendingWrite(long timeoutSeconds, boolean ignoreTimeout) throws IOException {
        if (pendingWrite == null) {
            return;
        }
        long startNanos = System.nanoTime();
        try {
            if (timeoutSeconds > 0) {
                pendingWrite.get(timeoutSeconds, TimeUnit.SECONDS);
            } else {
                pendingWrite.get();
            }
            stats.incBackgroundWriteTotal();
        } catch (TimeoutException e) {
            stats.incBackgroundWriteTimeouts();
            if (ignoreTimeout) {
                logger.warn("Timed out waiting for background VSR write for {}", fileName);
            } else {
                throw new IOException("Timed out waiting for background VSR write for " + fileName, e);
            }
        } catch (Exception e) {
            stats.incBackgroundWriteFailures();
            throw new IOException("Background VSR write failed for " + fileName, e.getCause());
        } finally {
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            stats.addBackgroundWriteWaitMillis(elapsed);
            pendingWrite = null;
        }
    }

    /**
     * Rolls the VSR back to hold exactly {@code rowCount} admitted rows. No-op if already
     * at the target. Throws if the target is higher than current or if the active VSR
     * doesn't have enough rows to trim (rollback crossed a rotation boundary).
     *
     * @param rowCount the desired row count after this call
     */
    public void rollbackTo(long rowCount) {
        if (rowCount > acceptedRows) {
            throw new IllegalStateException("Cannot rollback to " + rowCount + ": only " + acceptedRows + " rows in VSR");
        }
        if (rowCount == acceptedRows) {
            return;
        }
        ManagedVSR activeVSR = managedVSR.get();
        long diff = acceptedRows - rowCount;
        if (diff > activeVSR.getRowCount()) {
            throw new IllegalStateException("Cannot rollback " + diff + " rows: active VSR only has " + activeVSR.getRowCount() + " rows");
        }
        activeVSR.setRowCount(activeVSR.getRowCount() - (int) diff);
        acceptedRows = rowCount;
    }

    /**
     * Returns whether the schema can still evolve (native writer not yet initialized).
     *
     * @return true if the schema is mutable
     */
    public boolean isSchemaMutable() {
        return writer.isInitialized() == false;
    }

    // Visible for testing only
    ManagedVSR getActiveManagedVSR() {
        return managedVSR.get();
    }

    /**
     * Returns the row ID mapping produced during the last flush's sort-on-close
     * as a memory-efficient packed mapping, or null if no sorting was configured
     * or the file was empty.
     */
    public RowIdMapping getRowIdMapping() {
        return writer.getRowIdMapping();
    }

    /** Visible for testing — returns the pending background write future, or null. */
    Future<?> getPendingWrite() {
        return pendingWrite;
    }

    /** Visible for testing — injects a pending background write future to exercise close() paths. */
    void setPendingWrite(Future<?> future) {
        this.pendingWrite = future;
    }
}
