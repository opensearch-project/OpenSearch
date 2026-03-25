/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.writer.FieldValuePair;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

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
 *   <li>{@link #sync()} — fsyncs the Parquet file to durable storage after flush.</li>
 * </ol>
 *
 * <p>Field values are resolved to their Arrow vector types via {@link ArrowFieldRegistry}
 * during document transfer.
 */
public class VSRManager implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(VSRManager.class);

    private final AtomicReference<ManagedVSR> managedVSR = new AtomicReference<>();
    private final String fileName;
    private final VSRPool vsrPool;
    private final ThreadPool threadPool;
    private final boolean runAsync;
    private volatile Future<?> pendingWrite;
    private NativeParquetWriter writer;

    /**
     * Creates a new VSRManager with asynchronous background writes (production default).
     *
     * @param fileName output Parquet file path
     * @param schema Arrow schema for vector creation
     * @param bufferPool shared Arrow buffer pool
     * @param maxRowsPerVSR row threshold triggering VSR rotation
     * @param threadPool the thread pool for background native writes
     */
    public VSRManager(String fileName, Schema schema, ArrowBufferPool bufferPool, int maxRowsPerVSR, ThreadPool threadPool) {
        this(fileName, schema, bufferPool, maxRowsPerVSR, threadPool, true);
    }

    /**
     * Creates a new VSRManager.
     *
     * @param fileName output Parquet file path
     * @param schema Arrow schema for vector creation
     * @param bufferPool shared Arrow buffer pool
     * @param maxRowsPerVSR row threshold triggering VSR rotation
     * @param threadPool the thread pool for background native writes
     * @param runAsync if true, frozen VSR writes run on the background thread pool;
     *                 if false, they run on the calling thread (for benchmarks/tests)
     */
    public VSRManager(
        String fileName,
        Schema schema,
        ArrowBufferPool bufferPool,
        int maxRowsPerVSR,
        ThreadPool threadPool,
        boolean runAsync
    ) {
        this.fileName = fileName;
        this.vsrPool = new VSRPool("pool-" + fileName, schema, bufferPool, maxRowsPerVSR);
        this.threadPool = threadPool;
        this.runAsync = runAsync;
        this.managedVSR.set(vsrPool.getActiveVSR());
        initializeWriter();
    }

    /**
     * Adds a document to the active VSR, rotating if necessary.
     * Transfers collected fields from the document input into the active VSR
     * using the ArrowFieldRegistry to resolve typed vector writes.
     *
     * @param doc the document input containing field-value pairs
     */
    public void addDocument(ParquetDocumentInput doc) throws IOException {
        maybeRotateActiveVSR();
        ManagedVSR activeVSR = managedVSR.get();
        for (FieldValuePair pair : doc.getFinalInput()) {
            MappedFieldType fieldType = pair.getFieldType();
            ParquetField parquetField = ArrowFieldRegistry.getParquetField(fieldType.typeName());
            if (parquetField == null) {
                continue;
            }
            parquetField.createField(fieldType, activeVSR, pair.getValue());
        }
        int rowIndex = activeVSR.getRowCount();
        BigIntVector rowIdVector = (BigIntVector) activeVSR.getVector("_row_id");
        if (rowIdVector != null) {
            rowIdVector.setSafe(rowIndex, doc.getRowId());
        }
        activeVSR.setRowCount(rowIndex + 1);
    }

    /**
     * Handles VSR rotation after document addition if row threshold is reached.
     * Submits frozen VSR write to the background thread pool or runs it on the calling thread,
     * depending on the {@code runAsync} setting.
     */
    public void maybeRotateActiveVSR() throws IOException {
        awaitPendingWrite();
        boolean rotated = vsrPool.maybeRotateActiveVSR();
        if (!rotated) {
            return;
        }
        logger.debug("VSR rotation occurred for {}", fileName);
        ManagedVSR frozenVSR = vsrPool.getFrozenVSR();
        if (frozenVSR != null) {
            logger.debug("Writing frozen VSR {} ({} rows) for {}", frozenVSR.getId(), frozenVSR.getRowCount(), fileName);
            Runnable writeTask = () -> {
                try (ArrowExport export = frozenVSR.exportToArrow()) {
                    writer.write(export.getArrayAddress(), export.getSchemaAddress());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                vsrPool.completeVSR(frozenVSR);
                try {
                    vsrPool.unsetFrozenVSR();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
            if (runAsync) {
                pendingWrite = threadPool.executor(ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME).submit(writeTask);
            } else {
                writeTask.run();
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
        awaitPendingWrite();
        ManagedVSR currentVSR = managedVSR.get();
        if (currentVSR == null || currentVSR.getRowCount() == 0) {
            logger.debug("No data to flush for {}", fileName);
            return null;
        }
        logger.info("Flushing {} rows for {}", currentVSR.getRowCount(), fileName);
        currentVSR.moveToFrozen();
        try (ArrowExport export = currentVSR.exportToArrow()) {
            writer.write(export.getArrayAddress(), export.getSchemaAddress());
        }
        writer.close();
        ParquetFileMetadata metadata = writer.getMetadata();
        vsrPool.completeVSR(currentVSR);
        managedVSR.set(null);
        logger.debug("Flush completed for {} with metadata: {}", fileName, metadata);
        return metadata;
    }

    /**
     * Syncs the Parquet file to disk. Must be called after {@link #flush()}.
     */
    public void sync() throws IOException {
        awaitPendingWrite();
        writer.flush();
    }

    @Override
    public void close() {
        try {
            awaitPendingWrite();
            if (writer != null) {
                writer.close();
            }
            vsrPool.close();
            managedVSR.set(null);
        } catch (Exception e) {
            logger.error("Error during close for {}: {}", fileName, e.getMessage());
            throw new RuntimeException("Failed to close VSRManager: " + e.getMessage(), e);
        }
    }

    private void initializeWriter() {
        try (ArrowExport export = managedVSR.get().exportSchema()) {
            writer = new NativeParquetWriter(fileName, export.getSchemaAddress());
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Parquet writer: " + e.getMessage(), e);
        }
    }

    /**
     * Waits for any in-flight background write to complete.
     * Propagates exceptions from the background write as IOException.
     */
    private void awaitPendingWrite() throws IOException {
        if (pendingWrite == null) {
            return;
        }
        try {
            pendingWrite.get();
        } catch (ExecutionException e) {
            throw new IOException("Background VSR write failed for " + fileName, e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for background VSR write for " + fileName, e);
        } finally {
            pendingWrite = null;
        }
    }

    // Visible for testing only
    ManagedVSR getActiveManagedVSR() {
        return managedVSR.get();
    }
}
