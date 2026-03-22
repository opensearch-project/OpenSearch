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
import org.opensearch.parquet.bridge.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.writer.FieldValuePair;
import org.opensearch.parquet.writer.ParquetDocumentInput;

import java.io.IOException;
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
    private NativeParquetWriter writer;

    public VSRManager(String fileName, Schema schema, ArrowBufferPool bufferPool, int maxRowsPerVSR) {
        this.fileName = fileName;
        this.vsrPool = new VSRPool("pool-" + fileName, schema, bufferPool, maxRowsPerVSR);
        this.managedVSR.set(vsrPool.getActiveVSR());
        initializeWriter();
    }

    private void initializeWriter() {
        try (ArrowExport export = managedVSR.get().exportSchema()) {
            writer = new NativeParquetWriter(fileName, export.getSchemaAddress());
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Parquet writer: " + e.getMessage(), e);
        }
    }

    public ManagedVSR getActiveManagedVSR() {
        return managedVSR.get();
    }

    /**
     * Adds a document to the active VSR, rotating if necessary.
     * Transfers collected fields from the document input into the active VSR
     * using the ArrowFieldRegistry to resolve typed vector writes.
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
     * Writes frozen VSR data to Parquet via native bridge before creating new active VSR.
     */
    public void maybeRotateActiveVSR() throws IOException {
        boolean rotated = vsrPool.maybeRotateActiveVSR();
        if (!rotated) {
            return;
        }
        logger.debug("VSR rotation occurred for {}", fileName);
        ManagedVSR frozenVSR = vsrPool.getFrozenVSR();
        if (frozenVSR != null) {
            logger.debug("Writing frozen VSR {} ({} rows) for {}", frozenVSR.getId(), frozenVSR.getRowCount(), fileName);
            try (ArrowExport export = frozenVSR.exportToArrow()) {
                writer.write(export.getArrayAddress(), export.getSchemaAddress());
            }
            vsrPool.completeVSR(frozenVSR);
            vsrPool.unsetFrozenVSR();
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
        writer.flush();
    }

    @Override
    public void close() {
        try {
            if (writer != null) {
                writer.close();
            }
            vsrPool.close();
            managedVSR.set(null);
        } catch (Exception e) {
            logger.error("Error during close for {}: {}", fileName, e.getMessage(), e);
            throw new RuntimeException("Failed to close VSRManager: " + e.getMessage(), e);
        }
    }
}
