/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.vsr;

import com.parquet.parquetdataformat.bridge.ArrowExport;
import com.parquet.parquetdataformat.bridge.RustBridge;
import com.parquet.parquetdataformat.memory.ArrowBufferPool;
import com.parquet.parquetdataformat.writer.ParquetDocumentInput;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.WriteResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages VectorSchemaRoot lifecycle with integrated memory management and native call wrappers.
 * Provides a high-level interface for Parquet document operations using managed VSR abstractions.
 *
 * <p>This class orchestrates the following components:
 * <ul>
 *   <li>{@link ManagedVSR} - Thread-safe VSR with state management</li>
 *   <li>{@link VSRPool} - Resource pooling for VSRs</li>
 *   <li>{@link RustBridge} - Direct JNI calls to Rust backend</li>
 * </ul>
 */
public class VSRManager {

    private static final Logger logger = LogManager.getLogger(VSRManager.class);

    private ManagedVSR managedVSR;
    private Map<String, FieldVector> fieldVectorMap;
    private final Schema schema;
    private final String fileName;
    private final VSRPool vsrPool;

    public VSRManager(String fileName, Schema schema, ArrowBufferPool arrowBufferPool) {
        this.fileName = fileName;
        this.schema = schema;

        // Create VSR pool
        this.vsrPool = new VSRPool("pool-" + fileName, schema, arrowBufferPool);

        // Get active VSR from pool
        this.managedVSR = vsrPool.getActiveVSR();
        initializeFieldVectorMap();
        // Initialize writer lazily to avoid crashes
        initializeWriter();
    }

    private void initializeWriter() {
        try {
            // Export schema through managed VSR
            try (ArrowExport export = managedVSR.exportSchema()) {
                long schemaAddress = export.getSchemaAddress();

                // Direct native call - RustBridge handles all validation
                RustBridge.createWriter(fileName, schemaAddress);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Parquet writer: " + e.getMessage(), e);
        }
    }

    public WriteResult addToManagedVSR(ParquetDocumentInput document) throws IOException {
        // Ensure we have an active VSR (handle case where getActiveVSR() returns null)
        if (managedVSR == null) {
            managedVSR = vsrPool.getActiveVSR();
            if (managedVSR == null) {
                throw new IOException("No active VSR available");
            }
            reinitializeFieldVectorMap();
        }

        // Ensure VSR is in ACTIVE state for modifications
        if (managedVSR.getState() != VSRState.ACTIVE) {
            throw new IOException("Cannot add document - VSR is not active: " + managedVSR.getState());
        }

        logger.debug("addToManagedVSR called for {}, current row count: {}", fileName, managedVSR.getRowCount());

        try {
            // Since ParquetDocumentInput now works directly with ManagedVSR,
            // fields should already be populated in vectors via addField() calls.
            // We just need to finalize the document by calling addToWriter()
            // which will increment the row count.
            WriteResult result = document.addToWriter();

            logger.debug("After adding document to {}, row count: {}", fileName, managedVSR.getRowCount());

            // Check for VSR rotation AFTER successful document processing
            maybeRotateActiveVSR();

            return result;
        } catch (Exception e) {
            logger.error("Error in addToManagedVSR for {}: {}", fileName, e.getMessage(), e);
            throw new IOException("Failed to add document: " + e.getMessage(), e);
        }
    }

    public String flush(FlushIn flushIn) throws IOException {
        logger.info("Flush called for {}, row count: {}", fileName, managedVSR.getRowCount());
        try {
            // Only flush if we have data
            if (managedVSR.getRowCount() == 0) {
                logger.debug("No data to flush for {}, returning null", fileName);
                return null;
            }

            // Transition VSR to FROZEN state before flushing
            managedVSR.setState(VSRState.FROZEN);
            logger.info("Flushing {} rows for {}", managedVSR.getRowCount(), fileName);

            // Transition to FLUSHING state
            managedVSR.setState(VSRState.FLUSHING);

            // Direct native call - write the managed VSR data
            try (ArrowExport export = managedVSR.exportToArrow()) {
                RustBridge.write(fileName, export.getArrayAddress(), export.getSchemaAddress());
                RustBridge.closeWriter(fileName);
            }
            logger.info("Successfully flushed data for {}", fileName);

            return fileName;
        } catch (Exception e) {
            logger.error("Error in flush for {}: {}", fileName, e.getMessage(), e);
            throw new IOException("Failed to flush data: " + e.getMessage(), e);
        }
    }

    public void close() {
        try {
            // Direct native calls
            try {
                RustBridge.closeWriter(fileName);
                RustBridge.flushToDisk(fileName);
            } catch (IOException e) {
                logger.warn("Failed to close/flush writer for {}: {}", fileName, e.getMessage(), e);
            }

            // Close VSR Pool
            vsrPool.close();
            managedVSR = null;

        } catch (Exception e) {
            logger.error("Error during close for {}: {}", fileName, e.getMessage(), e);
        }
    }

    private boolean checkFlushConditions() {
        // TODO: Implement memory pressure-based flush conditions
        return false;
    }

    /**
     * Handles VSR rotation after successful document addition.
     * Checks if rotation is needed and immediately processes any frozen VSR.
     */
    public void maybeRotateActiveVSR() throws IOException {
        try {
            // Check if rotation is needed and perform it if safe
            boolean rotated = vsrPool.maybeRotateActiveVSR();

            if (rotated) {
                logger.debug("VSR rotation occurred after document addition for {}", fileName);

                // Get the frozen VSR that was just created by rotation
                ManagedVSR frozenVSR = vsrPool.getFrozenVSR();
                if (frozenVSR != null) {
                    logger.debug("Processing frozen VSR: {} with {} rows for {}",
                        frozenVSR.getId(), frozenVSR.getRowCount(), fileName);

                    // Write the frozen VSR data immediately
                    frozenVSR.setState(VSRState.FLUSHING);
                    try (ArrowExport export = frozenVSR.exportToArrow()) {
                        RustBridge.write(fileName, export.getArrayAddress(), export.getSchemaAddress());
                    }

                    logger.debug("Successfully wrote frozen VSR data for {}", fileName);

                    // Complete the VSR processing
                    vsrPool.completeVSR(frozenVSR);
                    vsrPool.unsetFrozenVSR();
                } else {
                    logger.warn("Rotation occurred but no frozen VSR found for {}", fileName);
                }

                // Update to new active VSR
                managedVSR = vsrPool.getActiveVSR();
                if (managedVSR == null) {
                    throw new IOException("No active VSR available after rotation");
                }

                // Reinitialize field vector map with new VSR
                reinitializeFieldVectorMap();

                logger.debug("VSR rotation completed for {}, new active VSR: {}, row count: {}",
                    fileName, managedVSR.getId(), managedVSR.getRowCount());
            }
        } catch (IOException e) {
            logger.error("Error during VSR rotation for {}: {}", fileName, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Checks if VSR rotation is needed based on row count and memory pressure.
     * If rotation occurs, updates the managed VSR reference and reinitializes field vectors.
     *
     * @deprecated Use handleVSRRotationAfterAddToManagedVSR() instead for safer rotation after document processing
     */
    @Deprecated
    private void checkAndHandleVSRRotation() throws IOException {
        // Get active VSR from pool - this will trigger rotation if needed
        ManagedVSR currentActive = vsrPool.getActiveVSR();

        // Check if we got a different VSR (rotation occurred)
        if (currentActive != managedVSR) {
            logger.debug("VSR rotation detected for {}, updating references", fileName);

            // Update the managed VSR reference
            managedVSR = currentActive;

            // Reinitialize field vector map with new VSR
            reinitializeFieldVectorMap();

            // Note: Writer initialization is not needed per VSR as it's per file
            logger.debug("VSR rotation completed for {}, new row count: {}", fileName, managedVSR.getRowCount());
        }
    }

    /**
     * Reinitializes the field vector map with the current managed VSR.
     * Called after VSR rotation to update vector references.
     */
    private void reinitializeFieldVectorMap() {
        fieldVectorMap.clear();
        initializeFieldVectorMap();
    }

    private void initializeFieldVectorMap() {
        fieldVectorMap = new HashMap<>();
        for (Field field : schema.getFields()) {
            String fieldName = field.getName();
            FieldVector fieldVector = managedVSR.getVector(fieldName);
            // Vector is already properly typed from ManagedVSR.getVector()
            fieldVectorMap.put(fieldName, fieldVector);
        }
    }

    /**
     * Gets the current active ManagedVSR for document input creation.
     *
     * @return The current managed VSR instance
     */
    public ManagedVSR getActiveManagedVSR() {
        return managedVSR;
    }
}
