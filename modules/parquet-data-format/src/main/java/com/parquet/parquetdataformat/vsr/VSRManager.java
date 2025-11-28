/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.vsr;

import com.parquet.parquetdataformat.bridge.ArrowExport;
import com.parquet.parquetdataformat.bridge.NativeParquetWriter;
import com.parquet.parquetdataformat.memory.ArrowBufferPool;
import com.parquet.parquetdataformat.writer.ParquetDocumentInput;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.WriteResult;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages VectorSchemaRoot lifecycle with integrated memory management and native call wrappers.
 * Provides a high-level interface for Parquet document operations using managed VSR abstractions.
 *
 * <p>This class orchestrates the following components:
 * <ul>
 *   <li>{@link ManagedVSR} - Thread-safe VSR with state management</li>
 *   <li>{@link VSRPool} - Resource pooling for VSRs</li>
 *   <li>{@link com.parquet.parquetdataformat.bridge.RustBridge} - Direct JNI calls to Rust backend</li>
 * </ul>
 */
public class VSRManager implements Closeable {
    private final AtomicReference<ManagedVSR> managedVSR = new AtomicReference<>();
    private Map<String, FieldVector> fieldVectorMap;
    private final Schema schema;
    private final String fileName;
    private final VSRPool vsrPool;
    private NativeParquetWriter writer;

    private static final Logger logger = LogManager.getLogger(VSRManager.class);


    public VSRManager(String fileName, Schema schema, ArrowBufferPool arrowBufferPool) {
        this.fileName = fileName;
        this.schema = schema;

        // Create VSR pool
        this.vsrPool = new VSRPool("pool-" + fileName, schema, arrowBufferPool);

        // Get active VSR from pool
        this.managedVSR.set(vsrPool.getActiveVSR());
        initializeFieldVectorMap();
        // Initialize writer lazily to avoid crashes
        initializeWriter();
    }

    private void initializeWriter() {
        try {
            try (ArrowExport export = managedVSR.get().exportSchema()) {
                writer = new NativeParquetWriter(fileName, export.getSchemaAddress());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Parquet writer: " + e.getMessage(), e);
        }
    }

    public WriteResult addToManagedVSR(ParquetDocumentInput document) throws IOException {
        ManagedVSR currentVSR = managedVSR.updateAndGet(vsr -> {
            if (vsr == null) {
                ManagedVSR newVSR = vsrPool.getActiveVSR();
                if (newVSR != null) {
                    reinitializeFieldVectorMap();
                }
                return newVSR;
            }
            return vsr;
        });

        if (currentVSR == null) {
            throw new IOException("No active VSR available");
        }
        if (currentVSR.getState() != VSRState.ACTIVE) {
            throw new IOException("Cannot add document - VSR is not active: " + currentVSR.getState());
        }

        logger.debug("addToManagedVSR called for {}, current row count: {}", fileName, currentVSR.getRowCount());

        try {
            // Since ParquetDocumentInput now works directly with ManagedVSR,
            // fields should already be populated in vectors via addField() calls.
            // We just need to finalize the document by calling addToWriter()
            // which will increment the row count.
            WriteResult result = document.addToWriter();

            logger.debug("After adding document to {}, row count: {}", fileName, currentVSR.getRowCount());

            // Check for VSR rotation AFTER successful document processing
            maybeRotateActiveVSR();

            return result;
        } catch (Exception e) {
            logger.error("Error in addToManagedVSR for {}: {}", fileName, e.getMessage(), e);
            throw new IOException("Failed to add document: " + e.getMessage(), e);
        }
    }

    public String flush(FlushIn flushIn) throws IOException {
        ManagedVSR currentVSR = managedVSR.get();
        logger.info("Flush called for {}, row count: {}", fileName, currentVSR.getRowCount());
        try {
            // Only flush if we have data
            if (currentVSR.getRowCount() == 0) {
                logger.debug("No data to flush for {}, returning null", fileName);
                return null;
            }

            // Transition VSR to FROZEN state before flushing
            currentVSR.moveToFrozen();
            logger.info("Flushing {} rows for {}", currentVSR.getRowCount(), fileName);

            // Write through native writer handle
            try (ArrowExport export = currentVSR.exportToArrow()) {
                writer.write(export.getArrayAddress(), export.getSchemaAddress());
                writer.close();
            }
            logger.info("Successfully flushed data for {}", fileName);

            return fileName;
        } catch (Exception e) {
            logger.error("Error in flush for {}: {}", fileName, e.getMessage(), e);
            throw new IOException("Failed to flush data: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
            vsrPool.close();
            managedVSR.set(null);
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
                    try (ArrowExport export = frozenVSR.exportToArrow()) {
                        writer.write(export.getArrayAddress(), export.getSchemaAddress());
                    }

                    logger.debug("Successfully wrote frozen VSR data for {}", fileName);

                    // Complete the VSR processing
                    vsrPool.completeVSR(frozenVSR);
                    vsrPool.unsetFrozenVSR();
                } else {
                    logger.warn("Rotation occurred but no frozen VSR found for {}", fileName);
                }

                // Update to new active VSR atomically with field vector map
                ManagedVSR oldVSR = managedVSR.get();
                ManagedVSR newVSR = vsrPool.getActiveVSR();
                if (newVSR == null) {
                    throw new IOException("No active VSR available after rotation");
                }
                updateVSRAndReinitialize(oldVSR, newVSR);

                // Reinitialize field vector map with new VSR
                reinitializeFieldVectorMap();

                logger.debug("VSR rotation completed for {}, new active VSR: {}, row count: {}",
                    fileName, newVSR.getId(), newVSR.getRowCount());
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
        ManagedVSR oldVSR = managedVSR.get();
        if (currentActive != oldVSR) {
            logger.debug("VSR rotation detected for {}, updating references", fileName);

            // Update the managed VSR reference atomically with field vector map
            updateVSRAndReinitialize(oldVSR, currentActive);

            // Note: Writer initialization is not needed per VSR as it's per file
            logger.debug("VSR rotation completed for {}, new row count: {}", fileName, currentActive.getRowCount());
        }
    }

    /**
     * Atomically updates managedVSR and reinitializes field vector map.
     */
    private void updateVSRAndReinitialize(ManagedVSR oldVSR, ManagedVSR newVSR) {
        if (managedVSR.compareAndSet(oldVSR, newVSR)) {
            reinitializeFieldVectorMap();
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
            FieldVector fieldVector = managedVSR.get().getVector(fieldName);
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
        return managedVSR.get();
    }
}
