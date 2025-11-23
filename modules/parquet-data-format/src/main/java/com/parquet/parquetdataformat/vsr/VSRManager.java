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
import com.parquet.parquetdataformat.memory.MemoryPressureMonitor;
import com.parquet.parquetdataformat.writer.ParquetDocumentInput;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
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

    public VSRManager(String fileName, Schema schema, ArrowBufferPool arrowBufferPool) {
        this.fileName = fileName;
        this.schema = schema;

        // Create memory monitor and buffer pool
        MemoryPressureMonitor memoryMonitor = new MemoryPressureMonitor(org.opensearch.common.settings.Settings.EMPTY);

        // Create VSR pool
        this.vsrPool = new VSRPool("pool-" + fileName, schema, memoryMonitor, arrowBufferPool);

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

        System.out.println("[JAVA] addToManagedVSR called, current row count: " + currentVSR.getRowCount());

        try {
            // Since ParquetDocumentInput now works directly with ManagedVSR,
            // fields should already be populated in vectors via addField() calls.
            // We just need to finalize the document by calling addToWriter()
            // which will increment the row count.
            WriteResult result = document.addToWriter();

            System.out.println("[JAVA] After adding document, row count: " + currentVSR.getRowCount());

            // Check for VSR rotation AFTER successful document processing
            maybeRotateActiveVSR();

            return result;
        } catch (Exception e) {
            System.out.println("[JAVA] ERROR in addToManagedVSR: " + e.getMessage());
            throw new IOException("Failed to add document: " + e.getMessage(), e);
        }
    }

    public String flush(FlushIn flushIn) throws IOException {
        ManagedVSR currentVSR = managedVSR.get();
        System.out.println("[JAVA] flush called, row count: " + currentVSR.getRowCount());
        try {
            // Only flush if we have data
            if (currentVSR.getRowCount() == 0) {
                System.out.println("[JAVA] No data to flush, returning null");
                return null;
            }

            // Transition VSR to FROZEN state before flushing
            currentVSR.setState(VSRState.FROZEN);
            System.out.println("[JAVA] Flushing " + currentVSR.getRowCount() + " rows");

            // Transition to FLUSHING state
            currentVSR.setState(VSRState.FLUSHING);

            // Write through native writer handle
            try (ArrowExport export = currentVSR.exportToArrow()) {
                writer.write(export.getArrayAddress(), export.getSchemaAddress());
                writer.close();
            }
            System.out.println("[JAVA] Successfully flushed data");

            return fileName;
        } catch (Exception e) {
            System.out.println("[JAVA] ERROR in flush: " + e.getMessage());
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
            System.err.println("Error during close: " + e.getMessage());
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
                System.out.println("[JAVA] VSR rotation occurred after document addition");

                // Get the frozen VSR that was just created by rotation
                ManagedVSR frozenVSR = vsrPool.getFrozenVSR();
                if (frozenVSR != null) {
                    System.out.println("[JAVA] Processing frozen VSR: " + frozenVSR.getId() +
                        " with " + frozenVSR.getRowCount() + " rows");

                    // Write the frozen VSR data immediately
                    frozenVSR.setState(VSRState.FLUSHING);
                    try (ArrowExport export = frozenVSR.exportToArrow()) {
                        writer.write(export.getArrayAddress(), export.getSchemaAddress());
                    }

                    System.out.println("[JAVA] Successfully wrote frozen VSR data");

                    // Complete the VSR processing
                    vsrPool.completeVSR(frozenVSR);
                    vsrPool.unsetFrozenVSR();
                } else {
                    System.err.println("[JAVA] WARNING: Rotation occurred but no frozen VSR found");
                }

                // Update to new active VSR atomically with field vector map
                ManagedVSR oldVSR = managedVSR.get();
                ManagedVSR newVSR = vsrPool.getActiveVSR();
                if (newVSR == null) {
                    throw new IOException("No active VSR available after rotation");
                }
                if (managedVSR.compareAndSet(oldVSR, newVSR)) {
                    reinitializeFieldVectorMap();
                }

                System.out.println("[JAVA] VSR rotation completed, new active VSR: " + newVSR.getId() +
                    ", row count: " + newVSR.getRowCount());
            }
        } catch (IOException e) {
            System.err.println("[JAVA] Error during VSR rotation: " + e.getMessage());
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
            System.out.println("[JAVA] VSR rotation detected, updating references");

            // Update the managed VSR reference atomically with field vector map
            if (managedVSR.compareAndSet(oldVSR, currentActive)) {
                reinitializeFieldVectorMap();
            }

            // Note: Writer initialization is not needed per VSR as it's per file
            System.out.println("[JAVA] VSR rotation completed, new row count: " + currentActive.getRowCount());
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
