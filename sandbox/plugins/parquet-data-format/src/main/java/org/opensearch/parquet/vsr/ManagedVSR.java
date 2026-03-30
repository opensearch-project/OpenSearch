/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.nativebridge.spi.ArrowExport;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Managed wrapper around an Apache Arrow {@link VectorSchemaRoot} with strict lifecycle enforcement.
 *
 * <p>Each instance follows the state machine: {@code ACTIVE → FROZEN → CLOSED}.
 * <ul>
 *   <li><strong>ACTIVE</strong> — Vectors are writable; row count can be incremented.</li>
 *   <li><strong>FROZEN</strong> — Read-only; data can be exported to the native writer via
 *       {@link #exportToArrow()} using the Arrow C Data Interface.</li>
 *   <li><strong>CLOSED</strong> — All Arrow resources (vectors and child allocator) are released.</li>
 * </ul>
 *
 * <p>State transitions are enforced: writing to a frozen VSR or closing an active VSR
 * <p>This class is NOT Thread-Safe. External synchronization is required
 * if instances are shared across threads.
 */
public class ManagedVSR implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(ManagedVSR.class);

    private final String id;
    private final VectorSchemaRoot vsr;
    private final BufferAllocator allocator;
    private final AtomicReference<VSRState> state = new AtomicReference<>(VSRState.ACTIVE);
    private final Map<String, Field> fields = new HashMap<>();

    /**
     * Creates a new ManagedVSR.
     *
     * @param id unique identifier for this VSR
     * @param schema Arrow schema defining the vector structure
     * @param allocator buffer allocator for Arrow memory
     */
    public ManagedVSR(String id, Schema schema, BufferAllocator allocator) {
        this.id = id;
        this.vsr = VectorSchemaRoot.create(schema, allocator);
        this.allocator = allocator;
        for (Field field : vsr.getSchema().getFields()) {
            fields.put(field.getName(), field);
        }
    }

    /** Returns the current row count. */
    public int getRowCount() {
        return vsr.getRowCount();
    }

    /**
     * Sets the row count.
     *
     * @param rowCount the new row count
     */
    public void setRowCount(int rowCount) {
        if (state.get() != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot modify VSR in state: " + state.get());
        }
        vsr.setRowCount(rowCount);
    }

    /**
     * Returns the vector for the given field name, or null if not found.
     * @param fieldName the field name
     * @return the field vector, or null
     */
    public FieldVector getVector(String fieldName) {
        if (state.get() != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot access vector in VSR state: " + state.get());
        }
        Field field = fields.get(fieldName);
        return field != null ? vsr.getVector(field) : null;
    }

    /** Transitions this VSR from ACTIVE to FROZEN state. */
    public void moveToFrozen() {
        if (state.compareAndSet(VSRState.ACTIVE, VSRState.FROZEN) == false) {
            throw new IllegalStateException("Cannot freeze VSR " + id + ": expected ACTIVE but was " + state.get());
        }
        logger.debug("State transition: ACTIVE -> FROZEN for VSR {}", id);
    }

    /**
     * Exports this VSR to Arrow C Data Interface for native handoff.
     * Only allowed when VSR is FROZEN.
     */
    public ArrowExport exportToArrow() {
        if (state.get() != VSRState.FROZEN) {
            throw new IllegalStateException("Cannot export VSR in state: " + state.get() + ". Must be FROZEN.");
        }
        ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportVectorSchemaRoot(allocator, vsr, null, arrowArray, arrowSchema);
        return new ArrowExport(arrowArray, arrowSchema);
    }

    /**
     * Exports only the schema to Arrow C Data Interface.
     */
    public ArrowSchema exportSchema() {
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, vsr.getSchema(), null, arrowSchema);
        return arrowSchema;
    }

    /**
     * Returns the current lifecycle state.
     *
     * @return the VSR state
     */
    public VSRState getState() {
        return state.get();
    }

    /**
     * Returns the unique identifier.
     *
     * @return the VSR id
     */
    public String getId() {
        return id;
    }

    @Override
    public void close() {
        if (state.get() == VSRState.CLOSED) {
            return;
        }
        if (state.get() == VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot close VSR " + id + ": must freeze first");
        }
        if (state.compareAndSet(VSRState.FROZEN, VSRState.CLOSED) == false) {
            throw new IllegalStateException("Expected VSR to be FROZEN but was " + state.get());
        }
        logger.debug("State transition: FROZEN -> CLOSED for VSR {}", id);
        if (vsr != null) {
            vsr.close();
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    @Override
    public String toString() {
        return "ManagedVSR{id='" + id + "', state=" + state.get() + ", rows=" + getRowCount() + "}";
    }
}
