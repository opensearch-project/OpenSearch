package com.parquet.parquetdataformat.vsr;

import com.parquet.parquetdataformat.bridge.ArrowExport;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Managed wrapper around VectorSchemaRoot that handles state transitions
 * for the ACTIVE/FROZEN/CLOSED lifecycle with controlled access methods.
 */
public class ManagedVSR implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(ManagedVSR.class);

    private final String id;
    private final VectorSchemaRoot vsr;
    private final BufferAllocator allocator;
    private VSRState state;
    private final Map<String, Field> fields = new HashMap<>();


    public ManagedVSR(String id, Schema schema, BufferAllocator allocator) {
        this.id = id;
        this.vsr = VectorSchemaRoot.create(schema, allocator);
        this.allocator = allocator;
        this.state = VSRState.ACTIVE;
        for (Field field : vsr.getSchema().getFields()) {
            fields.put(field.getName(), field);
        }
    }

    /**
     * Gets the current row count in this VSR.
     *
     * @return Number of rows currently in the VSR
     */
    public int getRowCount() {
        return vsr.getRowCount();
    }

    /**
     * Sets the row count for this VSR.
     * Only allowed when VSR is in ACTIVE state.
     *
     * @param rowCount New row count
     * @throws IllegalStateException if VSR is not active or is immutable
     */
    public void setRowCount(int rowCount) {
        if (state != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot modify VSR in state: " + state);
        }
        vsr.setRowCount(rowCount);
    }

    /**
     * Gets a field vector by name.
     * Only allowed when VSR is in ACTIVE state.
     *
     * @param fieldName Name of the field
     * @return FieldVector for the field, or null if not found
     * @throws IllegalStateException if VSR is not in ACTIVE state
     */
    public FieldVector getVector(String fieldName) {
        if (state != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot access vector in VSR state: " + state + ". VSR must be ACTIVE to access vectors.");
        }
        return vsr.getVector(fields.get(fieldName));
    }

    /**
     * Changes the state of this VSR.
     * Handles state transition logic and immutability.
     * This method is private to ensure controlled state transitions.
     *
     * @param newState New state to transition to
     */
    private void setState(VSRState newState) {
        VSRState oldState = state;
        state = newState;

        logger.debug("State transition: {} -> {} for VSR {}", oldState, newState, id);
    }

    /**
     * Transitions the VSR from ACTIVE to FROZEN state.
     * This is the only way to freeze a VSR.
     *
     * @throws IllegalStateException if VSR is not in ACTIVE state
     */
    public void moveToFrozen() {
        if (state != VSRState.ACTIVE) {
            throw new IllegalStateException(String.format(
                "Cannot freeze VSR %s: expected ACTIVE state but was %s", id, state));
        }
        setState(VSRState.FROZEN);
    }

    /**
     * Transitions the VSR from FROZEN to CLOSED state.
     * This method is private and only called by close().
     *
     * @throws IllegalStateException if VSR is not in FROZEN state
     */
    private void moveToClosed() {
        if (state != VSRState.FROZEN) {
            throw new IllegalStateException(String.format(
                "Cannot close VSR %s: expected FROZEN state but was %s", id, state));
        }
        setState(VSRState.CLOSED);

        // Clean up resources
        if (vsr != null) {
            vsr.close();
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    /**
     * Gets the current state of this VSR.
     *
     * @return Current VSRState
     */
    public VSRState getState() {
        return state;
    }

    /**
     * Exports this VSR to Arrow C Data Interface for Rust handoff.
     * Only allowed when VSR is FROZEN.
     *
     * @return ArrowExport containing ArrowArray and ArrowSchema
     * @throws IllegalStateException if VSR is not in correct state
     */
    public ArrowExport exportToArrow() {
        if (state != VSRState.FROZEN) {
            throw new IllegalStateException("Cannot export VSR in state: " + state + ". VSR must be FROZEN to export.");
        }

        ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);

        // Export the VectorSchemaRoot to C Data Interface
        Data.exportVectorSchemaRoot(allocator, vsr, null, arrowArray, arrowSchema);

        return new ArrowExport(arrowArray, arrowSchema);
    }

    public ArrowExport exportSchema() {
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);

        // Export the VectorSchemaRoot to C Data Interface
        Data.exportSchema(allocator, vsr.getSchema(), null, arrowSchema);

        return new ArrowExport(null, arrowSchema);
    }

    /**
     * Checks if this VSR is immutable (frozen).
     *
     * @return true if VSR cannot be modified
     */
    public boolean isImmutable() {
        return state != VSRState.ACTIVE;
    }


    /**
     * Gets the VSR ID.
     *
     * @return Unique identifier for this VSR
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the associated BufferAllocator.
     *
     * @return BufferAllocator used by this VSR
     */
    public BufferAllocator getAllocator() {
        return allocator;
    }

    /**
     * Closes this VSR and releases all resources.
     * This is the only way to transition a VSR to CLOSED state.
     * VSR must be in FROZEN state before it can be closed.
     *
     * @throws IllegalStateException if VSR is in ACTIVE state (must freeze first)
     */
    @Override
    public void close() {
        // If already CLOSED, do nothing (idempotent)
        if (state == VSRState.CLOSED) {
            return;
        }

        // If ACTIVE, must freeze first
        if (state == VSRState.ACTIVE) {
            throw new IllegalStateException(String.format(
                "Cannot close VSR %s: VSR is still ACTIVE. Must freeze VSR before closing.", id));
        }

        // If FROZEN, transition to CLOSED
        if (state == VSRState.FROZEN) {
            moveToClosed();
        } else {
            // This should never happen with current states, but defensive programming
            throw new IllegalStateException(String.format(
                "Cannot close VSR %s: unexpected state %s", id, state));
        }
    }


    @Override
    public String toString() {
        return String.format("ManagedVSR{id='%s', state=%s, rows=%d, immutable=%s}",
            id, state, getRowCount(), isImmutable());
    }
}
