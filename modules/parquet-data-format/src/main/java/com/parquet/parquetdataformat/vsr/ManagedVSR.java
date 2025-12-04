package com.parquet.parquetdataformat.vsr;

import com.parquet.parquetdataformat.bridge.ArrowExport;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.arrow.vector.BitVectorHelper.byteIndex;

/**
 * Managed wrapper around VectorSchemaRoot that handles state transitions
 * and provides thread-safe access for the ACTIVE/FROZEN lifecycle.
 */
public class ManagedVSR implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(ManagedVSR.class);

    private final String id;
    private final VectorSchemaRoot vsr;
    private final BufferAllocator allocator;
    private final AtomicReference<VSRState> state;
    private final ReadWriteLock lock;
    private final long createdTime;
    private final Map<String, Field> fields = new HashMap<>();


    public ManagedVSR(String id, VectorSchemaRoot vsr, BufferAllocator allocator) {
        this.id = id;
        this.vsr = vsr;
        this.allocator = allocator;
        this.state = new AtomicReference<>(VSRState.ACTIVE);
        this.lock = new ReentrantReadWriteLock();
        this.createdTime = System.currentTimeMillis();
        for (Field field : vsr.getSchema().getFields()) {
            fields.put(field.getName(), field);
        }
    }

    /**
     * Gets the underlying VectorSchemaRoot.
     * Should only be used when holding appropriate locks.
     *
     * @return VectorSchemaRoot instance
     */
    public VectorSchemaRoot getVSR() {
        return vsr;
    }

    /**
     * Gets the current row count in this VSR.
     * Thread-safe read operation.
     *
     * @return Number of rows currently in the VSR
     */
    public int getRowCount() {
        lock.readLock().lock();
        try {
            return vsr.getRowCount();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Sets the row count for this VSR.
     * Only allowed when VSR is in ACTIVE state.
     *
     * @param rowCount New row count
     * @throws IllegalStateException if VSR is not active or is immutable
     */
    public void setRowCount(int rowCount) {
        lock.writeLock().lock();
        try {
            if (state.get() != VSRState.ACTIVE) {
                throw new IllegalStateException("Cannot modify VSR in state: " + state.get());
            }
            vsr.setRowCount(rowCount);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets a field vector by name.
     * Thread-safe read operation.
     *
     * @param fieldName Name of the field
     * @return FieldVector for the field, or null if not found
     */
    public FieldVector getVector(String fieldName) {
        lock.readLock().lock();
        try {
            return vsr.getVector(fields.get(fieldName));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Changes the state of this VSR.
     * Handles state transition logic and immutability.
     *
     * @param newState New state to transition to
     */
    public void setState(VSRState newState) {
        VSRState oldState = state.getAndSet(newState);

        logger.debug("State transition: {} -> {} for VSR {}", oldState, newState, id);
    }

    /**
     * Gets the current state of this VSR.
     *
     * @return Current VSRState
     */
    public VSRState getState() {
        return state.get();
    }

    /**
     * Exports this VSR to Arrow C Data Interface for Rust handoff.
     * Only allowed when VSR is FROZEN or FLUSHING.
     *
     * @return ArrowExport containing ArrowArray and ArrowSchema
     * @throws IllegalStateException if VSR is not in correct state
     */
    public ArrowExport exportToArrow() {
        VSRState currentState = state.get();
        if (currentState != VSRState.FROZEN &&
            currentState != VSRState.FLUSHING) {
            throw new IllegalStateException("Cannot export VSR in state: " + currentState);
        }

        lock.readLock().lock();
        try {
            ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);

            // Export the VectorSchemaRoot to C Data Interface
            Data.exportVectorSchemaRoot(allocator, vsr, null, arrowArray, arrowSchema);

            return new ArrowExport(arrowArray, arrowSchema);
        } finally {
            lock.readLock().unlock();
        }
    }

    public ArrowExport exportSchema() {
        lock.readLock().lock();
        try {
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);

            // Export the VectorSchemaRoot to C Data Interface
            Data.exportSchema(allocator, vsr.getSchema(), null, arrowSchema);

            return new ArrowExport(null, arrowSchema);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Checks if this VSR is immutable (frozen).
     *
     * @return true if VSR cannot be modified
     */
    public boolean isImmutable() {
        VSRState currentState = state.get();
        return currentState != VSRState.ACTIVE;
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
     * Gets the creation timestamp.
     *
     * @return Creation time in milliseconds
     */
    public long getCreatedTime() {
        return createdTime;
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
     */
    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            if (state.get() != VSRState.CLOSED) {
                state.set(VSRState.CLOSED);
                vsr.close();
                allocator.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }


    @Override
    public String toString() {
        return String.format("ManagedVSR{id='%s', state=%s, rows=%d, immutable=%s}",
            id, state.get(), getRowCount(), isImmutable());
    }
}
