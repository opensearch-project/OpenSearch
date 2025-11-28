package com.parquet.parquetdataformat.vsr;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.parquet.parquetdataformat.memory.ArrowBufferPool;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages VectorSchemaRoot lifecycle with ACTIVE and FROZEN states as specified
 * in the Project Mustang design. Each ParquetWriter maintains a single ACTIVE VSR
 * for writing and a single FROZEN VSR for Rust handoff.
 */
public class VSRPool {

    private static final Logger logger = LogManager.getLogger(VSRPool.class);

    private final Schema schema;
    private final ArrowBufferPool bufferPool;
    private final String poolId;

    // VSR lifecycle management
    private final AtomicReference<ManagedVSR> activeVSR;
    private final AtomicReference<ManagedVSR> frozenVSR;
    private final AtomicInteger vsrCounter;

    // Configuration
    private final int maxRowsPerVSR;

    public VSRPool(String poolId, Schema schema, ArrowBufferPool arrowBufferPool) {
        this.poolId = poolId;
        this.schema = schema;
        this.bufferPool = arrowBufferPool;
        this.activeVSR = new AtomicReference<>();
        this.frozenVSR = new AtomicReference<>();
        this.vsrCounter = new AtomicInteger(0);

        // Configuration - could be made configurable
        this.maxRowsPerVSR = 50000; // Max rows before forcing freeze

        // Initialize with first active VSR
        initializeActiveVSR();
    }

    /**
     * Gets the current active VSR for writing.
     * Simply returns the current active VSR without any rotation logic.
     *
     * @return Active ManagedVSR for writing, or null if none exists
     */
    public ManagedVSR getActiveVSR() {
        return activeVSR.get();
    }

    /**
     * Checks if VSR rotation is needed and performs it if safe to do so.
     * Throws IOException if rotation is needed but frozen slot is occupied.
     *
     * @return true if rotation occurred, false if no rotation was needed
     * @throws IOException if rotation is needed but cannot be performed due to occupied frozen slot
     */
    public boolean maybeRotateActiveVSR() throws IOException {
        ManagedVSR current = activeVSR.get();

        // Check if rotation is needed
        if (current == null || !shouldRotateVSR(current)) {
            return false; // No rotation needed
        }

        // CRITICAL: Check if frozen slot is occupied before rotation
        if (frozenVSR.get() != null) {
            throw new IOException("Cannot rotate VSR: frozen slot is occupied. " +
                                "Previous frozen VSR has not been processed. This indicates a " +
                                "system bottleneck or processing failure.");
        }

        // Safe to rotate - perform the rotation
        synchronized (this) {
            // Double-check conditions under lock
            current = activeVSR.get();
            if (current == null || !shouldRotateVSR(current)) {
                return false; // Conditions changed while acquiring lock
            }

            // Check frozen slot again under lock
            if (frozenVSR.get() != null) {
                throw new IOException("Cannot rotate VSR: frozen slot became occupied during rotation");
            }

            // Freeze current VSR if it exists and has data
            if (current != null && current.getRowCount() > 0) {
                freezeVSR(current);
            }

            // Create new active VSR
            ManagedVSR newActive = createNewVSR();
            activeVSR.set(newActive);

            return true; // Rotation occurred
        }
    }

    /**
     * Freezes the current active VSR and creates a new active one.
     * The frozen VSR replaces any existing frozen VSR.
     *
     * @deprecated Use maybeRotateActiveVSR() instead for safer rotation with checks
     * @return Newly created active VSR
     */
    @Deprecated
    public ManagedVSR rotateActiveVSR() {
        synchronized (this) {
            ManagedVSR current = activeVSR.get();

            // Freeze current VSR if it exists and has data
            if (current != null && current.getRowCount() > 0) {
                freezeVSR(current);
            }

            // Create new active VSR
            ManagedVSR newActive = createNewVSR();
            activeVSR.set(newActive);

            return newActive;
        }
    }

    /**
     * Gets the frozen VSR for Rust processing.
     *
     * @return Frozen VSR, or null if none available
     */
    public ManagedVSR getFrozenVSR() {
        return frozenVSR.get();
    }

    public void unsetFrozenVSR() throws IOException {
        if (frozenVSR.get() == null) {
            throw new IOException("unsetFrozenVSR called when frozen VSR is not set");
        }
        if (!VSRState.CLOSED.equals(frozenVSR.get().getState())) {
            throw new IOException("frozenVSR cannot be unset, state is " + frozenVSR.get().getState());
        }
        frozenVSR.set(null);
    }

    /**
     * Takes the frozen VSR for processing and clears the frozen slot.
     *
     * @return Frozen VSR that was taken, or null if none available
     */
    public ManagedVSR takeFrozenVSR() {
        return frozenVSR.getAndSet(null);
    }

    /**
     * Completes VSR processing and cleans up resources.
     *
     * @param vsr VSR that has been processed
     */
    public void completeVSR(ManagedVSR vsr) {
        vsr.close();
    }

    /**
     * Forces all VSRs to be frozen for immediate processing.
     * Used during refresh or shutdown.
     */
    public void freezeAll() {
        ManagedVSR current = activeVSR.getAndSet(null);
        if (current != null && current.getRowCount() > 0) {
            freezeVSR(current);
        }
    }

    /**
     * Closes the pool and cleans up all resources.
     */
    public void close() {
        // Close active VSR
        ManagedVSR active = activeVSR.getAndSet(null);
        if (active != null) {
            active.close();
        }

        // Close frozen VSR
        ManagedVSR frozen = frozenVSR.getAndSet(null);
        if (frozen != null) {
            frozen.close();
        }
    }

    private void initializeActiveVSR() {
        ManagedVSR initial = createNewVSR();
        activeVSR.set(initial);
    }

    private ManagedVSR createNewVSR() {

        String vsrId = poolId + "-vsr-" + vsrCounter.incrementAndGet();
        BufferAllocator allocator = null;
        VectorSchemaRoot vsr = null;

        try {
            allocator = bufferPool.createChildAllocator(vsrId);
            ManagedVSR managedVSR = new ManagedVSR(vsrId, schema, allocator);

            // Success: ManagedVSR now owns the resources
            return managedVSR;
        } catch (Exception e) {
            // Clean up resources on failure since ManagedVSR couldn't take ownership
            if (vsr != null) {
                try {
                    vsr.close();
                } catch (Exception closeEx) {
                    e.addSuppressed(closeEx);
                }
            }
            if (allocator != null) {
                try {
                    allocator.close();
                } catch (Exception closeEx) {
                    e.addSuppressed(closeEx);
                }
            }
            throw new RuntimeException("Failed to create new VSR", e);
        }
    }

    private void freezeVSR(ManagedVSR vsr) {
        // Check if frozen slot is already occupied
        ManagedVSR previousFrozen = frozenVSR.get();
        if (previousFrozen != null) {
            // Never blindly overwrite a frozen VSR - this would cause data loss
            logger.error("Attempting to freeze VSR when frozen slot is occupied! " +
                        "Previous VSR: {} ({} rows), New VSR: {} ({} rows). " +
                        "This indicates a logic error - frozen VSR should be consumed before replacement.",
                        previousFrozen.getId(), previousFrozen.getRowCount(),
                        vsr.getId(), vsr.getRowCount());

            throw new IllegalStateException("Cannot freeze VSR: frozen slot is occupied by unprocessed VSR " +
                                          previousFrozen.getId() + ". This would cause data loss.");
        }

        // First freeze the VSR (validates ACTIVE -> FROZEN transition)
        vsr.moveToFrozen();

        // Safe to set frozen VSR since slot is empty and VSR is now frozen
        boolean success = frozenVSR.compareAndSet(null, vsr);
        if (!success) {
            // Race condition: another thread set frozen VSR between our check and set
            // This is a critical error since we can't revert the freeze operation
            throw new IllegalStateException("Race condition detected: frozen slot was occupied during freeze operation for VSR " + vsr.getId() + ", slot occupied by VSR " + frozenVSR.get().getId());
        }
    }

    private boolean shouldRotateVSR(ManagedVSR vsr) {
        return vsr.getRowCount() >= maxRowsPerVSR;
    }
}
