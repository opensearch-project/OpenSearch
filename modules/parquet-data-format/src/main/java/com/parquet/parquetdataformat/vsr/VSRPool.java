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
    private final ConcurrentHashMap<String, ManagedVSR> allVSRs;
    private final AtomicInteger vsrCounter;

    // Configuration
    private final int maxRowsPerVSR;

    public VSRPool(String poolId, Schema schema, ArrowBufferPool arrowBufferPool) {
        this.poolId = poolId;
        this.schema = schema;
        this.bufferPool = arrowBufferPool;
        this.activeVSR = new AtomicReference<>();
        this.frozenVSR = new AtomicReference<>();
        this.allVSRs = new ConcurrentHashMap<>();
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
        synchronized (this) {
            return activeVSR.get();
        }
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
        if (current == null || !shouldRotateVSR(current)) {
            return false; // No rotation needed
        }

        // CRITICAL: Check if frozen slot is occupied before rotation
        if (frozenVSR.get() != null) {
            throw new IOException("Cannot rotate VSR: frozen slot is occupied. " +
                                "Previous frozen VSR has not been processed. This indicates a " +
                                "system bottleneck or processing failure.");
        }

        synchronized (this) {
            current = activeVSR.get();
            if (current == null || !shouldRotateVSR(current) || frozenVSR.get() != null) {
                return false;
            }
            if (current.getRowCount() > 0) {
                freezeVSR(current);
            }
            activeVSR.set(createNewVSR());
            return true;
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
        ManagedVSR vsr = frozenVSR.get();
        if (vsr == null) {
            throw new IOException("unsetFrozenVSR called when frozen VSR is not set");
        }
        if (vsr.getState() != VSRState.CLOSED) {
            throw new IOException("frozenVSR cannot be unset, state is " + vsr.getState());
        }
        if (!frozenVSR.compareAndSet(vsr, null)) {
            throw new IOException("frozenVSR changed during unset");
        }
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
     * Marks a VSR as flushing (being processed by Rust).
     *
     * @param vsr VSR being processed
     */
    public void markFlushing(ManagedVSR vsr) {
        vsr.setState(VSRState.FLUSHING);
    }

    /**
     * Completes VSR processing and cleans up resources.
     *
     * @param vsr VSR that has been processed
     */
    public void completeVSR(ManagedVSR vsr) {
        vsr.setState(VSRState.CLOSED);
        vsr.close();
        allVSRs.remove(vsr.getId());
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
     * Gets statistics about the VSR pool.
     *
     * @return PoolStats with current state
     */
    public PoolStats getStats() {
        ManagedVSR active = activeVSR.get();
        ManagedVSR frozen = frozenVSR.get();
        int frozenCount = frozen != null ? 1 : 0;

        return new PoolStats(
            poolId,
            active != null ? active.getRowCount() : 0,
            frozenCount,
            allVSRs.size(),
            allVSRs.values().stream().mapToLong(ManagedVSR::getRowCount).sum()
        );
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

        // Close any remaining VSRs
        allVSRs.values().forEach(ManagedVSR::close);
        allVSRs.clear();
    }

    private void initializeActiveVSR() {
        activeVSR.set(createNewVSR());
    }

    private ManagedVSR createNewVSR() {
        String vsrId = poolId + "-vsr-" + vsrCounter.incrementAndGet();
        BufferAllocator allocator = null;
        VectorSchemaRoot vsr = null;
        try {
            allocator = bufferPool.createChildAllocator(vsrId);
            vsr = VectorSchemaRoot.create(schema, allocator);
            ManagedVSR managedVSR = new ManagedVSR(vsrId, vsr, allocator);
            allVSRs.put(vsrId, managedVSR);

            // Success: ManagedVSR now owns the resources
            return managedVSR;
        } catch (Exception e) {
            // Clean up resources on failure since ManagedVSR couldn't take ownership
            if (vsr != null) {
                try { vsr.close(); } catch (Exception ex) { e.addSuppressed(ex); }
            }
            if (allocator != null) {
                try { allocator.close(); } catch (Exception ex) { e.addSuppressed(ex); }
            }
            throw new RuntimeException("Failed to create new VSR", e);
        }
    }

    private void freezeVSR(ManagedVSR vsr) {
        vsr.setState(VSRState.FROZEN);
        if (!frozenVSR.compareAndSet(null, vsr)) {
            vsr.setState(VSRState.ACTIVE);
            throw new IllegalStateException("Frozen slot occupied during freeze");
        }
    }

    private boolean shouldRotateVSR(ManagedVSR vsr) {
        return vsr.getRowCount() >= maxRowsPerVSR;
    }

    /**
     * Statistics for the VSR pool.
     */
    public static class PoolStats {
        private final String poolId;
        private final long activeRowCount;
        private final int frozenVSRCount;
        private final int totalVSRCount;
        private final long totalRowCount;

        public PoolStats(String poolId, long activeRowCount, int frozenVSRCount,
                        int totalVSRCount, long totalRowCount) {
            this.poolId = poolId;
            this.activeRowCount = activeRowCount;
            this.frozenVSRCount = frozenVSRCount;
            this.totalVSRCount = totalVSRCount;
            this.totalRowCount = totalRowCount;
        }

        public String getPoolId() { return poolId; }
        public long getActiveRowCount() { return activeRowCount; }
        public int getFrozenVSRCount() { return frozenVSRCount; }
        public int getTotalVSRCount() { return totalVSRCount; }
        public long getTotalRowCount() { return totalRowCount; }
    }
}
