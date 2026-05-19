/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.parquet.memory.ArrowBufferPool;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Double-buffered pool managing one ACTIVE and one FROZEN {@link ManagedVSR} slot.
 *
 * <p>Handles row-count-based rotation: when the active VSR reaches the configured
 * {@code maxRowsPerVSR} threshold, it is frozen and a new active VSR is created.
 * If the frozen slot is still occupied (previous write in progress), rotation is
 * skipped and the active VSR continues accepting writes beyond the threshold.
 *
 * <p>Each new VSR receives its own child allocator from the shared {@link ArrowBufferPool},
 * <p>This class is NOT Thread-Safe. External synchronization is required
 * if instances are shared across threads.
 */
public class VSRPool implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(VSRPool.class);

    private volatile Schema schema;
    private final ArrowBufferPool bufferPool;
    private final String poolId;
    private final AtomicReference<ManagedVSR> activeVSR;
    private final AtomicReference<ManagedVSR> frozenVSR;
    private final AtomicInteger vsrCounter;
    private final int maxRowsPerVSR;

    /**
     * Creates a new VSRPool.
     *
     * @param poolId unique identifier for this pool
     * @param schema Arrow schema for VSR creation
     * @param bufferPool shared Arrow buffer pool
     * @param maxRowsPerVSR row threshold triggering rotation
     */
    public VSRPool(String poolId, Schema schema, ArrowBufferPool bufferPool, int maxRowsPerVSR) {
        this.poolId = poolId;
        this.schema = schema;
        this.bufferPool = bufferPool;
        this.activeVSR = new AtomicReference<>();
        this.frozenVSR = new AtomicReference<>();
        this.vsrCounter = new AtomicInteger(0);
        this.maxRowsPerVSR = maxRowsPerVSR;
        initializeActiveVSR();
    }

    /**
     * Returns the active VSR.
     *
     * @return the active ManagedVSR
     */
    public ManagedVSR getActiveVSR() {
        return activeVSR.get();
    }

    /**
     * Returns the frozen VSR.
     *
     * @return the frozen ManagedVSR, or null if none
     */
    public ManagedVSR getFrozenVSR() {
        return frozenVSR.get();
    }

    /**
     * Checks if rotation is needed and performs it if safe.
     * If the frozen slot is still occupied, rotation is skipped and the active VSR
     * continues accepting writes beyond the threshold.
     *
     * @return true if rotation occurred, false if not needed or skipped
     * @throws IOException if the active VSR swap fails unexpectedly
     */
    public boolean maybeRotateActiveVSR() throws IOException {
        ManagedVSR current = activeVSR.get();
        if (current == null || !shouldRotateVSR(current)) {
            return false;
        }
        if (frozenVSR.get() != null) {
            return false;
        }
        if (current.getRowCount() > 0) {
            freezeVSR(current);
        }
        ManagedVSR newVSR = createNewVSR();
        if (activeVSR.compareAndSet(current, newVSR) == false) {
            throw new IOException("Failed to set new active VSR during rotation");
        }
        return true;
    }

    /**
     * Clears the frozen VSR slot after it has been closed.
     *
     * @throws IllegalStateException if the frozen slot is empty or the VSR is not closed
     */
    public void unsetFrozenVSR() {
        ManagedVSR frozen = frozenVSR.get();
        if (frozen == null) {
            throw new IllegalStateException("unsetFrozenVSR called on a null VSR");
        }
        if (VSRState.CLOSED.equals(frozen.getState()) == false) {
            throw new IllegalStateException("frozenVSR needs to be in CLOSED state, current state is " + frozen.getState());
        }
        frozenVSR.set(null);
    }

    /**
     * Closes the given VSR, releasing its resources.
     *
     * @param vsr the ManagedVSR to close
     */
    public void completeVSR(ManagedVSR vsr) {
        vsr.close();
    }

    @Override
    public void close() {
        ManagedVSR active = activeVSR.get();
        ManagedVSR frozen = frozenVSR.get();
        Exception firstException = null;
        if (active != null) {
            try {
                if (active.getState() == VSRState.ACTIVE) {
                    active.moveToFrozen();
                }
                active.close();
                activeVSR.compareAndSet(active, null);
            } catch (Exception e) {
                firstException = e;
            }
        }
        if (frozen != null) {
            try {
                frozen.close();
                frozenVSR.compareAndSet(frozen, null);
            } catch (Exception e) {
                if (firstException != null) firstException.addSuppressed(e);
                else firstException = e;
            }
        }
        if (firstException != null) {
            throw new RuntimeException("VSRPool cleanup failed", firstException);
        }
    }

    private void initializeActiveVSR() {
        activeVSR.set(createNewVSR());
    }

    private ManagedVSR createNewVSR() {
        String vsrId = poolId + "-vsr-" + vsrCounter.incrementAndGet();
        BufferAllocator allocator = bufferPool.createChildAllocator(vsrId);
        return new ManagedVSR(vsrId, schema, allocator);
    }

    /**
     * Updates the schema used for creating new VSRs. Called when dynamic fields
     * are added to the active VSR so that subsequent VSRs include those fields.
     *
     * @param newSchema the updated schema
     */
    public void updateSchema(Schema newSchema) {
        this.schema = newSchema;
    }

    private void freezeVSR(ManagedVSR vsr) {
        ManagedVSR previousFrozen = frozenVSR.get();
        if (previousFrozen != null) {
            throw new IllegalStateException("Cannot freeze VSR: frozen slot is occupied by " + previousFrozen.getId());
        }
        vsr.moveToFrozen();
        if (!frozenVSR.compareAndSet(null, vsr)) {
            throw new IllegalStateException("Race condition: frozen slot occupied during freeze for VSR " + vsr.getId());
        }
    }

    private boolean shouldRotateVSR(ManagedVSR vsr) {
        return vsr.getRowCount() >= maxRowsPerVSR;
    }
}
