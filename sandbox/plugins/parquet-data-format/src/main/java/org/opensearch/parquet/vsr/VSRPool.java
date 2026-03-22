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
 * The frozen slot must be drained (exported and closed) before the next rotation
 * can occur; attempting to rotate with an occupied frozen slot throws {@link java.io.IOException}.
 *
 * <p>Each new VSR receives its own child allocator from the shared {@link ArrowBufferPool},
 * ensuring memory isolation between batches.
 */
public class VSRPool implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(VSRPool.class);

    private final Schema schema;
    private final ArrowBufferPool bufferPool;
    private final String poolId;
    private final AtomicReference<ManagedVSR> activeVSR;
    private final AtomicReference<ManagedVSR> frozenVSR;
    private final AtomicInteger vsrCounter;
    private final int maxRowsPerVSR;

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

    public ManagedVSR getActiveVSR() {
        return activeVSR.get();
    }

    public ManagedVSR getFrozenVSR() {
        return frozenVSR.get();
    }

    /**
     * Checks if rotation is needed and performs it if safe.
     *
     * @return true if rotation occurred
     * @throws IOException if rotation needed but frozen slot is occupied
     */
    public boolean maybeRotateActiveVSR() throws IOException {
        ManagedVSR current = activeVSR.get();
        if (current == null || !shouldRotateVSR(current)) {
            return false;
        }
        if (frozenVSR.get() != null) {
            throw new IOException("Cannot rotate VSR: frozen slot is occupied");
        }
        synchronized (this) {
            current = activeVSR.get();
            if (current == null || !shouldRotateVSR(current)) {
                return false;
            }
            if (frozenVSR.get() != null) {
                throw new IOException("Cannot rotate VSR: frozen slot became occupied during rotation");
            }
            if (current.getRowCount() > 0) {
                freezeVSR(current);
            }
            activeVSR.set(createNewVSR());
            return true;
        }
    }

    public void unsetFrozenVSR() throws IOException {
        ManagedVSR frozen = frozenVSR.get();
        if (frozen == null) {
            throw new IOException("unsetFrozenVSR called when frozen VSR is not set");
        }
        if (!VSRState.CLOSED.equals(frozen.getState())) {
            throw new IOException("frozenVSR cannot be unset, state is " + frozen.getState());
        }
        frozenVSR.set(null);
    }

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
                activeVSR.set(null);
            } catch (Exception e) {
                firstException = e;
            }
        }
        if (frozen != null) {
            try {
                frozen.close();
                frozenVSR.set(null);
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
