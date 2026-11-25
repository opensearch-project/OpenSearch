/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.concurrent.Semaphore;

/**
 * Node-wide budget of concurrently in-flight <em>prefetched</em> parts for multi-part
 * remote store downloads (see {@link ParallelPartInputStream}).
 *
 * <p>Every permit represents one extra byte-range request that is being read ahead of the
 * sequential writer and therefore one buffer of at most {@code part_size} bytes held in heap.
 * The total heap held by parallel part downloads on a node is thus bounded by
 * {@code maxPermits * part_size}.
 *
 * <p>Acquisition is strictly non-blocking: a caller that fails to obtain a permit simply does
 * not prefetch and falls back to reading the part itself, so the budget can never deadlock a
 * download, only reduce its parallelism.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ParallelDownloadPermits {

    private final AdjustableSemaphore semaphore;

    public ParallelDownloadPermits(int maxPermits) {
        if (maxPermits < 0) {
            throw new IllegalArgumentException("maxPermits must be >= 0, got " + maxPermits);
        }
        this.semaphore = new AdjustableSemaphore(maxPermits);
    }

    /**
     * Attempts to acquire one permit without blocking.
     * @return true if a permit was acquired, false otherwise
     */
    public boolean tryAcquire() {
        return semaphore.tryAcquire();
    }

    /**
     * Returns a permit previously obtained through {@link #tryAcquire()}.
     */
    public void release() {
        semaphore.release();
    }

    /**
     * Dynamically resizes the budget. Permits currently held are unaffected; a reduction
     * takes effect as held permits are released.
     */
    public void setMaxPermits(int maxPermits) {
        if (maxPermits < 0) {
            throw new IllegalArgumentException("maxPermits must be >= 0, got " + maxPermits);
        }
        semaphore.setMaxPermits(maxPermits);
    }

    public int getMaxPermits() {
        return semaphore.getMaxPermits();
    }

    // visible for testing
    int availablePermits() {
        return semaphore.availablePermits();
    }

    private static final class AdjustableSemaphore extends Semaphore {
        private final Object maxPermitsMutex = new Object();
        private int maxPermits;

        AdjustableSemaphore(int maxPermits) {
            super(maxPermits, false);
            this.maxPermits = maxPermits;
        }

        void setMaxPermits(int permits) {
            synchronized (maxPermitsMutex) {
                final int diff = Math.subtractExact(permits, maxPermits);
                if (diff > 0) {
                    release(diff);
                } else if (diff < 0) {
                    reducePermits(Math.negateExact(diff));
                }
                maxPermits = permits;
            }
        }

        int getMaxPermits() {
            synchronized (maxPermitsMutex) {
                return maxPermits;
            }
        }
    }
}
