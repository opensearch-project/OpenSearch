/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import java.util.concurrent.Semaphore;

/**
 * AdjustableSemaphore is Extended Semphore where we can change maxPermits.
 */
public class AdjustableSemaphore extends Semaphore {
    private final Object maxPermitsMutex = new Object();
    private int maxPermits;

    public AdjustableSemaphore(int maxPermits, boolean fair) {
        super(maxPermits, fair);
        this.maxPermits = maxPermits;
    }

    /**
     * Update the maxPermits in semaphore
     */
    public void setMaxPermits(int permits) {
        synchronized (maxPermitsMutex) {
            final int diff = Math.subtractExact(permits, maxPermits);
            if (diff > 0) {
                // add permits
                release(diff);
            } else if (diff < 0) {
                // remove permits
                reducePermits(Math.negateExact(diff));
            }
            maxPermits = permits;
        }
    }

    /**
     * Returns maxPermits.
     */
    public int getMaxPermits() {
        return maxPermits;
    }
}
