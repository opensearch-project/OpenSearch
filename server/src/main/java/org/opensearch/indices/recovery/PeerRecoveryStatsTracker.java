/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracker responsible for computing PeerRecoveryStats.
 *
 * @opensearch.internal
 */
public class PeerRecoveryStatsTracker {
    private final AtomicLong totalStartedRecoveries;
    private final AtomicLong totalFailedRecoveries;
    private final AtomicLong totalCompletedRecoveries;
    private final AtomicLong totalRetriedRecoveries;
    private final AtomicLong totalCancelledRecoveries;

    public PeerRecoveryStatsTracker() {
        totalStartedRecoveries = new AtomicLong();
        totalFailedRecoveries = new AtomicLong();
        totalCompletedRecoveries = new AtomicLong();
        totalRetriedRecoveries = new AtomicLong();
        totalCancelledRecoveries = new AtomicLong();
    }

    public void addTotalStartedRecoveries(long increment) {
        totalStartedRecoveries.addAndGet(increment);
    }

    public void addTotalFailedRecoveries(long increment) {
        totalFailedRecoveries.addAndGet(increment);
    }

    public void addTotalCompletedRecoveries(long increment) {
        totalCompletedRecoveries.addAndGet(increment);
    }

    public void addTotalRetriedRecoveries(long increment) {
        totalRetriedRecoveries.addAndGet(increment);
    }

    public void addTotalCancelledRecoveries(long increment) {
        totalCancelledRecoveries.addAndGet(increment);
    }

    public long getTotalCancelledRecoveries() {
        return totalCancelledRecoveries.get();
    }

    public long getTotalStartedRecoveries() {
        return totalStartedRecoveries.get();
    }

    public long getTotalFailedRecoveries() {
        return totalFailedRecoveries.get();
    }

    public long getTotalCompletedRecoveries() {
        return totalCompletedRecoveries.get();
    }

    public long getTotalRetriedRecoveries() {
        return totalRetriedRecoveries.get();
    }
}
