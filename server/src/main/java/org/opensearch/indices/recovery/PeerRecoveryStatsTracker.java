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
    private final AtomicLong total_started_recoveries;
    private final AtomicLong total_failed_recoveries;
    private final AtomicLong total_completed_recoveries;
    private final AtomicLong total_retried_recoveries;
    private final AtomicLong total_cancelled_recoveries;

    public PeerRecoveryStatsTracker() {
        total_started_recoveries = new AtomicLong();
        total_failed_recoveries = new AtomicLong();
        total_completed_recoveries = new AtomicLong();
        total_retried_recoveries = new AtomicLong();
        total_cancelled_recoveries = new AtomicLong();
    }

    public void incrementTotalStartedRelocation(long increment) {
        total_started_recoveries.addAndGet(increment);
    }

    public void incrementTotalFailedRelocation(long increment) {
        total_failed_recoveries.addAndGet(increment);
    }

    public void incrementTotalCompletedRelocation(long increment) {
        total_completed_recoveries.addAndGet(increment);
    }

    public void incrementTotalRetriedRelocation(long increment) {
        total_retried_recoveries.addAndGet(increment);
    }

    public void incrementTotalCancelledRelocation(long increment) {
        total_cancelled_recoveries.addAndGet(increment);
    }

    public long getTotalCancelledRecoveries() {
        return total_cancelled_recoveries.get();
    }

    public long getTotalStartedRecoveries() {
        return total_started_recoveries.get();
    }

    public long getTotalFailedRecoveries() {
        return total_failed_recoveries.get();
    }

    public long getTotalCompletedRecoveries() {
        return total_completed_recoveries.get();
    }

    public long getTotalRetriedRecoveries() {
        return total_retried_recoveries.get();
    }
}
