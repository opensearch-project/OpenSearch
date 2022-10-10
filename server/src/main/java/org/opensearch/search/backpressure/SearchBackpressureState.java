/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.search.backpressure.stats.CancelledTaskStats;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tracks the current state of task completions and cancellations.
 *
 * @opensearch.internal
 */
public class SearchBackpressureState {
    /**
     * The number of successful task completions.
     */
    private final AtomicLong completionCount = new AtomicLong();

    /**
     * The number of task cancellations due to limit breaches.
     */
    private final AtomicLong cancellationCount = new AtomicLong();

    /**
     * The number of times task cancellation limit was reached.
     */
    private final AtomicLong limitReachedCount = new AtomicLong();

    /**
     * Usage stats for the last cancelled task.
     */
    private final AtomicReference<CancelledTaskStats> lastCancelledTaskStats = new AtomicReference<>();

    public long getCompletionCount() {
        return completionCount.get();
    }

    long incrementCompletionCount() {
        return completionCount.incrementAndGet();
    }

    public long getCancellationCount() {
        return cancellationCount.get();
    }

    long incrementCancellationCount() {
        return cancellationCount.incrementAndGet();
    }

    public long getLimitReachedCount() {
        return limitReachedCount.get();
    }

    long incrementLimitReachedCount() {
        return limitReachedCount.incrementAndGet();
    }

    public CancelledTaskStats getLastCancelledTaskStats() {
        return lastCancelledTaskStats.get();
    }

    public void setLastCancelledTaskStats(CancelledTaskStats stats) {
        lastCancelledTaskStats.set(stats);
    }
}
