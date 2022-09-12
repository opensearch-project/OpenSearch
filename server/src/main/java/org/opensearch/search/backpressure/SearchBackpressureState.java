/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import java.util.concurrent.atomic.AtomicLong;

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

    public long getCompletionCount() {
        return completionCount.get();
    }

    public long incrementCompletionCount() {
        return completionCount.incrementAndGet();
    }

    public long getCancellationCount() {
        return cancellationCount.get();
    }

    public long incrementCancellationCount() {
        return cancellationCount.incrementAndGet();
    }

    public long getLimitReachedCount() {
        return limitReachedCount.get();
    }

    public long incrementLimitReachedCount() {
        return limitReachedCount.incrementAndGet();
    }
}
