/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Mutable tracker of a repeated operation.
 *
 * @opensearch.internal
 */
public class OperationMetrics {
    /**
     * The mean time it takes to complete the measured item.
     */
    private final MeanMetric time = new MeanMetric();
    /**
     * The current count of things being measured.
     * Useful when aggregating multiple metrics to see how many things are in flight.
     */
    private final AtomicLong current = new AtomicLong();
    /**
     * The non-decreasing count of failures
     */
    private final CounterMetric failed = new CounterMetric();

    /**
     * Invoked before the given operation begins.
     */
    public void before() {
        current.incrementAndGet();
    }

    /**
     * Invoked upon completion (success or failure) of the given operation
     * @param currentTime elapsed time of the operation
     */
    public void after(long currentTime) {
        current.decrementAndGet();
        time.inc(currentTime);
    }

    /**
     * Invoked upon failure of the operation.
     */
    public void failed() {
        failed.inc();
    }

    public void add(OperationMetrics other) {
        // Don't try copying over current, since in-flight requests will be linked to the existing metrics instance.
        failed.inc(other.failed.count());
        time.add(other.time);
    }

    /**
     * @return an immutable snapshot of the current metric values.
     */
    public OperationStats createStats() {
        return new OperationStats(time.count(), time.sum(), current.get(), failed.count());
    }
}
