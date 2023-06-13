/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.metrics.MeanMetric;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Mutable tracker of search pipeline processing operations.
 */
class SearchPipelineMetrics {
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

    public void before() {
        current.incrementAndGet();
    }

    public void after(long currentTime) {
        current.decrementAndGet();
        time.inc(currentTime);
    }

    public void failed() {
        failed.inc();
    }

    public void add(SearchPipelineMetrics other) {
        // Don't try copying over current, since in-flight requests will be linked to the existing metrics instance.
        failed.inc(other.failed.count());
        time.add(other.time);
    }

    SearchPipelineStats.Stats createStats() {
        return new SearchPipelineStats.Stats(time.count(), time.sum(), current.get(), failed.count());
    }
}
