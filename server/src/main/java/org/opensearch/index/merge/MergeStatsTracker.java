/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.merge;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.metrics.MeanMetric;

/**
 * Tracks live merge metrics (in-progress and completed) using thread-safe counters.
 * Use {@link #toMergeStats(double)} to produce a serializable {@link MergeStats} snapshot.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MergeStatsTracker {

    private final MeanMetric totalMerges = new MeanMetric();
    private final CounterMetric totalMergesNumDocs = new CounterMetric();
    private final CounterMetric totalMergesSizeInBytes = new CounterMetric();
    private final CounterMetric currentMerges = new CounterMetric();
    private final CounterMetric currentMergesNumDocs = new CounterMetric();
    private final CounterMetric currentMergesSizeInBytes = new CounterMetric();
    private final CounterMetric totalMergeStoppedTime = new CounterMetric();
    private final CounterMetric totalMergeThrottledTime = new CounterMetric();

    /**
     * Records the start of a merge operation, incrementing current merge counters.
     */
    public void beforeMerge(long numDocs, long sizeInBytes) {
        currentMerges.inc();
        currentMergesNumDocs.inc(numDocs);
        currentMergesSizeInBytes.inc(sizeInBytes);
    }

    /**
     * Records the completion of a merge operation, decrementing current and incrementing total counters.
     *
     * @param tookMS      time the merge took in milliseconds
     * @param numDocs     number of documents in the merge
     * @param sizeInBytes size of the merge in bytes
     */
    public void afterMerge(long tookMS, long numDocs, long sizeInBytes) {
        currentMerges.dec();
        currentMergesNumDocs.dec(numDocs);
        currentMergesSizeInBytes.dec(sizeInBytes);

        totalMergesNumDocs.inc(numDocs);
        totalMergesSizeInBytes.inc(sizeInBytes);
        totalMerges.inc(tookMS);
    }

    public void incStoppedTime(long timeMillis) {
        totalMergeStoppedTime.inc(timeMillis);
    }

    public void incThrottledTime(long timeMillis) {
        totalMergeThrottledTime.inc(timeMillis);
    }

    /**
     * Creates a snapshot of the current merge statistics.
     *
     * @param mbPerSecAutoThrottle the current auto-throttle rate in MB/sec,
     *                             or {@code Double.POSITIVE_INFINITY} if not throttled
     * @return a new {@link MergeStats} instance
     */
    public MergeStats toMergeStats(double mbPerSecAutoThrottle) {
        final MergeStats mergeStats = new MergeStats();
        mergeStats.add(
            totalMerges.count(),
            totalMerges.sum(),
            totalMergesNumDocs.count(),
            totalMergesSizeInBytes.count(),
            currentMerges.count(),
            currentMergesNumDocs.count(),
            currentMergesSizeInBytes.count(),
            totalMergeStoppedTime.count(),
            totalMergeThrottledTime.count(),
            mbPerSecAutoThrottle
        );
        return mergeStats;
    }
}
