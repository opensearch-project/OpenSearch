/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stats;

import org.opensearch.analytics.exec.profile.QueryProfile;
import org.opensearch.analytics.exec.profile.StageProfile;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Node-level aggregation of per-query analytics engine metrics.
 * Thread-safe — called from the search executor pool on every query completion.
 *
 * <p>Maintains rolling histograms (fixed-size circular buffers) for latency
 * percentiles and simple atomic counters for totals.
 *
 * @opensearch.internal
 */
public class QueryStatsService {

    private static final int HISTOGRAM_SIZE = 1024;

    // --- Counters ---
    private final AtomicLong totalQueries = new AtomicLong();
    private final AtomicLong failedQueries = new AtomicLong();
    private final AtomicLong cancelledQueries = new AtomicLong();

    // --- Stage counters by type ---
    private final AtomicLong shardFragmentStageCount = new AtomicLong();
    private final AtomicLong shardFragmentStageTotalMs = new AtomicLong();
    private final AtomicLong coordinatorReduceStageCount = new AtomicLong();
    private final AtomicLong coordinatorReduceStageTotalMs = new AtomicLong();

    // --- Rolling histograms for percentiles ---
    private final RollingHistogram planningTimeMs = new RollingHistogram(HISTOGRAM_SIZE);
    private final RollingHistogram executionTimeMs = new RollingHistogram(HISTOGRAM_SIZE);
    private final RollingHistogram coordinatorRows = new RollingHistogram(HISTOGRAM_SIZE);

    /**
     * Records a completed query's profile into the aggregate stats.
     *
     * @param profile the query profile snapshot (never null)
     * @param failed  true if the query failed
     */
    public void recordQuery(QueryProfile profile, boolean failed) {
        totalQueries.incrementAndGet();
        if (failed) {
            failedQueries.incrementAndGet();
        }

        planningTimeMs.record(profile.planningTimeMs());
        executionTimeMs.record(profile.executionTimeMs());

        long totalRows = 0;
        for (StageProfile stage : profile.stages()) {
            String type = stage.executionType();
            if ("SHARD_FRAGMENT".equals(type)) {
                shardFragmentStageCount.incrementAndGet();
                shardFragmentStageTotalMs.addAndGet(stage.elapsedMs());
            } else if ("COORDINATOR_REDUCE".equals(type)) {
                coordinatorReduceStageCount.incrementAndGet();
                coordinatorReduceStageTotalMs.addAndGet(stage.elapsedMs());
                totalRows += stage.rowsProcessed();
            }
        }
        coordinatorRows.record(totalRows);
    }

    /** Records a cancelled query (no profile available). */
    public void recordCancelled() {
        totalQueries.incrementAndGet();
        cancelledQueries.incrementAndGet();
    }

    /** Snapshots the current aggregate stats into an immutable POJO. */
    public AnalyticsQueryNodeStats snapshot() {
        return new AnalyticsQueryNodeStats(
            totalQueries.get(),
            failedQueries.get(),
            cancelledQueries.get(),
            planningTimeMs.percentile(50),
            planningTimeMs.percentile(95),
            planningTimeMs.percentile(99),
            executionTimeMs.percentile(50),
            executionTimeMs.percentile(95),
            executionTimeMs.percentile(99),
            coordinatorRows.percentile(50),
            coordinatorRows.percentile(95),
            shardFragmentStageCount.get(),
            shardFragmentStageTotalMs.get(),
            coordinatorReduceStageCount.get(),
            coordinatorReduceStageTotalMs.get()
        );
    }

    /**
     * Fixed-size circular buffer for approximate percentile computation.
     * Not perfectly accurate under high concurrency (racy index increment)
     * but good enough for operational metrics.
     */
    static final class RollingHistogram {
        private final AtomicLongArray values;
        private final AtomicLong writeIndex = new AtomicLong();
        private final int capacity;

        RollingHistogram(int capacity) {
            this.capacity = capacity;
            this.values = new AtomicLongArray(capacity);
        }

        void record(long value) {
            int idx = (int) (writeIndex.getAndIncrement() % capacity);
            values.set(idx, value);
        }

        long percentile(int p) {
            long count = writeIndex.get();
            int size = (int) Math.min(count, capacity);
            if (size == 0) return 0;

            long[] sorted = new long[size];
            for (int i = 0; i < size; i++) {
                sorted[i] = values.get(i);
            }
            Arrays.sort(sorted);
            int idx = Math.min((int) Math.ceil(p / 100.0 * size) - 1, size - 1);
            return sorted[Math.max(0, idx)];
        }
    }
}
