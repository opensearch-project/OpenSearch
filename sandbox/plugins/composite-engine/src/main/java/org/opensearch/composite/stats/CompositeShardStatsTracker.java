/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.stats;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.concurrent.atomic.LongAdder;

/**
 * Mutable, thread-safe shard-level statistics tracker for the composite engine.
 * Uses {@link LongAdder} for high-throughput counters. Call {@link #stats()} for an
 * immutable {@link CompositeShardStats} snapshot.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeShardStatsTracker {

    // Refresh + merge-on-refresh breakdown.
    private final LongAdder refreshTotal = new LongAdder();
    private final LongAdder refreshTimeMillis = new LongAdder();
    private final LongAdder refreshMergeTotal = new LongAdder();
    private final LongAdder refreshMergeTimeMillis = new LongAdder();
    private final LongAdder refreshMergeFailures = new LongAdder();

    // Standalone merge.
    private final LongAdder mergeTotal = new LongAdder();
    private final LongAdder mergeTimeMillis = new LongAdder();
    private final LongAdder mergeFailures = new LongAdder();

    // Write attempts + failures by format role.
    private final LongAdder writeTotal = new LongAdder();
    private final LongAdder writePrimaryFailures = new LongAdder();
    private final LongAdder writeSecondaryFailures = new LongAdder();

    // Dynamic mapping updates actually applied.
    private final LongAdder mappingUpdateExecutedTotal = new LongAdder();

    /** Returns an immutable point-in-time snapshot of all tracked statistics. */
    public CompositeShardStats stats() {
        return new CompositeShardStats(
            refreshTotal.sum(),
            refreshTimeMillis.sum(),
            refreshMergeTotal.sum(),
            refreshMergeTimeMillis.sum(),
            refreshMergeFailures.sum(),
            mergeTotal.sum(),
            mergeTimeMillis.sum(),
            mergeFailures.sum(),
            writeTotal.sum(),
            writePrimaryFailures.sum(),
            writeSecondaryFailures.sum(),
            mappingUpdateExecutedTotal.sum()
        );
    }

    // --- Refresh ---

    public void incRefreshTotal() {
        refreshTotal.increment();
    }

    public void addRefreshTimeMillis(long ms) {
        refreshTimeMillis.add(ms);
    }

    public void incRefreshMergeTotal() {
        refreshMergeTotal.increment();
    }

    public void addRefreshMergeTimeMillis(long ms) {
        refreshMergeTimeMillis.add(ms);
    }

    public void incRefreshMergeFailures() {
        refreshMergeFailures.increment();
    }

    // --- Merge ---

    public void incMergeTotal() {
        mergeTotal.increment();
    }

    public void addMergeTimeMillis(long ms) {
        mergeTimeMillis.add(ms);
    }

    public void incMergeFailures() {
        mergeFailures.increment();
    }

    // --- Write ---

    public void incWriteTotal() {
        writeTotal.increment();
    }

    public void incWritePrimaryFailures() {
        writePrimaryFailures.increment();
    }

    public void incWriteSecondaryFailures() {
        writeSecondaryFailures.increment();
    }

    // --- Mapping ---

    public void incMappingUpdateExecutedTotal() {
        mappingUpdateExecutedTotal.increment();
    }
}
