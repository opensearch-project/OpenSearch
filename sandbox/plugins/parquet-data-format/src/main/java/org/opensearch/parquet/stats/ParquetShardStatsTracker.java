/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.stats;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * Mutable, thread-safe shard-level statistics tracker for the Parquet data format plugin.
 * Uses LongAdder for high-throughput counters.
 * Call {@link #stats()} to obtain an immutable {@link ParquetShardStats} snapshot.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ParquetShardStatsTracker {

    // Indexing counters
    private final LongAdder docsIndexedTotal = new LongAdder();
    private final LongAdder indexTimeMillis = new LongAdder();

    // VSR Pipeline counters
    private final LongAdder vsrRotationsTotal = new LongAdder();

    // Native Write counters
    private final LongAdder nativeWriteTotal = new LongAdder();
    private final LongAdder nativeWriteTimeMillis = new LongAdder();
    private final LongAdder nativeWriteFailures = new LongAdder();
    private final LongAdder nativeFinalizeTotal = new LongAdder();
    private final LongAdder nativeFinalizeTimeMillis = new LongAdder();
    private final LongAdder nativeFinalizeFailures = new LongAdder();

    // Merge counters
    private final LongAdder mergeTotal = new LongAdder();
    private final LongAdder mergeTimeMillis = new LongAdder();
    private final LongAdder mergeFailures = new LongAdder();
    private final LongAdder mergeInputFilesTotal = new LongAdder();
    private final LongAdder mergeOutputRowsTotal = new LongAdder();
    // Per-merge: cumulative count + millis for the flush+sort+chunk pass that runs inside each merge.
    private final LongAdder flushAndSortChunkTotal = new LongAdder();
    private final LongAdder flushAndSortChunkTimeMillis = new LongAdder();
    // Highest row_id assigned across all merges of this shard.
    private final LongAccumulator rowIdMappingMax = new LongAccumulator(Long::max, 0L);

    // Background Write counters
    private final LongAdder backgroundWriteTotal = new LongAdder();
    private final LongAdder backgroundWriteWaitMillis = new LongAdder();
    private final LongAdder backgroundWriteTimeouts = new LongAdder();
    private final LongAdder backgroundWriteFailures = new LongAdder();

    /**
     * Returns an immutable point-in-time snapshot of all tracked statistics.
     */
    public ParquetShardStats stats() {
        return new ParquetShardStats(
            docsIndexedTotal.sum(),
            indexTimeMillis.sum(),
            vsrRotationsTotal.sum(),
            nativeWriteTotal.sum(),
            nativeWriteTimeMillis.sum(),
            nativeWriteFailures.sum(),
            nativeFinalizeTotal.sum(),
            nativeFinalizeTimeMillis.sum(),
            nativeFinalizeFailures.sum(),
            mergeTotal.sum(),
            mergeTimeMillis.sum(),
            mergeFailures.sum(),
            mergeInputFilesTotal.sum(),
            mergeOutputRowsTotal.sum(),
            flushAndSortChunkTotal.sum(),
            flushAndSortChunkTimeMillis.sum(),
            rowIdMappingMax.get(),
            backgroundWriteTotal.sum(),
            backgroundWriteWaitMillis.sum(),
            backgroundWriteTimeouts.sum(),
            backgroundWriteFailures.sum()
        );
    }

    // --- Indexing methods ---

    public void addDocsIndexed(long n) {
        docsIndexedTotal.add(n);
    }

    public void addIndexTimeMillis(long ms) {
        indexTimeMillis.add(ms);
    }

    // --- VSR Pipeline methods ---

    public void incVsrRotations() {
        vsrRotationsTotal.increment();
    }

    // --- Native Write methods ---

    public void incNativeWriteTotal() {
        nativeWriteTotal.increment();
    }

    public void addNativeWriteTimeMillis(long ms) {
        nativeWriteTimeMillis.add(ms);
    }

    public void incNativeWriteFailures() {
        nativeWriteFailures.increment();
    }

    public void incNativeFinalizeTotal() {
        nativeFinalizeTotal.increment();
    }

    public void addNativeFinalizeTimeMillis(long ms) {
        nativeFinalizeTimeMillis.add(ms);
    }

    public void incNativeFinalizeFailures() {
        nativeFinalizeFailures.increment();
    }

    // --- Merge methods ---

    public void incMergeTotal() {
        mergeTotal.increment();
    }

    public void addMergeTimeMillis(long ms) {
        mergeTimeMillis.add(ms);
    }

    public void incMergeFailures() {
        mergeFailures.increment();
    }

    public void addMergeInputFilesTotal(long n) {
        mergeInputFilesTotal.add(n);
    }

    public void addMergeOutputRowsTotal(long n) {
        mergeOutputRowsTotal.add(n);
    }

    public void addFlushAndSortChunkTotal(long n) {
        flushAndSortChunkTotal.add(n);
    }

    public void addFlushAndSortChunkTimeMillis(long ms) {
        flushAndSortChunkTimeMillis.add(ms);
    }

    public void updateRowIdMappingMax(long value) {
        rowIdMappingMax.accumulate(value);
    }

    // --- Background Write methods ---

    public void incBackgroundWriteTotal() {
        backgroundWriteTotal.increment();
    }

    public void addBackgroundWriteWaitMillis(long ms) {
        backgroundWriteWaitMillis.add(ms);
    }

    public void incBackgroundWriteTimeouts() {
        backgroundWriteTimeouts.increment();
    }

    public void incBackgroundWriteFailures() {
        backgroundWriteFailures.increment();
    }
}
