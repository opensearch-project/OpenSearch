/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.plugin.stats.DataFormatShardStatsTracker;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Mutable, thread-safe shard-level statistics tracker for the Parquet data format plugin.
 * Uses LongAdder for high-throughput counters and AtomicLong for gauges.
 * Call {@link #stats()} to obtain an immutable {@link ParquetShardStats} snapshot.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ParquetShardStatsTracker implements DataFormatShardStatsTracker {

    // Indexing counters
    private final LongAdder docsIndexedTotal = new LongAdder();
    private final LongAdder docsIndexedFailures = new LongAdder();
    private final LongAdder indexTimeMillis = new LongAdder();

    // VSR Pipeline counters + gauge
    private final LongAdder vsrRotationsTotal = new LongAdder();
    private final LongAdder vsrRotationWaitMillis = new LongAdder();
    private final AtomicLong vsrRowsCurrent = new AtomicLong();

    // Native Write counters
    private final LongAdder nativeWriteTotal = new LongAdder();
    private final LongAdder nativeWriteTimeMillis = new LongAdder();
    private final LongAdder nativeWriteFailures = new LongAdder();
    private final LongAdder nativeFinalizeTotal = new LongAdder();
    private final LongAdder nativeFinalizeTimeMillis = new LongAdder();
    private final LongAdder nativeFinalizeFailures = new LongAdder();
    private final LongAdder nativeSyncTotal = new LongAdder();
    private final LongAdder nativeSyncTimeMillis = new LongAdder();
    private final LongAdder nativeSyncFailures = new LongAdder();

    // Sort counters
    private final LongAdder sortTotal = new LongAdder();
    private final LongAdder sortTimeMillis = new LongAdder();
    private final LongAdder sortInMemoryTotal = new LongAdder();
    private final LongAdder sortStreamingTotal = new LongAdder();

    // Merge counters
    private final LongAdder mergeTotal = new LongAdder();
    private final LongAdder mergeTimeMillis = new LongAdder();
    private final LongAdder mergeFailures = new LongAdder();
    private final LongAdder mergeInputFilesTotal = new LongAdder();
    private final LongAdder mergeOutputRowsTotal = new LongAdder();

    // Rate Limiting counters
    private final LongAdder rateLimitPauseTimeMillis = new LongAdder();
    private final LongAdder rateLimitBytesWritten = new LongAdder();

    // Memory gauges
    private final AtomicLong arrowAllocatedBytes = new AtomicLong();
    private final AtomicLong arrowMaxBytes = new AtomicLong();
    private final AtomicLong rustWriterMemoryBytes = new AtomicLong();

    // Background Write counters
    private final LongAdder backgroundWriteTotal = new LongAdder();
    private final LongAdder backgroundWriteWaitMillis = new LongAdder();
    private final LongAdder backgroundWriteTimeouts = new LongAdder();

    /**
     * Returns an immutable point-in-time snapshot of all tracked statistics.
     */
    public ParquetShardStats stats() {
        return new ParquetShardStats(
            docsIndexedTotal.sum(),
            docsIndexedFailures.sum(),
            indexTimeMillis.sum(),
            vsrRotationsTotal.sum(),
            vsrRotationWaitMillis.sum(),
            vsrRowsCurrent.get(),
            nativeWriteTotal.sum(),
            nativeWriteTimeMillis.sum(),
            nativeWriteFailures.sum(),
            nativeFinalizeTotal.sum(),
            nativeFinalizeTimeMillis.sum(),
            nativeFinalizeFailures.sum(),
            nativeSyncTotal.sum(),
            nativeSyncTimeMillis.sum(),
            nativeSyncFailures.sum(),
            sortTotal.sum(),
            sortTimeMillis.sum(),
            sortInMemoryTotal.sum(),
            sortStreamingTotal.sum(),
            mergeTotal.sum(),
            mergeTimeMillis.sum(),
            mergeFailures.sum(),
            mergeInputFilesTotal.sum(),
            mergeOutputRowsTotal.sum(),
            rateLimitPauseTimeMillis.sum(),
            rateLimitBytesWritten.sum(),
            arrowAllocatedBytes.get(),
            arrowMaxBytes.get(),
            rustWriterMemoryBytes.get(),
            backgroundWriteTotal.sum(),
            backgroundWriteWaitMillis.sum(),
            backgroundWriteTimeouts.sum()
        );
    }

    // --- Indexing methods ---

    @Override
    public void addDocsIndexed(long n) {
        docsIndexedTotal.add(n);
    }

    @Override
    public void incIndexFailures() {
        docsIndexedFailures.increment();
    }

    public void incDocsIndexedFailures() {
        docsIndexedFailures.increment();
    }

    @Override
    public void addIndexTime(long ms) {
        indexTimeMillis.add(ms);
    }

    public void addIndexTimeMillis(long ms) {
        indexTimeMillis.add(ms);
    }

    // --- VSR Pipeline methods ---

    public void incVsrRotations() {
        vsrRotationsTotal.increment();
    }

    public void addVsrRotationWaitMillis(long ms) {
        vsrRotationWaitMillis.add(ms);
    }

    public void setVsrRowsCurrent(long rows) {
        vsrRowsCurrent.set(rows);
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

    public void incNativeSyncTotal() {
        nativeSyncTotal.increment();
    }

    public void addNativeSyncTimeMillis(long ms) {
        nativeSyncTimeMillis.add(ms);
    }

    public void incNativeSyncFailures() {
        nativeSyncFailures.increment();
    }

    // --- Sort methods ---

    public void incSortTotal() {
        sortTotal.increment();
    }

    public void addSortTimeMillis(long ms) {
        sortTimeMillis.add(ms);
    }

    public void incSortInMemoryTotal() {
        sortInMemoryTotal.increment();
    }

    public void incSortStreamingTotal() {
        sortStreamingTotal.increment();
    }

    // --- Merge methods ---

    @Override
    public void incMergeTotal() {
        mergeTotal.increment();
    }

    @Override
    public void addMergeTime(long ms) {
        mergeTimeMillis.add(ms);
    }

    public void addMergeTimeMillis(long ms) {
        mergeTimeMillis.add(ms);
    }

    @Override
    public void incMergeFailures() {
        mergeFailures.increment();
    }

    public void addMergeInputFilesTotal(long n) {
        mergeInputFilesTotal.add(n);
    }

    public void addMergeOutputRowsTotal(long n) {
        mergeOutputRowsTotal.add(n);
    }

    // --- Rate Limiting methods ---

    public void addRateLimitPauseTimeMillis(long ms) {
        rateLimitPauseTimeMillis.add(ms);
    }

    public void addRateLimitBytesWritten(long bytes) {
        rateLimitBytesWritten.add(bytes);
    }

    // --- Memory gauge methods ---

    public void setArrowAllocatedBytes(long bytes) {
        arrowAllocatedBytes.set(bytes);
    }

    public void setArrowMaxBytes(long bytes) {
        arrowMaxBytes.set(bytes);
    }

    public void setRustWriterMemoryBytes(long bytes) {
        rustWriterMemoryBytes.set(bytes);
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
}
