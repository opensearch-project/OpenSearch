/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.stats;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.concurrent.atomic.LongAdder;

/**
 * Mutable, thread-safe shard-level statistics tracker for the Lucene data format plugin.
 * Uses LongAdder for high-throughput counters.
 * Call {@link #stats()} to obtain an immutable {@link LuceneShardStats} snapshot.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneShardStatsTracker {

    // Indexing counters
    private final LongAdder docsIndexedTotal = new LongAdder();
    private final LongAdder docsIndexedFailures = new LongAdder();
    private final LongAdder indexTimeMillis = new LongAdder();

    // Flush counters
    private final LongAdder flushTotal = new LongAdder();
    private final LongAdder flushTimeMillis = new LongAdder();
    private final LongAdder flushForceMergeTimeMillis = new LongAdder();

    // Refresh counters
    private final LongAdder refreshTotal = new LongAdder();
    private final LongAdder refreshTimeMillis = new LongAdder();
    private final LongAdder refreshAddIndexesTimeMillis = new LongAdder();
    private final LongAdder refreshSegmentsIncorporatedTotal = new LongAdder();

    // Merge counters
    private final LongAdder mergeTotal = new LongAdder();
    private final LongAdder mergeTimeMillis = new LongAdder();
    private final LongAdder mergeFailures = new LongAdder();

    // Commit counters
    private final LongAdder commitTotal = new LongAdder();
    private final LongAdder commitTimeMillis = new LongAdder();
    private final LongAdder commitFailures = new LongAdder();
    private final LongAdder commitSyncTimeMillis = new LongAdder();

    // Delete counters
    private final LongAdder deleteTotal = new LongAdder();
    private final LongAdder deleteTimeMillis = new LongAdder();

    /**
     * Returns an immutable point-in-time snapshot of all tracked statistics.
     */
    public LuceneShardStats stats() {
        return new LuceneShardStats(
            docsIndexedTotal.sum(),
            docsIndexedFailures.sum(),
            indexTimeMillis.sum(),
            flushTotal.sum(),
            flushTimeMillis.sum(),
            flushForceMergeTimeMillis.sum(),
            refreshTotal.sum(),
            refreshTimeMillis.sum(),
            refreshAddIndexesTimeMillis.sum(),
            refreshSegmentsIncorporatedTotal.sum(),
            mergeTotal.sum(),
            mergeTimeMillis.sum(),
            mergeFailures.sum(),
            commitTotal.sum(),
            commitTimeMillis.sum(),
            commitFailures.sum(),
            commitSyncTimeMillis.sum(),
            deleteTotal.sum(),
            deleteTimeMillis.sum()
        );
    }

    // --- Indexing methods ---

    public void addDocsIndexed(long n) {
        docsIndexedTotal.add(n);
    }

    public void incDocsIndexedFailures() {
        docsIndexedFailures.increment();
    }

    public void addIndexTimeMillis(long ms) {
        indexTimeMillis.add(ms);
    }

    // --- Flush methods ---

    public void incFlushTotal() {
        flushTotal.increment();
    }

    public void addFlushTimeMillis(long ms) {
        flushTimeMillis.add(ms);
    }

    public void addFlushForceMergeTimeMillis(long ms) {
        flushForceMergeTimeMillis.add(ms);
    }

    // --- Refresh methods ---

    public void incRefreshTotal() {
        refreshTotal.increment();
    }

    public void addRefreshTimeMillis(long ms) {
        refreshTimeMillis.add(ms);
    }

    public void addRefreshAddIndexesTimeMillis(long ms) {
        refreshAddIndexesTimeMillis.add(ms);
    }

    public void incRefreshSegmentsIncorporatedTotal() {
        refreshSegmentsIncorporatedTotal.increment();
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

    // --- Commit methods ---

    public void incCommitTotal() {
        commitTotal.increment();
    }

    public void addCommitTimeMillis(long ms) {
        commitTimeMillis.add(ms);
    }

    public void incCommitFailures() {
        commitFailures.increment();
    }

    public void addCommitSyncTimeMillis(long ms) {
        commitSyncTimeMillis.add(ms);
    }

    // --- Delete methods ---

    public void incDeleteTotal() {
        deleteTotal.increment();
    }

    public void addDeleteTimeMillis(long ms) {
        deleteTimeMillis.add(ms);
    }
}
