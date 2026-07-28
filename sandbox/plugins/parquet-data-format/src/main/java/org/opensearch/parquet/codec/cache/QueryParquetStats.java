/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.cache;

import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Query-scoped accumulator that sums per-column {@link CacheStats} across every segment touched by
 * a single search, so the Parquet read-path cost can be summarized in one log line per query.
 *
 * <p>One instance is created per search (by {@code ParquetDocValuesDirectoryReader}) and shared by
 * all the per-segment {@code ParquetColumnReader}s that search opens. Each column reader
 * {@link #register(CacheStats) registers} its {@link CacheStats} when it is opened; the values are
 * summed <b>live</b> at {@link #summary()} time. Registering at open (rather than merging at close)
 * means the roll-up does not depend on reader-close ordering — by the time the per-query summary is
 * produced (end of search), every registered reader has finished collecting, so the live sums are
 * final. The registry is a {@link ConcurrentLinkedQueue} so concurrent segment slices can register
 * safely, and each reader mutates only its own {@link CacheStats} during collection.
 */
public final class QueryParquetStats {

    private final ConcurrentLinkedQueue<CacheStats> registered = new ConcurrentLinkedQueue<>();

    /** Registers a column reader's stats; its counters are summed live when {@link #summary()} runs. */
    public void register(CacheStats s) {
        if (s != null) {
            registered.add(s);
        }
    }

    /** True when nothing was recorded (used to suppress an empty summary). */
    public boolean isEmpty() {
        return registered.isEmpty();
    }

    /**
     * A single-line, human-readable per-query summary of the Parquet read-path counters (page-index
     * jump-table lookups, all-null page skips, page decodes, and slow single/repeated reads) summed
     * across every segment/column touched by the search.
     */
    public String summary() {
        long columns = 0;
        long jumpTableLookups = 0, allNullSkips = 0;
        long pageDecodes = 0, slowValueReads = 0, slowRepeatedReads = 0;
        for (CacheStats s : registered) {
            columns++;
            jumpTableLookups += s.pageIndexLookups();
            allNullSkips += s.allNullPageSkips();
            pageDecodes += s.pageDecodes();
            slowValueReads += s.slowValueReads();
            slowRepeatedReads += s.slowRepeatedReads();
        }
        return String.format(
            Locale.ROOT,
            "segments/columns=%d | L3 jumpTableLookups=%d | L4 allNullSkips=%d | "
                + "FFM: pageDecodes=%d slowValueReads=%d slowRepeatedReads=%d",
            columns,
            jumpTableLookups,
            allNullSkips,
            pageDecodes,
            slowValueReads,
            slowRepeatedReads
        );
    }
}
