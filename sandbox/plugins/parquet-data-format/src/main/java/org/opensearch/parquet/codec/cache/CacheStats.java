/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.cache;

/**
 * Per-column hit/miss counters for the Parquet DocValues cache layers, used to measure how much
 * each caching layer contributes during a query. One instance is owned by each
 * {@code ParquetColumnReader}; it is single-threaded (one segment per query thread), so counters
 * are plain {@code long}s with no synchronization.
 *
 * <p>Layer mapping (per the codec design):
 * <ul>
 *   <li><b>Layer 3</b> — OffsetIndex jump table ({@code pageForRow} binary search), consulted on every miss.</li>
 *   <li><b>Layer 4</b> — page-stat all-nulls skip; a miss resolved without decoding the page.</li>
 *   <li><b>FFM</b> — calls that cross the native boundary (page decodes and slow-path single/repeated
 *       reads). This is the cost the upper layers exist to avoid.</li>
 * </ul>
 */
public final class CacheStats {

    // Layer 3 — OffsetIndex jump-table lookups (one per miss).
    private long pageIndexLookups;

    // Layer 4 — all-nulls page skips (a miss resolved with no decode).
    private long allNullPageSkips;

    // FFM boundary crossings.
    private long pageDecodes;       // parquet_decode_page_at_row
    private long slowValueReads;    // parquet_read_value_at_row (single)
    private long slowRepeatedReads; // parquet_read_repeated_at_row

    // Phase timers (nanos), accumulated only when the timing logger is at TRACE.
    private long loadPageNanos;     // total time in loadPageContaining
    private long decodePageNanos;   // total time in decodePage
    private long ffmDecodeNanos;    // time spent in the parquet_decode_page_at_row FFM crossing(s)

    /** Records a Layer 3 jump-table lookup ({@code pageForRow}). */
    public void pageIndexLookup() {
        pageIndexLookups++;
    }

    /** Records a Layer 4 all-nulls page skip (miss resolved without a decode). */
    public void allNullPageSkip() {
        allNullPageSkips++;
    }

    /** Records an FFM page decode crossing. */
    public void pageDecode() {
        pageDecodes++;
    }

    /** Records an FFM slow-path single-value read crossing. */
    public void slowValueRead() {
        slowValueReads++;
    }

    /** Records an FFM slow-path repeated-value read crossing. */
    public void slowRepeatedRead() {
        slowRepeatedReads++;
    }

    public long pageDecodes() {
        return pageDecodes;
    }

    public long allNullPageSkips() {
        return allNullPageSkips;
    }

    public long pageIndexLookups() {
        return pageIndexLookups;
    }

    public long slowValueReads() {
        return slowValueReads;
    }

    public long slowRepeatedReads() {
        return slowRepeatedReads;
    }

    /** Total FFM boundary crossings across all access paths. */
    public long ffmCrossings() {
        return pageDecodes + slowValueReads + slowRepeatedReads;
    }

    /** Adds elapsed nanos spent in {@code loadPageContaining}. */
    public void addLoadPageNanos(long n) {
        loadPageNanos += n;
    }

    /** Adds elapsed nanos spent in {@code decodePage}. */
    public void addDecodePageNanos(long n) {
        decodePageNanos += n;
    }

    /** Adds elapsed nanos spent in the {@code parquet_decode_page_at_row} FFM crossing. */
    public void addFfmDecodeNanos(long n) {
        ffmDecodeNanos += n;
    }

    public long loadPageNanos() {
        return loadPageNanos;
    }

    public long decodePageNanos() {
        return decodePageNanos;
    }

    public long ffmDecodeNanos() {
        return ffmDecodeNanos;
    }

    /** True when no access has been recorded (used to suppress empty summaries). */
    public boolean isEmpty() {
        return pageDecodes == 0 && pageIndexLookups == 0 && allNullPageSkips == 0 && slowValueReads == 0 && slowRepeatedReads == 0;
    }
}
