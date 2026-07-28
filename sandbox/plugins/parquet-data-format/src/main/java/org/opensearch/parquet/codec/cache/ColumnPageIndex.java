/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.cache;

/**
 * Layer 3 (jump table) + Layer 4 (page-stat skip) — an in-memory, binary-searchable view
 * of a column's Parquet OffsetIndex + ColumnIndex. Built once per {@code (segment, column)}
 * when the column reader is opened, from the parallel arrays returned by the native
 * {@code parquet_get_column_page_index} call.
 *
 * <p>Parallel arrays are indexed by page. {@code firstRowOf} is ascending with
 * {@code firstRowOf[0] == 0}; the number of rows in page {@code p} is
 * {@code firstRowOf[p+1] - firstRowOf[p]} (or {@code totalRows - firstRowOf[p]} for the
 * last page).
 *
 * <p>This is the task-3 (Layer 3/4) structure; the column-reader wrapper (task 2) builds
 * and owns it, so it is introduced here. Task 3 refines and adds dedicated unit tests.
 */
public final class ColumnPageIndex {

    private final long[] firstRowOf;
    private final long[] fileOffsetOf;
    private final int[] compressedSizeOf;
    private final long[] nullCountOf;
    private final long[] minOf;
    private final long[] maxOf;
    private final long totalRows;

    public ColumnPageIndex(
        long[] firstRowOf,
        long[] fileOffsetOf,
        int[] compressedSizeOf,
        long[] nullCountOf,
        long[] minOf,
        long[] maxOf,
        long totalRows
    ) {
        int n = firstRowOf.length;
        if (fileOffsetOf.length != n || compressedSizeOf.length != n || nullCountOf.length != n || minOf.length != n || maxOf.length != n) {
            throw new IllegalArgumentException("ColumnPageIndex parallel arrays must have equal length");
        }
        this.firstRowOf = firstRowOf;
        this.fileOffsetOf = fileOffsetOf;
        this.compressedSizeOf = compressedSizeOf;
        this.nullCountOf = nullCountOf;
        this.minOf = minOf;
        this.maxOf = maxOf;
        this.totalRows = totalRows;
    }

    /** Number of pages. */
    public int pageCount() {
        return firstRowOf.length;
    }

    /** Global index of the first row of page {@code p}. */
    public long firstRowOf(int p) {
        return firstRowOf[p];
    }

    /** Number of rows in page {@code p}. */
    public long numRowsOf(int p) {
        long next = (p + 1 < firstRowOf.length) ? firstRowOf[p + 1] : totalRows;
        return next - firstRowOf[p];
    }

    /** File byte offset of page {@code p} (0 when unknown). */
    public long fileOffsetOf(int p) {
        return fileOffsetOf[p];
    }

    /** Compressed size of page {@code p} in bytes (0 when unknown). */
    public int compressedSizeOf(int p) {
        return compressedSizeOf[p];
    }

    /** Null count of page {@code p}, or -1 when unknown. */
    public long nullCountOf(int p) {
        return nullCountOf[p];
    }

    /** Per-page min value raw bits (numeric columns only; 0 otherwise). */
    public long minOf(int p) {
        return minOf[p];
    }

    /** Per-page max value raw bits (numeric columns only; 0 otherwise). */
    public long maxOf(int p) {
        return maxOf[p];
    }

    /** Total number of rows across all pages. */
    public long totalRows() {
        return totalRows;
    }

    /**
     * Binary search: returns the page index containing global row {@code row}, or -1 when
     * {@code row} is out of range. O(log P).
     */
    public int pageForRow(long row) {
        if (row < 0 || row >= totalRows) {
            return -1;
        }
        int lo = 0;
        int hi = firstRowOf.length - 1;
        // Find the highest page whose firstRow <= row.
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (firstRowOf[mid] <= row) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return hi; // hi is the last page with firstRow <= row
    }

    /**
     * Layer 4 fast path: true when page {@code p} is known to be entirely nulls
     * ({@code nullCount == rowsInPage}). Returns false when the null count is unknown (-1).
     */
    public boolean isAllNulls(int p) {
        long nc = nullCountOf[p];
        return nc >= 0 && nc == numRowsOf(p);
    }
}
