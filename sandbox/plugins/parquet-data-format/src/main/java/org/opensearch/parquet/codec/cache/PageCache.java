/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.cache;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Layer 1 (page-resident value cache) + Layer 2 (page-resident presence bitset) for a
 * single decoded Parquet page of a single-valued column.
 *
 * <p>Holds the inclusive global row range {@code [firstRow, lastRow]} and, for primitive
 * columns, <b>off-heap views</b> of the decoded values (raw {@code long} bits, one slot per
 * row) and the packed presence bitset — {@link MemorySegment} slices of the column reader's
 * rotating {@link BufferPool} slots, served in place with zero on-heap copies. For
 * {@code BYTE_ARRAY} columns the value bytes are copied to a heap {@code byte[]} + CSR
 * offsets because Lucene's {@code BytesRef} contract requires a heap array; presence stays
 * off-heap.
 *
 * <p>Iterators serve cache hits with a presence bit-test plus an indexed segment read — no
 * FFM call, no heap allocation. A single instance per column is resident at a time (sliding
 * window, ascending doc IDs, no LRU). The backing segments belong to the producer's
 * {@link BufferPool} arena: they stay valid until the producer closes (grow events replace a
 * slot's backing segment but never free the old one), and must not be read after close.
 */
public final class PageCache {

    /** Inclusive global index of the first row in the cached page. */
    public long firstRow;
    /** Inclusive global index of the last row in the cached page. */
    public long lastRow;

    /**
     * Off-heap view of the decoded raw bits, one {@code long} slot per row (primitive
     * columns). Null for binary columns.
     */
    public MemorySegment values;

    /** Backing byte buffer for binary columns (concatenated value bytes). Null for primitives. */
    public byte[] byteBuf;
    /** CSR offsets into {@link #byteBuf}, length {@code rowsInPage + 1}. Null for primitives. */
    public int[] byteOffsets;

    /**
     * Off-heap view of the packed presence bitset: bit {@code i} set when row
     * {@code firstRow + i} is non-null. One {@code long} word per 64 rows.
     */
    public MemorySegment presenceBits;

    /** Number of rows in the cached page. */
    public int rowCount() {
        return (int) (lastRow - firstRow + 1);
    }

    /**
     * Constant-time presence test for a global row that lies within {@code [firstRow, lastRow]}.
     */
    public boolean isPresent(long row) {
        int idx = (int) (row - firstRow);
        long word = presenceBits.getAtIndex(ValueLayout.JAVA_LONG, idx >> 6);
        return (word & (1L << (idx & 63))) != 0L;
    }

    /** Returns the raw {@code long} bits for a primitive value at the given global row. */
    public long valueAt(long row) {
        return values.getAtIndex(ValueLayout.JAVA_LONG, row - firstRow);
    }

    /** True when the given global row falls within this cached page's range. */
    public boolean contains(long row) {
        return row >= firstRow && row <= lastRow;
    }
}
