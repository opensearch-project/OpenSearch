/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.bridge;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * One decoded batch of a numeric Parquet column, read in place.
 *
 * <p>Holds the inclusive global row range {@code [firstRow, lastRow]} and off-heap views of the
 * decoded values and the packed presence bitset. Both views are borrowed Arrow buffers, read with
 * no on-heap copy; they are valid only until the next batch call on the owning cursor, which always
 * replaces this batch first. {@link #valueKind} selects the width and sign of the per-row read in
 * {@link #valueAt}.
 *
 * @param firstRow          inclusive global index of the first row in the batch
 * @param lastRow           inclusive global index of the last row in the batch
 * @param values            off-heap view of the decoded values, interpreted according to {@link #valueKind}
 * @param valueKind         element interpretation of {@code values}; one of the {@code KIND_*} constants
 * @param presenceBits      off-heap view of the packed presence bitset (bit {@code presenceBitOffset + i}
 *                          is set when row {@code firstRow + i} is non-null); {@code null} means every row is present
 * @param presenceBitOffset first presence bit of this batch within {@code presenceBits} (borrowed bitmaps are bit-sliced)
 */
public record DecodedBatch(long firstRow, long lastRow, MemorySegment values, int valueKind, MemorySegment presenceBits,
    int presenceBitOffset) {

    /** {@link #values} holds one {@code long} of raw bits per row (i64/u64 bits). */
    public static final int KIND_LONG = 1;
    /** {@link #values} holds one sign-extending {@code int} per row. */
    public static final int KIND_INT = 2;
    /** {@link #values} holds one zero-extending {@code int} per row (u32 bits). */
    public static final int KIND_UINT_BITS = 3;
    /** {@link #values} holds one sign-extending {@code short} per row. */
    public static final int KIND_SHORT = 4;
    /** {@link #values} holds one zero-extending {@code short} per row. */
    public static final int KIND_USHORT = 5;
    /** {@link #values} holds one sign-extending {@code byte} per row. */
    public static final int KIND_BYTE = 6;
    /** {@link #values} holds one zero-extending {@code byte} per row. */
    public static final int KIND_UBYTE = 7;
    /** {@link #values} holds one {@code long} of raw f64 bits per row; re-encoded to a Lucene sortable long. */
    public static final int KIND_DOUBLE = 8;
    /** {@link #values} holds one {@code int} of raw f32 bits per row; re-encoded to a sign-extended sortable int. */
    public static final int KIND_FLOAT = 9;

    /**
     * Constant-time presence test for a global row, which must fall within
     * {@code [firstRow, lastRow]}. Reads a single byte: the bitmap is only guaranteed to be
     * byte-addressable, so a wider read could reach past its last significant byte.
     */
    public boolean isPresent(long row) {
        if (contains(row) == false) {
            throw new IndexOutOfBoundsException("row " + row + " outside batch [" + firstRow + ", " + lastRow + "]");
        }
        if (presenceBits == null) {
            return true;
        }
        long idx = row - firstRow + presenceBitOffset;
        byte bits = presenceBits.get(ValueLayout.JAVA_BYTE, idx >>> 3);
        return (bits & (1 << (idx & 7))) != 0;
    }

    /**
     * Returns the value at the given global row as a Lucene numeric doc-values {@code long}. Integral
     * kinds sign- or zero-extend the stored width. The float/double kinds re-encode the raw IEEE-754
     * bits into Lucene's order-preserving "sortable" form ({@code doubleToSortableLong} /
     * {@code floatToSortableInt}, the latter sign-extended) - the encoding OpenSearch's float/double
     * fielddata reverses with {@code sortableLongToDouble} / {@code sortableIntToFloat}.
     */
    public long valueAt(long row) {
        long idx = row - firstRow;
        return switch (valueKind) {
            case KIND_LONG -> values.getAtIndex(ValueLayout.JAVA_LONG, idx);
            case KIND_INT -> values.getAtIndex(ValueLayout.JAVA_INT, idx);
            case KIND_UINT_BITS -> Integer.toUnsignedLong(values.getAtIndex(ValueLayout.JAVA_INT, idx));
            case KIND_SHORT -> values.getAtIndex(ValueLayout.JAVA_SHORT, idx);
            case KIND_USHORT -> Short.toUnsignedLong(values.getAtIndex(ValueLayout.JAVA_SHORT, idx));
            case KIND_BYTE -> values.get(ValueLayout.JAVA_BYTE, idx);
            case KIND_UBYTE -> Byte.toUnsignedLong(values.get(ValueLayout.JAVA_BYTE, idx));
            case KIND_DOUBLE -> {
                long bits = values.getAtIndex(ValueLayout.JAVA_LONG, idx);
                yield bits ^ ((bits >> 63) & 0x7fffffffffffffffL);
            }
            case KIND_FLOAT -> {
                int bits = values.getAtIndex(ValueLayout.JAVA_INT, idx);
                yield (long) (bits ^ ((bits >> 31) & 0x7fffffff));
            }
            default -> throw new IllegalStateException("unknown value kind " + valueKind);
        };
    }

    /** True when the given global row falls within this batch's range. */
    public boolean contains(long row) {
        return row >= firstRow && row <= lastRow;
    }
}
