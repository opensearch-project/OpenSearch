/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.docvaluescodec.bridge;

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

    /** {@link #values} holds one {@code long} of raw bits per row (i64/u64/f64 bits). */
    public static final int KIND_LONG = 1;
    /** {@link #values} holds one sign-extending {@code int} per row. */
    public static final int KIND_INT = 2;
    /** {@link #values} holds one zero-extending {@code int} per row (u32/f32 bits). */
    public static final int KIND_UINT_BITS = 3;
    /** {@link #values} holds one sign-extending {@code short} per row. */
    public static final int KIND_SHORT = 4;
    /** {@link #values} holds one zero-extending {@code short} per row. */
    public static final int KIND_USHORT = 5;
    /** {@link #values} holds one sign-extending {@code byte} per row. */
    public static final int KIND_BYTE = 6;
    /** {@link #values} holds one zero-extending {@code byte} per row. */
    public static final int KIND_UBYTE = 7;

    /** Constant-time presence test for a global row within {@code [firstRow, lastRow]}. */
    public boolean isPresent(long row) {
        if (presenceBits == null) {
            return true;
        }
        long idx = row - firstRow + presenceBitOffset;
        long word = presenceBits.getAtIndex(ValueLayout.JAVA_LONG, idx >>> 6);
        return (word & (1L << (idx & 63))) != 0L;
    }

    /** Returns the raw {@code long} bits for the value at the given global row. */
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
            default -> throw new IllegalStateException("unknown value kind " + valueKind);
        };
    }

    /** True when the given global row falls within this batch's range. */
    public boolean contains(long row) {
        return row >= firstRow && row <= lastRow;
    }
}
