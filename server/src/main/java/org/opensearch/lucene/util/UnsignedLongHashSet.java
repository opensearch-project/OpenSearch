/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lucene.util;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.opensearch.common.Numbers;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/** Set of unsigned-longs, optimized for docvalues usage */
public final class UnsignedLongHashSet implements Accountable {
    private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(UnsignedLongHashSet.class);

    private static final long MISSING = Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG;

    final long[] table;
    final int mask;
    final boolean hasMissingValue;
    final int size;
    /** minimum value in the set, or Numbers.MAX_UNSIGNED_LONG_VALUE_AS_LONG for an empty set */
    public final long minValue;
    /** maximum value in the set, or Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG for an empty set */
    public final long maxValue;

    /** Construct a set. Values must be in sorted order. */
    public UnsignedLongHashSet(long[] values) {
        int tableSize = Math.toIntExact(values.length * 3L / 2);
        tableSize = 1 << PackedInts.bitsRequired(tableSize); // make it a power of 2
        assert tableSize >= values.length * 3L / 2;
        table = new long[tableSize];
        Arrays.fill(table, MISSING);
        mask = tableSize - 1;
        boolean hasMissingValue = false;
        int size = 0;
        long previousValue = Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG; // for assert
        for (long value : values) {
            if (value == MISSING) {
                size += hasMissingValue ? 0 : 1;
                hasMissingValue = true;
            } else if (add(value)) {
                ++size;
            }
            assert Long.compareUnsigned(value, previousValue) >= 0 : " values must be provided in sorted order";
            previousValue = value;
        }
        this.hasMissingValue = hasMissingValue;
        this.size = size;
        this.minValue = values.length == 0 ? Numbers.MAX_UNSIGNED_LONG_VALUE_AS_LONG : values[0];
        this.maxValue = values.length == 0 ? Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG : values[values.length - 1];
    }

    private boolean add(long l) {
        assert l != MISSING;
        final int slot = Long.hashCode(l) & mask;
        for (int i = slot;; i = (i + 1) & mask) {
            if (table[i] == MISSING) {
                table[i] = l;
                return true;
            } else if (table[i] == l) {
                // already added
                return false;
            }
        }
    }

    /**
     * check for membership in the set.
     *
     * <p>You should use {@link #minValue} and {@link #maxValue} to guide/terminate iteration before
     * calling this.
     */
    public boolean contains(long l) {
        if (l == MISSING) {
            return hasMissingValue;
        }
        final int slot = Long.hashCode(l) & mask;
        for (int i = slot;; i = (i + 1) & mask) {
            if (table[i] == MISSING) {
                return false;
            } else if (table[i] == l) {
                return true;
            }
        }
    }

    /** returns a stream of all values contained in this set */
    public LongStream stream() {
        LongStream stream = Arrays.stream(table).filter(v -> v != MISSING);
        if (hasMissingValue) {
            stream = LongStream.concat(LongStream.of(MISSING), stream);
        }
        return stream;
    }

    @Override
    public int hashCode() {
        return Objects.hash(size, minValue, maxValue, mask, hasMissingValue, Arrays.hashCode(table));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof UnsignedLongHashSet) {
            UnsignedLongHashSet that = (UnsignedLongHashSet) obj;
            return size == that.size
                && minValue == that.minValue
                && maxValue == that.maxValue
                && mask == that.mask
                && hasMissingValue == that.hasMissingValue
                && Arrays.equals(table, that.table);
        }
        return false;
    }

    @Override
    public String toString() {
        return stream().mapToObj(Long::toUnsignedString).collect(Collectors.joining(", ", "[", "]"));
    }

    /** number of elements in the set */
    public int size() {
        return size;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES + RamUsageEstimator.sizeOfObject(table);
    }
}
