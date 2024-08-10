/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.lucene.util.ArrayUtil;

/**
 * Packed value representation of aggregation buckets for the filter rewrite optimization.
 * It is convenient to represent our buckets in this way while traversing PointValues.PointTree.
 */
public final class PackedValueRanges {
    static ArrayUtil.ByteArrayComparator comparator;
    byte[][] lowers; // inclusive
    byte[][] uppers; // exclusive
    int size;
    int byteLen;

    public PackedValueRanges(byte[][] lowers, byte[][] uppers) {
        this.lowers = lowers;
        this.uppers = uppers;
        assert lowers.length == uppers.length;
        this.size = lowers.length;
        this.byteLen = lowers[0].length;
        comparator = ArrayUtil.getUnsignedComparator(byteLen);
    }

    public static int compareByteValue(byte[] value1, byte[] value2) {
        return comparator.compare(value1, 0, value2, 0);
    }

    public static boolean withinLowerBound(byte[] value, byte[] lowerBound) {
        return compareByteValue(value, lowerBound) >= 0;
    }

    public static boolean withinUpperBound(byte[] value, byte[] upperBound) {
        return compareByteValue(value, upperBound) < 0;
    }

    public byte[] getLower(int idx) {
        return lowers[idx];
    }

    public byte[] getUpper(int idx) {
        return uppers[idx];
    }

    public boolean withinLowerBound(byte[] value, int idx) {
        return PackedValueRanges.withinLowerBound(value, lowers[idx]);
    }

    public boolean withinUpperBound(byte[] value, int idx) {
        return PackedValueRanges.withinUpperBound(value, uppers[idx]);
    }

    public boolean withinRange(byte[] value, int idx) {
        return withinLowerBound(value, idx) && withinUpperBound(value, idx);
    }

    public int firstRangeIndex(byte[] globalMin, byte[] globalMax) {
        if (compareByteValue(lowers[0], globalMax) > 0) {
            return -1;
        }
        int i = 0;
        while (compareByteValue(uppers[i], globalMin) <= 0) {
            i++;
            if (i >= size) {
                return -1;
            }
        }
        return i;
    }
}
