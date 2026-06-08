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
 * Internal ranges representation for the filter rewrite optimization
 */
public final class Ranges {
    byte[][] lowers; // inclusive
    byte[][] uppers; // exclusive
    int size;
    int byteLen;
    ArrayUtil.ByteArrayComparator comparator;

    Ranges(byte[][] lowers, byte[][] uppers) {
        this.lowers = lowers;
        this.uppers = uppers;
        assert lowers.length == uppers.length;
        this.size = lowers.length;
        this.byteLen = lowers[0].length;
        comparator = ArrayUtil.getUnsignedComparator(byteLen);
    }

    public int getSize() {
        return size;
    }

    public byte[][] getLowers() {
        return lowers;
    }

    public byte[][] getUppers() {
        return uppers;
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

    public int compareByteValue(byte[] value1, byte[] value2) {
        return comparator.compare(value1, 0, value2, 0);
    }

    public boolean withinLowerBound(byte[] value, byte[] lowerBound) {
        return compareByteValue(value, lowerBound) >= 0;
    }

    public boolean withinUpperBound(byte[] value, byte[] upperBound) {
        return compareByteValue(value, upperBound) < 0;
    }
}
