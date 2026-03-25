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
 *
 * @opensearch.internal
 */
public abstract class Ranges {

    private final int size;
    private final ArrayUtil.ByteArrayComparator comparator;

    protected Ranges(int size, int byteLength) {
        this.size = size;
        this.comparator = ArrayUtil.getUnsignedComparator(byteLength);
    }

    public static Ranges forByteArrays(byte[][] lowers, byte[][] uppers) {
        return new ByteArrayRanges(lowers, uppers);
    }

    public int getSize() {
        return size;
    }

    final int compare(byte[] value1, byte[] value2) {
        return comparator.compare(value1, 0, value2, 0);
    }

    public abstract byte[] lower(int index);

    public abstract byte[] upper(int index);

    public final int firstRangeIndex(byte[] globalMin, byte[] globalMax) {
        if (getSize() == 0 || compare(lower(0), globalMax) > 0) {
            return -1;
        }
        int i = 0;
        while (compare(upper(i), globalMin) <= 0) {
            i++;
            if (i >= size) {
                return -1;
            }
        }
        return i;
    }

    public final boolean withinLowerBound(int index, byte[] value) {
        return compare(value, lower(index)) >= 0;
    }

    public final boolean withinUpperBound(int index, byte[] value) {
        return compare(value, upper(index)) < 0;
    }

    private static final class ByteArrayRanges extends Ranges {
        private final byte[][] lowers;
        private final byte[][] uppers;

        public ByteArrayRanges(byte[][] lowers, byte[][] uppers) {
            super(lowers.length, lowers.length == 0 ? 0 : lowers[0].length);
            this.lowers = lowers;
            this.uppers = uppers;
        }

        @Override
        public byte[] lower(int index) {
            return lowers[index];
        }

        @Override
        public byte[] upper(int index) {
            return uppers[index];
        }
    }
}
