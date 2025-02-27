/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.search.comparators;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.sandbox.document.BigIntegerPoint;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.comparators.NumericComparator;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.Numbers;

import java.io.IOException;
import java.math.BigInteger;

/** The comparator for unsigned long numeric type */
public class UnsignedLongComparator extends NumericComparator<BigInteger> {
    private final BigInteger[] values;
    protected BigInteger topValue;
    protected BigInteger bottom;

    public UnsignedLongComparator(int numHits, String field, BigInteger missingValue, boolean reverse, Pruning pruning) {
        super(field, missingValue != null ? missingValue : Numbers.MIN_UNSIGNED_LONG_VALUE, reverse, pruning, BigIntegerPoint.BYTES);
        values = new BigInteger[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
        return values[slot1].compareTo(values[slot2]);
    }

    @Override
    public void setTopValue(BigInteger value) {
        super.setTopValue(value);
        topValue = value;
    }

    @Override
    public BigInteger value(int slot) {
        return values[slot];
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
        return new UnsignedLongLeafComparator(context);
    }

    @Override
    protected long missingValueAsComparableLong() {
        // Apache Lucene will not use unsigned comparison for long values, so we have to convert to double
        // first (with possible lost of precision) and than to sortable long.
        return NumericUtils.doubleToSortableLong(missingValue.doubleValue());
    }

    @Override
    protected long sortableBytesToLong(byte[] bytes) {
        // Apache Lucene will not use unsigned comparison for long values, so we have to convert to double
        // first (with possible lost of precision) and than to sortable long.
        return NumericUtils.doubleToSortableLong(NumericUtils.sortableBytesToBigInt(bytes, 0, BigIntegerPoint.BYTES).doubleValue());
    }

    /** Leaf comparator for {@link UnsignedLongComparator} that provides skipping functionality */
    public class UnsignedLongLeafComparator extends NumericLeafComparator {

        public UnsignedLongLeafComparator(LeafReaderContext context) throws IOException {
            super(context);
        }

        private BigInteger getValueForDoc(int doc) throws IOException {
            if (docValues.advanceExact(doc)) {
                return Numbers.toUnsignedBigInteger(docValues.longValue());
            } else {
                return missingValue;
            }
        }

        @Override
        public void setBottom(int slot) throws IOException {
            bottom = values[slot];
            super.setBottom(slot);
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            return bottom.compareTo(getValueForDoc(doc));
        }

        @Override
        public int compareTop(int doc) throws IOException {
            return topValue.compareTo(getValueForDoc(doc));
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
            values[slot] = getValueForDoc(doc);
            super.copy(slot, doc);
        }

        @Override
        protected long bottomAsComparableLong() {
            // Apache Lucene will not use unsigned comparison for long values, so we have to convert to double
            // first (with possible lost of precision) and than to sortable long.
            return NumericUtils.doubleToSortableLong(bottom.doubleValue());
        }

        @Override
        protected long topAsComparableLong() {
            // Apache Lucene will not use unsigned comparison for long values, so we have to convert to double
            // first (with possible lost of precision) and than to sortable long.
            return NumericUtils.doubleToSortableLong(topValue.doubleValue());
        }
    }
}
