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
import org.apache.lucene.search.comparators.NumericComparator;
import org.opensearch.common.Numbers;

import java.io.IOException;
import java.math.BigInteger;

/** The comparator for unsigned long numeric type */
public class UnsignedLongComparator extends NumericComparator<BigInteger> {
    private final BigInteger[] values;
    protected BigInteger topValue;
    protected BigInteger bottom;

    public UnsignedLongComparator(int numHits, String field, BigInteger missingValue, boolean reverse, boolean enableSkipping) {
        super(field, missingValue != null ? missingValue : Numbers.MIN_UNSIGNED_LONG_VALUE, reverse, enableSkipping, BigIntegerPoint.BYTES);
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
        protected boolean isMissingValueCompetitive() {
            int result = missingValue.compareTo(bottom);
            // in reverse (desc) sort missingValue is competitive when it's greater or equal to bottom,
            // in asc sort missingValue is competitive when it's smaller or equal to bottom
            return reverse ? (result >= 0) : (result <= 0);
        }

        @Override
        protected void encodeBottom(byte[] packedValue) {
            BigIntegerPoint.encodeDimension(bottom, packedValue, 0);
        }

        @Override
        protected void encodeTop(byte[] packedValue) {
            BigIntegerPoint.encodeDimension(topValue, packedValue, 0);
        }
    }
}
