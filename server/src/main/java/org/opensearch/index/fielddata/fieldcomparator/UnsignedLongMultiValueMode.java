/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.opensearch.common.Numbers;
import org.opensearch.index.fielddata.AbstractNumericDocValues;
import org.opensearch.index.fielddata.FieldData;
import org.opensearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Locale;

/**
 * Defines what values to pick in the case a document contains multiple values for an unsigned long field.
 *
 * @opensearch.internal
 */
enum UnsignedLongMultiValueMode {
    /**
     * Pick the sum of all the values.
     */
    SUM {
        @Override
        protected long pick(SortedNumericDocValues values) throws IOException {
            final int count = values.docValueCount();
            long total = 0;
            for (int index = 0; index < count; ++index) {
                total += values.nextValue();
            }
            return total;
        }

        @Override
        protected long pick(
            SortedNumericDocValues values,
            long missingValue,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            int totalCount = 0;
            long totalValue = 0;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }

                    final int docCount = values.docValueCount();
                    for (int index = 0; index < docCount; ++index) {
                        totalValue += values.nextValue();
                    }
                    totalCount += docCount;
                }
            }
            return totalCount > 0 ? totalValue : missingValue;
        }
    },

    /**
     * Pick the average of all the values.
     */
    AVG {
        @Override
        protected long pick(SortedNumericDocValues values) throws IOException {
            final int count = values.docValueCount();
            long total = 0;
            for (int index = 0; index < count; ++index) {
                total += values.nextValue();
            }
            return count > 1 ? divideUnsignedAndRoundUp(total, count) : total;
        }

        @Override
        protected long pick(
            SortedNumericDocValues values,
            long missingValue,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            int totalCount = 0;
            long totalValue = 0;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final int docCount = values.docValueCount();
                    for (int index = 0; index < docCount; ++index) {
                        totalValue += values.nextValue();
                    }
                    totalCount += docCount;
                }
            }
            if (totalCount < 1) {
                return missingValue;
            }
            return totalCount > 1 ? divideUnsignedAndRoundUp(totalValue, totalCount) : totalValue;
        }
    },

    /**
     * Pick the median of the values.
     */
    MEDIAN {
        @Override
        protected long pick(SortedNumericDocValues values) throws IOException {
            int count = values.docValueCount();
            long firstValue = values.nextValue();
            if (count == 1) {
                return firstValue;
            } else if (count == 2) {
                long total = firstValue + values.nextValue();
                return (total >>> 1) + (total & 1);
            } else if (firstValue >= 0) {
                for (int i = 1; i < (count - 1) / 2; ++i) {
                    values.nextValue();
                }
                if (count % 2 == 0) {
                    long total = values.nextValue() + values.nextValue();
                    return (total >>> 1) + (total & 1);
                } else {
                    return values.nextValue();
                }
            }

            final long[] docValues = new long[count];
            docValues[0] = firstValue;
            int firstPositiveIndex = 0;
            for (int i = 1; i < count; ++i) {
                docValues[i] = values.nextValue();
                if (docValues[i] >= 0 && firstPositiveIndex == 0) {
                    firstPositiveIndex = i;
                }
            }
            final int mid = ((count - 1) / 2 + firstPositiveIndex) % count;
            if (count % 2 == 0) {
                long total = docValues[mid] + docValues[(mid + 1) % count];
                return (total >>> 1) + (total & 1);
            } else {
                return docValues[mid];
            }
        }
    },

    /**
     * Pick the lowest value.
     */
    MIN {
        @Override
        protected long pick(SortedNumericDocValues values) throws IOException {
            final int count = values.docValueCount();
            final long min = values.nextValue();
            if (count == 1 || min > 0) {
                return min;
            }
            for (int i = 1; i < count; ++i) {
                long val = values.nextValue();
                if (val >= 0) {
                    return val;
                }
            }
            return min;
        }

        @Override
        protected long pick(
            SortedNumericDocValues values,
            long missingValue,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            boolean hasValue = false;
            long minValue = Numbers.MAX_UNSIGNED_LONG_VALUE_AS_LONG;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final long docMin = pick(values);
                    minValue = Long.compareUnsigned(docMin, minValue) < 0 ? docMin : minValue;
                    hasValue = true;
                }
            }
            return hasValue ? minValue : missingValue;
        }
    },

    /**
     * Pick the highest value.
     */
    MAX {
        @Override
        protected long pick(SortedNumericDocValues values) throws IOException {
            final int count = values.docValueCount();
            long max = values.nextValue();
            long val;
            for (int i = 1; i < count; ++i) {
                val = values.nextValue();
                if (max < 0 && val >= 0) {
                    return max;
                }
                max = val;
            }
            return max;
        }

        @Override
        protected long pick(
            SortedNumericDocValues values,
            long missingValue,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            boolean hasValue = false;
            long maxValue = Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final long docMax = pick(values);
                    maxValue = Long.compareUnsigned(maxValue, docMax) < 0 ? docMax : maxValue;
                    hasValue = true;
                }
            }
            return hasValue ? maxValue : missingValue;
        }
    };

    /**
     * A case insensitive version of {@link #valueOf(String)}
     *
     * @throws IllegalArgumentException if the given string doesn't match a sort mode or is <code>null</code>.
     */
    private static UnsignedLongMultiValueMode fromString(String sortMode) {
        try {
            return valueOf(sortMode.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
            throw new IllegalArgumentException("Illegal sort mode: " + sortMode);
        }
    }

    /**
     * Convert a {@link MultiValueMode} to a {@link UnsignedLongMultiValueMode}.
     */
    public static UnsignedLongMultiValueMode toUnsignedSortMode(MultiValueMode sortMode) {
        return fromString(sortMode.name());
    }

    /**
     * Return a {@link NumericDocValues} instance that can be used to sort documents
     * with this mode and the provided values. When a document has no value,
     * <code>missingValue</code> is returned.
     * <p>
     * Allowed Modes: SUM, AVG, MEDIAN, MIN, MAX
     */
    public NumericDocValues select(final SortedNumericDocValues values) {
        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton != null) {
            return singleton;
        } else {
            return new AbstractNumericDocValues() {

                private long value;

                @Override
                public boolean advanceExact(int target) throws IOException {
                    if (values.advanceExact(target)) {
                        value = pick(values);
                        return true;
                    }
                    return false;
                }

                @Override
                public int docID() {
                    return values.docID();
                }

                @Override
                public long longValue() throws IOException {
                    return value;
                }
            };
        }
    }

    protected long pick(SortedNumericDocValues values) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link NumericDocValues} instance that can be used to sort root documents
     * with this mode, the provided values and filters for root/inner documents.
     * <p>
     * For every root document, the values of its inner documents will be aggregated.
     * If none of the inner documents has a value, then <code>missingValue</code> is returned.
     * <p>
     * Allowed Modes: SUM, AVG, MIN, MAX
     * <p>
     * NOTE: Calling the returned instance on docs that are not root docs is illegal
     *       The returned instance can only be evaluate the current and upcoming docs
     */
    public NumericDocValues select(
        final SortedNumericDocValues values,
        final long missingValue,
        final BitSet parentDocs,
        final DocIdSetIterator childDocs,
        int maxDoc,
        int maxChildren
    ) throws IOException {
        if (parentDocs == null || childDocs == null) {
            return FieldData.replaceMissing(DocValues.emptyNumeric(), missingValue);
        }

        return new AbstractNumericDocValues() {

            int lastSeenParentDoc = -1;
            long lastEmittedValue = missingValue;

            @Override
            public boolean advanceExact(int parentDoc) throws IOException {
                assert parentDoc >= lastSeenParentDoc : "can only evaluate current and upcoming parent docs";
                if (parentDoc == lastSeenParentDoc) {
                    return true;
                } else if (parentDoc == 0) {
                    lastEmittedValue = missingValue;
                    return true;
                }
                final int prevParentDoc = parentDocs.prevSetBit(parentDoc - 1);
                final int firstChildDoc;
                if (childDocs.docID() > prevParentDoc) {
                    firstChildDoc = childDocs.docID();
                } else {
                    firstChildDoc = childDocs.advance(prevParentDoc + 1);
                }

                lastSeenParentDoc = parentDoc;
                lastEmittedValue = pick(values, missingValue, childDocs, firstChildDoc, parentDoc, maxChildren);
                return true;
            }

            @Override
            public int docID() {
                return lastSeenParentDoc;
            }

            @Override
            public long longValue() {
                return lastEmittedValue;
            }
        };
    }

    protected long pick(
        SortedNumericDocValues values,
        long missingValue,
        DocIdSetIterator docItr,
        int startDoc,
        int endDoc,
        int maxChildren
    ) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Copied from {@link Long#divideUnsigned(long, long)} and {@link Long#remainderUnsigned(long, long)}
     */
    private static long divideUnsignedAndRoundUp(long dividend, long divisor) {
        assert divisor > 0;
        final long q = (dividend >>> 1) / divisor << 1;
        final long r = dividend - q * divisor;
        final long quotient = q + ((r | ~(r - divisor)) >>> (Long.SIZE - 1));
        final long rem = r - ((~(r - divisor) >> (Long.SIZE - 1)) & divisor);
        return quotient + Math.round((double) rem / divisor);
    }
}
