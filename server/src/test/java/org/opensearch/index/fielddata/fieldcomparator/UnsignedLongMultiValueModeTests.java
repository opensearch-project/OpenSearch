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
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.Numbers;
import org.opensearch.index.fielddata.AbstractNumericDocValues;
import org.opensearch.index.fielddata.AbstractSortedNumericDocValues;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.function.Supplier;

public class UnsignedLongMultiValueModeTests extends OpenSearchTestCase {
    private static FixedBitSet randomRootDocs(int maxDoc) {
        FixedBitSet set = new FixedBitSet(maxDoc);
        for (int i = 0; i < maxDoc; ++i) {
            if (randomBoolean()) {
                set.set(i);
            }
        }
        // the last doc must be a root doc
        set.set(maxDoc - 1);
        return set;
    }

    private static FixedBitSet randomInnerDocs(FixedBitSet rootDocs) {
        FixedBitSet innerDocs = new FixedBitSet(rootDocs.length());
        for (int i = 0; i < innerDocs.length(); ++i) {
            if (!rootDocs.get(i) && randomBoolean()) {
                innerDocs.set(i);
            }
        }
        return innerDocs;
    }

    public void testSingleValuedLongs() throws Exception {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final long[] array = new long[numDocs];
        final FixedBitSet docsWithValue = randomBoolean() ? null : new FixedBitSet(numDocs);
        for (int i = 0; i < array.length; ++i) {
            if (randomBoolean()) {
                array[i] = randomUnsignedLong().longValue();
                if (docsWithValue != null) {
                    docsWithValue.set(i);
                }
            } else if (docsWithValue != null && randomBoolean()) {
                docsWithValue.set(i);
            }
        }

        final Supplier<SortedNumericDocValues> multiValues = () -> DocValues.singleton(new AbstractNumericDocValues() {
            int docId = -1;

            @Override
            public boolean advanceExact(int target) throws IOException {
                this.docId = target;
                return docsWithValue == null || docsWithValue.get(docId);
            }

            @Override
            public int docID() {
                return docId;
            }

            @Override
            public long longValue() {
                return array[docId];
            }
        });
        verifySortedNumeric(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verifySortedNumeric(multiValues, numDocs, rootDocs, innerDocs, Integer.MAX_VALUE);
        verifySortedNumeric(multiValues, numDocs, rootDocs, innerDocs, randomIntBetween(1, numDocs));
    }

    public void testMultiValuedLongs() throws Exception {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final long[][] array = new long[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            final long[] values = new long[randomInt(4)];
            for (int j = 0; j < values.length; ++j) {
                values[j] = randomUnsignedLong().longValue();
            }
            Arrays.sort(values);
            array[i] = values;
        }
        final Supplier<SortedNumericDocValues> multiValues = () -> new AbstractSortedNumericDocValues() {
            int doc;
            int i;

            @Override
            public long nextValue() {
                return array[doc][i++];
            }

            @Override
            public boolean advanceExact(int doc) {
                this.doc = doc;
                i = 0;
                return array[doc].length > 0;
            }

            @Override
            public int docValueCount() {
                return array[doc].length;
            }
        };
        verifySortedNumeric(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verifySortedNumeric(multiValues, numDocs, rootDocs, innerDocs, Integer.MAX_VALUE);
        verifySortedNumeric(multiValues, numDocs, rootDocs, innerDocs, randomIntBetween(1, numDocs));
    }

    private void verifySortedNumeric(Supplier<SortedNumericDocValues> supplier, int maxDoc) throws IOException {
        for (UnsignedLongMultiValueMode mode : UnsignedLongMultiValueMode.values()) {
            SortedNumericDocValues values = supplier.get();
            final NumericDocValues selected = mode.select(values);
            for (int i = 0; i < maxDoc; ++i) {
                Long actual = null;
                if (selected.advanceExact(i)) {
                    actual = selected.longValue();
                    verifyLongValueCanCalledMoreThanOnce(selected, actual);
                }

                BigInteger expected = null;
                if (values.advanceExact(i)) {
                    int numValues = values.docValueCount();
                    if (mode == UnsignedLongMultiValueMode.MAX) {
                        expected = Numbers.MIN_UNSIGNED_LONG_VALUE;
                    } else if (mode == UnsignedLongMultiValueMode.MIN) {
                        expected = Numbers.MAX_UNSIGNED_LONG_VALUE;
                    } else {
                        expected = BigInteger.ZERO;
                    }
                    for (int j = 0; j < numValues; ++j) {
                        if (mode == UnsignedLongMultiValueMode.SUM || mode == UnsignedLongMultiValueMode.AVG) {
                            expected = expected.add(Numbers.toUnsignedBigInteger(values.nextValue()));
                        } else if (mode == UnsignedLongMultiValueMode.MIN) {
                            expected = expected.min(Numbers.toUnsignedBigInteger(values.nextValue()));
                        } else if (mode == UnsignedLongMultiValueMode.MAX) {
                            expected = expected.max(Numbers.toUnsignedBigInteger(values.nextValue()));
                        }
                    }
                    if (mode == UnsignedLongMultiValueMode.AVG) {
                        expected = Numbers.toUnsignedBigInteger(expected.longValue());
                        expected = numValues > 1
                            ? new BigDecimal(expected).divide(new BigDecimal(numValues), RoundingMode.HALF_UP).toBigInteger()
                            : expected;
                    } else if (mode == UnsignedLongMultiValueMode.MEDIAN) {
                        final Long[] docValues = new Long[numValues];
                        for (int j = 0; j < numValues; ++j) {
                            docValues[j] = values.nextValue();
                        }
                        Arrays.sort(docValues, Long::compareUnsigned);
                        int value = numValues / 2;
                        if (numValues % 2 == 0) {
                            expected = Numbers.toUnsignedBigInteger(docValues[value - 1])
                                .add(Numbers.toUnsignedBigInteger(docValues[value]));
                            expected = Numbers.toUnsignedBigInteger(expected.longValue());
                            expected = new BigDecimal(expected).divide(new BigDecimal(2), RoundingMode.HALF_UP).toBigInteger();
                        } else {
                            expected = Numbers.toUnsignedBigInteger(docValues[value]);
                        }
                    }
                }

                final Long expectedLong = expected == null ? null : expected.longValue();
                assertEquals(mode.toString() + " docId=" + i, expectedLong, actual);
            }
        }
    }

    private void verifyLongValueCanCalledMoreThanOnce(NumericDocValues values, long expected) throws IOException {
        for (int j = 0, numCall = randomIntBetween(1, 10); j < numCall; j++) {
            assertEquals(expected, values.longValue());
        }
    }

    private void verifySortedNumeric(
        Supplier<SortedNumericDocValues> supplier,
        int maxDoc,
        FixedBitSet rootDocs,
        FixedBitSet innerDocs,
        int maxChildren
    ) throws IOException {
        for (long missingValue : new long[] { 0, randomUnsignedLong().longValue() }) {
            for (UnsignedLongMultiValueMode mode : new UnsignedLongMultiValueMode[] {
                UnsignedLongMultiValueMode.MIN,
                UnsignedLongMultiValueMode.MAX,
                UnsignedLongMultiValueMode.SUM,
                UnsignedLongMultiValueMode.AVG }) {
                SortedNumericDocValues values = supplier.get();
                final NumericDocValues selected = mode.select(
                    values,
                    missingValue,
                    rootDocs,
                    new BitSetIterator(innerDocs, 0L),
                    maxDoc,
                    maxChildren
                );
                int prevRoot = -1;
                for (int root = rootDocs.nextSetBit(0); root != -1; root = root + 1 < maxDoc ? rootDocs.nextSetBit(root + 1) : -1) {
                    assertTrue(selected.advanceExact(root));
                    final long actual = selected.longValue();
                    verifyLongValueCanCalledMoreThanOnce(selected, actual);

                    BigInteger expected = BigInteger.ZERO;
                    if (mode == UnsignedLongMultiValueMode.MAX) {
                        expected = Numbers.MIN_UNSIGNED_LONG_VALUE;
                    } else if (mode == UnsignedLongMultiValueMode.MIN) {
                        expected = Numbers.MAX_UNSIGNED_LONG_VALUE;
                    }
                    int numValues = 0;
                    int count = 0;
                    for (int child = innerDocs.nextSetBit(prevRoot + 1); child != -1 && child < root; child = innerDocs.nextSetBit(
                        child + 1
                    )) {
                        if (values.advanceExact(child)) {
                            if (++count > maxChildren) {
                                break;
                            }
                            for (int j = 0; j < values.docValueCount(); ++j) {
                                if (mode == UnsignedLongMultiValueMode.SUM || mode == UnsignedLongMultiValueMode.AVG) {
                                    expected = expected.add(Numbers.toUnsignedBigInteger(values.nextValue()));
                                } else if (mode == UnsignedLongMultiValueMode.MIN) {
                                    expected = expected.min(Numbers.toUnsignedBigInteger(values.nextValue()));
                                } else if (mode == UnsignedLongMultiValueMode.MAX) {
                                    expected = expected.max(Numbers.toUnsignedBigInteger(values.nextValue()));
                                }
                                ++numValues;
                            }
                        }
                    }
                    final long expectedLong;
                    if (numValues == 0) {
                        expectedLong = missingValue;
                    } else if (mode == UnsignedLongMultiValueMode.AVG) {
                        expected = Numbers.toUnsignedBigInteger(expected.longValue());
                        expected = numValues > 1
                            ? new BigDecimal(expected).divide(new BigDecimal(numValues), RoundingMode.HALF_UP).toBigInteger()
                            : expected;
                        expectedLong = expected.longValue();
                    } else {
                        expectedLong = expected.longValue();
                    }

                    assertEquals(mode.toString() + " docId=" + root, expectedLong, actual);

                    prevRoot = root;
                }
            }
        }
    }
}
