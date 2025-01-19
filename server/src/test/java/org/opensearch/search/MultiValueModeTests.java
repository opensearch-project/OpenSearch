/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.Numbers;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.fielddata.AbstractBinaryDocValues;
import org.opensearch.index.fielddata.AbstractNumericDocValues;
import org.opensearch.index.fielddata.AbstractSortedDocValues;
import org.opensearch.index.fielddata.AbstractSortedNumericDocValues;
import org.opensearch.index.fielddata.AbstractSortedSetDocValues;
import org.opensearch.index.fielddata.FieldData;
import org.opensearch.index.fielddata.NumericDoubleValues;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.index.fielddata.SortedNumericUnsignedLongValues;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class MultiValueModeTests extends OpenSearchTestCase {

    @FunctionalInterface
    private interface Supplier<T> {
        T get() throws IOException;
    }

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
                array[i] = randomLong();
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
                return docsWithValue == null ? true : docsWithValue.get(docId);
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
                values[j] = randomLong();
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
        for (MultiValueMode mode : MultiValueMode.values()) {
            SortedNumericDocValues values = supplier.get();
            final NumericDocValues selected = mode.select(values);
            for (int i = 0; i < maxDoc; ++i) {
                Long actual = null;
                if (selected.advanceExact(i)) {
                    actual = selected.longValue();
                    verifyLongValueCanCalledMoreThanOnce(selected, actual);
                }

                Long expected = null;
                if (values.advanceExact(i)) {
                    int numValues = values.docValueCount();
                    if (mode == MultiValueMode.MAX) {
                        expected = Long.MIN_VALUE;
                    } else if (mode == MultiValueMode.MIN) {
                        expected = Long.MAX_VALUE;
                    } else {
                        expected = 0L;
                    }
                    for (int j = 0; j < numValues; ++j) {
                        if (mode == MultiValueMode.SUM || mode == MultiValueMode.AVG) {
                            expected += values.nextValue();
                        } else if (mode == MultiValueMode.MIN) {
                            expected = Math.min(expected, values.nextValue());
                        } else if (mode == MultiValueMode.MAX) {
                            expected = Math.max(expected, values.nextValue());
                        }
                    }
                    if (mode == MultiValueMode.AVG) {
                        expected = numValues > 1 ? Math.round((double) expected / (double) numValues) : expected;
                    } else if (mode == MultiValueMode.MEDIAN) {
                        int value = numValues / 2;
                        if (numValues % 2 == 0) {
                            for (int j = 0; j < value - 1; ++j) {
                                values.nextValue();
                            }
                            expected = Math.round(((double) values.nextValue() + values.nextValue()) / 2.0);
                        } else {
                            for (int j = 0; j < value; ++j) {
                                values.nextValue();
                            }
                            expected = values.nextValue();
                        }
                    }
                }

                assertEquals(mode.toString() + " docId=" + i, expected, actual);
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
        for (long missingValue : new long[] { 0, randomLong() }) {
            for (MultiValueMode mode : new MultiValueMode[] {
                MultiValueMode.MIN,
                MultiValueMode.MAX,
                MultiValueMode.SUM,
                MultiValueMode.AVG }) {
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

                    long expected = 0;
                    if (mode == MultiValueMode.MAX) {
                        expected = Long.MIN_VALUE;
                    } else if (mode == MultiValueMode.MIN) {
                        expected = Long.MAX_VALUE;
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
                                if (mode == MultiValueMode.SUM || mode == MultiValueMode.AVG) {
                                    expected += values.nextValue();
                                } else if (mode == MultiValueMode.MIN) {
                                    expected = Math.min(expected, values.nextValue());
                                } else if (mode == MultiValueMode.MAX) {
                                    expected = Math.max(expected, values.nextValue());
                                }
                                ++numValues;
                            }
                        }
                    }
                    if (numValues == 0) {
                        expected = missingValue;
                    } else if (mode == MultiValueMode.AVG) {
                        expected = numValues > 1 ? Math.round((double) expected / (double) numValues) : expected;
                    }

                    assertEquals(mode.toString() + " docId=" + root, expected, actual);

                    prevRoot = root;
                }
            }
        }
    }

    public void testSingleValuedDoubles() throws Exception {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final double[] array = new double[numDocs];
        final FixedBitSet docsWithValue = randomBoolean() ? null : new FixedBitSet(numDocs);
        for (int i = 0; i < array.length; ++i) {
            if (randomBoolean()) {
                array[i] = randomDouble();
                if (docsWithValue != null) {
                    docsWithValue.set(i);
                }
            } else if (docsWithValue != null && randomBoolean()) {
                docsWithValue.set(i);
            }
        }
        final Supplier<SortedNumericDoubleValues> multiValues = () -> FieldData.singleton(new NumericDoubleValues() {
            int docID;

            @Override
            public boolean advanceExact(int doc) throws IOException {
                docID = doc;
                return docsWithValue == null || docsWithValue.get(doc);
            }

            @Override
            public double doubleValue() {
                return array[docID];
            }
        });
        verifySortedNumericDouble(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verifySortedNumericDouble(multiValues, numDocs, rootDocs, innerDocs, Integer.MAX_VALUE);
        verifySortedNumericDouble(multiValues, numDocs, rootDocs, innerDocs, randomIntBetween(1, numDocs));
    }

    public void testMultiValuedDoubles() throws Exception {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final double[][] array = new double[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            final double[] values = new double[randomInt(4)];
            for (int j = 0; j < values.length; ++j) {
                values[j] = randomDouble();
            }
            Arrays.sort(values);
            array[i] = values;
        }
        final Supplier<SortedNumericDoubleValues> multiValues = () -> new SortedNumericDoubleValues() {
            int doc;
            int i;

            @Override
            public double nextValue() {
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
        verifySortedNumericDouble(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verifySortedNumericDouble(multiValues, numDocs, rootDocs, innerDocs, Integer.MAX_VALUE);
        verifySortedNumericDouble(multiValues, numDocs, rootDocs, innerDocs, randomIntBetween(1, numDocs));
    }

    private void verifySortedNumericDouble(Supplier<SortedNumericDoubleValues> supplier, int maxDoc) throws IOException {
        for (MultiValueMode mode : MultiValueMode.values()) {
            SortedNumericDoubleValues values = supplier.get();
            final NumericDoubleValues selected = mode.select(values);
            for (int i = 0; i < maxDoc; ++i) {
                Double actual = null;
                if (selected.advanceExact(i)) {
                    actual = selected.doubleValue();
                    verifyDoubleValueCanCalledMoreThanOnce(selected, actual);
                }

                Double expected = null;
                if (values.advanceExact(i)) {
                    int numValues = values.docValueCount();
                    if (mode == MultiValueMode.MAX) {
                        expected = Double.NEGATIVE_INFINITY;
                    } else if (mode == MultiValueMode.MIN) {
                        expected = Double.POSITIVE_INFINITY;
                    } else {
                        expected = 0d;
                    }
                    for (int j = 0; j < numValues; ++j) {
                        if (mode == MultiValueMode.SUM || mode == MultiValueMode.AVG) {
                            expected += values.nextValue();
                        } else if (mode == MultiValueMode.MIN) {
                            expected = Math.min(expected, values.nextValue());
                        } else if (mode == MultiValueMode.MAX) {
                            expected = Math.max(expected, values.nextValue());
                        }
                    }
                    if (mode == MultiValueMode.AVG) {
                        expected = expected / numValues;
                    } else if (mode == MultiValueMode.MEDIAN) {
                        int value = numValues / 2;
                        if (numValues % 2 == 0) {
                            for (int j = 0; j < value - 1; ++j) {
                                values.nextValue();
                            }
                            expected = (values.nextValue() + values.nextValue()) / 2.0;
                        } else {
                            for (int j = 0; j < value; ++j) {
                                values.nextValue();
                            }
                            expected = values.nextValue();
                        }
                    }
                }

                assertEquals(mode.toString() + " docId=" + i, expected, actual);
            }
        }
    }

    private void verifyDoubleValueCanCalledMoreThanOnce(NumericDoubleValues values, double expected) throws IOException {
        for (int j = 0, numCall = randomIntBetween(1, 10); j < numCall; j++) {
            assertTrue(Double.compare(values.doubleValue(), expected) == 0);
        }
    }

    private void verifySortedNumericDouble(
        Supplier<SortedNumericDoubleValues> supplier,
        int maxDoc,
        FixedBitSet rootDocs,
        FixedBitSet innerDocs,
        int maxChildren
    ) throws IOException {
        for (long missingValue : new long[] { 0, randomLong() }) {
            for (MultiValueMode mode : new MultiValueMode[] {
                MultiValueMode.MIN,
                MultiValueMode.MAX,
                MultiValueMode.SUM,
                MultiValueMode.AVG }) {
                SortedNumericDoubleValues values = supplier.get();
                final NumericDoubleValues selected = mode.select(
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
                    final double actual = selected.doubleValue();
                    verifyDoubleValueCanCalledMoreThanOnce(selected, actual);

                    double expected = 0.0;
                    if (mode == MultiValueMode.MAX) {
                        expected = Long.MIN_VALUE;
                    } else if (mode == MultiValueMode.MIN) {
                        expected = Long.MAX_VALUE;
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
                                if (mode == MultiValueMode.SUM || mode == MultiValueMode.AVG) {
                                    expected += values.nextValue();
                                } else if (mode == MultiValueMode.MIN) {
                                    expected = Math.min(expected, values.nextValue());
                                } else if (mode == MultiValueMode.MAX) {
                                    expected = Math.max(expected, values.nextValue());
                                }
                                ++numValues;
                            }
                        }
                    }
                    if (numValues == 0) {
                        expected = missingValue;
                    } else if (mode == MultiValueMode.AVG) {
                        expected = expected / numValues;
                    }

                    assertEquals(mode.toString() + " docId=" + root, expected, actual, 0.1);

                    prevRoot = root;
                }
            }
        }
    }

    public void testSingleValuedStrings() throws Exception {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final BytesRef[] array = new BytesRef[numDocs];
        final FixedBitSet docsWithValue = randomBoolean() ? null : new FixedBitSet(numDocs);
        for (int i = 0; i < array.length; ++i) {
            if (randomBoolean()) {
                array[i] = new BytesRef(randomAlphaOfLengthBetween(8, 8));
                if (docsWithValue != null) {
                    docsWithValue.set(i);
                }
            } else {
                array[i] = new BytesRef();
                if (docsWithValue != null && randomBoolean()) {
                    docsWithValue.set(i);
                }
            }
        }
        final Supplier<SortedBinaryDocValues> multiValues = () -> FieldData.singleton(new AbstractBinaryDocValues() {
            int docID;

            @Override
            public boolean advanceExact(int target) throws IOException {
                docID = target;
                return docsWithValue == null || docsWithValue.get(docID);
            }

            @Override
            public BytesRef binaryValue() {
                return BytesRef.deepCopyOf(array[docID]);
            }
        });
        verifySortedBinary(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verifySortedBinary(multiValues, numDocs, rootDocs, innerDocs, Integer.MAX_VALUE);
        verifySortedBinary(multiValues, numDocs, rootDocs, innerDocs, randomIntBetween(1, numDocs));
    }

    public void testMultiValuedStrings() throws Exception {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final BytesRef[][] array = new BytesRef[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            final BytesRef[] values = new BytesRef[randomInt(4)];
            for (int j = 0; j < values.length; ++j) {
                values[j] = new BytesRef(randomAlphaOfLengthBetween(8, 8));
            }
            Arrays.sort(values);
            array[i] = values;
        }
        final Supplier<SortedBinaryDocValues> multiValues = () -> new SortedBinaryDocValues() {
            int doc;
            int i;

            @Override
            public BytesRef nextValue() {
                return BytesRef.deepCopyOf(array[doc][i++]);
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
        verifySortedBinary(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verifySortedBinary(multiValues, numDocs, rootDocs, innerDocs, Integer.MAX_VALUE);
        verifySortedBinary(multiValues, numDocs, rootDocs, innerDocs, randomIntBetween(1, numDocs));
    }

    private void verifySortedBinary(Supplier<SortedBinaryDocValues> supplier, int maxDoc) throws IOException {
        for (BytesRef missingValue : new BytesRef[] { new BytesRef(), new BytesRef(randomAlphaOfLengthBetween(8, 8)) }) {
            for (MultiValueMode mode : new MultiValueMode[] { MultiValueMode.MIN, MultiValueMode.MAX }) {
                SortedBinaryDocValues values = supplier.get();
                final BinaryDocValues selected = mode.select(values, missingValue);
                for (int i = 0; i < maxDoc; ++i) {
                    assertTrue(selected.advanceExact(i));
                    final BytesRef actual = selected.binaryValue();
                    verifyBinaryValueCanCalledMoreThanOnce(selected, actual);

                    BytesRef expected = null;
                    if (values.advanceExact(i) == false) {
                        expected = missingValue;
                    } else {
                        int numValues = values.docValueCount();
                        for (int j = 0; j < numValues; ++j) {
                            if (expected == null) {
                                expected = BytesRef.deepCopyOf(values.nextValue());
                            } else {
                                BytesRef value = values.nextValue();
                                if (mode == MultiValueMode.MIN) {
                                    expected = expected.compareTo(value) <= 0 ? expected : BytesRef.deepCopyOf(value);
                                } else if (mode == MultiValueMode.MAX) {
                                    expected = expected.compareTo(value) > 0 ? expected : BytesRef.deepCopyOf(value);
                                }
                            }
                        }
                        if (expected == null) {
                            expected = missingValue;
                        }
                    }

                    assertEquals(mode.toString() + " docId=" + i, expected, actual);
                }
            }
        }
    }

    private void verifyBinaryValueCanCalledMoreThanOnce(BinaryDocValues values, BytesRef expected) throws IOException {
        for (int j = 0, numCall = randomIntBetween(1, 10); j < numCall; j++) {
            assertEquals(values.binaryValue(), expected);
        }
    }

    private void verifySortedBinary(
        Supplier<SortedBinaryDocValues> supplier,
        int maxDoc,
        FixedBitSet rootDocs,
        FixedBitSet innerDocs,
        int maxChildren
    ) throws IOException {
        for (BytesRef missingValue : new BytesRef[] { new BytesRef(), new BytesRef(randomAlphaOfLengthBetween(8, 8)) }) {
            for (MultiValueMode mode : new MultiValueMode[] { MultiValueMode.MIN, MultiValueMode.MAX }) {
                SortedBinaryDocValues values = supplier.get();
                final BinaryDocValues selected = mode.select(
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
                    final BytesRef actual = selected.binaryValue();
                    verifyBinaryValueCanCalledMoreThanOnce(selected, actual);

                    BytesRef expected = null;
                    int count = 0;
                    for (int child = innerDocs.nextSetBit(prevRoot + 1); child != -1 && child < root; child = innerDocs.nextSetBit(
                        child + 1
                    )) {
                        if (values.advanceExact(child)) {
                            if (++count > maxChildren) {
                                break;
                            }
                            for (int j = 0; j < values.docValueCount(); ++j) {
                                if (expected == null) {
                                    expected = BytesRef.deepCopyOf(values.nextValue());
                                } else {
                                    BytesRef value = values.nextValue();
                                    if (mode == MultiValueMode.MIN) {
                                        expected = expected.compareTo(value) <= 0 ? expected : BytesRef.deepCopyOf(value);
                                    } else if (mode == MultiValueMode.MAX) {
                                        expected = expected.compareTo(value) > 0 ? expected : BytesRef.deepCopyOf(value);
                                    }
                                }
                            }
                        }
                    }
                    if (expected == null) {
                        expected = missingValue;
                    }

                    assertEquals(mode.toString() + " docId=" + root, expected, actual);

                    prevRoot = root;
                }
            }
        }
    }

    public void testSingleValuedOrds() throws Exception {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final int[] array = new int[numDocs];
        for (int i = 0; i < array.length; ++i) {
            if (randomBoolean()) {
                array[i] = randomInt(1000);
            } else {
                array[i] = -1;
            }
        }
        final Supplier<SortedSetDocValues> multiValues = () -> DocValues.singleton(new AbstractSortedDocValues() {
            private int docID = -1;

            @Override
            public boolean advanceExact(int target) throws IOException {
                docID = target;
                return array[docID] != -1;
            }

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int ordValue() {
                return array[docID];
            }

            @Override
            public BytesRef lookupOrd(int ord) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getValueCount() {
                return 1 << 20;
            }
        });
        verifySortedSet(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verifySortedSet(multiValues, numDocs, rootDocs, innerDocs, Integer.MAX_VALUE);
        verifySortedSet(multiValues, numDocs, rootDocs, innerDocs, randomIntBetween(1, numDocs));
    }

    public void testMultiValuedOrds() throws Exception {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final long[][] array = new long[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            final long[] values = new long[randomInt(4)];
            for (int j = 0; j < values.length; ++j) {
                values[j] = j == 0 ? randomInt(1000) : values[j - 1] + 1 + randomInt(1000);
            }
            array[i] = values;
        }
        final Supplier<SortedSetDocValues> multiValues = () -> new AbstractSortedSetDocValues() {
            int doc;
            int i;

            @Override
            public long nextOrd() {
                if (i < array[doc].length) {
                    return array[doc][i++];
                } else {
                    return NO_MORE_ORDS;
                }
            }

            @Override
            public boolean advanceExact(int docID) {
                this.doc = docID;
                i = 0;
                return array[doc].length > 0;
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getValueCount() {
                return 1 << 20;
            }

            @Override
            public int docValueCount() {
                return array[doc].length;
            }
        };
        verifySortedSet(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verifySortedSet(multiValues, numDocs, rootDocs, innerDocs, Integer.MAX_VALUE);
        verifySortedSet(multiValues, numDocs, rootDocs, innerDocs, randomIntBetween(1, numDocs));
    }

    public void testSingleValuedUnsignedLongs() throws Exception {
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

        final Supplier<SortedNumericUnsignedLongValues> multiValues = () -> new SortedNumericUnsignedLongValues() {
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
            public long nextValue() {
                return array[docId];
            }

            @Override
            public int docValueCount() {
                return 1;
            }
        };
        verifySortedUnsignedLong(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verifySortedUnsignedLong(multiValues, numDocs, rootDocs, innerDocs, Integer.MAX_VALUE);
        verifySortedUnsignedLong(multiValues, numDocs, rootDocs, innerDocs, randomIntBetween(1, numDocs));
    }

    public void testMultiValuedUnsignedLongs() throws Exception {
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
        final Supplier<SortedNumericUnsignedLongValues> multiValues = () -> new SortedNumericUnsignedLongValues() {
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

            @Override
            public int docID() {
                return doc;
            }
        };
        verifySortedUnsignedLong(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verifySortedUnsignedLong(multiValues, numDocs, rootDocs, innerDocs, Integer.MAX_VALUE);
        verifySortedUnsignedLong(multiValues, numDocs, rootDocs, innerDocs, randomIntBetween(1, numDocs));
    }

    private void verifySortedSet(Supplier<SortedSetDocValues> supplier, int maxDoc) throws IOException {
        for (MultiValueMode mode : new MultiValueMode[] { MultiValueMode.MIN, MultiValueMode.MAX }) {
            SortedSetDocValues values = supplier.get();
            final SortedDocValues selected = mode.select(values);
            for (int i = 0; i < maxDoc; ++i) {
                long actual = -1;
                if (selected.advanceExact(i)) {
                    actual = selected.ordValue();
                    verifyOrdValueCanCalledMoreThanOnce(selected, selected.ordValue());
                }
                int expected = -1;
                if (values.advanceExact(i)) {
                    for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
                        if (expected == -1) {
                            expected = (int) ord;
                        } else {
                            if (mode == MultiValueMode.MIN) {
                                expected = Math.min(expected, (int) ord);
                            } else if (mode == MultiValueMode.MAX) {
                                expected = Math.max(expected, (int) ord);
                            }
                        }
                    }
                }

                assertEquals(mode.toString() + " docId=" + i, expected, actual);
            }
        }
    }

    private void verifyOrdValueCanCalledMoreThanOnce(SortedDocValues values, long expected) throws IOException {
        for (int j = 0, numCall = randomIntBetween(1, 10); j < numCall; j++) {
            assertEquals(values.ordValue(), expected);
        }
    }

    private void verifySortedSet(
        Supplier<SortedSetDocValues> supplier,
        int maxDoc,
        FixedBitSet rootDocs,
        FixedBitSet innerDocs,
        int maxChildren
    ) throws IOException {
        for (MultiValueMode mode : new MultiValueMode[] { MultiValueMode.MIN, MultiValueMode.MAX }) {
            SortedSetDocValues values = supplier.get();
            final SortedDocValues selected = mode.select(values, rootDocs, new BitSetIterator(innerDocs, 0L), maxChildren);
            int prevRoot = -1;
            for (int root = rootDocs.nextSetBit(0); root != -1; root = root + 1 < maxDoc ? rootDocs.nextSetBit(root + 1) : -1) {
                int actual = -1;
                if (selected.advanceExact(root)) {
                    actual = selected.ordValue();
                    verifyOrdValueCanCalledMoreThanOnce(selected, actual);
                }
                int expected = -1;
                int count = 0;
                for (int child = innerDocs.nextSetBit(prevRoot + 1); child != -1 && child < root; child = innerDocs.nextSetBit(child + 1)) {
                    if (values.advanceExact(child)) {
                        if (++count > maxChildren) {
                            break;
                        }
                        for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
                            if (expected == -1) {
                                expected = (int) ord;
                            } else {
                                if (mode == MultiValueMode.MIN) {
                                    expected = Math.min(expected, (int) ord);
                                } else if (mode == MultiValueMode.MAX) {
                                    expected = Math.max(expected, (int) ord);
                                }
                            }
                        }
                    }
                }

                assertEquals(mode.toString() + " docId=" + root, expected, actual);

                prevRoot = root;
            }
        }
    }

    private void verifySortedUnsignedLong(Supplier<SortedNumericUnsignedLongValues> supplier, int maxDoc) throws IOException {
        for (MultiValueMode mode : MultiValueMode.values()) {
            SortedNumericUnsignedLongValues values = supplier.get();
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
                    if (mode == MultiValueMode.MAX) {
                        expected = Numbers.MIN_UNSIGNED_LONG_VALUE;
                    } else if (mode == MultiValueMode.MIN) {
                        expected = Numbers.MAX_UNSIGNED_LONG_VALUE;
                    } else {
                        expected = BigInteger.ZERO;
                    }
                    for (int j = 0; j < numValues; ++j) {
                        if (mode == MultiValueMode.SUM || mode == MultiValueMode.AVG) {
                            expected = expected.add(Numbers.toUnsignedBigInteger(values.nextValue()));
                        } else if (mode == MultiValueMode.MIN) {
                            expected = expected.min(Numbers.toUnsignedBigInteger(values.nextValue()));
                        } else if (mode == MultiValueMode.MAX) {
                            expected = expected.max(Numbers.toUnsignedBigInteger(values.nextValue()));
                        }
                    }
                    if (mode == MultiValueMode.AVG) {
                        expected = Numbers.toUnsignedBigInteger(expected.longValue());
                        expected = numValues > 1
                            ? new BigDecimal(expected).divide(new BigDecimal(numValues), RoundingMode.HALF_UP).toBigInteger()
                            : expected;
                    } else if (mode == MultiValueMode.MEDIAN) {
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

    private void verifySortedUnsignedLong(
        Supplier<SortedNumericUnsignedLongValues> supplier,
        int maxDoc,
        FixedBitSet rootDocs,
        FixedBitSet innerDocs,
        int maxChildren
    ) throws IOException {
        for (long missingValue : new long[] { 0, randomUnsignedLong().longValue() }) {
            for (MultiValueMode mode : new MultiValueMode[] {
                MultiValueMode.MIN,
                MultiValueMode.MAX,
                MultiValueMode.SUM,
                MultiValueMode.AVG }) {
                SortedNumericUnsignedLongValues values = supplier.get();
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
                    if (mode == MultiValueMode.MAX) {
                        expected = Numbers.MIN_UNSIGNED_LONG_VALUE;
                    } else if (mode == MultiValueMode.MIN) {
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
                                if (mode == MultiValueMode.SUM || mode == MultiValueMode.AVG) {
                                    expected = expected.add(Numbers.toUnsignedBigInteger(values.nextValue()));
                                } else if (mode == MultiValueMode.MIN) {
                                    expected = expected.min(Numbers.toUnsignedBigInteger(values.nextValue()));
                                } else if (mode == MultiValueMode.MAX) {
                                    expected = expected.max(Numbers.toUnsignedBigInteger(values.nextValue()));
                                }
                                ++numValues;
                            }
                        }
                    }
                    final long expectedLong;
                    if (numValues == 0) {
                        expectedLong = missingValue;
                    } else if (mode == MultiValueMode.AVG) {
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

    public void testValidOrdinals() {
        assertThat(MultiValueMode.SUM.ordinal(), equalTo(0));
        assertThat(MultiValueMode.AVG.ordinal(), equalTo(1));
        assertThat(MultiValueMode.MEDIAN.ordinal(), equalTo(2));
        assertThat(MultiValueMode.MIN.ordinal(), equalTo(3));
        assertThat(MultiValueMode.MAX.ordinal(), equalTo(4));
    }

    public void testWriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            MultiValueMode.SUM.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            MultiValueMode.AVG.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            MultiValueMode.MEDIAN.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(2));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            MultiValueMode.MIN.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(3));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            MultiValueMode.MAX.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(4));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(MultiValueMode.readMultiValueModeFrom(in), equalTo(MultiValueMode.SUM));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(MultiValueMode.readMultiValueModeFrom(in), equalTo(MultiValueMode.AVG));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(2);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(MultiValueMode.readMultiValueModeFrom(in), equalTo(MultiValueMode.MEDIAN));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(3);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(MultiValueMode.readMultiValueModeFrom(in), equalTo(MultiValueMode.MIN));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(4);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(MultiValueMode.readMultiValueModeFrom(in), equalTo(MultiValueMode.MAX));
            }
        }
    }

    public void testFromString() {
        assertThat(MultiValueMode.fromString("sum"), equalTo(MultiValueMode.SUM));
        assertThat(MultiValueMode.fromString("avg"), equalTo(MultiValueMode.AVG));
        assertThat(MultiValueMode.fromString("median"), equalTo(MultiValueMode.MEDIAN));
        assertThat(MultiValueMode.fromString("min"), equalTo(MultiValueMode.MIN));
        assertThat(MultiValueMode.fromString("max"), equalTo(MultiValueMode.MAX));
    }
}
