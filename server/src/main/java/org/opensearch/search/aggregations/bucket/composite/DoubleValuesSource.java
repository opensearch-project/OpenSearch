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

package org.opensearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitArray;
import org.opensearch.common.util.DoubleArray;
import org.opensearch.common.lease.Releasables;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;

import java.io.IOException;

/**
 * A {@link SingleDimensionValuesSource} for doubles.
 *
 * @opensearch.internal
 */
class DoubleValuesSource extends SingleDimensionValuesSource<Double> {
    private final CheckedFunction<LeafReaderContext, SortedNumericDoubleValues, IOException> docValuesFunc;
    private final BitArray bits;
    private DoubleArray values;
    private double currentValue;
    private boolean missingCurrentValue;

    DoubleValuesSource(
        BigArrays bigArrays,
        MappedFieldType fieldType,
        CheckedFunction<LeafReaderContext, SortedNumericDoubleValues, IOException> docValuesFunc,
        DocValueFormat format,
        boolean missingBucket,
        MissingOrder missingOrder,
        int size,
        int reverseMul
    ) {
        super(bigArrays, format, fieldType, missingBucket, missingOrder, size, reverseMul);
        this.docValuesFunc = docValuesFunc;
        this.bits = missingBucket ? new BitArray(100, bigArrays) : null;
        this.values = bigArrays.newDoubleArray(Math.min(size, 100), false);
    }

    @Override
    void copyCurrent(int slot) {
        values = bigArrays.grow(values, slot + 1);
        if (missingBucket && missingCurrentValue) {
            bits.clear(slot);
        } else {
            assert missingCurrentValue == false;
            if (missingBucket) {
                bits.set(slot);
            }
            values.set(slot, currentValue);
        }
    }

    @Override
    int compare(int from, int to) {
        if (missingBucket) {
            int result = missingOrder.compare(() -> bits.get(from) == false, () -> bits.get(to) == false, reverseMul);
            if (MissingOrder.unknownOrder(result) == false) {
                return result;
            }
        }
        return compareValues(values.get(from), values.get(to));
    }

    @Override
    int compareCurrent(int slot) {
        if (missingBucket) {
            int result = missingOrder.compare(() -> missingCurrentValue, () -> bits.get(slot) == false, reverseMul);
            if (MissingOrder.unknownOrder(result) == false) {
                return result;
            }
        }
        return compareValues(currentValue, values.get(slot));
    }

    @Override
    int compareCurrentWithAfter() {
        if (missingBucket) {
            int result = missingOrder.compare(() -> missingCurrentValue, () -> afterValue == null, reverseMul);
            if (MissingOrder.unknownOrder(result) == false) {
                return result;
            }
        }
        return compareValues(currentValue, afterValue);
    }

    @Override
    int hashCode(int slot) {
        if (missingBucket && bits.get(slot) == false) {
            return 0;
        } else {
            return Double.hashCode(values.get(slot));
        }
    }

    @Override
    int hashCodeCurrent() {
        if (missingCurrentValue) {
            return 0;
        } else {
            return Double.hashCode(currentValue);
        }
    }

    private int compareValues(double v1, double v2) {
        return Double.compare(v1, v2) * reverseMul;
    }

    @Override
    void setAfter(Comparable value) {
        if (missingBucket && value == null) {
            afterValue = null;
        } else if (value instanceof Number) {
            afterValue = ((Number) value).doubleValue();
        } else {
            afterValue = format.parseDouble(value.toString(), false, () -> {
                throw new IllegalArgumentException("now() is not supported in [after] key");
            });
        }
    }

    @Override
    Double toComparable(int slot) {
        if (missingBucket && bits.get(slot) == false) {
            return null;
        }
        assert missingBucket == false || bits.get(slot);
        return values.get(slot);
    }

    @Override
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
        final SortedNumericDoubleValues dvs = docValuesFunc.apply(context);
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (dvs.advanceExact(doc)) {
                    int num = dvs.docValueCount();
                    for (int i = 0; i < num; i++) {
                        currentValue = dvs.nextValue();
                        missingCurrentValue = false;
                        next.collect(doc, bucket);
                    }
                } else if (missingBucket) {
                    missingCurrentValue = true;
                    next.collect(doc, bucket);
                }
            }
        };
    }

    @Override
    LeafBucketCollector getLeafCollector(Comparable value, LeafReaderContext context, LeafBucketCollector next) {
        if (value.getClass() != Double.class) {
            throw new IllegalArgumentException("Expected Double, got " + value.getClass());
        }
        currentValue = (Double) value;
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                next.collect(doc, bucket);
            }
        };
    }

    @Override
    SortedDocsProducer createSortedDocsProducerOrNull(IndexReader reader, Query query) {
        return null;
    }

    @Override
    public void close() {
        Releasables.close(values, bits);
    }
}
