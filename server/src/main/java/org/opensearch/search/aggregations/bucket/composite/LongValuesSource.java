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

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitArray;
import org.opensearch.common.util.LongArray;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongUnaryOperator;
import java.util.function.ToLongFunction;

/**
 * A {@link SingleDimensionValuesSource} for longs.
 *
 * @opensearch.internal
 */
public class LongValuesSource extends SingleDimensionValuesSource<Long> {
    private final BigArrays bigArrays;
    private final CheckedFunction<LeafReaderContext, SortedNumericDocValues, IOException> docValuesFunc;
    private final LongUnaryOperator rounding;

    private BitArray bits;
    private LongArray values;
    private long currentValue;
    private boolean missingCurrentValue;

    public LongValuesSource(
        BigArrays bigArrays,
        MappedFieldType fieldType,
        CheckedFunction<LeafReaderContext, SortedNumericDocValues, IOException> docValuesFunc,
        LongUnaryOperator rounding,
        DocValueFormat format,
        boolean missingBucket,
        MissingOrder missingOrder,
        int size,
        int reverseMul
    ) {
        super(bigArrays, format, fieldType, missingBucket, missingOrder, size, reverseMul);
        this.bigArrays = bigArrays;
        this.docValuesFunc = docValuesFunc;
        this.rounding = rounding;
        this.bits = missingBucket ? new BitArray(Math.min(size, 100), bigArrays) : null;
        this.values = bigArrays.newLongArray(Math.min(size, 100), false);
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
            int result = missingOrder.compare(() -> missingCurrentValue, () -> Objects.isNull(afterValue), reverseMul);
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
            return Long.hashCode(values.get(slot));
        }
    }

    @Override
    int hashCodeCurrent() {
        if (missingCurrentValue) {
            return 0;
        } else {
            return Long.hashCode(currentValue);
        }
    }

    private int compareValues(long v1, long v2) {
        return Long.compare(v1, v2) * reverseMul;
    }

    @Override
    protected void setAfter(Comparable value) {
        if (missingBucket && value == null) {
            afterValue = null;
        } else {
            // parse the value from a string in case it is a date or a formatted unsigned long.
            afterValue = format.parseLong(value.toString(), false, () -> {
                throw new IllegalArgumentException("now() is not supported in [after] key");
            });
        }
    }

    @Override
    Long toComparable(int slot) {
        if (missingBucket && bits.get(slot) == false) {
            return null;
        }
        return values.get(slot);
    }

    @Override
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
        final SortedNumericDocValues dvs = docValuesFunc.apply(context);
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
        if (value.getClass() != Long.class) {
            throw new IllegalArgumentException("Expected Long, got " + value.getClass());
        }
        currentValue = (Long) value;
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                next.collect(doc, bucket);
            }
        };
    }

    static Query extractQuery(Query query) {
        if (query instanceof BoostQuery) {
            return extractQuery(((BoostQuery) query).getQuery());
        } else if (query instanceof IndexOrDocValuesQuery) {
            return extractQuery(((IndexOrDocValuesQuery) query).getIndexQuery());
        } else if (query instanceof ConstantScoreQuery) {
            return extractQuery(((ConstantScoreQuery) query).getQuery());
        } else {
            return query;
        }
    }

    /**
     * Returns true if we can use <code>query</code> with a {@link SortedDocsProducer} on <code>fieldName</code>.
     */
    static boolean checkMatchAllOrRangeQuery(Query query, String fieldName) {
        if (query == null) {
            return true;
        } else if (query.getClass() == MatchAllDocsQuery.class) {
            return true;
        } else if (query instanceof PointRangeQuery) {
            PointRangeQuery pointQuery = (PointRangeQuery) query;
            return fieldName.equals(pointQuery.getField());
        } else if (query instanceof FieldExistsQuery) {
            FieldExistsQuery existsQuery = (FieldExistsQuery) query;
            return fieldName.equals(existsQuery.getField());
        } else {
            return false;
        }
    }

    @Override
    SortedDocsProducer createSortedDocsProducerOrNull(IndexReader reader, Query query) {
        query = extractQuery(query);
        if (checkIfSortedDocsIsApplicable(reader, fieldType) == false || checkMatchAllOrRangeQuery(query, fieldType.name()) == false) {
            return null;
        }
        final byte[] lowerPoint;
        final byte[] upperPoint;
        if (query instanceof PointRangeQuery) {
            final PointRangeQuery rangeQuery = (PointRangeQuery) query;
            lowerPoint = rangeQuery.getLowerPoint();
            upperPoint = rangeQuery.getUpperPoint();
        } else {
            lowerPoint = null;
            upperPoint = null;
        }

        if (fieldType instanceof NumberFieldMapper.NumberFieldType) {
            NumberFieldMapper.NumberFieldType ft = (NumberFieldMapper.NumberFieldType) fieldType;
            final ToLongFunction<byte[]> toBucketFunction;

            switch (ft.typeName()) {
                case "long":
                    toBucketFunction = (value) -> rounding.applyAsLong(LongPoint.decodeDimension(value, 0));
                    break;

                case "int":
                case "short":
                case "byte":
                    toBucketFunction = (value) -> rounding.applyAsLong(IntPoint.decodeDimension(value, 0));
                    break;

                default:
                    return null;
            }
            return new PointsSortedDocsProducer(fieldType.name(), toBucketFunction, lowerPoint, upperPoint);
        } else if (fieldType instanceof DateFieldMapper.DateFieldType) {
            ToLongFunction<byte[]> decode = ((DateFieldMapper.DateFieldType) fieldType).resolution()::parsePointAsMillis;
            ToLongFunction<byte[]> toBucketFunction = value -> rounding.applyAsLong(decode.applyAsLong(value));
            return new PointsSortedDocsProducer(fieldType.name(), toBucketFunction, lowerPoint, upperPoint);
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        Releasables.close(values, bits);
    }
}
