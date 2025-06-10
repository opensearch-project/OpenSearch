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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.StringFieldType;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongConsumer;

/**
 * A {@link SingleDimensionValuesSource} for binary source ({@link BytesRef}).
 *
 * @opensearch.internal
 */
class BinaryValuesSource extends SingleDimensionValuesSource<BytesRef> {
    private final LongConsumer breakerConsumer;
    private final CheckedFunction<LeafReaderContext, SortedBinaryDocValues, IOException> docValuesFunc;
    private ObjectArray<BytesRef> values;
    private ObjectArray<BytesRefBuilder> valueBuilders;
    private BytesRef currentValue;

    BinaryValuesSource(
        BigArrays bigArrays,
        LongConsumer breakerConsumer,
        MappedFieldType fieldType,
        CheckedFunction<LeafReaderContext, SortedBinaryDocValues, IOException> docValuesFunc,
        DocValueFormat format,
        boolean missingBucket,
        MissingOrder missingOrder,
        int size,
        int reverseMul
    ) {
        super(bigArrays, format, fieldType, missingBucket, missingOrder, size, reverseMul);
        this.breakerConsumer = breakerConsumer;
        this.docValuesFunc = docValuesFunc;
        this.values = bigArrays.newObjectArray(Math.min(size, 100));
        this.valueBuilders = bigArrays.newObjectArray(Math.min(size, 100));
    }

    @Override
    void copyCurrent(int slot) {
        values = bigArrays.grow(values, slot + 1);
        valueBuilders = bigArrays.grow(valueBuilders, slot + 1);
        BytesRefBuilder builder = valueBuilders.get(slot);
        int byteSize = builder == null ? 0 : builder.bytes().length;
        if (builder == null) {
            builder = new BytesRefBuilder();
            valueBuilders.set(slot, builder);
        }
        if (missingBucket && currentValue == null) {
            values.set(slot, null);
        } else {
            assert currentValue != null;
            builder.copyBytes(currentValue);
            breakerConsumer.accept(builder.bytes().length - byteSize);
            values.set(slot, builder.get());
        }
    }

    @Override
    int compare(int from, int to) {
        if (missingBucket) {
            int result = missingOrder.compare(() -> Objects.isNull(values.get(from)), () -> Objects.isNull(values.get(to)), reverseMul);
            if (MissingOrder.unknownOrder(result) == false) {
                return result;
            }
        }
        return compareValues(values.get(from), values.get(to));
    }

    @Override
    int compareCurrent(int slot) {
        if (missingBucket) {
            int result = missingOrder.compare(() -> Objects.isNull(currentValue), () -> Objects.isNull(values.get(slot)), reverseMul);
            if (MissingOrder.unknownOrder(result) == false) {
                return result;
            }
        }
        return compareValues(currentValue, values.get(slot));
    }

    @Override
    int compareCurrentWithAfter() {
        if (missingBucket) {
            int result = missingOrder.compare(() -> Objects.isNull(currentValue), () -> Objects.isNull(afterValue), reverseMul);
            if (MissingOrder.unknownOrder(result) == false) {
                return result;
            }
        }
        return compareValues(currentValue, afterValue);
    }

    @Override
    int hashCode(int slot) {
        if (missingBucket && values.get(slot) == null) {
            return 0;
        } else {
            return values.get(slot).hashCode();
        }
    }

    @Override
    int hashCodeCurrent() {
        if (missingBucket && currentValue == null) {
            return 0;
        } else {
            return currentValue.hashCode();
        }
    }

    int compareValues(BytesRef v1, BytesRef v2) {
        return v1.compareTo(v2) * reverseMul;
    }

    @Override
    void setAfter(Comparable value) {
        if (missingBucket && value == null) {
            afterValue = null;
        } else if (value.getClass() == String.class) {
            afterValue = format.parseBytesRef(value.toString());
        } else {
            throw new IllegalArgumentException("invalid value, expected string, got " + value.getClass().getSimpleName());
        }
    }

    @Override
    BytesRef toComparable(int slot) {
        return values.get(slot);
    }

    @Override
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
        final SortedBinaryDocValues dvs = docValuesFunc.apply(context);
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (dvs.advanceExact(doc)) {
                    int num = dvs.docValueCount();
                    for (int i = 0; i < num; i++) {
                        currentValue = dvs.nextValue();
                        next.collect(doc, bucket);
                    }
                } else if (missingBucket) {
                    currentValue = null;
                    next.collect(doc, bucket);
                }
            }
        };
    }

    @Override
    LeafBucketCollector getLeafCollector(Comparable value, LeafReaderContext context, LeafBucketCollector next) {
        if (value.getClass() != BytesRef.class) {
            throw new IllegalArgumentException("Expected BytesRef, got " + value.getClass());
        }
        currentValue = (BytesRef) value;
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                next.collect(doc, bucket);
            }
        };
    }

    @Override
    SortedDocsProducer createSortedDocsProducerOrNull(IndexReader reader, Query query) {
        if (checkIfSortedDocsIsApplicable(reader, fieldType) == false
            || (fieldType == null || fieldType.unwrap() instanceof StringFieldType == false)
            || (query != null && query.getClass() != MatchAllDocsQuery.class)) {
            return null;
        }
        return new TermsSortedDocsProducer(fieldType.name());
    }

    @Override
    public void close() {
        Releasables.close(values, valueBuilders);
    }
}
