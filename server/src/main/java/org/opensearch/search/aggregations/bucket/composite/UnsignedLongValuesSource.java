/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.Numbers;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitArray;
import org.opensearch.common.util.LongArray;
import org.opensearch.common.lease.Releasables;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Objects;

/**
 * A {@link SingleDimensionValuesSource} for unsigned longs.
 *
 * @opensearch.internal
 */
public class UnsignedLongValuesSource extends SingleDimensionValuesSource<BigInteger> {
    private final BigArrays bigArrays;
    private final CheckedFunction<LeafReaderContext, SortedNumericDocValues, IOException> docValuesFunc;

    private BitArray bits;
    private LongArray values;
    private long currentValue;
    private boolean missingCurrentValue;

    public UnsignedLongValuesSource(
        BigArrays bigArrays,
        MappedFieldType fieldType,
        CheckedFunction<LeafReaderContext, SortedNumericDocValues, IOException> docValuesFunc,
        DocValueFormat format,
        boolean missingBucket,
        MissingOrder missingOrder,
        int size,
        int reverseMul
    ) {
        super(bigArrays, format, fieldType, missingBucket, missingOrder, size, reverseMul);
        this.bigArrays = bigArrays;
        this.docValuesFunc = docValuesFunc;
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
        return compareValues(currentValue, afterValue.longValue());
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

    @Override
    protected void setAfter(Comparable value) {
        if (missingBucket && value == null) {
            afterValue = null;
        } else {
            // parse the value from a string in case it is a date or a formatted unsigned long.
            afterValue = format.parseUnsignedLong(value.toString(), false, () -> {
                throw new IllegalArgumentException("now() is not supported in [after] key");
            });
        }
    }

    @Override
    BigInteger toComparable(int slot) {
        if (missingBucket && bits.get(slot) == false) {
            return null;
        }
        return Numbers.toUnsignedBigInteger(values.get(slot));
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
        // We still accept long here (not BigInteger)
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

    @Override
    public void close() {
        Releasables.close(values, bits);
    }

    private int compareValues(long v1, long v2) {
        return Long.compareUnsigned(v1, v2) * reverseMul;
    }

    @Override
    SortedDocsProducer createSortedDocsProducerOrNull(IndexReader reader, Query query) {
        query = LongValuesSource.extractQuery(query);
        if (checkIfSortedDocsIsApplicable(reader, fieldType) == false
            || LongValuesSource.checkMatchAllOrRangeQuery(query, fieldType.name()) == false) {
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
            if (ft.typeName() == "unsigned_long") {
                return new UnsignedLongPointsSortedDocsProducer(fieldType.name(), lowerPoint, upperPoint);
            }
        }

        return null;
    }
}
