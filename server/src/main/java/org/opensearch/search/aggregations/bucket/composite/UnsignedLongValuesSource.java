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
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;

import java.io.IOException;
import java.util.function.LongUnaryOperator;

/**
 * A {@link SingleDimensionValuesSource} for unsigned longs.
 *
 * @opensearch.internal
 */
public class UnsignedLongValuesSource extends LongValuesSource {
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
        super(bigArrays, fieldType, docValuesFunc, LongUnaryOperator.identity(), format, missingBucket, missingOrder, size, reverseMul);
    }

    @Override
    int compareValues(long v1, long v2) {
        return Long.compareUnsigned(v1, v2) * reverseMul;
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
            if (ft.typeName() == "unsigned_long") {
                return new UnsignedLongPointsSortedDocsProducer(fieldType.name(), lowerPoint, upperPoint);
            }
        }

        return null;
    }
}
