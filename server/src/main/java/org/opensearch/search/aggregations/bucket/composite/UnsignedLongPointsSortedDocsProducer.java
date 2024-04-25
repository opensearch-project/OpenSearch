/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.sandbox.document.BigIntegerPoint;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.DocIdSetBuilder;
import org.opensearch.common.Numbers;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;

/**
 * A {@link SortedDocsProducer} that can sort documents based on numerics indexed for unsigned long field.
 *
 * @opensearch.internal
 */
class UnsignedLongPointsSortedDocsProducer extends SortedDocsProducer {
    private final byte[] lowerPointQuery;
    private final byte[] upperPointQuery;

    UnsignedLongPointsSortedDocsProducer(String field, byte[] lowerPointQuery, byte[] upperPointQuery) {
        super(field);
        this.lowerPointQuery = lowerPointQuery;
        this.upperPointQuery = upperPointQuery;
    }

    @Override
    DocIdSet processLeaf(Query query, CompositeValuesCollectorQueue queue, LeafReaderContext context, boolean fillDocIdSet)
        throws IOException {
        final PointValues values = context.reader().getPointValues(field);
        if (values == null) {
            // no value for the field
            return DocIdSet.EMPTY;
        }
        BigInteger lowerBucket = Numbers.MIN_UNSIGNED_LONG_VALUE;
        Comparable<?> lowerValue = queue.getLowerValueLeadSource();
        if (lowerValue != null) {
            if (lowerValue.getClass() != BigInteger.class) {
                throw new IllegalStateException("expected BigInteger, got " + lowerValue.getClass());
            }
            lowerBucket = (BigInteger) lowerValue;
        }

        BigInteger upperBucket = Numbers.MAX_UNSIGNED_LONG_VALUE;
        Comparable<?> upperValue = queue.getUpperValueLeadSource();
        if (upperValue != null) {
            if (upperValue.getClass() != BigInteger.class) {
                throw new IllegalStateException("expected BigInteger, got " + upperValue.getClass());
            }
            upperBucket = (BigInteger) upperValue;
        }
        DocIdSetBuilder builder = fillDocIdSet ? new DocIdSetBuilder(context.reader().maxDoc(), values, field) : null;
        Visitor visitor = new Visitor(
            context,
            queue,
            builder,
            values.getBytesPerDimension(),
            lowerBucket.longValue(),
            upperBucket.longValue()
        );
        try {
            values.intersect(visitor);
            visitor.flush();
        } catch (CollectionTerminatedException exc) {}
        return fillDocIdSet ? builder.build() : DocIdSet.EMPTY;
    }

    private class Visitor implements PointValues.IntersectVisitor {
        final LeafReaderContext context;
        final CompositeValuesCollectorQueue queue;
        final DocIdSetBuilder builder;
        final int maxDoc;
        final int bytesPerDim;
        final long lowerBucket;
        final long upperBucket;

        DocIdSetBuilder bucketDocsBuilder;
        DocIdSetBuilder.BulkAdder adder;
        int remaining;
        long lastBucket;
        boolean first = true;

        Visitor(
            LeafReaderContext context,
            CompositeValuesCollectorQueue queue,
            DocIdSetBuilder builder,
            int bytesPerDim,
            long lowerBucket,
            long upperBucket
        ) {
            this.context = context;
            this.maxDoc = context.reader().maxDoc();
            this.queue = queue;
            this.builder = builder;
            this.lowerBucket = lowerBucket;
            this.upperBucket = upperBucket;
            this.bucketDocsBuilder = new DocIdSetBuilder(maxDoc);
            this.bytesPerDim = bytesPerDim;
        }

        @Override
        public void grow(int count) {
            remaining = count;
            adder = bucketDocsBuilder.grow(count);
        }

        @Override
        public void visit(int docID) throws IOException {
            throw new IllegalStateException("should never be called");
        }

        @Override
        public void visit(int docID, byte[] packedValue) throws IOException {
            if (compare(packedValue, packedValue) != PointValues.Relation.CELL_CROSSES_QUERY) {
                remaining--;
                return;
            }

            long bucket = BigIntegerPoint.decodeDimension(packedValue, 0).longValue();
            if (first == false && Long.compareUnsigned(bucket, lastBucket) != 0) {
                final DocIdSet docIdSet = bucketDocsBuilder.build();
                if (processBucket(queue, context, docIdSet.iterator(), lastBucket, builder) &&
                // lower bucket is inclusive
                    Long.compareUnsigned(lowerBucket, lastBucket) != 0) {
                    // this bucket does not have any competitive composite buckets,
                    // we can early terminate the collection because the remaining buckets are guaranteed
                    // to be greater than this bucket.
                    throw new CollectionTerminatedException();
                }
                bucketDocsBuilder = new DocIdSetBuilder(maxDoc);
                assert remaining > 0;
                adder = bucketDocsBuilder.grow(remaining);
            }
            lastBucket = bucket;
            first = false;
            adder.add(docID);
            remaining--;
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            if ((upperPointQuery != null && Arrays.compareUnsigned(minPackedValue, 0, bytesPerDim, upperPointQuery, 0, bytesPerDim) > 0)
                || (lowerPointQuery != null
                    && Arrays.compareUnsigned(maxPackedValue, 0, bytesPerDim, lowerPointQuery, 0, bytesPerDim) < 0)) {
                // does not match the query
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }

            // check the current bounds
            if (Long.compareUnsigned(lowerBucket, Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG) != 0) {
                long maxBucket = BigIntegerPoint.decodeDimension(maxPackedValue, 0).longValue();
                if (Long.compareUnsigned(maxBucket, lowerBucket) < 0) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
            }

            if (Long.compareUnsigned(upperBucket, Numbers.MAX_UNSIGNED_LONG_VALUE_AS_LONG) != 0) {
                long minBucket = BigIntegerPoint.decodeDimension(minPackedValue, 0).longValue();
                if (Long.compareUnsigned(minBucket, upperBucket) > 0) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
            }
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }

        public void flush() throws IOException {
            if (first == false) {
                final DocIdSet docIdSet = bucketDocsBuilder.build();
                processBucket(queue, context, docIdSet.iterator(), lastBucket, builder);
                bucketDocsBuilder = null;
            }
        }
    }
}
