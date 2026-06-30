/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.sandbox.document.BigIntegerPoint;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.IntsRef;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumericPointEncoder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

/**
 * An approximate-able version of {@link PointRangeQuery}. It creates an instance of {@link PointRangeQuery} but short-circuits the intersect logic
 * after {@code size} is hit
 */
public class ApproximatePointRangeQuery extends ApproximateQuery {
    public static final Function<byte[], String> LONG_FORMAT = bytes -> Long.toString(LongPoint.decodeDimension(bytes, 0));
    public static final Function<byte[], String> INT_FORMAT = bytes -> Integer.toString(IntPoint.decodeDimension(bytes, 0));
    public static final Function<byte[], String> HALF_FLOAT_FORMAT = bytes -> Float.toString(HalfFloatPoint.decodeDimension(bytes, 0));
    public static final Function<byte[], String> FLOAT_FORMAT = bytes -> Float.toString(FloatPoint.decodeDimension(bytes, 0));
    public static final Function<byte[], String> DOUBLE_FORMAT = bytes -> Double.toString(DoublePoint.decodeDimension(bytes, 0));
    public static final Function<byte[], String> UNSIGNED_LONG_FORMAT = bytes -> BigIntegerPoint.decodeDimension(bytes, 0).toString();

    private int size;
    private SortOrder sortOrder;
    public PointRangeQuery pointRangeQuery;
    private final Function<byte[], String> valueToString;

    public ApproximatePointRangeQuery(
        String field,
        byte[] lowerPoint,
        byte[] upperPoint,
        int numDims,
        Function<byte[], String> valueToString
    ) {
        this(field, lowerPoint, upperPoint, numDims, SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO, null, valueToString);
    }

    protected ApproximatePointRangeQuery(
        String field,
        byte[] lowerPoint,
        byte[] upperPoint,
        int numDims,
        int size,
        SortOrder sortOrder,
        Function<byte[], String> valueToString
    ) {
        this.size = size;
        this.sortOrder = sortOrder;
        this.valueToString = valueToString;
        this.pointRangeQuery = new PointRangeQuery(field, lowerPoint, upperPoint, numDims) {
            @Override
            protected String toString(int dimension, byte[] value) {
                return valueToString.apply(value);
            }
        };
    }

    public int getSize() {
        return this.size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public SortOrder getSortOrder() {
        return this.sortOrder;
    }

    public void setSortOrder(SortOrder sortOrder) {
        this.sortOrder = sortOrder;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        return super.rewrite(indexSearcher);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        pointRangeQuery.visit(visitor);
    }

    @Override
    public final ConstantScoreWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        final ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(pointRangeQuery.getBytesPerDim());

        Weight pointRangeQueryWeight = pointRangeQuery.createWeight(searcher, scoreMode, boost);

        return new ConstantScoreWeight(this, boost) {

            // we pull this from PointRangeQuery since it is final
            private boolean matches(byte[] packedValue) {
                for (int dim = 0; dim < pointRangeQuery.getNumDims(); dim++) {
                    int offset = dim * pointRangeQuery.getBytesPerDim();
                    if (comparator.compare(packedValue, offset, pointRangeQuery.getLowerPoint(), offset) < 0) {
                        // Doc's value is too low, in this dimension
                        return false;
                    }
                    if (comparator.compare(packedValue, offset, pointRangeQuery.getUpperPoint(), offset) > 0) {
                        // Doc's value is too high, in this dimension
                        return false;
                    }
                }
                return true;
            }

            // we pull this from PointRangeQuery since it is final
            private PointValues.Relation relate(byte[] minPackedValue, byte[] maxPackedValue) {
                boolean crosses = false;

                for (int dim = 0; dim < pointRangeQuery.getNumDims(); dim++) {
                    int offset = dim * pointRangeQuery.getBytesPerDim();

                    if (comparator.compare(minPackedValue, offset, pointRangeQuery.getUpperPoint(), offset) > 0
                        || comparator.compare(maxPackedValue, offset, pointRangeQuery.getLowerPoint(), offset) < 0) {
                        return PointValues.Relation.CELL_OUTSIDE_QUERY;
                    }

                    crosses |= comparator.compare(minPackedValue, offset, pointRangeQuery.getLowerPoint(), offset) < 0
                        || comparator.compare(maxPackedValue, offset, pointRangeQuery.getUpperPoint(), offset) > 0;
                }

                if (crosses) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                } else {
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                }
            }

            public PointValues.IntersectVisitor getIntersectVisitor(DocIdSetBuilder result, long[] docCount) {
                return new PointValues.IntersectVisitor() {

                    DocIdSetBuilder.BulkAdder adder;

                    @Override
                    public void grow(int count) {
                        adder = result.grow(count);
                    }

                    @Override
                    public void visit(int docID) {
                        // it is possible that size < 1024 and docCount < size but we will continue to count through all the 1024 docs
                        adder.add(docID);
                        docCount[0]++;
                    }

                    @Override
                    public void visit(DocIdSetIterator iterator) throws IOException {
                        adder.add(iterator);
                    }

                    @Override
                    public void visit(IntsRef ref) {
                        adder.add(ref);
                        docCount[0] += ref.length;
                    }

                    @Override
                    public void visit(int docID, byte[] packedValue) {
                        if (matches(packedValue)) {
                            visit(docID);
                        }
                    }

                    @Override
                    public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                        if (matches(packedValue)) {
                            adder.add(iterator);
                        }
                    }

                    @Override
                    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                        return relate(minPackedValue, maxPackedValue);
                    }
                };
            }

            // we pull this from PointRangeQuery since it is final
            private boolean checkValidPointValues(PointValues values) throws IOException {
                if (values == null) {
                    // No docs in this segment/field indexed any points
                    return false;
                }

                if (values.getNumIndexDimensions() != pointRangeQuery.getNumDims()) {
                    throw new IllegalArgumentException(
                        "field=\""
                            + pointRangeQuery.getField()
                            + "\" was indexed with numIndexDimensions="
                            + values.getNumIndexDimensions()
                            + " but this query has numDims="
                            + pointRangeQuery.getNumDims()
                    );
                }
                if (pointRangeQuery.getBytesPerDim() != values.getBytesPerDimension()) {
                    throw new IllegalArgumentException(
                        "field=\""
                            + pointRangeQuery.getField()
                            + "\" was indexed with bytesPerDim="
                            + values.getBytesPerDimension()
                            + " but this query has bytesPerDim="
                            + pointRangeQuery.getBytesPerDim()
                    );
                }
                return true;
            }

            private void intersectLeft(PointValues.PointTree pointTree, PointValues.IntersectVisitor visitor, long[] docCount)
                throws IOException {
                intersectLeft(visitor, pointTree, docCount);
                assert pointTree.moveToParent() == false;
            }

            private void intersectRight(PointValues.PointTree pointTree, PointValues.IntersectVisitor visitor, long[] docCount)
                throws IOException {
                intersectRight(visitor, pointTree, docCount);
                assert pointTree.moveToParent() == false;
            }

            // custom intersect visitor to walk the left of the tree
            public void intersectLeft(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree, long[] docCount)
                throws IOException {
                if (docCount[0] >= size) {
                    return;
                }
                PointValues.Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
                if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
                    return;
                }
                // Handle leaf nodes
                if (pointTree.moveToChild() == false) {
                    if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
                        pointTree.visitDocIDs(visitor);
                    } else {
                        // CELL_CROSSES_QUERY
                        pointTree.visitDocValues(visitor);
                    }
                    return;
                }
                // For CELL_INSIDE_QUERY, check if we can skip right child
                if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
                    long leftSize = pointTree.size();
                    long needed = size - docCount[0];

                    if (leftSize >= needed) {
                        // Process only left child
                        intersectLeft(visitor, pointTree, docCount);
                        pointTree.moveToParent();
                        return;
                    }
                }
                // We need both children - now clone right
                PointValues.PointTree rightChild = null;
                if (pointTree.moveToSibling()) {
                    rightChild = pointTree.clone();
                    pointTree.moveToParent();
                    pointTree.moveToChild();
                }
                // Process both children: left first, then right if needed
                intersectLeft(visitor, pointTree, docCount);
                if (docCount[0] < size && rightChild != null) {
                    intersectLeft(visitor, rightChild, docCount);
                }
                pointTree.moveToParent();
            }

            // custom intersect visitor to walk the right of tree (from rightmost leaf going left)
            public void intersectRight(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree, long[] docCount)
                throws IOException {
                if (docCount[0] >= size) {
                    return;
                }
                PointValues.Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
                if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
                    return;
                }
                // Handle leaf nodes
                if (pointTree.moveToChild() == false) {
                    if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
                        pointTree.visitDocIDs(visitor);
                    } else {
                        // CELL_CROSSES_QUERY
                        pointTree.visitDocValues(visitor);
                    }
                    return;
                }
                // Internal node - get left child reference (we're at left child initially)
                PointValues.PointTree leftChild = pointTree.clone();
                // Move to right child if it exists
                boolean hasRightChild = pointTree.moveToSibling();
                // For CELL_INSIDE_QUERY, check if we can skip left child
                if (r == PointValues.Relation.CELL_INSIDE_QUERY && hasRightChild) {
                    long rightSize = pointTree.size();
                    long needed = size - docCount[0];
                    if (rightSize >= needed) {
                        // Right child has all we need - only process right
                        intersectRight(visitor, pointTree, docCount);
                        pointTree.moveToParent();
                        return;
                    }
                }
                // Process both children: right first (for DESC), then left if needed
                if (hasRightChild) {
                    intersectRight(visitor, pointTree, docCount);
                }
                if (docCount[0] < size) {
                    intersectRight(visitor, leftChild, docCount);
                }
                pointTree.moveToParent();
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                LeafReader reader = context.reader();
                long[] docCount = { 0 };

                PointValues values = reader.getPointValues(pointRangeQuery.getField());
                if (checkValidPointValues(values) == false) {
                    return null;
                }
                // values.size(): total points indexed, In most cases: values.size() ≈ number of documents (assuming single-valued fields)
                if (size > values.size()) {
                    return pointRangeQueryWeight.scorerSupplier(context);
                } else {
                    if (sortOrder == null || sortOrder.equals(SortOrder.ASC)) {
                        return new ScorerSupplier() {

                            final DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values);
                            final PointValues.IntersectVisitor visitor = getIntersectVisitor(result, docCount);
                            long cost = -1;

                            @Override
                            public Scorer get(long leadCost) throws IOException {
                                intersectLeft(values.getPointTree(), visitor, docCount);
                                DocIdSetIterator iterator = result.build().iterator();
                                return new ConstantScoreScorer(score(), scoreMode, iterator);
                            }

                            @Override
                            public long cost() {
                                if (cost == -1) {
                                    // Computing the cost may be expensive, so only do it if necessary
                                    cost = values.estimateDocCount(visitor);
                                    assert cost >= 0;
                                }
                                return cost;
                            }
                        };
                    } else {
                        // we need to fetch size + deleted docs since the collector will prune away deleted docs resulting in fewer results
                        // than expected
                        final int deletedDocs = reader.numDeletedDocs();
                        size += deletedDocs;
                        return new ScorerSupplier() {

                            final DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values);
                            final PointValues.IntersectVisitor visitor = getIntersectVisitor(result, docCount);
                            long cost = -1;

                            @Override
                            public Scorer get(long leadCost) throws IOException {
                                intersectRight(values.getPointTree(), visitor, docCount);
                                DocIdSetIterator iterator = result.build().iterator();
                                return new ConstantScoreScorer(score(), scoreMode, iterator);
                            }

                            @Override
                            public long cost() {
                                if (cost == -1) {
                                    // Computing the cost may be expensive, so only do it if necessary
                                    cost = values.estimateDocCount(visitor);
                                    assert cost >= 0;
                                }
                                return cost;
                            }
                        };
                    }
                }
            }

            @Override
            public int count(LeafReaderContext context) throws IOException {
                return pointRangeQueryWeight.count(context);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    private byte[] computeEffectiveBound(SearchContext context, boolean isLowerBound) {
        byte[] originalBound = isLowerBound ? pointRangeQuery.getLowerPoint() : pointRangeQuery.getUpperPoint();
        boolean isAscending = sortOrder == null || sortOrder.equals(SortOrder.ASC);
        if ((isLowerBound && isAscending) || (isLowerBound == false && isAscending == false)) {
            Object searchAfterValue = context.request().source().searchAfter()[0];
            MappedFieldType fieldType = context.getQueryShardContext().fieldMapper(pointRangeQuery.getField());
            if (fieldType instanceof NumericPointEncoder encoder) {
                return encoder.encodePoint(searchAfterValue, isLowerBound);
            }
        }
        return originalBound;
    }

    @Override
    public boolean canApproximate(SearchContext context) {
        if (context == null) {
            return false;
        }
        if (context.aggregations() != null) {
            return false;
        }
        // Exclude approximation when "track_total_hits": true
        if (context.trackTotalHitsUpTo() == SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
            return false;
        }
        // size 0 could be set for caching
        if (context.from() + context.size() == 0) {
            this.setSize(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO);
        } else {
            // We add +1 to ensure we collect at least one more document than required. This guarantees correct relation value:
            // - If we find exactly trackTotalHitsUpTo docs: relation = EQUAL_TO
            // - If we find > trackTotalHitsUpTo docs: relation = GREATER_THAN_OR_EQUAL_TO
            // With +1, we will consistently get GREATER_THAN_OR_EQUAL_TO relation.
            this.setSize(Math.max(context.from() + context.size(), context.trackTotalHitsUpTo()) + 1);
        }
        if (context.request() != null && context.request().source() != null) {
            if (context.request().source().sorts() != null && context.request().source().sorts().size() > 1) {
                return false;
            }
            FieldSortBuilder primarySortField = FieldSortBuilder.getPrimaryFieldSortOrNull(context.request().source());
            if (primarySortField != null) {
                if (!primarySortField.fieldName().equals(pointRangeQuery.getField())) {
                    return false;
                }
                if (primarySortField.missing() != null) {
                    // Cannot sort documents missing this field.
                    return false;
                }
                this.setSortOrder(primarySortField.order());
                if (context.request().source().searchAfter() != null) {
                    byte[] lower;
                    byte[] upper;
                    if (sortOrder == SortOrder.ASC) {
                        lower = computeEffectiveBound(context, true);
                        upper = pointRangeQuery.getUpperPoint();
                    } else {
                        lower = pointRangeQuery.getLowerPoint();
                        upper = computeEffectiveBound(context, false);
                    }
                    this.pointRangeQuery = new PointRangeQuery(pointRangeQuery.getField(), lower, upper, pointRangeQuery.getNumDims()) {
                        @Override
                        protected String toString(int dimension, byte[] value) {
                            return valueToString.apply(value);
                        }
                    };
                }
            }
            return context.request().source().terminateAfter() == SearchContext.DEFAULT_TERMINATE_AFTER;
        }
        return true;
    }

    @Override
    public final int hashCode() {
        return pointRangeQuery.hashCode();
    }

    @Override
    public final boolean equals(Object o) {
        return sameClassAs(o) && equalsTo(getClass().cast(o));
    }

    private boolean equalsTo(ApproximatePointRangeQuery other) {
        return Objects.equals(pointRangeQuery, other.pointRangeQuery);
    }

    @Override
    public final String toString(String field) {
        final StringBuilder sb = new StringBuilder();
        sb.append("Approximate(");
        sb.append(pointRangeQuery.toString());
        sb.append(")");

        return sb.toString();
    }
}
