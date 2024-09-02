/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.IntsRef;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * An approximate-able version of {@link PointRangeQuery}. It creates an instance of {@link PointRangeQuery} but short-circuits the intersect logic
 * after {@code size} is hit
 */
public abstract class ApproximatePointRangeQuery extends ApproximateQuery {
    private int size;

    private SortOrder sortOrder;

    public final PointRangeQuery pointRangeQuery;

    protected ApproximatePointRangeQuery(String field, byte[] lowerPoint, byte[] upperPoint, int numDims) {
        this(field, lowerPoint, upperPoint, numDims, 10_000, null);
    }

    protected ApproximatePointRangeQuery(String field, byte[] lowerPoint, byte[] upperPoint, int numDims, int size) {
        this(field, lowerPoint, upperPoint, numDims, size, null);
    }

    protected ApproximatePointRangeQuery(String field, byte[] lowerPoint, byte[] upperPoint, int numDims, int size, SortOrder sortOrder) {
        this.size = size;
        this.sortOrder = sortOrder;
        this.pointRangeQuery = new PointRangeQuery(field, lowerPoint, upperPoint, numDims) {
            @Override
            protected String toString(int dimension, byte[] value) {
                return super.toString(field);
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
    public void visit(QueryVisitor visitor) {
        pointRangeQuery.visit(visitor);
    }

    @Override
    public final ConstantScoreWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        Weight pointRangeQueryWeight = pointRangeQuery.createWeight(searcher, scoreMode, boost);

        return new ConstantScoreWeight(this, boost) {

            private final ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(pointRangeQuery.getBytesPerDim());

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
                        // and collect less, but it won't hurt performance
                        if (docCount[0] < size) {
                            adder.add(docID);
                            docCount[0]++;
                        }
                    }

                    @Override
                    public void visit(DocIdSetIterator iterator) throws IOException {
                        adder.add(iterator);
                    }

                    @Override
                    public void visit(IntsRef ref) {
                        for (int i = 0; i < ref.length; i++) {
                            adder.add(ref.ints[ref.offset + i]);
                        }
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
                PointValues.Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
                if (docCount[0] > size) {
                    return;
                }
                switch (r) {
                    case CELL_OUTSIDE_QUERY:
                        // This cell is fully outside the query shape: stop recursing
                        break;
                    case CELL_INSIDE_QUERY:
                        // If the cell is fully inside, we keep moving to child until we reach a point where we can no longer move or when
                        // we have sufficient doc count. We first move down and then move to the left child
                        if (pointTree.moveToChild() && docCount[0] < size) {
                            do {
                                intersectLeft(visitor, pointTree, docCount);
                            } while (pointTree.moveToSibling() && docCount[0] < size);
                            pointTree.moveToParent();
                        } else {
                            // we're at the leaf node, if we're under the size, visit all the docIds in this node.
                            if (docCount[0] < size) {
                                pointTree.visitDocIDs(visitor);
                            }
                        }
                        break;
                    case CELL_CROSSES_QUERY:
                        // The cell crosses the shape boundary, or the cell fully contains the query, so we fall
                        // through and do full filtering:
                        if (pointTree.moveToChild() && docCount[0] < size) {
                            do {
                                intersectLeft(visitor, pointTree, docCount);
                            } while (pointTree.moveToSibling() && docCount[0] < size);
                            pointTree.moveToParent();
                        } else {
                            // TODO: we can assert that the first value here in fact matches what the pointTree
                            // claimed?
                            // Leaf node; scan and filter all points in this block:
                            if (docCount[0] < size) {
                                pointTree.visitDocValues(visitor);
                            }
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Unreachable code");
                }
            }

            // custom intersect visitor to walk the right of tree
            public void intersectRight(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree, long[] docCount)
                throws IOException {
                PointValues.Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
                if (docCount[0] > size) {
                    return;
                }
                switch (r) {
                    case CELL_OUTSIDE_QUERY:
                        // This cell is fully outside the query shape: stop recursing
                        break;

                    case CELL_INSIDE_QUERY:
                        // If the cell is fully inside, we keep moving right as long as the point tree size is over our size requirement
                        if (pointTree.size() > size && docCount[0] < size && moveRight(pointTree)) {
                            intersectRight(visitor, pointTree, docCount);
                            pointTree.moveToParent();
                        }
                        // if point tree size is no longer over, we have to go back one level where it still was over and the intersect left
                        else if (pointTree.size() <= size && docCount[0] < size) {
                            pointTree.moveToParent();
                            intersectLeft(visitor, pointTree, docCount);
                        }
                        // if we've reached leaf, it means out size is under the size of the leaf, we can just collect all docIDs
                        else {
                            // Leaf node; scan and filter all points in this block:
                            if (docCount[0] < size) {
                                pointTree.visitDocIDs(visitor);
                            }
                        }
                        break;
                    case CELL_CROSSES_QUERY:
                        // If the cell is fully inside, we keep moving right as long as the point tree size is over our size requirement
                        if (pointTree.size() > size && docCount[0] < size && moveRight(pointTree)) {
                            intersectRight(visitor, pointTree, docCount);
                            pointTree.moveToParent();
                        }
                        // if point tree size is no longer over, we have to go back one level where it still was over and the intersect left
                        else if (pointTree.size() <= size && docCount[0] < size) {
                            pointTree.moveToParent();
                            intersectLeft(visitor, pointTree, docCount);
                        }
                        // if we've reached leaf, it means out size is under the size of the leaf, we can just collect all doc values
                        else {
                            // Leaf node; scan and filter all points in this block:
                            if (docCount[0] < size) {
                                pointTree.visitDocValues(visitor);
                            }
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Unreachable code");
                }
            }

            public boolean moveRight(PointValues.PointTree pointTree) throws IOException {
                return pointTree.moveToChild() && pointTree.moveToSibling();
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                LeafReader reader = context.reader();
                long[] docCount = { 0 };

                PointValues values = reader.getPointValues(pointRangeQuery.getField());
                if (checkValidPointValues(values) == false) {
                    return null;
                }
                final Weight weight = this;
                if (size > values.size()) {
                    return pointRangeQueryWeight.scorerSupplier(context);
                } else {
                    if (sortOrder == null || sortOrder.equals(SortOrder.ASC)) {
                        return new ScorerSupplier() {

                            final DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, pointRangeQuery.getField());
                            final PointValues.IntersectVisitor visitor = getIntersectVisitor(result, docCount);
                            long cost = -1;

                            @Override
                            public Scorer get(long leadCost) throws IOException {
                                intersectLeft(values.getPointTree(), visitor, docCount);
                                DocIdSetIterator iterator = result.build().iterator();
                                return new ConstantScoreScorer(weight, score(), scoreMode, iterator);
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

                            final DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, pointRangeQuery.getField());
                            final PointValues.IntersectVisitor visitor = getIntersectVisitor(result, docCount);
                            long cost = -1;

                            @Override
                            public Scorer get(long leadCost) throws IOException {
                                intersectRight(values.getPointTree(), visitor, docCount);
                                DocIdSetIterator iterator = result.build().iterator();
                                return new ConstantScoreScorer(weight, score(), scoreMode, iterator);
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
            public Scorer scorer(LeafReaderContext context) throws IOException {
                ScorerSupplier scorerSupplier = scorerSupplier(context);
                if (scorerSupplier == null) {
                    return null;
                }
                return scorerSupplier.get(Long.MAX_VALUE);
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

    @Override
    public boolean canApproximate(SearchContext context) {
        if (context == null) {
            return false;
        }
        if (context.aggregations() != null) {
            return false;
        }
        if (!(context.query() instanceof ApproximateIndexOrDocValuesQuery)) {
            return false;
        }
        this.setSize(Math.max(context.from() + context.size(), context.trackTotalHitsUpTo()));
        if (context.request() != null && context.request().source() != null) {
            FieldSortBuilder primarySortField = FieldSortBuilder.getPrimaryFieldSortOrNull(context.request().source());
            if (primarySortField != null
                && primarySortField.missing() == null
                && primarySortField.getFieldName().equals(((RangeQueryBuilder) context.request().source().query()).fieldName())) {
                if (primarySortField.order() == SortOrder.DESC) {
                    this.setSortOrder(SortOrder.DESC);
                }
            }
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
        return Objects.equals(pointRangeQuery.getField(), other.pointRangeQuery.getField())
            && pointRangeQuery.getNumDims() == other.pointRangeQuery.getNumDims()
            && pointRangeQuery.getBytesPerDim() == other.pointRangeQuery.getBytesPerDim()
            && Arrays.equals(pointRangeQuery.getLowerPoint(), other.pointRangeQuery.getLowerPoint())
            && Arrays.equals(pointRangeQuery.getUpperPoint(), other.pointRangeQuery.getUpperPoint());
    }

    @Override
    public final String toString(String field) {
        final StringBuilder sb = new StringBuilder();
        if (pointRangeQuery.getField().equals(field) == false) {
            sb.append(pointRangeQuery.getField());
            sb.append(':');
        }

        // print ourselves as "range per dimension"
        for (int i = 0; i < pointRangeQuery.getNumDims(); i++) {
            if (i > 0) {
                sb.append(',');
            }

            int startOffset = pointRangeQuery.getBytesPerDim() * i;

            sb.append('[');
            sb.append(
                toString(
                    i,
                    ArrayUtil.copyOfSubArray(pointRangeQuery.getLowerPoint(), startOffset, startOffset + pointRangeQuery.getBytesPerDim())
                )
            );
            sb.append(" TO ");
            sb.append(
                toString(
                    i,
                    ArrayUtil.copyOfSubArray(pointRangeQuery.getUpperPoint(), startOffset, startOffset + pointRangeQuery.getBytesPerDim())
                )
            );
            sb.append(']');
        }

        return sb.toString();
    }

    /**
     * Returns a string of a single value in a human-readable format for debugging. This is used by
     * {@link #toString()}.
     *
     * @param dimension dimension of the particular value
     * @param value     single value, never null
     * @return human readable value for debugging
     */
    protected abstract String toString(int dimension, byte[] value);
}
