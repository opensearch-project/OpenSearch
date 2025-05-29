/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
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
    private int size;

    private SortOrder sortOrder;

    public final PointRangeQuery pointRangeQuery;

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

            // custom intersect visitor to walk the right of tree (from rightmost leaf going left)
            public void intersectRight(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree, long[] docCount)
                throws IOException {
                if (docCount[0] >= size) {
                    return;
                }
                PointValues.Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
                switch (r) {
                    case CELL_INSIDE_QUERY:
                    case CELL_CROSSES_QUERY:
                        if (pointTree.moveToChild() && docCount[0] < size) {
                            PointValues.PointTree leftChild = pointTree.clone();
                            // BKD is binary today, so one moveToSibling() is enough to land on the right child.
                            // If PointTree ever becomes n-ary, update the traversal below to visit all siblings or re-enable a full loop.
                            if (pointTree.moveToSibling()) {
                                // We have two children - visit right first
                                intersectRight(visitor, pointTree, docCount);
                                // Then visit left if we still need more docs
                                if (docCount[0] < size) {
                                    intersectRight(visitor, leftChild, docCount);
                                }
                            } else {
                                // Only one child - visit it
                                intersectRight(visitor, leftChild, docCount);
                            }
                            pointTree.moveToParent();
                        } else {
                            if (docCount[0] < size) {
                                if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
                                    pointTree.visitDocIDs(visitor);
                                } else {
                                    pointTree.visitDocValues(visitor);
                                }
                            }
                        }
                        break;
                    case CELL_OUTSIDE_QUERY:
                        break;
                    default:
                        throw new IllegalArgumentException("Unreachable code");
                }
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                LeafReader reader = context.reader();
                long[] docCount = { 0 };

                PointValues values = reader.getPointValues(pointRangeQuery.getField());
                if (checkValidPointValues(values) == false) {
                    return null;
                }
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
            FieldSortBuilder primarySortField = FieldSortBuilder.getPrimaryFieldSortOrNull(context.request().source());
            if (primarySortField != null) {
                if (!primarySortField.fieldName().equals(pointRangeQuery.getField())) {
                    return false;
                }
                if (primarySortField.missing() != null) {
                    // Cannot sort documents missing this field.
                    return false;
                }
                if (context.request().source().searchAfter() != null) {
                    // TODO: We *could* optimize searchAfter, especially when this is the only sort field, but existing pruning is pretty
                    // good.
                    return false;
                }
                this.setSortOrder(primarySortField.order());
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
