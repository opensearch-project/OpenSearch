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
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * An approximate-able version of {@link PointRangeQuery}. It creates an instance of {@link PointRangeQuery} but short-circuits the intersect logic
 * after {@code size} is hit
 */
public abstract class ApproximatePointRangeQuery extends Query {
    private int size;

    private SortOrder sortOrder;

    private long[] docCount = { 0 };

    private final PointRangeQuery pointRangeQuery;

    protected ApproximatePointRangeQuery(String field, byte[] lowerPoint, byte[] upperPoint, int numDims) {
        this(field, lowerPoint, upperPoint, numDims, 10_000, SortOrder.ASC);
    }

    protected ApproximatePointRangeQuery(String field, byte[] lowerPoint, byte[] upperPoint, int numDims, int size) {
        this(field, lowerPoint, upperPoint, numDims, size, SortOrder.ASC);
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
    public final ApproximateConstantScoreWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        Weight pointRangeQueryWeight = pointRangeQuery.createWeight(searcher, scoreMode, boost);

        return new ApproximateConstantScoreWeight(this, boost) {

            private final ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(pointRangeQuery.getBytesPerDim());

            private boolean matches(byte[] packedValue) {
                return relate(packedValue, packedValue) != PointValues.Relation.CELL_OUTSIDE_QUERY;
            }

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

            public PointValues.IntersectVisitor getIntersectVisitor(DocIdSetBuilder result) {
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

            private void intersectLeft(PointValues.PointTree pointTree, PointValues.IntersectVisitor visitor) throws IOException {
                intersectLeft(visitor, pointTree);
                assert pointTree.moveToParent() == false;
            }

            private void intersectRight(PointValues.PointTree pointTree, PointValues.IntersectVisitor visitor) throws IOException {
                intersectRight(visitor, pointTree);
                assert pointTree.moveToParent() == false;
            }

            // custom intersect visitor to walk the left of the tree
            public long intersectLeft(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree) throws IOException {
                PointValues.Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
                if (docCount[0] >= size) {
                    return 0;
                }
                switch (r) {
                    case CELL_OUTSIDE_QUERY:
                        // This cell is fully outside the query shape: stop recursing
                        break;
                    case CELL_INSIDE_QUERY:
                        // If the cell is fully inside, we keep moving to child until we reach a point where we can no longer move or when
                        // we have sufficient doc count. We first move down and then move to the left child
                        if (pointTree.moveToChild()) {
                            do {
                                docCount[0] += intersectLeft(visitor, pointTree);
                            } while (pointTree.moveToSibling() && docCount[0] <= size);
                            pointTree.moveToParent();
                        } else {
                            // we're at the leaf node, if we're under the size, visit all the docIds in this node.
                            if (docCount[0] < size) {
                                pointTree.visitDocIDs(visitor);
                                docCount[0] += pointTree.size();
                                return docCount[0];
                            } else break;
                        }
                        break;
                    case CELL_CROSSES_QUERY:
                        // The cell crosses the shape boundary, or the cell fully contains the query, so we fall
                        // through and do full filtering:
                        if (pointTree.moveToChild()) {
                            do {
                                docCount[0] += intersectLeft(visitor, pointTree);
                            } while (pointTree.moveToSibling() && docCount[0] <= size);
                            pointTree.moveToParent();
                        } else {
                            // TODO: we can assert that the first value here in fact matches what the pointTree
                            // claimed?
                            // Leaf node; scan and filter all points in this block:
                            if (docCount[0] < size) {
                                pointTree.visitDocValues(visitor);
                            } else break;
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Unreachable code");
                }
                // docCount can be updated by the local visitor so we ensure that we return docCount after pointTree.visitDocValues(visitor)
                return docCount[0] > 0 ? docCount[0] : 0;
            }

            // custom intersect visitor to walk the right of tree
            public long intersectRight(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree) throws IOException {
                PointValues.Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
                if (docCount[0] >= size) {
                    return 0;
                }
                switch (r) {
                    case CELL_OUTSIDE_QUERY:
                        // This cell is fully outside the query shape: stop recursing
                        break;
                    case CELL_INSIDE_QUERY:
                        // If the cell is fully inside, we keep moving to child until we reach a point where we can no longer move or when
                        // we have sufficient doc count. We first move down and then move right
                        if (pointTree.moveToChild()) {
                            while (pointTree.moveToSibling() && docCount[0] <= size) {
                                docCount[0] += intersectRight(visitor, pointTree);
                            }
                            pointTree.moveToParent();
                        } else {
                            // we're at the leaf node, if we're under the size, visit all the docIds in this node.
                            if (docCount[0] <= size) {
                                pointTree.visitDocIDs(visitor);
                                docCount[0] += pointTree.size();
                                return docCount[0];
                            } else break;
                        }
                        break;
                    case CELL_CROSSES_QUERY:
                        // The cell crosses the shape boundary, or the cell fully contains the query, so we fall
                        // through and do full filtering:
                        if (pointTree.moveToChild()) {
                            do {
                                docCount[0] += intersectRight(visitor, pointTree);
                            } while (pointTree.moveToSibling() && docCount[0] <= size);
                            pointTree.moveToParent();
                        } else {
                            // TODO: we can assert that the first value here in fact matches what the pointTree
                            // claimed?
                            // Leaf node; scan and filter all points in this block:
                            if (docCount[0] <= size) {
                                pointTree.visitDocValues(visitor);
                            } else break;
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Unreachable code");
                }
                // docCount can be updated by the local visitor, so we ensure that we return docCount after
                // pointTree.visitDocValues(visitor)
                return docCount[0] > 0 ? docCount[0] : 0;
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                LeafReader reader = context.reader();

                PointValues values = reader.getPointValues(pointRangeQuery.getField());
                if (checkValidPointValues(values) == false) {
                    return null;
                }

                if (values.getDocCount() == 0) {
                    return null;
                } else {
                    final byte[] fieldPackedLower = values.getMinPackedValue();
                    final byte[] fieldPackedUpper = values.getMaxPackedValue();
                    for (int i = 0; i < pointRangeQuery.getNumDims(); ++i) {
                        int offset = i * pointRangeQuery.getBytesPerDim();
                        if (comparator.compare(pointRangeQuery.getLowerPoint(), offset, fieldPackedUpper, offset) > 0
                            || comparator.compare(pointRangeQuery.getUpperPoint(), offset, fieldPackedLower, offset) < 0) {
                            // If this query is a required clause of a boolean query, then returning null here
                            // will help make sure that we don't call ScorerSupplier#get on other required clauses
                            // of the same boolean query, which is an expensive operation for some queries (e.g.
                            // multi-term queries).
                            return null;
                        }
                    }
                }

                boolean allDocsMatch;
                if (values.getDocCount() == reader.maxDoc()) {
                    final byte[] fieldPackedLower = values.getMinPackedValue();
                    final byte[] fieldPackedUpper = values.getMaxPackedValue();
                    allDocsMatch = true;
                    for (int i = 0; i < pointRangeQuery.getNumDims(); ++i) {
                        int offset = i * pointRangeQuery.getBytesPerDim();
                        if (comparator.compare(pointRangeQuery.getLowerPoint(), offset, fieldPackedLower, offset) > 0
                            || comparator.compare(pointRangeQuery.getUpperPoint(), offset, fieldPackedUpper, offset) < 0) {
                            allDocsMatch = false;
                            break;
                        }
                    }
                } else {
                    allDocsMatch = false;
                }

                final Weight weight = this;
                if (allDocsMatch) {
                    // all docs have a value and all points are within bounds, so everything matches
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) {
                            return new ConstantScoreScorer(weight, score(), scoreMode, DocIdSetIterator.all(reader.maxDoc()));
                        }

                        @Override
                        public long cost() {
                            return reader.maxDoc();
                        }
                    };
                } else {
                    if (sortOrder.equals(SortOrder.ASC)) {
                        return new ScorerSupplier() {

                            final DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, pointRangeQuery.getField());
                            final PointValues.IntersectVisitor visitor = getIntersectVisitor(result);
                            long cost = -1;

                            @Override
                            public Scorer get(long leadCost) throws IOException {
                                intersectLeft(values.getPointTree(), visitor);
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
                    return new ScorerSupplier() {

                        final DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, pointRangeQuery.getField());
                        final PointValues.IntersectVisitor visitor = getIntersectVisitor(result);
                        long cost = -1;

                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            intersectRight(values.getPointTree(), visitor);
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
                return true;
            }
        };
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
     * @param value single value, never null
     * @return human readable value for debugging
     */
    protected abstract String toString(int dimension, byte[] value);
}
