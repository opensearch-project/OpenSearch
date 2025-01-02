/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

/**
 * A query that matches all documents that contain a set of integer numbers represented by bitmap
 *
 * @opensearch.internal
 */
public class BitmapIndexQuery extends Query implements Accountable {

    private final RoaringBitmap bitmap;
    private final String field;

    public BitmapIndexQuery(String field, RoaringBitmap bitmap) {
        this.bitmap = bitmap;
        this.field = field;
    }

    private static BytesRefIterator bitmapEncodedIterator(RoaringBitmap bitmap) {
        return new BytesRefIterator() {
            private final Iterator<Integer> iterator = bitmap.iterator();
            private final BytesRef encoded = new BytesRef(new byte[Integer.BYTES]);

            @Override
            public BytesRef next() {
                int value;
                if (iterator.hasNext()) {
                    value = iterator.next();
                } else {
                    return null;
                }
                IntPoint.encodeDimension(value, encoded.bytes, 0);
                return encoded;
            }
        };
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                ScorerSupplier scorerSupplier = scorerSupplier(context);
                if (scorerSupplier == null) {
                    return null;
                }
                return scorerSupplier.get(Long.MAX_VALUE);
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final Weight weight = this;
                LeafReader reader = context.reader();
                // get point value
                // only works for one dimension
                PointValues values = reader.getPointValues(field);
                if (values == null) {
                    return null;
                }
                if (values.getNumIndexDimensions() != 1) {
                    throw new IllegalArgumentException("field must have only one dimension");
                }

                return new ScorerSupplier() {
                    long cost = -1; // calculate lazily, and only once

                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, field);
                        MergePointVisitor visitor = new MergePointVisitor(result);
                        values.intersect(visitor);
                        return new ConstantScoreScorer(weight, score(), scoreMode, result.build().iterator());
                    }

                    @Override
                    public long cost() {
                        if (cost == -1) {
                            cost = bitmap.getLongCardinality();
                        }
                        return cost;
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // This query depend only on segment-immutable structure points
                return true;
            }
        };
    }

    private class MergePointVisitor implements PointValues.IntersectVisitor {
        private final DocIdSetBuilder result;
        private final BytesRefIterator iterator;
        private BytesRef nextQueryPoint;
        private final ArrayUtil.ByteArrayComparator comparator;
        private DocIdSetBuilder.BulkAdder adder;

        public MergePointVisitor(DocIdSetBuilder result)
            throws IOException {
            this.result = result;
            this.comparator = ArrayUtil.getUnsignedComparator(Integer.BYTES);
            this.iterator = bitmapEncodedIterator(bitmap);
            nextQueryPoint = iterator.next();
        }

        @Override
        public void grow(int count) {
            adder = result.grow(count);
        }

        @Override
        public void visit(int docID) {
            adder.add(docID);
        }

        @Override
        public void visit(DocIdSetIterator iterator) throws IOException {
            adder.add(iterator);
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

        private boolean matches(byte[] packedValue) {
            while (nextQueryPoint != null) {
                int cmp = comparator.compare(nextQueryPoint.bytes, nextQueryPoint.offset, packedValue, 0);
                if (cmp == 0) {
                    return true;
                } else if (cmp < 0) {
                    // Query point is before index point, so we move to next query point
                    try {
                        nextQueryPoint = iterator.next();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    // Query point is after index point, so we don't collect and we return:
                    break;
                }
            }
            return false;
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            while (nextQueryPoint != null) {
                int cmpMin =
                    comparator.compare(nextQueryPoint.bytes, nextQueryPoint.offset, minPackedValue, 0);
                if (cmpMin < 0) {
                    // query point is before the start of this cell
                    try {
                        nextQueryPoint = iterator.next();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    continue;
                }
                int cmpMax =
                    comparator.compare(nextQueryPoint.bytes, nextQueryPoint.offset, maxPackedValue, 0);
                if (cmpMax > 0) {
                    // query point is after the end of this cell
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }

                if (cmpMin == 0 && cmpMax == 0) {
                    // NOTE: we only hit this if we are on a cell whose min and max values are exactly equal
                    // to our point,
                    // which can easily happen if many (> 512) docs share this one value
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                } else {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
            }

            // We exhausted all points in the query:
            return PointValues.Relation.CELL_OUTSIDE_QUERY;
        }
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (bitmap.isEmpty()) {
            return new MatchNoDocsQuery();
        }
        return super.rewrite(indexSearcher);
    }

    @Override
    public String toString(String field) {
        return "BitmapIndexQuery(field=" + field + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (sameClassAs(other) == false) {
            return false;
        }
        BitmapIndexQuery that = (BitmapIndexQuery) other;
        return field.equals(that.field) && bitmap.equals(that.bitmap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, bitmap);
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.shallowSizeOfInstance(BitmapIndexQuery.class) + RamUsageEstimator.sizeOfObject(field)
            + RamUsageEstimator.sizeOfObject(bitmap);
    }
}
