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

import java.io.IOException;
import java.util.Objects;

import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

/**
 * A query that matches all documents that contain a set of integer numbers represented by bitmap
 *
 * @opensearch.internal
 */
public class BitmapIndexQuery extends Query implements Accountable {

    private final RoaringBitmap bitmap;
    private final String field;

    public BitmapIndexQuery(String field, RoaringBitmap bitmap) {
        checkArgs(field, bitmap);
        this.bitmap = bitmap;
        this.field = field;
    }

    static void checkArgs(String field, RoaringBitmap bitmap) {
        if (field == null) {
            throw new IllegalArgumentException("field must not be null");
        }
        if (bitmap == null) {
            throw new IllegalArgumentException("bitmap must not be null");
        }
    }

    interface BitmapIterator extends BytesRefIterator {
        // wrap IntIterator.next()
        BytesRef next();

        // expose PeekableIntIterator.advanceIfNeeded, advance as long as the next value is smaller than target
        void advance(byte[] target);
    }

    private static BitmapIterator bitmapEncodedIterator(RoaringBitmap bitmap) {
        return new BitmapIterator() {
            private final PeekableIntIterator iterator = bitmap.getIntIterator();
            private final BytesRef encoded = new BytesRef(new byte[Integer.BYTES]);

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

            public void advance(byte[] target) {
                iterator.advanceIfNeeded(IntPoint.decodeDimension(target, 0));
            }
        };
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            // get cardinality is not cheap enough to do when supplying scorers, so do it once per weight
            final long cardinality = bitmap.getLongCardinality();

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                LeafReader reader = context.reader();
                // get the point value which should be one dimension, since bitmap saves integers
                PointValues values = reader.getPointValues(field);
                if (values == null) {
                    return null;
                }
                if (values.getNumIndexDimensions() != 1) {
                    throw new IllegalArgumentException("field must have only one dimension");
                }

                return new ScorerSupplier() {
                    long cost = -1;

                    final DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values);
                    final MergePointVisitor visitor = new MergePointVisitor(result);

                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        values.intersect(visitor);
                        return new ConstantScoreScorer(score(), scoreMode, result.build().iterator());
                    }

                    @Override
                    public long cost() {
                        if (cost == -1) {
                            // rough estimate of the cost, 20 times penalty is based on the experiment results
                            // details in https://github.com/opensearch-project/OpenSearch/pull/16936
                            cost = cardinality * 20;
                        }
                        return cost;
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // This query depend only on segment-immutable structure — points
                return true;
            }
        };
    }

    private class MergePointVisitor implements PointValues.IntersectVisitor {
        private final DocIdSetBuilder result;
        private final BitmapIterator iterator;
        private BytesRef nextQueryPoint;
        private final ArrayUtil.ByteArrayComparator comparator;
        private DocIdSetBuilder.BulkAdder adder;

        public MergePointVisitor(DocIdSetBuilder result) throws IOException {
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
                    iterator.advance(packedValue);
                    nextQueryPoint = iterator.next();
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
                int cmpMin = comparator.compare(nextQueryPoint.bytes, nextQueryPoint.offset, minPackedValue, 0);
                if (cmpMin < 0) {
                    // query point is before the start of this cell
                    iterator.advance(minPackedValue);
                    nextQueryPoint = iterator.next();
                    continue;
                }
                int cmpMax = comparator.compare(nextQueryPoint.bytes, nextQueryPoint.offset, maxPackedValue, 0);
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
        return "BitmapIndexQuery(field=" + this.field + ")";
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
        return RamUsageEstimator.shallowSizeOfInstance(BitmapIndexQuery.class) + RamUsageEstimator.sizeOf(field) + bitmap
            .getLongSizeInBytes();
    }
}
