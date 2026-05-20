/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.document.LongPoint;
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
import java.util.Arrays;
import java.util.Objects;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

/**
 * A query that matches all documents that contain a set of long values represented by a 64-bit bitmap
 *
 * @opensearch.internal
 */
public class Bitmap64IndexQuery extends Query implements Accountable {

    private final Roaring64NavigableMap bitmap;
    private final String field;

    public Bitmap64IndexQuery(String field, Roaring64NavigableMap bitmap) {
        checkArgs(field, bitmap);
        this.bitmap = bitmap;
        this.field = field;
    }

    static void checkArgs(String field, Roaring64NavigableMap bitmap) {
        if (field == null) throw new IllegalArgumentException("field must not be null");
        if (bitmap == null) throw new IllegalArgumentException("bitmap must not be null");
    }

    interface BitmapIterator extends BytesRefIterator {
        BytesRef next();

        void advance(byte[] target);
    }

    private static BitmapIterator bitmapEncodedIterator(Roaring64NavigableMap bitmap) {
        return new BitmapIterator() {
            private final org.roaringbitmap.longlong.LongIterator it = bitmap.getLongIterator();
            private final BytesRef encoded = new BytesRef(new byte[Long.BYTES]);
            private final byte[] currentBytes = new byte[Long.BYTES];
            private boolean hasBuffered = false;

            @Override
            public BytesRef next() {
                if (hasBuffered) {
                    hasBuffered = false;
                    System.arraycopy(currentBytes, 0, encoded.bytes, 0, Long.BYTES);
                    return encoded;
                }

                if (!it.hasNext()) return null;

                long v = it.next();
                LongPoint.encodeDimension(v, encoded.bytes, 0);
                return encoded;
            }

            @Override
            public void advance(byte[] target) {
                while (it.hasNext()) {
                    long v = it.next();
                    LongPoint.encodeDimension(v, currentBytes, 0);

                    if (Arrays.compareUnsigned(currentBytes, target) >= 0) {
                        hasBuffered = true;
                        return;
                    }
                }
            }
        };
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {

            final long cardinality = bitmap.getLongCardinality();

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                LeafReader reader = context.reader();
                PointValues values = reader.getPointValues(field);
                if (values == null) return null;

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
                        if (cost == -1) cost = cardinality * 20; // same heuristic as 32-bit
                        return cost;
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true; // depends only on segment points
            }
        };
    }

    private class MergePointVisitor implements PointValues.IntersectVisitor {

        private final DocIdSetBuilder result;
        private final BitmapIterator iterator;
        private BytesRef nextQueryPoint;
        private final ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(Long.BYTES);
        private DocIdSetBuilder.BulkAdder adder;

        MergePointVisitor(DocIdSetBuilder result) {
            this.result = result;
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
            if (matches(packedValue)) visit(docID);
        }

        @Override
        public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
            if (matches(packedValue)) adder.add(iterator);
        }

        private boolean matches(byte[] packedValue) {
            while (nextQueryPoint != null) {
                int cmp = comparator.compare(nextQueryPoint.bytes, nextQueryPoint.offset, packedValue, 0);
                if (cmp == 0) return true;
                if (cmp < 0) {
                    iterator.advance(packedValue);
                    nextQueryPoint = iterator.next();
                } else break;
            }
            return false;
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            while (nextQueryPoint != null) {
                int cmpMin = comparator.compare(nextQueryPoint.bytes, nextQueryPoint.offset, minPackedValue, 0);
                if (cmpMin < 0) {
                    iterator.advance(minPackedValue);
                    nextQueryPoint = iterator.next();
                    continue;
                }
                int cmpMax = comparator.compare(nextQueryPoint.bytes, nextQueryPoint.offset, maxPackedValue, 0);
                if (cmpMax > 0) return PointValues.Relation.CELL_OUTSIDE_QUERY;
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
            return PointValues.Relation.CELL_OUTSIDE_QUERY;
        }
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        if (bitmap.isEmpty()) return new MatchNoDocsQuery();
        return super.rewrite(searcher);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) visitor.visitLeaf(this);
    }

    @Override
    public String toString(String field) {
        return "Bitmap64IndexQuery(field=" + this.field + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (sameClassAs(other) == false) return false;
        Bitmap64IndexQuery that = (Bitmap64IndexQuery) other;
        return field.equals(that.field) && bitmap.equals(that.bitmap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, bitmap);
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.shallowSizeOfInstance(Bitmap64IndexQuery.class) + RamUsageEstimator.sizeOf(field) + bitmap
            .getLongSizeInBytes();
    }
}
