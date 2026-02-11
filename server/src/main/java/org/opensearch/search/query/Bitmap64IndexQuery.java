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
import java.util.Objects;

import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

/**
 * A query that matches all documents that contain a set of long numbers represented by a 64-bit bitmap.
 * <p>
 * Uses {@link Roaring64NavigableMap} with signed long mode ({@code signedLongs=true}) to match the
 * sort order of Lucene's {@link LongPoint} encoding in the BKD tree. The bitmap <b>must</b> be
 * constructed with {@code new Roaring64NavigableMap(true)} — unsigned mode will produce incorrect
 * results for negative values because the merge-join requires the iterator and point index to
 * traverse values in the same order.
 * <p>
 * Supports the full signed {@code long} range ({@code Long.MIN_VALUE} to {@code Long.MAX_VALUE}).
 * Not compatible with {@code unsigned_long} fields, which use {@code BigIntegerPoint} (16-byte encoding).
 * <p>
 * Cross-language note: other roaring bitmap implementations (C, Go, Python) typically use unsigned
 * 64-bit semantics. For positive values (0 to 2^63-1), the bit patterns are identical and fully
 * interoperable. Values outside this range will have different signed/unsigned interpretations
 * but the underlying bit patterns remain consistent.
 * <p>
 * Serialization uses the portable format via {@code serializePortable}/{@code deserializePortable},
 * which is structurally compatible with the RoaringFormatSpec 64-bit extension. The cookie-based
 * header validation in the roaring bitmap library reliably distinguishes 32-bit and 64-bit formats,
 * enabling safe fallback deserialization.
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
        if (field == null) {
            throw new IllegalArgumentException("field must not be null");
        }
        if (bitmap == null) {
            throw new IllegalArgumentException("bitmap must not be null");
        }
    }

    interface Bitmap64Iterator extends BytesRefIterator {
        // wrap LongIterator.next()
        BytesRef next();

        // advance as long as the next value is smaller than target
        void advance(byte[] target);
    }

    private static Bitmap64Iterator bitmap64EncodedIterator(Roaring64NavigableMap bitmap) {
        return new Bitmap64Iterator() {
            private final LongIterator iterator = bitmap.getLongIterator();
            private final BytesRef encoded = new BytesRef(new byte[Long.BYTES]);
            private long buffered;
            private boolean hasBuffered;

            {
                if (iterator.hasNext()) {
                    buffered = iterator.next();
                    hasBuffered = true;
                }
            }

            public BytesRef next() {
                if (!hasBuffered) {
                    return null;
                }
                LongPoint.encodeDimension(buffered, encoded.bytes, 0);
                if (iterator.hasNext()) {
                    buffered = iterator.next();
                } else {
                    hasBuffered = false;
                }
                return encoded;
            }

            public void advance(byte[] target) {
                long targetVal = LongPoint.decodeDimension(target, 0);
                while (hasBuffered && buffered < targetVal) {
                    if (iterator.hasNext()) {
                        buffered = iterator.next();
                    } else {
                        hasBuffered = false;
                    }
                }
            }
        };
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            final long cardinality = bitmap.getLongCardinality();

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                LeafReader reader = context.reader();
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
                            cost = cardinality * 20;
                        }
                        return cost;
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }

    private class MergePointVisitor implements PointValues.IntersectVisitor {
        private final DocIdSetBuilder result;
        private final Bitmap64Iterator iterator;
        private BytesRef nextQueryPoint;
        private final ArrayUtil.ByteArrayComparator comparator;
        private DocIdSetBuilder.BulkAdder adder;

        public MergePointVisitor(DocIdSetBuilder result) throws IOException {
            this.result = result;
            this.comparator = ArrayUtil.getUnsignedComparator(Long.BYTES);
            this.iterator = bitmap64EncodedIterator(bitmap);
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
                    iterator.advance(packedValue);
                    nextQueryPoint = iterator.next();
                } else {
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
                    iterator.advance(minPackedValue);
                    nextQueryPoint = iterator.next();
                    continue;
                }
                int cmpMax = comparator.compare(nextQueryPoint.bytes, nextQueryPoint.offset, maxPackedValue, 0);
                if (cmpMax > 0) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }

                if (cmpMin == 0 && cmpMax == 0) {
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                } else {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
            }

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
        return "Bitmap64IndexQuery(field=" + this.field + ")";
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
