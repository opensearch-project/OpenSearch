/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Objects;

import org.roaringbitmap.RoaringBitmap;

/**
 * Filter with bitmap
 * <p>
 * Similar to Lucene SortedNumericDocValuesSetQuery
 */
public class BitmapDocValuesQuery extends Query implements Accountable {

    final String field;
    final RoaringBitmap bitmap;
    final long min;
    final long max;

    public BitmapDocValuesQuery(String field, RoaringBitmap bitmap) {
        this.field = field;
        this.bitmap = bitmap;
        min = bitmap.first();
        max = bitmap.last();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), field);
                final NumericDocValues singleton = DocValues.unwrapSingleton(values);
                final TwoPhaseIterator iterator;
                if (singleton != null) {
                    iterator = new TwoPhaseIterator(singleton) {
                        @Override
                        public boolean matches() throws IOException {
                            long value = singleton.longValue();
                            return value >= min && value <= max && bitmap.contains((int) value);
                        }

                        @Override
                        public float matchCost() {
                            return 5; // 2 comparisons, possible lookup in the bitmap
                        }
                    };
                } else {
                    iterator = new TwoPhaseIterator(values) {
                        @Override
                        public boolean matches() throws IOException {
                            int count = values.docValueCount();
                            for (int i = 0; i < count; i++) {
                                final long value = values.nextValue();
                                if (value < min) {
                                    continue;
                                } else if (value > max) {
                                    return false; // values are sorted, terminate
                                } else if (bitmap.contains((int) value)) {
                                    return true;
                                }
                            }
                            return false;
                        }

                        @Override
                        public float matchCost() {
                            return 5; // 2 comparisons, possible lookup in the bitmap
                        }
                    };
                }
                return new ConstantScoreScorer(this, score(), scoreMode, iterator);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, field);
            }
        };
    }

    @Override
    public String toString(String field) {
        // bitmap may contain high cardinality, so choose to not show the actual values in it
        return field + " cardinality: " + bitmap.getLongCardinality();
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (bitmap.getLongCardinality() == 0) {
            return new MatchNoDocsQuery();
        }
        return super.rewrite(indexSearcher);
    }

    @Override
    public boolean equals(Object other) {
        if (sameClassAs(other) == false) {
            return false;
        }
        BitmapDocValuesQuery that = (BitmapDocValuesQuery) other;
        return field.equals(that.field) && bitmap.equals(that.bitmap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, bitmap);
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.shallowSizeOfInstance(BitmapDocValuesQuery.class) + RamUsageEstimator.sizeOfObject(field)
            + RamUsageEstimator.sizeOfObject(bitmap);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }
}
