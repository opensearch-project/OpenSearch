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
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Objects;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import static org.opensearch.search.query.Bitmap64IndexQuery.checkArgs;

/**
 * 64-bit Bitmap DocValues Query
 * Same logic as BitmapDocValuesQuery but supports long values.
 */
public class Bitmap64DocValuesQuery extends Query implements Accountable {

    final String field;
    final Roaring64NavigableMap bitmap;
    final long min;
    final long max;

    public Bitmap64DocValuesQuery(String field, Roaring64NavigableMap bitmap) {
        checkArgs(field, bitmap);
        this.field = field;
        this.bitmap = bitmap;
        if (!bitmap.isEmpty()) {
            min = bitmap.first();
            max = bitmap.last();
        } else {
            min = 0;
            max = 0;
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), field);
                final NumericDocValues singleton = DocValues.unwrapSingleton(values);

                final TwoPhaseIterator iterator;

                if (singleton != null) {
                    iterator = new TwoPhaseIterator(singleton) {
                        @Override
                        public boolean matches() throws IOException {
                            long value = singleton.longValue();
                            return value >= min && value <= max && bitmap.contains(value);
                        }

                        @Override
                        public float matchCost() {
                            return 5;
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
                                    return false;
                                } else if (bitmap.contains(value)) {
                                    return true;
                                }
                            }
                            return false;
                        }

                        @Override
                        public float matchCost() {
                            return 5;
                        }
                    };
                }

                final Scorer scorer = new ConstantScoreScorer(score(), scoreMode, iterator);
                return new Weight.DefaultScorerSupplier(scorer);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, field);
            }
        };
    }

    @Override
    public String toString(String field) {
        return "Bitmap64DocValuesQuery(field=" + this.field + ")";
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (bitmap.isEmpty()) {
            return new MatchNoDocsQuery();
        }
        return super.rewrite(indexSearcher);
    }

    @Override
    public boolean equals(Object other) {
        if (sameClassAs(other) == false) {
            return false;
        }
        Bitmap64DocValuesQuery that = (Bitmap64DocValuesQuery) other;
        return field.equals(that.field) && bitmap.equals(that.bitmap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, bitmap);
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.shallowSizeOfInstance(Bitmap64DocValuesQuery.class) + RamUsageEstimator.sizeOf(field) + bitmap
            .getLongSizeInBytes();
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }
}
