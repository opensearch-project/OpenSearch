/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.document;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.opensearch.common.Numbers;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Objects;

/**
 * The {@link org.apache.lucene.document.SortedNumericDocValuesRangeQuery} implementation for unsigned long numeric data type.
 *
 * @opensearch.internal
 */
public abstract class SortedUnsignedLongDocValuesRangeQuery extends Query {
    private final String field;
    private final long lowerValue;
    private final long upperValue;

    SortedUnsignedLongDocValuesRangeQuery(String field, BigInteger lowerValue, BigInteger upperValue) {
        this.field = Objects.requireNonNull(field);
        this.lowerValue = lowerValue.longValue();
        this.upperValue = upperValue.longValue();
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        SortedUnsignedLongDocValuesRangeQuery that = (SortedUnsignedLongDocValuesRangeQuery) obj;
        return Objects.equals(field, that.field) && lowerValue == that.lowerValue && upperValue == that.upperValue;
    }

    @Override
    public int hashCode() {
        int h = classHash();
        h = 31 * h + field.hashCode();
        h = 31 * h + Long.hashCode(lowerValue);
        h = 31 * h + Long.hashCode(upperValue);
        return h;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String field) {
        StringBuilder b = new StringBuilder();
        if (this.field.equals(field) == false) {
            b.append(this.field).append(":");
        }
        return b.append("[")
            .append(Long.toUnsignedString(lowerValue))
            .append(" TO ")
            .append(Long.toUnsignedString(upperValue))
            .append("]")
            .toString();
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        if (Long.compareUnsigned(lowerValue, Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG) == 0
            && Long.compareUnsigned(upperValue, Numbers.MAX_UNSIGNED_LONG_VALUE_AS_LONG) == 0) {
            return new FieldExistsQuery(field);
        }
        return super.rewrite(searcher);
    }

    abstract SortedNumericDocValues getValues(LeafReader reader, String field) throws IOException;

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, field);
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                SortedNumericDocValues values = getValues(context.reader(), field);
                if (values == null) {
                    return null;
                }
                final NumericDocValues singleton = DocValues.unwrapSingleton(values);
                final TwoPhaseIterator iterator;
                if (singleton != null) {
                    iterator = new TwoPhaseIterator(singleton) {
                        @Override
                        public boolean matches() throws IOException {
                            final long value = singleton.longValue();
                            return Long.compareUnsigned(value, lowerValue) >= 0 && Long.compareUnsigned(value, upperValue) <= 0;
                        }

                        @Override
                        public float matchCost() {
                            return 2; // 2 comparisons
                        }
                    };
                } else {
                    iterator = new TwoPhaseIterator(values) {
                        @Override
                        public boolean matches() throws IOException {
                            for (int i = 0, count = values.docValueCount(); i < count; ++i) {
                                final long value = values.nextValue();
                                if (Long.compareUnsigned(value, lowerValue) < 0) {
                                    continue;
                                }
                                // Values are sorted, so the first value that is >= lowerValue is our best
                                // candidate
                                return Long.compareUnsigned(value, upperValue) <= 0;
                            }
                            return false; // all values were < lowerValue
                        }

                        @Override
                        public float matchCost() {
                            return 2; // 2 comparisons
                        }
                    };
                }
                return new ConstantScoreScorer(this, score(), scoreMode, iterator);
            }
        };
    }

    public static Query newSlowRangeQuery(String field, BigInteger lowerValue, BigInteger upperValue) {
        return new SortedUnsignedLongDocValuesRangeQuery(field, lowerValue, upperValue) {
            @Override
            SortedNumericDocValues getValues(LeafReader reader, String field) throws IOException {
                FieldInfo info = reader.getFieldInfos().fieldInfo(field);
                if (info == null) {
                    // Queries have some optimizations when one sub scorer returns null rather
                    // than a scorer that does not match any documents
                    return null;
                }
                return DocValues.getSortedNumeric(reader, field);
            }
        };
    }
}
