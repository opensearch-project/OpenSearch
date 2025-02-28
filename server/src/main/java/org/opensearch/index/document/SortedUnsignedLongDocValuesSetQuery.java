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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.opensearch.lucene.util.UnsignedLongHashSet;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Objects;

/**
 * The {@link org.apache.lucene.document.SortedNumericDocValuesSetQuery} implementation for unsigned long numeric data type.
 *
 * @opensearch.internal
 */
public abstract class SortedUnsignedLongDocValuesSetQuery extends Query {

    private final String field;
    private final UnsignedLongHashSet numbers;

    SortedUnsignedLongDocValuesSetQuery(String field, BigInteger[] numbers) {
        this.field = Objects.requireNonNull(field);
        Arrays.sort(numbers);
        this.numbers = new UnsignedLongHashSet(Arrays.stream(numbers).mapToLong(n -> n.longValue()).toArray());
    }

    @Override
    public String toString(String field) {
        return new StringBuilder().append(field).append(": ").append(numbers.toString()).toString();
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (numbers.size() == 0) {
            return new MatchNoDocsQuery();
        }
        return super.rewrite(indexSearcher);
    }

    @Override
    public boolean equals(Object other) {
        if (sameClassAs(other) == false) {
            return false;
        }
        SortedUnsignedLongDocValuesSetQuery that = (SortedUnsignedLongDocValuesSetQuery) other;
        return field.equals(that.field) && numbers.equals(that.numbers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, numbers);
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
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
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
                            long value = singleton.longValue();
                            return Long.compareUnsigned(value, numbers.minValue) >= 0
                                && Long.compareUnsigned(value, numbers.maxValue) <= 0
                                && numbers.contains(value);
                        }

                        @Override
                        public float matchCost() {
                            return 5; // 2 comparisions, possible lookup in the set
                        }
                    };
                } else {
                    iterator = new TwoPhaseIterator(values) {
                        @Override
                        public boolean matches() throws IOException {
                            int count = values.docValueCount();
                            for (int i = 0; i < count; i++) {
                                final long value = values.nextValue();
                                if (Long.compareUnsigned(value, numbers.minValue) < 0) {
                                    continue;
                                } else if (Long.compareUnsigned(value, numbers.maxValue) > 0) {
                                    return false; // values are sorted, terminate
                                } else if (numbers.contains(value)) {
                                    return true;
                                }
                            }
                            return false;
                        }

                        @Override
                        public float matchCost() {
                            return 5; // 2 comparisons, possible lookup in the set
                        }
                    };
                }
                final Scorer scorer = new ConstantScoreScorer(score(), scoreMode, iterator);
                return new DefaultScorerSupplier(scorer);
            }
        };
    }

    public static Query newSlowSetQuery(String field, BigInteger... values) {
        return new SortedUnsignedLongDocValuesSetQuery(field, values) {
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

    public static Query newSlowExactQuery(String field, BigInteger value) {
        return new SortedUnsignedLongDocValuesRangeQuery(field, value, value) {
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
