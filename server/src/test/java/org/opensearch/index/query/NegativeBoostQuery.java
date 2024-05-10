/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;

/**
 * Similar to Lucene's BoostQuery, but will accept negative boost values (which is normally wrong, since scores
 * should not be negative). Useful for testing that other query types guard against negative input scores.
 */
public class NegativeBoostQuery extends Query {
    private final Query query;
    private final float boost;

    public NegativeBoostQuery(Query query, float boost) {
        if (boost >= 0) {
            throw new IllegalArgumentException("Expected negative boost. Use BoostQuery if boost is non-negative.");
        }
        this.boost = boost;
        this.query = query;
    }

    @Override
    public String toString(String field) {
        StringBuilder builder = new StringBuilder();
        builder.append("(");
        builder.append(query.toString(field));
        builder.append(")");
        builder.append("^");
        builder.append(boost);
        return builder.toString();
    }

    @Override
    public void visit(QueryVisitor visitor) {
        query.visit(visitor);
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(NegativeBoostQuery other) {
        return query.equals(other.query) && Float.floatToIntBits(boost) == Float.floatToIntBits(other.boost);
    }

    @Override
    public int hashCode() {
        int h = classHash();
        h = 31 * h + query.hashCode();
        h = 31 * h + Float.floatToIntBits(boost);
        return h;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        final float negativeBoost = this.boost;
        Weight delegate = query.createWeight(searcher, scoreMode, boost);
        return new Weight(this) {
            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                return delegate.explain(context, doc);
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                Scorer delegateScorer = delegate.scorer(context);
                return new Scorer(this) {
                    @Override
                    public DocIdSetIterator iterator() {
                        return delegateScorer.iterator();
                    }

                    @Override
                    public float getMaxScore(int upTo) throws IOException {
                        return delegateScorer.getMaxScore(upTo);
                    }

                    @Override
                    public float score() throws IOException {
                        return delegateScorer.score() * negativeBoost;
                    }

                    @Override
                    public int docID() {
                        return delegateScorer.docID();
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return delegate.isCacheable(ctx);
            }
        };
    }
}
