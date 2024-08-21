/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Base class for a query that can be approximated.
 *
 * This class is heavily inspired by {@link org.apache.lucene.search.IndexOrDocValuesQuery}. It acts as a wrapper that consumer two queries, a regular query and an approximate version of the same. By default, it executes the regular query and returns {@link Weight#scorer} for the original query. At run-time, depending on certain constraints, we can re-write the {@code Weight} to use the approximate weight instead.
 */
public final class ApproximateScoreQuery extends Query {

    private final Query originalQuery;
    private final ApproximateableQuery approximationQuery;

    private Weight originalQueryWeight, approximationQueryWeight;

    private SearchContext context;

    public ApproximateScoreQuery(Query originalQuery, ApproximateableQuery approximationQuery) {
        this(originalQuery, approximationQuery, null, null);
    }

    public ApproximateScoreQuery(
        Query originalQuery,
        ApproximateableQuery approximationQuery,
        Weight originalQueryWeight,
        Weight approximationQueryWeight
    ) {
        this.originalQuery = originalQuery;
        this.approximationQuery = approximationQuery;
        this.originalQueryWeight = originalQueryWeight;
        this.approximationQueryWeight = approximationQueryWeight;
    }

    public Query getOriginalQuery() {
        return originalQuery;
    }

    public ApproximateableQuery getApproximationQuery() {
        return approximationQuery;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        originalQueryWeight = originalQuery.createWeight(searcher, scoreMode, boost);
        approximationQueryWeight = approximationQuery.createWeight(searcher, scoreMode, boost);

        return new Weight(this) {
            @Override
            public Explanation explain(LeafReaderContext leafReaderContext, int doc) throws IOException {
                return originalQueryWeight.explain(leafReaderContext, doc);
            }

            @Override
            public Matches matches(LeafReaderContext leafReaderContext, int doc) throws IOException {
                return originalQueryWeight.matches(leafReaderContext, doc);
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext leafReaderContext) throws IOException {
                final ScorerSupplier originalQueryScoreSupplier = originalQueryWeight.scorerSupplier(leafReaderContext);
                final ScorerSupplier approximationQueryScoreSupplier = approximationQueryWeight.scorerSupplier(leafReaderContext);
                if (originalQueryScoreSupplier == null || approximationQueryScoreSupplier == null) {
                    return null;
                }

                return new ScorerSupplier() {
                    @Override
                    public Scorer get(long l) throws IOException {
                        if (approximationQuery.canApproximate(context)) {
                            return approximationQueryScoreSupplier.get(l);
                        }
                        return originalQueryScoreSupplier.get(l);
                    }

                    @Override
                    public long cost() {
                        return originalQueryScoreSupplier.cost();
                    }
                };
            }

            @Override
            public Scorer scorer(LeafReaderContext leafReaderContext) throws IOException {
                ScorerSupplier scorerSupplier = scorerSupplier(leafReaderContext);
                if (scorerSupplier == null) {
                    return null;
                }
                return scorerSupplier.get(Long.MAX_VALUE);
            }

            @Override
            public boolean isCacheable(LeafReaderContext leafReaderContext) {
                return originalQueryWeight.isCacheable(leafReaderContext);
            }
        };
    }

    public void setContext(SearchContext context) {
        this.context = context;
    };

    @Override
    public String toString(String s) {
        return "ApproximateScoreQuery(originalQuery="
            + originalQuery.toString()
            + ", approximationQuery="
            + approximationQuery.toString()
            + ")";
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {
        QueryVisitor v = queryVisitor.getSubVisitor(BooleanClause.Occur.MUST, this);
        originalQuery.visit(v);
        approximationQuery.visit(v);
    }

    @Override
    public boolean equals(Object o) {
        if (!sameClassAs(o)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int h = classHash();
        h = 31 * h + originalQuery.hashCode();
        h = 31 * h + approximationQuery.hashCode();
        return h;
    }
}
