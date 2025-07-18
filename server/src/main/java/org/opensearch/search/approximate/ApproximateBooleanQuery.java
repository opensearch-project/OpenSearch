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
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;

/**
 * An approximate-able version of {@link BooleanQuery}. For single clause boolean queries,
 * it unwraps the query into the singular clause and ensures approximation is applied.
 */
public class ApproximateBooleanQuery extends ApproximateQuery {
    public final BooleanQuery boolQuery;
    private final int size;
    private final List<BooleanClause> clauses;

    public ApproximateBooleanQuery(BooleanQuery boolQuery) {
        this(boolQuery, SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO);
    }

    protected ApproximateBooleanQuery(BooleanQuery boolQuery, int size) {
        this.boolQuery = boolQuery;
        this.size = size;
        this.clauses = boolQuery.clauses();
    }

    public BooleanQuery getBooleanQuery() {
        return boolQuery;
    }

    public Query getClauseQuery() {
        return clauses.get(0).query();
    }

    public static Query unwrap(Query unwrapBoolQuery) {
        Query clauseQuery = unwrapBoolQuery instanceof ApproximateBooleanQuery
            ? ((ApproximateBooleanQuery) unwrapBoolQuery).getClauseQuery()
            : ((BooleanQuery) unwrapBoolQuery).clauses().get(0).query();
        if (clauseQuery instanceof ApproximateBooleanQuery nestedBool) {
            return unwrap(nestedBool);
        } else {
            return clauseQuery;
        }
    }

    @Override
    protected boolean canApproximate(SearchContext context) {
        if (context == null) {
            return false;
        }

        // Don't approximate if we need accurate total hits
        if (context.trackTotalHitsUpTo() == SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
            return false;
        }

        // Don't approximate if we have aggregations
        if (context.aggregations() != null) {
            return false;
        }

        // For single clause boolean queries, check if the clause can be approximated
        if (clauses.size() == 1 && clauses.get(0).occur() != BooleanClause.Occur.MUST_NOT) {
            BooleanClause singleClause = clauses.get(0);
            Query clauseQuery = singleClause.query();

            // If the clause is already an ApproximateScoreQuery, we can approximate + set context
            if (clauseQuery instanceof ApproximateScoreQuery approximateScoreQuery) {
                if (approximateScoreQuery.getApproximationQuery() instanceof ApproximateBooleanQuery nestedBool) {
                    return nestedBool.canApproximate(context);
                }
                return approximateScoreQuery.getApproximationQuery().canApproximate(context);
            }
        }

        return clauses.size() > 1 && clauses.stream().allMatch(clause -> clause.occur() == BooleanClause.Occur.FILTER);
    }

    @Override
    public ConstantScoreWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        // For single clause boolean queries, delegate to the clause's createWeight
        if (clauses.size() == 1 && clauses.get(0).occur() != BooleanClause.Occur.MUST_NOT) {
            Query clauseQuery = clauses.get(0).query();

            // If it's a scoring query, wrap it in a ConstantScoreQuery to ensure constant scoring
            if (!(clauseQuery instanceof ConstantScoreQuery)) {
                clauseQuery = new ConstantScoreQuery(clauseQuery);
            }

            return (ConstantScoreWeight) clauseQuery.createWeight(searcher, scoreMode, boost);
        }

        // For multi-clause boolean queries, create a custom weight
        return new ApproximateBooleanWeight(searcher, scoreMode, boost);
    }

    /**
     * Custom Weight implementation for ApproximateBooleanQuery that handles multi-clause boolean queries.
     * This is a basic implementation that behaves like a regular filter boolean query for now.
     */
    private class ApproximateBooleanWeight extends ConstantScoreWeight {
        private final Weight booleanWeight;
        private final ScoreMode scoreMode;

        public ApproximateBooleanWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            super(ApproximateBooleanQuery.this, boost);
            // Create a weight for the underlying boolean query
            this.booleanWeight = boolQuery.createWeight(searcher, scoreMode, boost);
            this.scoreMode = scoreMode;
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return false;
        }

        // public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
        // ScorerSupplier scorerSupplier = scorerSupplier(context);
        // if (scorerSupplier == null) {
        // // No docs match
        // return null;
        // }
        //
        // scorerSupplier.setTopLevelScoringClause();
        // return scorerSupplier.bulkScorer();
        // }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            // Get the scorer supplier from the underlying boolean weight
            final ScorerSupplier booleanScorer = booleanWeight.scorerSupplier(context);
            if (booleanScorer == null) {
                return null;
            }

            // return new ApproximateBooleanScorerSupplier();
            // Return a wrapper scorer supplier that delegates to the boolean scorer
            return new ScorerSupplier() {
                @Override
                public Scorer get(long leadCost) throws IOException {
                    Scorer scorer = booleanScorer.get(leadCost);
                    if (scorer == null) {
                        return null;
                    }
                    return new ConstantScoreScorer(score(), scoreMode, scorer.iterator());
                }

                @Override
                public long cost() {
                    return booleanScorer.cost();
                }

                @Override
                public BulkScorer bulkScorer() throws IOException {
                    // For now, just delegate to the standard bulk scorer
                    // In the future, this is where we would implement our custom bulk scorer
                    return booleanScorer.bulkScorer();
                }
            };
        }

    }

    @Override
    public String toString(String s) {
        return "ApproximateBooleanQuery(" + boolQuery.toString(s) + ")";
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {
        boolQuery.visit(queryVisitor);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ApproximateBooleanQuery that = (ApproximateBooleanQuery) o;
        return size == that.size && boolQuery.equals(that.boolQuery);
    }

    @Override
    public int hashCode() {
        return boolQuery.hashCode();
    }
}
