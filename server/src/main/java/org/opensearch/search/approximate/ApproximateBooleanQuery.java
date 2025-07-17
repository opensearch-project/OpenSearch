/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
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
    private ApproximateBooleanQuery booleanQuery;
    public boolean isUnwrapped = false;

    public ApproximateBooleanQuery(BooleanQuery boolQuery) {
        this(boolQuery, SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO);
    }

    protected ApproximateBooleanQuery(BooleanQuery boolQuery, int size) {
        this.boolQuery = boolQuery;
        this.size = size;
        this.clauses = boolQuery.clauses();
    }

    public ApproximateBooleanQuery getBooleanQuery() {
        return booleanQuery;
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
        booleanQuery = this;
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

        return false;
    }

    @Override
    public ConstantScoreWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost){
        return new ConstantScoreWeight(this, boost) {

            /**
             * @param ctx
             * @return {@code true} if the object can be cached against a given leaf
             */
            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }

            /**
             * Get a {@link ScorerSupplier}, which allows knowing the cost of the {@link Scorer} before
             * building it. A scorer supplier for the same {@link LeafReaderContext} instance may be requested
             * multiple times as part of a single search call.
             *
             * <p><strong>Note:</strong> It must return null if the scorer is null.
             *
             * @param context the leaf reader context
             * @return a {@link ScorerSupplier} providing the scorer, or null if scorer is null
             * @throws IOException if an IOException occurs
             * @see Scorer
             * @see DefaultScorerSupplier
             */
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                return null;
            }
        };
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
