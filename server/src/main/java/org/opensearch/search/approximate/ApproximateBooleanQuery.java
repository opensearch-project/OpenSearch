/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
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
                approximateScoreQuery.setContext(context);
                return true;
            }
        }

        return false;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        // Handle single clause boolean queries by unwrapping them and applying approximation
        if (clauses.size() == 1) {
            BooleanClause singleClause = clauses.get(0);
            Query clauseQuery = singleClause.query();

            // If the single clause is an ApproximateScoreQuery, set its context
            if (clauseQuery instanceof ApproximateScoreQuery approximateQuery) {
                return approximateQuery;
            }

            return clauseQuery.rewrite(indexSearcher);
        }

        // For multi-clause boolean queries, use the default rewrite behavior
        return super.rewrite(indexSearcher);
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
