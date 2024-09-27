/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Weight;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Entry-point for the approximation framework.
 * This class is heavily inspired by {@link org.apache.lucene.search.IndexOrDocValuesQuery}. It acts as a wrapper that consumer two queries, a regular query and an approximate version of the same. By default, it executes the regular query and returns {@link Weight#scorer} for the original query. At run-time, depending on certain constraints, we can re-write the {@code Weight} to use the approximate weight instead.
 */
public class ApproximateScoreQuery extends Query {

    private final Query originalQuery;
    private final ApproximateQuery approximationQuery;

    protected Query resolvedQuery;

    public ApproximateScoreQuery(Query originalQuery, ApproximateQuery approximationQuery) {
        this.originalQuery = originalQuery;
        this.approximationQuery = approximationQuery;
    }

    public Query getOriginalQuery() {
        return originalQuery;
    }

    public ApproximateQuery getApproximationQuery() {
        return approximationQuery;
    }

    @Override
    public final Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (resolvedQuery == null) {
            throw new IllegalStateException("Cannot rewrite resolved query without setContext being called");
        }
        return resolvedQuery.rewrite(indexSearcher);
    }

    public void setContext(SearchContext context) {
        if (resolvedQuery != null) {
            throw new IllegalStateException("Query already resolved, duplicate call to setContext");
        }
        resolvedQuery = approximationQuery.canApproximate(context) ? approximationQuery : originalQuery;
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
