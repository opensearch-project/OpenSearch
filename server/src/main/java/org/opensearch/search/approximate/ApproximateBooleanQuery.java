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
import org.apache.lucene.search.QueryVisitor;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortOrder;

import java.util.List;

public class ApproximateBooleanQuery extends ApproximateQuery {
    public final BooleanQuery boolQuery;
    private final int size;
    private final SortOrder sortOrder;
    private final List<BooleanClause> clauses;

    public ApproximateBooleanQuery(BooleanQuery boolQuery) {
        this(boolQuery, SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO, null);
    }

    protected ApproximateBooleanQuery(BooleanQuery boolQuery, int size, SortOrder sortOrder) {
        this.boolQuery = boolQuery;
        this.size = size;
        this.sortOrder = sortOrder;
        this.clauses = boolQuery.clauses();
    }

    @Override
    protected boolean canApproximate(SearchContext context) {
        return false;
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
        return size == that.size && sortOrder == that.sortOrder && boolQuery.equals(that.boolQuery);
    }

    @Override
    public int hashCode() {
        int result = boolQuery.hashCode();
        result = 31 * result + size;
        result = 31 * result + (sortOrder != null ? sortOrder.hashCode() : 0);
        return result;
    }
}
