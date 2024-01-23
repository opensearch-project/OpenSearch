/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;

/**
 * Class to visit the query builder tree and also track the level information.
 * Increments the counters related to Search Query type.
 */
final class SearchQueryCategorizingVisitor implements QueryBuilderVisitor {
    private final int level;
    private final SearchQueryCounters searchQueryCounters;

    public SearchQueryCategorizingVisitor(SearchQueryCounters searchQueryCounters) {
        this(searchQueryCounters, 0);
    }

    private SearchQueryCategorizingVisitor(SearchQueryCounters counters, int level) {
        this.searchQueryCounters = counters;
        this.level = level;
    }

    public void accept(QueryBuilder qb) {
        searchQueryCounters.incrementCounter(qb, level);
    }

    public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
        return new SearchQueryCategorizingVisitor(searchQueryCounters, level + 1);
    }
}
