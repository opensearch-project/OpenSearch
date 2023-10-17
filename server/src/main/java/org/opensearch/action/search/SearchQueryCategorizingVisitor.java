/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.telemetry.metrics.tags.Tags;

/**
 * Class to visit the querybuilder tree and also track the level information.
 * Increments the counters related to Search Query type.
 */
public class SearchQueryCategorizingVisitor implements QueryBuilderVisitor {
    public static final String LEVEL_TAG = "level";
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
        if (qb instanceof BoolQueryBuilder) {
            searchQueryCounters.boolCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof FunctionScoreQueryBuilder) {
            searchQueryCounters.functionScoreCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof MatchQueryBuilder) {
            searchQueryCounters.matchCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof MatchPhraseQueryBuilder) {
            searchQueryCounters.matchPhrasePrefixCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof MultiMatchQueryBuilder) {
            searchQueryCounters.multiMatchCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof QueryStringQueryBuilder) {
            searchQueryCounters.queryStringQueryCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof RangeQueryBuilder) {
            searchQueryCounters.rangeCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof RegexpQueryBuilder) {
            searchQueryCounters.regexCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof TermQueryBuilder) {
            searchQueryCounters.termCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof WildcardQueryBuilder) {
            searchQueryCounters.wildcardCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else {
            searchQueryCounters.otherQueryCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        }
    }

    public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
        return new SearchQueryCategorizingVisitor(searchQueryCounters, level + 1);
    }
}
