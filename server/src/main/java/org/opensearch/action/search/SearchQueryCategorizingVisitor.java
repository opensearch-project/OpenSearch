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
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.DistanceFeatureQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.FieldMaskingSpanQueryBuilder;
import org.opensearch.index.query.FuzzyQueryBuilder;
import org.opensearch.index.query.GeoBoundingBoxQueryBuilder;
import org.opensearch.index.query.GeoDistanceQueryBuilder;
import org.opensearch.index.query.GeoPolygonQueryBuilder;
import org.opensearch.index.query.GeoShapeQueryBuilder;
import org.opensearch.index.query.IntervalQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.index.query.ScriptQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Class to visit the query builder tree and also track the level information.
 * Increments the counters related to Search Query type.
 */
final class SearchQueryCategorizingVisitor implements QueryBuilderVisitor {
    private static final String LEVEL_TAG = "level";
    private final int level;
    private final SearchQueryCounters searchQueryCounters;
    private final Map<Class<? extends QueryBuilder>, Counter> queryHandlers;

    public SearchQueryCategorizingVisitor(SearchQueryCounters searchQueryCounters) {
        this(searchQueryCounters, 0);
    }

    private SearchQueryCategorizingVisitor(SearchQueryCounters counters, int level) {
        this.searchQueryCounters = counters;
        this.level = level;
        this.queryHandlers = initializeQueryHandlers();
    }

    private Map<Class<? extends QueryBuilder>, Counter> initializeQueryHandlers() {
        Map<Class<? extends QueryBuilder>, Counter> handlers = new HashMap<>();

        handlers.put(BoolQueryBuilder.class, searchQueryCounters.boolCounter);
        handlers.put(FunctionScoreQueryBuilder.class, searchQueryCounters.functionScoreCounter);
        handlers.put(MatchQueryBuilder.class, searchQueryCounters.matchCounter);
        handlers.put(MatchPhraseQueryBuilder.class, searchQueryCounters.matchPhrasePrefixCounter);
        handlers.put(MultiMatchQueryBuilder.class, searchQueryCounters.multiMatchCounter);
        handlers.put(QueryStringQueryBuilder.class, searchQueryCounters.queryStringQueryCounter);
        handlers.put(RangeQueryBuilder.class, searchQueryCounters.rangeCounter);
        handlers.put(RegexpQueryBuilder.class, searchQueryCounters.regexCounter);
        handlers.put(TermQueryBuilder.class, searchQueryCounters.termCounter);
        handlers.put(WildcardQueryBuilder.class, searchQueryCounters.wildcardCounter);
        handlers.put(BoostingQueryBuilder.class, searchQueryCounters.boostCounter);
        handlers.put(ConstantScoreQueryBuilder.class, searchQueryCounters.constantScoreCounter);
        handlers.put(DisMaxQueryBuilder.class, searchQueryCounters.disMaxCounter);
        handlers.put(DistanceFeatureQueryBuilder.class, searchQueryCounters.distanceFeatureCounter);
        handlers.put(ExistsQueryBuilder.class, searchQueryCounters.existsCounter);
        handlers.put(FieldMaskingSpanQueryBuilder.class, searchQueryCounters.fieldMaskingSpanCounter);
        handlers.put(FuzzyQueryBuilder.class, searchQueryCounters.fuzzyCounter);
        handlers.put(GeoBoundingBoxQueryBuilder.class, searchQueryCounters.geoBoundingBoxCounter);
        handlers.put(GeoDistanceQueryBuilder.class, searchQueryCounters.geoDistanceCounter);
        handlers.put(GeoPolygonQueryBuilder.class, searchQueryCounters.geoPolygonCounter);
        handlers.put(GeoShapeQueryBuilder.class, searchQueryCounters.geoShapeCounter);
        handlers.put(IntervalQueryBuilder.class, searchQueryCounters.intervalCounter);
        handlers.put(MatchAllQueryBuilder.class, searchQueryCounters.matchallCounter);
        handlers.put(PrefixQueryBuilder.class, searchQueryCounters.prefixCounter);
        handlers.put(ScriptQueryBuilder.class, searchQueryCounters.scriptCounter);
        handlers.put(SimpleQueryStringBuilder.class, searchQueryCounters.simpleQueryStringCounter);

        return handlers;
    }

    public void accept(QueryBuilder qb) {
        Counter counter = queryHandlers.get(qb.getClass());
        if (counter != null) {
            counter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else {
            searchQueryCounters.otherQueryCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        }
    }

    public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
        return new SearchQueryCategorizingVisitor(searchQueryCounters, level + 1);
    }
}
