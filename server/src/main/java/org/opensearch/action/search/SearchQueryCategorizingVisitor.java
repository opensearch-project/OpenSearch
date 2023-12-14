/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.index.query.*;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
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
    private final Map<Class<? extends QueryBuilder>, Consumer<QueryBuilder>> queryHandlers;

    public SearchQueryCategorizingVisitor(SearchQueryCounters searchQueryCounters) {
        this(searchQueryCounters, 0);
    }

    private SearchQueryCategorizingVisitor(SearchQueryCounters counters, int level) {
        this.searchQueryCounters = counters;
        this.level = level;
        this.queryHandlers = initializeQueryHandlers();
    }

    private Map<Class<? extends QueryBuilder>, Consumer<QueryBuilder>> initializeQueryHandlers() {
        Map<Class<? extends QueryBuilder>, Consumer<QueryBuilder>> handlers = new HashMap<>();

        handlers.put(BoolQueryBuilder.class, this::handleBoolQuery);
        handlers.put(FunctionScoreQueryBuilder.class, this::handleFunctionScoreQuery);
        handlers.put(MatchQueryBuilder.class, this::handleMatchQuery);
        handlers.put(MatchPhraseQueryBuilder.class, this::handleMatchPhrasePrefixQuery);
        handlers.put(MultiMatchQueryBuilder.class, this::handleMultiMatchQuery);
        handlers.put(QueryStringQueryBuilder.class, this::handleQueryStringQuery);
        handlers.put(RangeQueryBuilder.class, this::handleRangeQuery);
        handlers.put(RegexpQueryBuilder.class, this::handleRegexpQuery);
        handlers.put(TermQueryBuilder.class, this::handleTermQuery);
        handlers.put(WildcardQueryBuilder.class, this::handleWildcardQuery);
        handlers.put(BoostingQueryBuilder.class, this::handleBoostingQuery);
        handlers.put(ConstantScoreQueryBuilder.class, this::handleConstantScoreQuery);
        handlers.put(DisMaxQueryBuilder.class, this::handleDisMaxQuery);
        handlers.put(DistanceFeatureQueryBuilder.class, this::handleDistanceFeatureQuery);
        handlers.put(ExistsQueryBuilder.class, this::handleExistsQuery);
        handlers.put(FieldMaskingSpanQueryBuilder.class, this::handleFieldMaskingSpanQuery);
        handlers.put(FuzzyQueryBuilder.class, this::handleFuzzyQuery);
        handlers.put(GeoBoundingBoxQueryBuilder.class, this::handleGeoBoundingBoxQuery);
        handlers.put(GeoDistanceQueryBuilder.class, this::handleGeoDistanceQuery);
        handlers.put(GeoPolygonQueryBuilder.class, this::handleGeoPolygonQuery);
        handlers.put(GeoShapeQueryBuilder.class, this::handleGeoShapeQuery);
        handlers.put(IntervalQueryBuilder.class, this::handleIntervalQuery);
        handlers.put(MatchAllQueryBuilder.class, this::handleMatchAllQuery);
        handlers.put(PrefixQueryBuilder.class, this::handlePrefixQuery);
        handlers.put(ScriptQueryBuilder.class, this::handleScriptQuery);
        handlers.put(SimpleQueryStringBuilder.class, this::handleSimpleQueryStringQuery);

        return handlers;
    }

    public void accept(QueryBuilder qb) {
        queryHandlers.getOrDefault(qb.getClass(), this::handleOtherQuery).accept(qb);
    }

    public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
        return new SearchQueryCategorizingVisitor(searchQueryCounters, level + 1);
    }

    private void handleBoolQuery(QueryBuilder qb) {
        searchQueryCounters.boolCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleFunctionScoreQuery(QueryBuilder qb) {
        searchQueryCounters.functionScoreCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleMatchQuery(QueryBuilder qb) {
        searchQueryCounters.matchCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleMatchPhrasePrefixQuery(QueryBuilder qb) {
        searchQueryCounters.matchPhrasePrefixCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleMultiMatchQuery(QueryBuilder qb) {
        searchQueryCounters.multiMatchCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleQueryStringQuery(QueryBuilder qb) {
        searchQueryCounters.queryStringQueryCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleRangeQuery(QueryBuilder qb) {
        searchQueryCounters.rangeCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleRegexpQuery(QueryBuilder qb) {
        searchQueryCounters.regexCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleTermQuery(QueryBuilder qb) {
        searchQueryCounters.termCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleWildcardQuery(QueryBuilder qb) {
        searchQueryCounters.wildcardCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleBoostingQuery(QueryBuilder qb) {
        searchQueryCounters.boostCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleConstantScoreQuery(QueryBuilder qb) {
        searchQueryCounters.constantScoreCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleDisMaxQuery(QueryBuilder qb) {
        searchQueryCounters.disMaxCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleDistanceFeatureQuery(QueryBuilder qb) {
        searchQueryCounters.distanceFeatureCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleExistsQuery(QueryBuilder qb) {
        searchQueryCounters.existsCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleFieldMaskingSpanQuery(QueryBuilder qb) {
        searchQueryCounters.fieldMaskingSpanCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleFuzzyQuery(QueryBuilder qb) {
        searchQueryCounters.fuzzyCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleGeoBoundingBoxQuery(QueryBuilder qb) {
        searchQueryCounters.geoBoundingBoxCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleGeoDistanceQuery(QueryBuilder qb) {
        searchQueryCounters.geoDistanceCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleGeoPolygonQuery(QueryBuilder qb) {
        searchQueryCounters.geoPolygonCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleGeoShapeQuery(QueryBuilder qb) {
        searchQueryCounters.geoShapeCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleIntervalQuery(QueryBuilder qb) {
        searchQueryCounters.intervalCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleMatchAllQuery(QueryBuilder qb) {
        searchQueryCounters.matchallCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handlePrefixQuery(QueryBuilder qb) {
        searchQueryCounters.prefixCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleScriptQuery(QueryBuilder qb) {
        searchQueryCounters.scriptCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleSimpleQueryStringQuery(QueryBuilder qb) {
        searchQueryCounters.simpleQueryStringCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    private void handleOtherQuery(QueryBuilder qb) {
        searchQueryCounters.otherQueryCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }
}
