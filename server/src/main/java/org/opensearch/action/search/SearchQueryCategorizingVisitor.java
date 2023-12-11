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
import org.opensearch.telemetry.metrics.tags.Tags;

/**
 * Class to visit the querybuilder tree and also track the level information.
 * Increments the counters related to Search Query type.
 */
final class SearchQueryCategorizingVisitor implements QueryBuilderVisitor {
    private static final String LEVEL_TAG = "level";
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
        }
        if (qb instanceof BoostingQueryBuilder) {
            searchQueryCounters.boostCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof ConstantScoreQueryBuilder) {
            searchQueryCounters.constantScoreCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof DisMaxQueryBuilder) {
            searchQueryCounters.disMaxCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof DistanceFeatureQueryBuilder) {
            searchQueryCounters.distanceFeatureCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof ExistsQueryBuilder) {
            searchQueryCounters.existsCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof FieldMaskingSpanQueryBuilder) {
            searchQueryCounters.fieldMaskingSpanCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof FuzzyQueryBuilder) {
            searchQueryCounters.fuzzyCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof GeoBoundingBoxQueryBuilder) {
            searchQueryCounters.geoBoundingBoxCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof GeoDistanceQueryBuilder) {
            searchQueryCounters.geoDistanceCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof GeoPolygonQueryBuilder) {
            searchQueryCounters.geoPolygonCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof GeoShapeQueryBuilder) {
            searchQueryCounters.geoShapeCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof IntervalQueryBuilder) {
            searchQueryCounters.intervalCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof MatchAllQueryBuilder) {
            searchQueryCounters.matchallCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof PrefixQueryBuilder) {
            searchQueryCounters.prefixCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof ScriptQueryBuilder) {
            searchQueryCounters.scriptCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else if (qb instanceof SimpleQueryStringBuilder) {
            searchQueryCounters.simpleQueryStringCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        } else {
            searchQueryCounters.otherQueryCounter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        }
    }

    public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
        return new SearchQueryCategorizingVisitor(searchQueryCounters, level + 1);
    }
}
