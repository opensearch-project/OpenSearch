/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.adjacency.AdjacencyMatrixAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.opensearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.opensearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.sampler.DiversifiedAggregationBuilder;
import org.opensearch.search.aggregations.bucket.sampler.SamplerAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.SignificantTextAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.*;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Increments the counters related to Aggregation Search Queries.
 */
public class SearchQueryAggregationCategorizer {

    private static final String TYPE_TAG = "type";
    private final SearchQueryCounters searchQueryCounters;
    private final Map<Class<? extends AggregationBuilder>, Function<AggregationBuilder, String>> aggregationHandlers;

    public SearchQueryAggregationCategorizer(SearchQueryCounters searchQueryCounters) {
        this.searchQueryCounters = searchQueryCounters;
        this.aggregationHandlers = initializeAggregationHandlers();
    }

    private Map<Class<? extends AggregationBuilder>, Function<AggregationBuilder, String>> initializeAggregationHandlers() {
        Map<Class<? extends AggregationBuilder>, Function<AggregationBuilder, String>> handlers = new HashMap<>();

        handlers.put(TermsAggregationBuilder.class, aggregationBuilder -> TermsAggregationBuilder.NAME);
        handlers.put(AvgAggregationBuilder.class, aggregationBuilder -> AvgAggregationBuilder.NAME);
        handlers.put(SumAggregationBuilder.class, aggregationBuilder -> SumAggregationBuilder.NAME);
        handlers.put(MaxAggregationBuilder.class, aggregationBuilder -> MaxAggregationBuilder.NAME);
        handlers.put(MinAggregationBuilder.class, aggregationBuilder -> MinAggregationBuilder.NAME);
        handlers.put(ScriptedMetricAggregationBuilder.class, aggregationBuilder -> ScriptedMetricAggregationBuilder.NAME);
        handlers.put(ExtendedStatsAggregationBuilder.class, aggregationBuilder -> ExtendedStatsAggregationBuilder.NAME);
        handlers.put(FilterAggregationBuilder.class, aggregationBuilder -> FilterAggregationBuilder.NAME);
        handlers.put(FiltersAggregationBuilder.class, aggregationBuilder -> FiltersAggregationBuilder.NAME);
        handlers.put(AdjacencyMatrixAggregationBuilder.class, aggregationBuilder -> AdjacencyMatrixAggregationBuilder.NAME);
        handlers.put(SamplerAggregationBuilder.class, aggregationBuilder -> SamplerAggregationBuilder.NAME);
        handlers.put(DiversifiedAggregationBuilder.class, aggregationBuilder -> DiversifiedAggregationBuilder.NAME);
        handlers.put(GlobalAggregationBuilder.class, aggregationBuilder -> GlobalAggregationBuilder.NAME);
        handlers.put(MissingAggregationBuilder.class, aggregationBuilder -> MissingAggregationBuilder.NAME);
        handlers.put(NestedAggregationBuilder.class, aggregationBuilder -> NestedAggregationBuilder.NAME);
        handlers.put(ReverseNestedAggregationBuilder.class, aggregationBuilder -> ReverseNestedAggregationBuilder.NAME);
        handlers.put(GeoDistanceAggregationBuilder.class, aggregationBuilder -> GeoDistanceAggregationBuilder.NAME);
        handlers.put(HistogramAggregationBuilder.class, aggregationBuilder -> HistogramAggregationBuilder.NAME);
        handlers.put(SignificantTermsAggregationBuilder.class, aggregationBuilder -> SignificantTermsAggregationBuilder.NAME);
        handlers.put(SignificantTextAggregationBuilder.class, aggregationBuilder -> SignificantTextAggregationBuilder.NAME);
        handlers.put(DateHistogramAggregationBuilder.class, aggregationBuilder -> DateHistogramAggregationBuilder.NAME);
        handlers.put(RangeAggregationBuilder.class, aggregationBuilder -> RangeAggregationBuilder.NAME);
        handlers.put(DateRangeAggregationBuilder.class, aggregationBuilder -> DateRangeAggregationBuilder.NAME);
        handlers.put(IpRangeAggregationBuilder.class, aggregationBuilder -> IpRangeAggregationBuilder.NAME);
        handlers.put(PercentilesAggregationBuilder.class, aggregationBuilder -> PercentilesAggregationBuilder.NAME);
        handlers.put(PercentileRanksAggregationBuilder.class, aggregationBuilder -> PercentileRanksAggregationBuilder.NAME);
        handlers.put(MedianAbsoluteDeviationAggregationBuilder.class, aggregationBuilder -> MedianAbsoluteDeviationAggregationBuilder.NAME);
        handlers.put(CardinalityAggregationBuilder.class, aggregationBuilder -> CardinalityAggregationBuilder.NAME);
        handlers.put(TopHitsAggregationBuilder.class, aggregationBuilder -> TopHitsAggregationBuilder.NAME);
        handlers.put(GeoCentroidAggregationBuilder.class, aggregationBuilder -> GeoCentroidAggregationBuilder.NAME);
        handlers.put(CompositeAggregationBuilder.class, aggregationBuilder -> CompositeAggregationBuilder.NAME);
        handlers.put(MultiTermsAggregationBuilder.class, aggregationBuilder -> MultiTermsAggregationBuilder.NAME);

        return handlers;
    }

    public void incrementSearchQueryAggregationCounters(Collection<AggregationBuilder> aggregatorFactories) {
        for (AggregationBuilder aggregationBuilder : aggregatorFactories) {
            String aggregationType = getAggregationType(aggregationBuilder);
            searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, aggregationType));
        }
    }

    private String getAggregationType(AggregationBuilder aggregationBuilder) {
        return aggregationHandlers.getOrDefault(aggregationBuilder.getClass(), this::handleUnknownAggregation).apply(aggregationBuilder);
    }

    private String handleUnknownAggregation(AggregationBuilder aggregationBuilder) {
        return "other";
    }
}
