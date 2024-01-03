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
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MedianAbsoluteDeviationAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Increments the counters related to Aggregation Search Queries.
 */
public class SearchQueryAggregationCategorizer {

    private static final String TYPE_TAG = "type";
    private final SearchQueryCounters searchQueryCounters;
    private final Map<Class<? extends AggregationBuilder>, String> aggregationHandlers;

    public SearchQueryAggregationCategorizer(SearchQueryCounters searchQueryCounters) {
        this.searchQueryCounters = searchQueryCounters;
        this.aggregationHandlers = new HashMap<>();

        initializeAggregationHandlers();
    }

    private void initializeAggregationHandlers() {

        aggregationHandlers.put(TermsAggregationBuilder.class, TermsAggregationBuilder.NAME);
        aggregationHandlers.put(AvgAggregationBuilder.class, AvgAggregationBuilder.NAME);
        aggregationHandlers.put(SumAggregationBuilder.class, SumAggregationBuilder.NAME);
        aggregationHandlers.put(MaxAggregationBuilder.class, MaxAggregationBuilder.NAME);
        aggregationHandlers.put(MinAggregationBuilder.class, MinAggregationBuilder.NAME);
        aggregationHandlers.put(ScriptedMetricAggregationBuilder.class, ScriptedMetricAggregationBuilder.NAME);
        aggregationHandlers.put(ExtendedStatsAggregationBuilder.class, ExtendedStatsAggregationBuilder.NAME);
        aggregationHandlers.put(FilterAggregationBuilder.class, FilterAggregationBuilder.NAME);
        aggregationHandlers.put(FiltersAggregationBuilder.class, FiltersAggregationBuilder.NAME);
        aggregationHandlers.put(AdjacencyMatrixAggregationBuilder.class, AdjacencyMatrixAggregationBuilder.NAME);
        aggregationHandlers.put(SamplerAggregationBuilder.class, SamplerAggregationBuilder.NAME);
        aggregationHandlers.put(DiversifiedAggregationBuilder.class, DiversifiedAggregationBuilder.NAME);
        aggregationHandlers.put(GlobalAggregationBuilder.class, GlobalAggregationBuilder.NAME);
        aggregationHandlers.put(MissingAggregationBuilder.class, MissingAggregationBuilder.NAME);
        aggregationHandlers.put(NestedAggregationBuilder.class, NestedAggregationBuilder.NAME);
        aggregationHandlers.put(ReverseNestedAggregationBuilder.class, ReverseNestedAggregationBuilder.NAME);
        aggregationHandlers.put(GeoDistanceAggregationBuilder.class, GeoDistanceAggregationBuilder.NAME);
        aggregationHandlers.put(HistogramAggregationBuilder.class, HistogramAggregationBuilder.NAME);
        aggregationHandlers.put(SignificantTermsAggregationBuilder.class, SignificantTermsAggregationBuilder.NAME);
        aggregationHandlers.put(SignificantTextAggregationBuilder.class, SignificantTextAggregationBuilder.NAME);
        aggregationHandlers.put(DateHistogramAggregationBuilder.class, DateHistogramAggregationBuilder.NAME);
        aggregationHandlers.put(RangeAggregationBuilder.class, RangeAggregationBuilder.NAME);
        aggregationHandlers.put(DateRangeAggregationBuilder.class, DateRangeAggregationBuilder.NAME);
        aggregationHandlers.put(IpRangeAggregationBuilder.class, IpRangeAggregationBuilder.NAME);
        aggregationHandlers.put(PercentilesAggregationBuilder.class, PercentilesAggregationBuilder.NAME);
        aggregationHandlers.put(PercentileRanksAggregationBuilder.class, PercentileRanksAggregationBuilder.NAME);
        aggregationHandlers.put(MedianAbsoluteDeviationAggregationBuilder.class, MedianAbsoluteDeviationAggregationBuilder.NAME);
        aggregationHandlers.put(CardinalityAggregationBuilder.class, CardinalityAggregationBuilder.NAME);
        aggregationHandlers.put(TopHitsAggregationBuilder.class, TopHitsAggregationBuilder.NAME);
        aggregationHandlers.put(GeoCentroidAggregationBuilder.class, GeoCentroidAggregationBuilder.NAME);
        aggregationHandlers.put(CompositeAggregationBuilder.class, CompositeAggregationBuilder.NAME);
        aggregationHandlers.put(MultiTermsAggregationBuilder.class, MultiTermsAggregationBuilder.NAME);
    }

    public void incrementSearchQueryAggregationCounters(Collection<AggregationBuilder> aggregatorFactories) {
        for (AggregationBuilder aggregationBuilder : aggregatorFactories) {
            String aggregationType = getAggregationType(aggregationBuilder);
            searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, aggregationType));
        }
    }

    private String getAggregationType(AggregationBuilder aggregationBuilder) {
        return aggregationHandlers.getOrDefault(aggregationBuilder.getClass(), "other");
    }
}
