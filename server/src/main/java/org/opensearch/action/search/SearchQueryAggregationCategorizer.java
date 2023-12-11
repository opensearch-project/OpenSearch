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
import org.opensearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.metrics.WeightedAvgAggregationBuilder;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.Collection;

public class SearchQueryAggregationCategorizer {
    private static final String TYPE_TAG = "type";
    private static final String VALUE_COUNT_AGGREGATION = "value_count";
    private static final String AVG_AGGREGATION = "avg";
    private static final String WEIGHTED_AVG_AGGREGATION = "weighted_avg";
    private static final String MAX_AGGREGATION = "max";
    private static final String MIN_AGGREGATION = "min";
    private static final String SUM_AGGREGATION = "sum";
    private static final String STATS_AGGREGATION = "stats";
    private static final String EXTENDED_STATS_AGGREGATION = "extended_stats";
    private static final String FILTER_AGGREGATION = "filter";
    private static final String FILTERS_AGGREGATION = "filters";
    private static final String ADJACENCY_MATRIX_AGGREGATION = "adjacency_matrix";
    private static final String SAMPLER_AGGREGATION = "sampler";
    private static final String DIVERSIFIED_AGGREGATION = "diversified";
    private static final String GLOBAL_AGGREGATION = "global";
    private static final String MISSING_AGGREGATION = "missing";
    private static final String NESTED_AGGREGATION = "nested";
    private static final String REVERSE_NESTED_AGGREGATION = "reverse_nested";
    private static final String GEO_DISTANCE_AGGREGATION = "geo_distance";
    private static final String HISTOGRAM_AGGREGATION = "histogram";
    private static final String SIGNIFICANT_TERMS_AGGREGATION = "significant_terms";
    private static final String SIGNIFICANT_TEXT_AGGREGATION = "significant_text";
    private static final String DATE_HISTOGRAM_AGGREGATION = "date_histogram";
    private static final String RANGE_AGGREGATION = "range";
    private static final String DATE_RANGE_AGGREGATION = "date_range";
    private static final String IP_RANGE_AGGREGATION = "ip_range";
    private static final String TERMS_AGGREGATION = "terms";
    private static final String PERCENTILES_AGGREGATION = "percentiles";
    private static final String PERCENTILE_RANKS_AGGREGATION = "percentile_ranks";
    private static final String MEDIAN_ABSOLUTE_DEVIATION_AGGREGATION = "median_absolute_deviation";
    private static final String CARDINALITY_AGGREGATION = "cardinality";
    private static final String TOP_HITS_AGGREGATION = "top_hits";
    private static final String GEO_CENTROID_AGGREGATION = "geo_centroid";
    private static final String SCRIPTED_METRIC_AGGREGATION = "scripted_metric";
    private static final String COMPOSITE_AGGREGATION = "composite";
    private static final String MULTI_TERMS_AGGREGATION = "multi_terms";
    private static final String OTHER_AGGREGATION = "other";
    private final SearchQueryCounters searchQueryCounters;

    public SearchQueryAggregationCategorizer(SearchQueryCounters searchQueryCounters) {
        this.searchQueryCounters = searchQueryCounters;
    }

    public void incrementSearchQueryAggregationCounters(Collection<AggregationBuilder> aggregatorFactories) {
        for (AggregationBuilder aggregationBuilder : aggregatorFactories) {
            if (aggregationBuilder instanceof ValueCountAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, VALUE_COUNT_AGGREGATION));
            } else if (aggregationBuilder instanceof AvgAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, AVG_AGGREGATION));
            } else if (aggregationBuilder instanceof WeightedAvgAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, WEIGHTED_AVG_AGGREGATION));
            } else if (aggregationBuilder instanceof MaxAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, MAX_AGGREGATION));
            } else if (aggregationBuilder instanceof MinAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, MIN_AGGREGATION));
            } else if (aggregationBuilder instanceof SumAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, SUM_AGGREGATION));
            } else if (aggregationBuilder instanceof StatsAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, STATS_AGGREGATION));
            } else if (aggregationBuilder instanceof ExtendedStatsAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, EXTENDED_STATS_AGGREGATION));
            } else if (aggregationBuilder instanceof FilterAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, FILTER_AGGREGATION));
            } else if (aggregationBuilder instanceof FiltersAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, FILTERS_AGGREGATION));
            } else if (aggregationBuilder instanceof AdjacencyMatrixAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, ADJACENCY_MATRIX_AGGREGATION));
            } else if (aggregationBuilder instanceof SamplerAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, SAMPLER_AGGREGATION));
            } else if (aggregationBuilder instanceof DiversifiedAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, DIVERSIFIED_AGGREGATION));
            } else if (aggregationBuilder instanceof GlobalAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, GLOBAL_AGGREGATION));
            } else if (aggregationBuilder instanceof MissingAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, MISSING_AGGREGATION));
            } else if (aggregationBuilder instanceof NestedAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, NESTED_AGGREGATION));
            } else if (aggregationBuilder instanceof ReverseNestedAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, REVERSE_NESTED_AGGREGATION));
            } else if (aggregationBuilder instanceof GeoDistanceAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, GEO_DISTANCE_AGGREGATION));
            } else if (aggregationBuilder instanceof HistogramAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, HISTOGRAM_AGGREGATION));
            } else if (aggregationBuilder instanceof SignificantTermsAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, SIGNIFICANT_TERMS_AGGREGATION));
            } else if (aggregationBuilder instanceof SignificantTextAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, SIGNIFICANT_TEXT_AGGREGATION));
            } else if (aggregationBuilder instanceof DateHistogramAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, DATE_HISTOGRAM_AGGREGATION));
            } else if (aggregationBuilder instanceof RangeAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, RANGE_AGGREGATION));
            } else if (aggregationBuilder instanceof DateRangeAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, DATE_RANGE_AGGREGATION));
            } else if (aggregationBuilder instanceof IpRangeAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, IP_RANGE_AGGREGATION));
            } else if (aggregationBuilder instanceof TermsAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, TERMS_AGGREGATION));
            } else if (aggregationBuilder instanceof PercentilesAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, PERCENTILES_AGGREGATION));
            } else if (aggregationBuilder instanceof PercentileRanksAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, PERCENTILE_RANKS_AGGREGATION));
            } else if (aggregationBuilder instanceof MedianAbsoluteDeviationAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, MEDIAN_ABSOLUTE_DEVIATION_AGGREGATION));
            } else if (aggregationBuilder instanceof CardinalityAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, CARDINALITY_AGGREGATION));
            } else if (aggregationBuilder instanceof TopHitsAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, TOP_HITS_AGGREGATION));
            } else if (aggregationBuilder instanceof GeoCentroidAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, GEO_CENTROID_AGGREGATION));
            } else if (aggregationBuilder instanceof ScriptedMetricAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, SCRIPTED_METRIC_AGGREGATION));
            } else if (aggregationBuilder instanceof CompositeAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, COMPOSITE_AGGREGATION));
            } else if (aggregationBuilder instanceof MultiTermsAggregationBuilder) {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, MULTI_TERMS_AGGREGATION));
            } else {
                searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, OTHER_AGGREGATION));
            }
        }
    }
}
