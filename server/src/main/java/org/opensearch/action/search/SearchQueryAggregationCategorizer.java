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

        handlers.put(TermsAggregationBuilder.class, aggregationBuilder -> ((TermsAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(AvgAggregationBuilder.class, aggregationBuilder -> ((AvgAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(SumAggregationBuilder.class, aggregationBuilder -> ((SumAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(MaxAggregationBuilder.class, aggregationBuilder -> ((MaxAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(MinAggregationBuilder.class, aggregationBuilder -> ((MinAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(ScriptedMetricAggregationBuilder.class, aggregationBuilder -> ((ScriptedMetricAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(ExtendedStatsAggregationBuilder.class, aggregationBuilder -> ((ExtendedStatsAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(FilterAggregationBuilder.class, aggregationBuilder -> ((FilterAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(FiltersAggregationBuilder.class, aggregationBuilder -> ((FiltersAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(AdjacencyMatrixAggregationBuilder.class, aggregationBuilder -> ((AdjacencyMatrixAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(SamplerAggregationBuilder.class, aggregationBuilder -> ((SamplerAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(DiversifiedAggregationBuilder.class, aggregationBuilder -> ((DiversifiedAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(GlobalAggregationBuilder.class, aggregationBuilder -> ((GlobalAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(MissingAggregationBuilder.class, aggregationBuilder -> ((MissingAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(NestedAggregationBuilder.class, aggregationBuilder -> ((NestedAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(ReverseNestedAggregationBuilder.class, aggregationBuilder -> ((ReverseNestedAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(GeoDistanceAggregationBuilder.class, aggregationBuilder -> ((GeoDistanceAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(HistogramAggregationBuilder.class, aggregationBuilder -> ((HistogramAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(SignificantTermsAggregationBuilder.class, aggregationBuilder -> ((SignificantTermsAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(SignificantTextAggregationBuilder.class, aggregationBuilder -> ((SignificantTextAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(DateHistogramAggregationBuilder.class, aggregationBuilder -> ((DateHistogramAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(RangeAggregationBuilder.class, aggregationBuilder -> ((RangeAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(DateRangeAggregationBuilder.class, aggregationBuilder -> ((DateRangeAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(IpRangeAggregationBuilder.class, aggregationBuilder -> ((IpRangeAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(PercentilesAggregationBuilder.class, aggregationBuilder -> ((PercentilesAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(PercentileRanksAggregationBuilder.class, aggregationBuilder -> ((PercentileRanksAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(MedianAbsoluteDeviationAggregationBuilder.class, aggregationBuilder -> ((MedianAbsoluteDeviationAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(CardinalityAggregationBuilder.class, aggregationBuilder -> ((CardinalityAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(TopHitsAggregationBuilder.class, aggregationBuilder -> ((TopHitsAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(GeoCentroidAggregationBuilder.class, aggregationBuilder -> ((GeoCentroidAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(CompositeAggregationBuilder.class, aggregationBuilder -> ((CompositeAggregationBuilder) aggregationBuilder).NAME);
        handlers.put(MultiTermsAggregationBuilder.class, aggregationBuilder -> ((MultiTermsAggregationBuilder) aggregationBuilder).NAME);

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
