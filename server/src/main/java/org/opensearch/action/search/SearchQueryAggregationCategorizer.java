/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.Collection;

/**
 * Increments the counters related to Aggregation Search Queries.
 */
public class SearchQueryAggregationCategorizer {

    private static final String TYPE_TAG = "type";
    private final SearchQueryCounters searchQueryCounters;

    public SearchQueryAggregationCategorizer(SearchQueryCounters searchQueryCounters) {
        this.searchQueryCounters = searchQueryCounters;
    }

    public void incrementSearchQueryAggregationCounters(Collection<AggregationBuilder> aggregatorFactories) {
        for (AggregationBuilder aggregationBuilder : aggregatorFactories) {
            incrementCountersRecursively(aggregationBuilder);
        }
    }

    private void incrementCountersRecursively(AggregationBuilder aggregationBuilder) {
        // Increment counters for the current aggregation
        String aggregationType = aggregationBuilder.getType();
        searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, aggregationType));

        // Recursively process sub-aggregations if any
        Collection<AggregationBuilder> subAggregations = aggregationBuilder.getSubAggregations();
        if (subAggregations != null && !subAggregations.isEmpty()) {
            for (AggregationBuilder subAggregation : subAggregations) {
                incrementCountersRecursively(subAggregation);
            }
        }

        // Process pipeline aggregations
        Collection<PipelineAggregationBuilder> pipelineAggregations = aggregationBuilder.getPipelineAggregations();
        for (PipelineAggregationBuilder pipelineAggregation : pipelineAggregations) {
            String pipelineAggregationType = pipelineAggregation.getType();
            searchQueryCounters.aggCounter.add(1, Tags.create().addTag(TYPE_TAG, pipelineAggregationType));
        }
    }
}
