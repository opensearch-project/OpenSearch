/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;

/**
 * {@link ReduceableSearchResult} returned by the {@link AggregationCollectorManager} which merges the aggregation with the one present in
 * query results
 */
public class AggregationReduceableSearchResult implements ReduceableSearchResult {
    private final InternalAggregations aggregations;

    public AggregationReduceableSearchResult(InternalAggregations aggregations) {
        this.aggregations = aggregations;
    }

    @Override
    public void reduce(QuerySearchResult result) throws IOException {
        if (!result.hasAggs()) {
            result.aggregations(aggregations);
        } else {
            // the aggregations result from reduce of either global or non-global aggs is present so lets combine it with other aggs
            // as well
            final InternalAggregations existingAggregations = result.aggregations().expand();
            final InternalAggregations finalReducedAggregations = InternalAggregations.merge(existingAggregations, aggregations);
            result.aggregations(finalReducedAggregations);
        }
    }
}
