/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.opensearch.search.internal.SearchContext;

import java.util.Collections;
import java.util.List;

/**
 * {@link AggregationProcessor} implementation to be used with {@link org.opensearch.search.query.ConcurrentQueryPhaseSearcher}. It takes
 * care of performing shard level reduce on Aggregation results collected as part of concurrent execution among slices. This is done to
 * avoid the increase in aggregation result sets returned by each shard to coordinator where final reduce happens for results received from
 * all the shards
 */
public class ConcurrentAggregationProcessor extends DefaultAggregationProcessor {

    @Override
    public void populateResult(SearchContext context, List<InternalAggregation> aggregations) {
        InternalAggregations internalAggregations = InternalAggregations.from(aggregations);
        // Reduce the aggregations across slices before sending to the coordinator. We will perform shard level reduce iff multiple slices
        // were created to execute this request and it used concurrent segment search path
        // TODO: Add the check for flag that the request was executed using concurrent search
        if (context.searcher().getSlices().length > 1) {
            // using reduce is fine here instead of topLevelReduce as pipeline aggregation is evaluated on the coordinator after all
            // documents are collected across shards for an aggregation
            internalAggregations = InternalAggregations.reduce(
                Collections.singletonList(internalAggregations),
                context.aggregationReduceContext()
            );
        }
        context.queryResult().aggregations(internalAggregations);
    }
}
