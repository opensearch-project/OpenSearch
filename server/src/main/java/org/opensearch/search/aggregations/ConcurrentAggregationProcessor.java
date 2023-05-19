/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;

/**
 * {@link AggregationProcessor} implementation to be used with {@link org.opensearch.search.query.ConcurrentQueryPhaseSearcher}. It takes
 * care of performing shard level reduce on Aggregation results collected as part of concurrent execution among slices. This is done to
 * avoid the increase in aggregation result sets returned by each shard to coordinator where final reduce happens for results received from
 * all the shards
 */
public class ConcurrentAggregationProcessor extends DefaultAggregationProcessor {
    protected AggregationCollectorManager createAggregationCollectorManager(SearchContext context) throws IOException {
        AggregatorFactories factories = context.aggregations().factories();
        List<Aggregator> globalAggregators = factories.createTopLevelGlobalAggregators(context);
        return new AggregationCollectorManager(context, globalAggregators);
    }
}
