/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.query.CollectorResult;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

/**
 * {@link CollectorManager} to take care of global aggregation operators in case of concurrent segment search
 */
public class GlobalAggCollectorManager extends AggregationCollectorManager {

    private Collector collector;

    public GlobalAggCollectorManager(SearchContext context) throws IOException {
        super(context, context.aggregations().factories()::createTopLevelGlobalAggregators, CollectorResult.REASON_AGGREGATION_GLOBAL);
        collector = Objects.requireNonNull(super.newCollector(), "collector instance is null");
    }

    @Override
    public Collector newCollector() throws IOException {
        if (collector != null) {
            final Collector toReturn = collector;
            collector = null;
            return toReturn;
        } else {
            return super.newCollector();
        }
    }

    @Override
    protected AggregationReduceableSearchResult buildAggregationResult(InternalAggregations internalAggregations) {
        // Reduce the aggregations across slices before sending to the coordinator. We will perform shard level reduce as long as any slices
        // were created so that we can apply shard level bucket count thresholds in the reduce phase.
        return new AggregationReduceableSearchResult(
            InternalAggregations.reduce(Collections.singletonList(internalAggregations), context.partialOnShard())
        );
    }
}
