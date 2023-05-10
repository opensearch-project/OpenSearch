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
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.opensearch.search.aggregations.DefaultAggregationProcessor.createCollector;

/**
 * {@link CollectorManager} to take care of aggregation operators both in case of concurrent and non-concurrent
 * segment search
 */
public class AggregationCollectorManager implements CollectorManager<Collector, ReduceableSearchResult> {
    private final SearchContext context;

    public AggregationCollectorManager(SearchContext context) {
        this.context = context;
    }

    @Override
    public Collector newCollector() throws IOException {
        List<Aggregator> nonGlobalAggregators = context.aggregations().factories().createTopLevelNonGlobalAggregators(context);
        assert !nonGlobalAggregators.isEmpty() : "Expected atleast one non global aggregator to be present";
        context.aggregations().addNonGlobalAggregators(nonGlobalAggregators);
        return createCollector(context, nonGlobalAggregators);
    }

    @Override
    public ReduceableSearchResult reduce(Collection<Collector> collectors) throws IOException {
        return (result) -> {};
    }
}
