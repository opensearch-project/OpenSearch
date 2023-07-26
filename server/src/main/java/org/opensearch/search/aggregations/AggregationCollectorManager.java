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
import org.opensearch.common.CheckedFunction;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.query.InternalProfileCollector;
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Common {@link CollectorManager} used by both concurrent and non-concurrent aggregation path and also for global and non-global
 * aggregation operators
 */
class AggregationCollectorManager implements CollectorManager<Collector, ReduceableSearchResult> {
    private final SearchContext context;
    private final CheckedFunction<SearchContext, List<Aggregator>, IOException> aggProvider;
    private final String collectorReason;

    AggregationCollectorManager(
        SearchContext context,
        CheckedFunction<SearchContext, List<Aggregator>, IOException> aggProvider,
        String collectorReason
    ) {
        this.context = context;
        this.aggProvider = aggProvider;
        this.collectorReason = collectorReason;
    }

    @Override
    public Collector newCollector() throws IOException {
        final Collector collector = createCollector(context, aggProvider.apply(context), collectorReason);
        // For Aggregations we should not have a NO_OP_Collector
        assert collector != BucketCollector.NO_OP_COLLECTOR;
        return collector;
    }

    @Override
    public ReduceableSearchResult reduce(Collection<Collector> collectors) throws IOException {
        final List<Aggregator> aggregators = context.bucketCollectorProcessor().toAggregators(collectors);
        final List<InternalAggregation> internals = new ArrayList<>(aggregators.size());
        context.aggregations().resetBucketMultiConsumer();
        for (Aggregator aggregator : aggregators) {
            try {
                // post collection is called in ContextIndexSearcher after search on leaves are completed
                internals.add(aggregator.buildTopLevel());
            } catch (IOException e) {
                throw new AggregationExecutionException("Failed to build aggregation [" + aggregator.name() + "]", e);
            }
        }

        final InternalAggregations internalAggregations = InternalAggregations.from(internals);
        // Reduce the aggregations across slices before sending to the coordinator. We will perform shard level reduce iff multiple slices
        // were created to execute this request and it used concurrent segment search path
        // TODO: Add the check for flag that the request was executed using concurrent search
        if (collectors.size() > 1) {
            // using reduce is fine here instead of topLevelReduce as pipeline aggregation is evaluated on the coordinator after all
            // documents are collected across shards for an aggregation
            return new AggregationReduceableSearchResult(
                InternalAggregations.reduce(Collections.singletonList(internalAggregations), context.partialOnShard())
            );
        } else {
            return new AggregationReduceableSearchResult(internalAggregations);
        }
    }

    static Collector createCollector(SearchContext context, List<Aggregator> collectors, String reason) throws IOException {
        Collector collector = MultiBucketCollector.wrap(collectors);
        ((BucketCollector) collector).preCollection();
        if (context.getProfilers() != null) {
            collector = new InternalProfileCollector(
                collector,
                reason,
                // TODO: report on child aggs as well
                Collections.emptyList()
            );
        }
        return collector;
    }
}
