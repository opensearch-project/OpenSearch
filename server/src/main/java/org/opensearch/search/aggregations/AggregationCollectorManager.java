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
import org.apache.lucene.search.Query;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.query.CollectorResult;
import org.opensearch.search.profile.query.InternalProfileCollector;
import org.opensearch.search.query.QueryPhaseExecutionException;
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.opensearch.search.aggregations.DefaultAggregationProcessor.createCollector;

/**
 * {@link CollectorManager} to take care of aggregation operators both in case of concurrent and non-concurrent
 * segment search
 */
public class AggregationCollectorManager implements CollectorManager<Collector, ReduceableSearchResult> {
    private final SearchContext context;
    private final List<Aggregator> globalAggregators;

    public AggregationCollectorManager(SearchContext context, List<Aggregator> globalAggregators) {
        this.context = context;
        this.globalAggregators = globalAggregators;
    }

    @Override
    public Collector newCollector() throws IOException {
        List<Aggregator> nonGlobalAggregators = context.aggregations().factories().createTopLevelNonGlobalAggregators(context);
        return createCollector(context, nonGlobalAggregators);
    }

    @Override
    public ReduceableSearchResult reduce(Collection<Collector> collectors) throws IOException {
        processGlobalAggregators(context);

        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.addAll(globalAggregators);

        for (Collector collector: collectors) {
            if (collector instanceof Aggregator) {
                aggregators.add((Aggregator)collector);
            } else if (collector instanceof MultiBucketCollector) {
                for (Collector inner: ((MultiBucketCollector)collector).getCollectors()) {
                    if (inner instanceof Aggregator) {
                        aggregators.add((Aggregator)inner);
                    }
                }
            }
        }

        final List<InternalAggregation> internals = new ArrayList<>(aggregators.size());
        context.aggregations().resetBucketMultiConsumer();
        for (Aggregator aggregator : aggregators) {
            try {
                aggregator.postCollection();
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
            return (result) -> 
                result.aggregations(
                    InternalAggregations.reduce(
                        Collections.singletonList(internalAggregations),
                        context.aggregationReduceContext())
            );
        } else {
            return (result) -> {
                result.aggregations(internalAggregations);
            };
        }
    }
    
    private void processGlobalAggregators(SearchContext context) {
        // optimize the global collector based execution
        BucketCollector globalsCollector = MultiBucketCollector.wrap(globalAggregators);
        Query query = context.buildFilteredQuery(Queries.newMatchAllQuery());

        try {
            final Collector collector;
            if (context.getProfilers() == null) {
                collector = globalsCollector;
            } else {
                InternalProfileCollector profileCollector = new InternalProfileCollector(
                    globalsCollector,
                    CollectorResult.REASON_AGGREGATION_GLOBAL,
                    // TODO: report on sub collectors
                    Collections.emptyList()
                );
                collector = profileCollector;
                // start a new profile with this collector
                context.getProfilers().addQueryProfiler().setCollector(profileCollector);
            }
            globalsCollector.preCollection();
            context.searcher().search(query, collector);
        } catch (Exception e) {
            throw new QueryPhaseExecutionException(context.shardTarget(), "Failed to execute global aggregators", e);
        }
    }
}
