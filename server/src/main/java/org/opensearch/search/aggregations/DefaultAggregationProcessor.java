/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.query.CollectorResult;
import org.opensearch.search.profile.query.InternalProfileCollector;
import org.opensearch.search.query.QueryPhaseExecutionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * {@link AggregationProcessor} implementation which is used with {@link org.opensearch.search.query.QueryPhase.DefaultQueryPhaseSearcher}.
 * This is the default implementation which works when collection for aggregations happen in sequential manner. It doesn't perform any
 * reduce on the collected documents at shard level
 */
public class DefaultAggregationProcessor implements AggregationProcessor {
    @Override
    public void preProcess(SearchContext context) {
        if (context.aggregations() != null) {
            try {
                AggregatorFactories factories = context.aggregations().factories();
                List<Aggregator> globalAggregators = factories.createTopLevelGlobalAggregators(context);
                context.aggregations().addGlobalAggregators(globalAggregators);
                if (factories.hasNonGlobalAggregator()) {
                    context.queryCollectorManagers().put(AggregationProcessor.class, new AggregationCollectorManager(context));
                }
            } catch (IOException e) {
                throw new AggregationInitializationException("Could not initialize aggregators", e);
            }
        }
    }

    @Override
    public void processGlobalAggregators(SearchContext context) {
        List<Aggregator> globals = context.aggregations().getGlobalAggregators();
        // optimize the global collector based execution
        if (!globals.isEmpty()) {
            BucketCollector globalsCollector = MultiBucketCollector.wrap(globals);
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

    @Override
    public void postProcess(SearchContext context) {
        if (context.aggregations() == null) {
            context.queryResult().aggregations(null);
            return;
        }

        if (context.queryResult().hasAggs()) {
            // no need to compute the aggs twice, they should be computed on a per context basis
            return;
        }

        List<Aggregator> globals = context.aggregations().getGlobalAggregators();
        // optimize the global collector based execution
        processGlobalAggregators(context);

        List<Aggregator> nonGlobals = context.aggregations().getNonGlobalAggregators();
        // create a combined list of all the aggregators
        List<Aggregator> allAggregator = new ArrayList<>(globals);
        allAggregator.addAll(nonGlobals);

        List<InternalAggregation> aggregations = new ArrayList<>(allAggregator.size());
        context.aggregations().resetBucketMultiConsumer();
        for (Aggregator aggregator : allAggregator) {
            try {
                aggregator.postCollection();
                aggregations.add(aggregator.buildTopLevel());
            } catch (IOException e) {
                throw new AggregationExecutionException("Failed to build aggregation [" + aggregator.name() + "]", e);
            }
        }
        populateResult(context, aggregations);

        // disable aggregations so that they don't run on next pages in case of scrolling
        context.aggregations(null);
        context.queryCollectorManagers().remove(AggregationProcessor.class);
    }

    /**
     * Method to populate aggregation results in {@link org.opensearch.search.query.QuerySearchResult} to send to coordinator
     * @param context {@link SearchContext} for the request
     * @param aggregations list of {@link InternalAggregation} created post document collections for aggregations
     */
    protected void populateResult(SearchContext context, List<InternalAggregation> aggregations) {
        context.queryResult().aggregations(new InternalAggregations(aggregations));
    }

    static Collector createCollector(SearchContext context, List<Aggregator> collectors) throws IOException {
        Collector collector = MultiBucketCollector.wrap(collectors);
        ((BucketCollector) collector).preCollection();
        if (context.getProfilers() != null) {
            collector = new InternalProfileCollector(
                collector,
                CollectorResult.REASON_AGGREGATION,
                // TODO: report on child aggs as well
                Collections.emptyList()
            );
        }
        return collector;
    }
}
