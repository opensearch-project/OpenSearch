/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.search.Collector;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.query.CollectorResult;
import org.opensearch.search.profile.query.InternalProfileCollector;
import org.opensearch.search.query.QueryPhaseExecutionException;
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;
import java.util.Collection;
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
                context.queryCollectorManagers().put(AggregationProcessor.class, createAggregationCollectorManager(context));
            } catch (IOException e) {
                throw new AggregationInitializationException("Could not initialize aggregators", e);
            }
        }
    }

    protected AggregationCollectorManager createAggregationCollectorManager(SearchContext context) throws IOException {
        AggregatorFactories factories = context.aggregations().factories();
        List<Aggregator> globalAggregators = factories.createTopLevelGlobalAggregators(context);
        return new AggregationCollectorManager(context, globalAggregators) {
            private Collector collector;
            @Override
            public Collector newCollector() throws IOException {
                if (collector == null) {
                    collector = super.newCollector();
                }
                return collector;
            }

            @Override
            public ReduceableSearchResult reduce(Collection<Collector> collectors) throws IOException {
                return super.reduce(List.of(collector));
            }
        };
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
        
        AggregationCollectorManager collectorManager = (AggregationCollectorManager)context.queryCollectorManagers().get(AggregationProcessor.class);
        try {
            collectorManager.reduce(List.of()).reduce(context.queryResult());
        } catch (IOException ex) {
            throw new QueryPhaseExecutionException(context.shardTarget(), "Post processing failed", ex);
        }

        // disable aggregations so that they don't run on next pages in case of scrolling
        context.aggregations(null);
        context.queryCollectorManagers().remove(AggregationProcessor.class);
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
