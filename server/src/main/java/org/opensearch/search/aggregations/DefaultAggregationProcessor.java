/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.search.Query;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.query.InternalProfileComponent;
import org.opensearch.search.query.QueryPhaseExecutionException;

import java.io.IOException;
import java.util.List;

/**
 * {@link AggregationProcessor} implementation which is used with {@link org.opensearch.search.query.QueryPhase.DefaultQueryPhaseSearcher}.
 * This is the default implementation which works when collection for aggregations happen in sequential manner. It doesn't perform any
 * reduce on the collected documents at shard level
 */
public class DefaultAggregationProcessor implements AggregationProcessor {

    private final BucketCollectorProcessor bucketCollectorProcessor = new BucketCollectorProcessor();

    @Override
    public void preProcess(SearchContext context) {
        try {
            if (context.aggregations() != null) {
                // update the bucket collector process as there is aggregation in the request
                context.setBucketCollectorProcessor(bucketCollectorProcessor);
                if (context.aggregations().factories().hasNonGlobalAggregator()) {
                    context.queryCollectorManagers()
                        .put(NonGlobalAggCollectorManager.class, new NonGlobalAggCollectorManagerWithSingleCollector(context));
                }
                // initialize global aggregators as well, such that any failure to initialize can be caught before executing the request
                if (context.aggregations().factories().hasGlobalAggregator()) {
                    context.queryCollectorManagers()
                        .put(GlobalAggCollectorManager.class, new GlobalAggCollectorManagerWithSingleCollector(context));
                }
            }
        } catch (IOException ex) {
            throw new AggregationInitializationException("Could not initialize aggregators", ex);
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

        final AggregationCollectorManager nonGlobalCollectorManager = (AggregationCollectorManager) context.queryCollectorManagers()
            .get(NonGlobalAggCollectorManager.class);
        final AggregationCollectorManager globalCollectorManager = (AggregationCollectorManager) context.queryCollectorManagers()
            .get(GlobalAggCollectorManager.class);
        try {
            if (nonGlobalCollectorManager != null) {
                nonGlobalCollectorManager.reduce(List.of()).reduce(context.queryResult());
            }

            try {
                if (globalCollectorManager != null) {
                    Query query = context.buildFilteredQuery(Queries.newMatchAllQuery());
                    if (context.getProfilers() != null) {
                        context.getProfilers()
                            .addQueryProfiler()
                            .setCollector((InternalProfileComponent) globalCollectorManager.newCollector());
                    }
                    context.searcher().search(query, globalCollectorManager.newCollector());
                    globalCollectorManager.reduce(List.of()).reduce(context.queryResult());
                }
            } catch (Exception e) {
                throw new QueryPhaseExecutionException(context.shardTarget(), "Failed to execute global aggregators", e);
            }
        } catch (IOException ex) {
            throw new QueryPhaseExecutionException(context.shardTarget(), "Post processing failed for aggregators", ex);
        }

        // disable aggregations so that they don't run on next pages in case of scrolling
        context.aggregations(null);
        context.queryCollectorManagers().remove(NonGlobalAggCollectorManager.class);
        context.queryCollectorManagers().remove(GlobalAggCollectorManager.class);
    }
}
