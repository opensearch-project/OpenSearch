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
import org.opensearch.search.profile.query.InternalProfileCollectorManager;
import org.opensearch.search.profile.query.InternalProfileComponent;
import org.opensearch.search.query.QueryPhaseExecutionException;
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;
import java.util.Collections;

/**
 * {@link AggregationProcessor} implementation to be used with {@link org.opensearch.search.query.ConcurrentQueryPhaseSearcher}. It takes
 * care of performing shard level reduce on Aggregation results collected as part of concurrent execution among slices. This is done to
 * avoid the increase in aggregation result sets returned by each shard to coordinator where final reduce happens for results received from
 * all the shards
 */
public class ConcurrentAggregationProcessor implements AggregationProcessor {

    private final BucketCollectorProcessor bucketCollectorProcessor = new BucketCollectorProcessor();

    @Override
    public void preProcess(SearchContext context) {
        try {
            if (context.aggregations() != null) {
                // update the bucket collector process as there is aggregation in the request
                context.setBucketCollectorProcessor(bucketCollectorProcessor);
                if (context.aggregations().factories().hasNonGlobalAggregator()) {
                    context.queryCollectorManagers().put(NonGlobalAggCollectorManager.class, new NonGlobalAggCollectorManager(context));
                }
                // initialize global aggregators as well, such that any failure to initialize can be caught before executing the request
                if (context.aggregations().factories().hasGlobalAggregator()) {
                    context.queryCollectorManagers().put(GlobalAggCollectorManager.class, new GlobalAggCollectorManager(context));
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

        // for concurrent case we will perform only global aggregation in post process as QueryResult is already populated with results of
        // processing the non-global aggregation
        CollectorManager<? extends Collector, ReduceableSearchResult> globalCollectorManager = context.queryCollectorManagers()
            .get(GlobalAggCollectorManager.class);
        try {
            if (globalCollectorManager != null) {
                Query query = context.buildFilteredQuery(Queries.newMatchAllQuery());
                globalCollectorManager = new InternalProfileCollectorManager(
                    globalCollectorManager,
                    CollectorResult.REASON_AGGREGATION_GLOBAL,
                    Collections.emptyList()
                );
                if (context.getProfilers() != null) {
                    context.getProfilers().addQueryProfiler().setCollector((InternalProfileComponent) globalCollectorManager);
                }
                final ReduceableSearchResult result = context.searcher().search(query, globalCollectorManager);
                result.reduce(context.queryResult());
            }
        } catch (Exception e) {
            throw new QueryPhaseExecutionException(context.shardTarget(), "Failed to execute global aggregators", e);
        }

        // disable aggregations so that they don't run on next pages in case of scrolling
        context.aggregations(null);
        context.queryCollectorManagers().remove(NonGlobalAggCollectorManager.class);
        context.queryCollectorManagers().remove(GlobalAggCollectorManager.class);
    }
}
