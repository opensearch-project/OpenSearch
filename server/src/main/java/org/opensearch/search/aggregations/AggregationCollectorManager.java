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
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.opensearch.search.aggregations.AggregatorTreeEvaluator.evaluateAndRecreateIfNeeded;

/**
 * Common {@link CollectorManager} used by both concurrent and non-concurrent aggregation path and also for global and non-global
 * aggregation operators
 *
 * @opensearch.internal
 */
public abstract class AggregationCollectorManager implements CollectorManager<Collector, ReduceableSearchResult> {
    protected final SearchContext context;
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
        final Collector collector = createCollector(context, aggProvider);
        // For Aggregations we should not have a NO_OP_Collector
        assert collector != BucketCollector.NO_OP_COLLECTOR;
        return collector;
    }

    public String getCollectorReason() {
        return collectorReason;
    }

    public abstract String getCollectorName();

    @Override
    public ReduceableSearchResult reduce(Collection<Collector> collectors) throws IOException {
        final List<InternalAggregation> internals = context.bucketCollectorProcessor().toInternalAggregations(collectors);
        assert internals.stream().noneMatch(Objects::isNull);
        context.aggregations().resetBucketMultiConsumer();

        final InternalAggregations internalAggregations = InternalAggregations.from(internals);
        return buildAggregationResult(internalAggregations);
    }

    protected AggregationReduceableSearchResult buildAggregationResult(InternalAggregations internalAggregations) {
        return new AggregationReduceableSearchResult(internalAggregations);
    }

    static Collector createCollector(SearchContext searchContext, CheckedFunction<SearchContext, List<Aggregator>, IOException> aggProvider)
        throws IOException {
        Collector collector = MultiBucketCollector.wrap(aggProvider.apply(searchContext));

        // Evaluate streaming decision and potentially recreate tree
        collector = evaluateAndRecreateIfNeeded(collector, searchContext, aggProvider);

        ((BucketCollector) collector).preCollection();
        return collector;
    }
}
