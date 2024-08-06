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
        final Collector collector = createCollector(aggProvider.apply(context));
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

        // PipelineTreeSource is serialized to the coordinators on older OpenSearch versions for bwc but is deprecated in latest release
        // To handle that we need to add it in the InternalAggregations object sent in QuerySearchResult.
        final InternalAggregations internalAggregations = new InternalAggregations(
            internals,
            context.request().source().aggregations()::buildPipelineTree
        );
        return buildAggregationResult(internalAggregations);
    }

    protected AggregationReduceableSearchResult buildAggregationResult(InternalAggregations internalAggregations) {
        return new AggregationReduceableSearchResult(internalAggregations);
    }

    static Collector createCollector(List<Aggregator> collectors) throws IOException {
        Collector collector = MultiBucketCollector.wrap(collectors);
        ((BucketCollector) collector).preCollection();
        return collector;
    }
}
