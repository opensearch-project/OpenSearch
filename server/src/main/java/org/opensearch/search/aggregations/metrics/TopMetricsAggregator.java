/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.SubSearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * top_metrics aggregator backed by top_hits collection/reduction behavior.
 *
 * @opensearch.internal
 */
class TopMetricsAggregator extends MetricsAggregator {

    private final TopHitsAggregator delegate;
    private final List<String> metricFields;

    TopMetricsAggregator(
        FetchPhase fetchPhase,
        SubSearchContext subSearchContext,
        String name,
        List<String> metricFields,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.metricFields = List.copyOf(metricFields);
        this.delegate = new TopHitsAggregator(fetchPhase, subSearchContext, name, context, parent, metadata);
    }

    @Override
    public ScoreMode scoreMode() {
        return delegate.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        return delegate.getLeafCollector(ctx, sub);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrd) throws IOException {
        InternalTopHits topHits = (InternalTopHits) delegate.buildAggregation(owningBucketOrd);
        return new InternalTopMetrics(name, topHits, metricFields, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalTopHits topHits = (InternalTopHits) delegate.buildEmptyAggregation();
        return new InternalTopMetrics(name, topHits, metricFields, metadata());
    }

    @Override
    protected void doClose() {
        delegate.close();
    }
}
