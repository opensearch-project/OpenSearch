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
import org.opensearch.search.profile.query.CollectorResult;
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * {@link CollectorManager} to take care of non-global aggregation operators in case of non-concurrent segment search. This
 * CollectorManager returns the same collector instance (i.e. created in constructor of super class) on each newCollector call
 */
public class NonGlobalAggCollectorManagerWithSingleCollector extends AggregationCollectorManager {

    private final Collector collector;

    public NonGlobalAggCollectorManagerWithSingleCollector(SearchContext context) throws IOException {
        super(context, context.aggregations().factories()::createTopLevelNonGlobalAggregators, CollectorResult.REASON_AGGREGATION);
        collector = Objects.requireNonNull(super.newCollector(), "collector instance is null");
    }

    @Override
    public Collector newCollector() throws IOException {
        return collector;
    }

    @Override
    public ReduceableSearchResult reduce(Collection<Collector> collectors) throws IOException {
        assert collectors.isEmpty() : "Reduce on NonGlobalAggregationCollectorManagerWithCollector called with non-empty collectors";
        return super.reduce(List.of(collector));
    }
}
