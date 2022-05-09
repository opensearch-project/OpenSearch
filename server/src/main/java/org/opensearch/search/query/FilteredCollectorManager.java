/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Weight;
import org.opensearch.common.lucene.search.FilteredCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Manager for the FilteredCollector
 *
 * @opensearch.internal
 */
class FilteredCollectorManager implements CollectorManager<FilteredCollector, ReduceableSearchResult> {
    private final CollectorManager<? extends Collector, ReduceableSearchResult> manager;
    private final Weight filter;

    FilteredCollectorManager(CollectorManager<? extends Collector, ReduceableSearchResult> manager, Weight filter) {
        this.manager = manager;
        this.filter = filter;
    }

    @Override
    public FilteredCollector newCollector() throws IOException {
        return new FilteredCollector(manager.newCollector(), filter);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ReduceableSearchResult reduce(Collection<FilteredCollector> collectors) throws IOException {
        final Collection<Collector> subCollectors = new ArrayList<>();

        for (final FilteredCollector collector : collectors) {
            subCollectors.add(collector.getCollector());
        }

        return ((CollectorManager<Collector, ReduceableSearchResult>) manager).reduce(subCollectors);
    }
}
