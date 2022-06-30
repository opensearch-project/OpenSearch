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
import org.opensearch.common.lucene.MinimumScoreCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Manager for the MinimumCollector
 */
class MinimumCollectorManager implements CollectorManager<MinimumScoreCollector, ReduceableSearchResult> {
    private final CollectorManager<? extends Collector, ReduceableSearchResult> manager;
    private final float minimumScore;

    MinimumCollectorManager(CollectorManager<? extends Collector, ReduceableSearchResult> manager, float minimumScore) {
        this.manager = manager;
        this.minimumScore = minimumScore;
    }

    @Override
    public MinimumScoreCollector newCollector() throws IOException {
        return new MinimumScoreCollector(manager.newCollector(), minimumScore);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ReduceableSearchResult reduce(Collection<MinimumScoreCollector> collectors) throws IOException {
        final Collection<Collector> subCollectors = new ArrayList<>();

        for (final MinimumScoreCollector collector : collectors) {
            subCollectors.add(collector.getCollector());
        }

        return ((CollectorManager<Collector, ReduceableSearchResult>) manager).reduce(subCollectors);
    }
}
