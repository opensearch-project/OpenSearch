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
import org.apache.lucene.search.MultiCollectorManager;
import org.opensearch.search.profile.query.InternalProfileCollectorManager;
import org.opensearch.search.profile.query.ProfileCollectorManager;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Context used for the query collector manager
 *
 * @opensearch.internal
 */
public abstract class QueryCollectorManagerContext {
    private static class QueryCollectorManager implements CollectorManager<Collector, ReduceableSearchResult> {
        private final MultiCollectorManager manager;

        private QueryCollectorManager(Collection<CollectorManager<? extends Collector, ReduceableSearchResult>> managers) {
            this.manager = new MultiCollectorManager(managers.toArray(new CollectorManager<?, ?>[0]));
        }

        @Override
        public Collector newCollector() throws IOException {
            return manager.newCollector();
        }

        @Override
        public ReduceableSearchResult reduce(Collection<Collector> collectors) throws IOException {
            final Object[] results = manager.reduce(collectors);

            final ReduceableSearchResult[] transformed = new ReduceableSearchResult[results.length];
            for (int i = 0; i < results.length; ++i) {
                assert results[i] instanceof ReduceableSearchResult;
                transformed[i] = (ReduceableSearchResult) results[i];
            }

            return reduceWith(transformed);
        }

        protected ReduceableSearchResult reduceWith(final ReduceableSearchResult[] results) {
            return (QuerySearchResult result) -> {
                for (final ReduceableSearchResult r : results) {
                    r.reduce(result);
                }
            };
        }
    }

    /**
     * Create query {@link CollectorManager} tree using the provided query collector contexts
     * @param collectorContexts list of {@link QueryCollectorContext}
     * @return {@link CollectorManager} representing the manager tree for the query
     */
    public static CollectorManager<? extends Collector, ReduceableSearchResult> createQueryCollectorManager(
        List<QueryCollectorContext> collectorContexts
    ) throws IOException {
        CollectorManager<?, ReduceableSearchResult> manager = null;
        for (QueryCollectorContext ctx : collectorContexts) {
            manager = ctx.createManager(manager);
        }
        return manager;
    }

    public static CollectorManager<? extends Collector, ReduceableSearchResult> createMultiCollectorManager(
        List<CollectorManager<?, ReduceableSearchResult>> managers
    ) {
        return new QueryCollectorManager(managers);
    }

    public static ProfileCollectorManager<? extends Collector, ReduceableSearchResult> createQueryCollectorManagerWithProfiler(
        List<QueryCollectorContext> collectors
    ) throws IOException {
        InternalProfileCollectorManager manager = null;

        for (QueryCollectorContext ctx : collectors) {
            manager = ctx.createWithProfiler(manager);
        }

        return manager;
    }
}
