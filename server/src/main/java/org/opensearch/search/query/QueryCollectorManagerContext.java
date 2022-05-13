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
import java.util.ArrayList;
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

    private static class OpaqueQueryCollectorManager extends QueryCollectorManager {
        private OpaqueQueryCollectorManager(Collection<CollectorManager<? extends Collector, ReduceableSearchResult>> managers) {
            super(managers);
        }

        @Override
        protected ReduceableSearchResult reduceWith(final ReduceableSearchResult[] results) {
            return (QuerySearchResult result) -> {};
        }
    }

    public static CollectorManager<? extends Collector, ReduceableSearchResult> createOpaqueCollectorManager(
        List<CollectorManager<? extends Collector, ReduceableSearchResult>> managers
    ) throws IOException {
        return new OpaqueQueryCollectorManager(managers);
    }

    public static CollectorManager<? extends Collector, ReduceableSearchResult> createMultiCollectorManager(
        List<QueryCollectorContext> collectors
    ) throws IOException {
        final Collection<CollectorManager<? extends Collector, ReduceableSearchResult>> managers = new ArrayList<>();

        CollectorManager<?, ReduceableSearchResult> manager = null;
        for (QueryCollectorContext ctx : collectors) {
            manager = ctx.createManager(manager);
            managers.add(manager);
        }

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
