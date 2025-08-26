/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.Query;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Registry class to load all collector context spec factories during cluster bootstrapping
 */
public final class QueryCollectorContextSpecRegistry {
    private static final List<QueryCollectorContextSpecFactory> registry = new CopyOnWriteArrayList<>();

    private QueryCollectorContextSpecRegistry() {}

    /**
     * Get all collector context spec factories
     * @return list of collector context spec factories
     */
    public static List<QueryCollectorContextSpecFactory> getCollectorContextSpecFactories() {
        return registry;
    }

    /**
     * Register factory
     * @param factory collector context spec factory defined in plugin
     */
    public static void registerFactory(QueryCollectorContextSpecFactory factory) {
        registry.add(factory);
    }

    /**
     * Get collector context spec
     * @param searchContext search context
     * @param query required to create collectorContext spec
     * @param queryCollectorArguments query collector arguments
     * @return collector context spec
     * @throws IOException
     */
    public static Optional<QueryCollectorContextSpec> getQueryCollectorContextSpec(
        final SearchContext searchContext,
        final Query query,
        final QueryCollectorArguments queryCollectorArguments
    ) throws IOException {
        Iterator<QueryCollectorContextSpecFactory> iterator = registry.iterator();
        while (iterator.hasNext()) {
            QueryCollectorContextSpecFactory factory = iterator.next();
            Optional<QueryCollectorContextSpec> spec = factory.createQueryCollectorContextSpec(
                searchContext,
                query,
                queryCollectorArguments
            );
            if (spec.isEmpty() == false) {
                return spec;
            }
        }
        return Optional.empty();
    }
}
