/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.Query;
import org.opensearch.plugins.SearchPlugin;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * QueryCollectorContextFactoryRegistery
 */
public class QueryCollectorContextFactoryRegistery {
    private static final Map<Class<?>, QueryCollectorContextFactory> registry = new ConcurrentHashMap<>();

    public static QueryCollectorContextFactory getFactory(Query query) {
        // QueryCollectorContextFactory factory = registry.get(classType);
        // if (factory == null) {
        // throw new IllegalArgumentException("No factory found for query builder: " + classType);
        // }
        // return factory;

        return registry.entrySet()
            .stream()
            .filter(entry -> entry.getKey().isInstance(query))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(null);
    }

    public static void registerFactory(SearchPlugin.FactorySpec<?> specs) {
        registry.put(specs.getClassType(), specs.getQueryCollectorContextFactory());
    }
}
