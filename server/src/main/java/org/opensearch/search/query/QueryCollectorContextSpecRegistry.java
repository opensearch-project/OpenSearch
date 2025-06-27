/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.Query;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Registry class to load all collector context spec factories during cluster bootstrapping
 */
public class QueryCollectorContextSpecRegistry {

    private static final List<QueryCollectorContextSpecFactory> registry = new CopyOnWriteArrayList<>();

    static QueryCollectorContextSpecFactory getFactory(Query query) {

        return registry.stream().filter(entry -> entry.supports(query)).findFirst().orElse(null);
    }

    /**
     * Register factory
     * @param factory collector context spec factory defined in plugin
     */
    public static void registerFactory(QueryCollectorContextSpecFactory factory) {
        registry.add(factory);
    }
}
