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
 * QueryCollectorContextFactoryRegistery
 */
public class QueryCollectorContextSpecRegistry {

    private static final List<QueryCollectorContextSpecFactory> registry = new CopyOnWriteArrayList<>();

    public static QueryCollectorContextSpecFactory getFactory(Query query) {

        return registry.stream().filter(entry -> entry.supports(query)).findFirst().orElse(null);
    }

    public static void registerFactory(QueryCollectorContextSpecFactory factory) {
        registry.add(factory);
    }
}
