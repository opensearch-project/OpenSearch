/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

/**
 * Creates a {@link QueryRegistry} populated with all supported query translators.
 */
public class QueryRegistryFactory {

    private QueryRegistryFactory() {}

    /** Creates a registry with all supported query translators. */
    public static QueryRegistry create() {
        QueryRegistry registry = new QueryRegistry();
        registry.register(new TermQueryTranslator());
        registry.register(new MatchAllQueryTranslator());
        // TODO: add other query translators
        return registry;
    }
}
