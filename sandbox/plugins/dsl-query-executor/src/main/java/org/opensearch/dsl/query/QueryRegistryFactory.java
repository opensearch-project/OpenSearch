/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.opensearch.dsl.query.range.RangeQueryTranslator;

/**
 * Creates a {@link QueryRegistry} populated with all supported query translators.
 */
public class QueryRegistryFactory {

    /** Shared, immutable-after-init registry returned by {@link #create()}. */
    private static final QueryRegistry INSTANCE = newInstance();

    private QueryRegistryFactory() {}

    /** A fresh {@link QueryRegistry} with all supported translators. */
    // VisibleForTesting - tests use this to register extra translators without mutating the shared instance.
    public static QueryRegistry newInstance() {
        QueryRegistry registry = new QueryRegistry();
        registry.register(new TermQueryTranslator());
        registry.register(new TermsQueryTranslator());
        registry.register(new MatchAllQueryTranslator());
        registry.register(new ExistsQueryTranslator());
        registry.register(new RangeQueryTranslator());
        // TODO: add other query translators
        return registry;
    }

    /** Returns the shared registry. All callers see the same instance. */
    public static QueryRegistry create() {
        return INSTANCE;
    }
}
