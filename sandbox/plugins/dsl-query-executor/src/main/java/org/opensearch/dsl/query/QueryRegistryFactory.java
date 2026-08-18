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
 * Returns the process-wide {@link QueryRegistry} populated with all supported query
 * translators. Registrations are effectively immutable after class init, and the registry
 * is safe to share across threads (concurrent reads, no writes at steady state).
 */
public class QueryRegistryFactory {

    /**
     * Built once at class init. The registry is populated in a private helper (not inline
     * so we can keep the {@code register(...)} calls readable) and cached forever.
     */
    private static final QueryRegistry INSTANCE = newInstance();

    private QueryRegistryFactory() {}

    /**
     * Returns a fresh registry populated with all supported translators. Production code should
     * use {@link #create()} (the shared, effectively-immutable singleton); callers that need to
     * register additional translators — e.g. tests — must use this so they never mutate the
     * shared instance.
     */
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
