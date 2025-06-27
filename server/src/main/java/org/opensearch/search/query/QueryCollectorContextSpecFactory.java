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

/**
 *  interface of QueryCollectorContext spec factory
 */
public interface QueryCollectorContextSpecFactory {
    /**
     * check if query is supported to initialize the factory
     * @param query sent in the search request
     * @return true if query satisfies the factory initialization criteria
     */
    boolean supports(Query query);

    /**
     *
     * @param searchContext context needed to create collector context spec
     * @param hasFilterCollector flag true if filter collector there
     * @return QueryCollectorContextSpec
     * @throws IOException
     */
    QueryCollectorContextSpec createQueryCollectorContextSpec(SearchContext searchContext, boolean hasFilterCollector) throws IOException;
}
