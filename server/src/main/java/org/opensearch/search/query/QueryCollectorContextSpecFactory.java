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
 *  QueryCollectorContextFactory
 */
public interface QueryCollectorContextSpecFactory {
    boolean supports(Query query);

    QueryCollectorContextSpec create(SearchContext searchContext, boolean hasFilterCollector) throws IOException;
}
