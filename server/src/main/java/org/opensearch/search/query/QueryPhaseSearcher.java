/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Query;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.LinkedList;

/**
 * The extension point which allows to plug in custom search implementation to be
 * used at {@link QueryPhase}.
 *
 * @opensearch.internal
 */
public interface QueryPhaseSearcher {
    /**
     * Perform search using {@link CollectorManager}
     * @param searchContext search context
     * @param searcher context index searcher
     * @param query query
     * @param hasTimeout "true" if timeout was set, "false" otherwise
     * @return is rescoring required or not
     * @throws IOException IOException
     */
    boolean searchWith(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException;
}
