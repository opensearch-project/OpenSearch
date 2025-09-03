/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.lease.Releasable;
import org.opensearch.search.aggregations.SearchResultsCollector;

import java.io.IOException;
import java.util.List;

// TODO make this <Query, Collector> generic type
public interface EngineSearcher extends Releasable {

    /**
     * The source that caused this searcher to be acquired.
     */
    String source();

    /**
     * Search using substrait query plan bytes and call the result collectors
     */
    default void search(byte[] substraitInput, List<SearchResultsCollector<?>> collectors) throws IOException {
        throw new UnsupportedOperationException();
    }
}
