/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.search.aggregations.SearchResultsCollector;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

@ExperimentalApi
// TODO make this <Query, Collector> generic type
public interface EngineSearcher<Q,C> extends Releasable {

    /**
     * The source that caused this searcher to be acquired.
     */
    String source();

    /**
     * Search using substrait query plan bytes and call the result collectors
     */
    default void search(Q query, List<SearchResultsCollector<C>> collectors) throws IOException {
        throw new UnsupportedOperationException();
    }

    default long search(Q query, Long runtimePtr) throws IOException {
        throw new UnsupportedOperationException();
    }

    default long search(Q query, Long tokioRuntimePtr, Long globalRuntimePtr) throws IOException {
        throw new UnsupportedOperationException();
    }
}
