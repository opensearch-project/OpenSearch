/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.read;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.engine.exec.bridge.SearcherOperations;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.util.Map;

/**
 * The Search execution engine interface
 * @param <Q> The engine searcher {@link EngineSearcher}
 * @param <R> The engine reader manager {@link EngineReaderManager}
 */
@ExperimentalApi
public interface SearchExecEngine<Q extends EngineSearcher, R> {

    SearchContext createContext(
        ReaderContext readerContext,
        ShardSearchRequest request,
        SearchShardTarget searchShardTarget,
        SearchShardTask task,
        BigArrays bigArrays
    ) throws IOException;

    Map<String, Object[]> execute(SearchContext context) throws IOException;

    SearcherOperations<Q, R> getSearcherOperations();
}
