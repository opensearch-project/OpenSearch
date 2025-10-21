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
 * Generic read engine interface that provides searcher operations and query phase execution
 * @param <C> Context type for query execution
 * @param <S> Searcher type that extends EngineSearcher
 * @param <R> Reference manager type
 */
@ExperimentalApi
public abstract class SearchExecEngine<C extends SearchContext, S extends EngineSearcher<?>, R> implements SearcherOperations<S, R> {
    /**
     * Create a search context for this engine
     */
    public abstract C createContext(
        ReaderContext readerContext,
        ShardSearchRequest request,
        SearchShardTarget searchShardTarget,
        SearchShardTask task,
        BigArrays bigArrays
    ) throws IOException;

    /**
     * execute query
     * TODO : Result type
     * @return query results
     */
    public abstract Map<String, Object[]> execute(C context) throws IOException;
}
