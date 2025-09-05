/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.GenericQueryPhaseSearcher;
import org.opensearch.search.query.QueryPhaseExecutor;

import java.io.IOException;

/**
 * Generic read engine interface that provides searcher operations and query phase execution
 * @param <C> Context type for query execution
 * @param <S> Searcher type that extends EngineSearcher
 * @param <R> Reference manager type
 * @param <Q> Query type
 */
@ExperimentalApi
// TODO too many templatized types
public abstract class SearchExecEngine<C extends SearchContext, S extends EngineSearcher<?,?>, R, Q> implements SearcherOperations<S, R> {

    /**
     * Get the query phase searcher for this engine
     */
    public abstract GenericQueryPhaseSearcher<C,S, Q> getQueryPhaseSearcher();

    /**
     * Get the query phase executor for this engine
     */
    public abstract QueryPhaseExecutor<C> getQueryPhaseExecutor();

    /**
     * Create a search context for this engine
     */
    public abstract C createContext(ReaderContext readerContext, ShardSearchRequest request, SearchShardTask task) throws IOException;
}
