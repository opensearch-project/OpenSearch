/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.exec.FileStats;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;

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
     * Create a search context for this engine
     */
    public abstract C createContext(ReaderContext readerContext, ShardSearchRequest request, SearchShardTarget searchShardTarget, SearchShardTask task, BigArrays bigArrays, SearchContext originalContext, ClusterService clusterService) throws IOException;

    /**
     * execute Query Phase
     */
    public abstract void executeQueryPhase(C context) throws IOException;

    public abstract void executeQueryPhaseAsync(C context, Executor executor, ActionListener<Map<String, Object[]>> listener);

    /**
     * execute Fetch Phase
     */
    public abstract void executeFetchPhase(C context) throws IOException;

    /**
     * Fetch Segment Stats
     */
    public abstract Map<String, FileStats> fetchSegmentStats() throws IOException;
}
