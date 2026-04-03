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
import org.opensearch.vectorized.execution.search.spi.QueryResult;

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

    public abstract void executeQueryPhaseAsync(C context, Executor executor, ActionListener<QueryResult> listener);

    /**
     * execute Fetch Phase
     */
    public abstract void executeFetchPhase(C context) throws IOException;

    /**
     * Fetch Segment Stats
     */
    public abstract Map<String, FileStats> fetchSegmentStats() throws IOException;

    /**
     * Execute an indexed query using Lucene indexes to accelerate reads.
     * Default implementation throws UnsupportedOperationException.
     *
     * @param luceneReader  The Lucene DirectoryReader for this shard
     * @param query         The Lucene query to execute
     * @param numPartitions Number of execution partitions
     * @param bitsetMode    0 = AND (intersect), 1 = OR (union)
     * @param listener      ActionListener to receive the stream pointer
     */
    public void executeIndexedQuery(
        org.apache.lucene.index.DirectoryReader luceneReader,
        org.apache.lucene.search.Query query,
        int numPartitions,
        int bitsetMode,
        ActionListener<Long> listener
    ) {
        listener.onFailure(new UnsupportedOperationException("Indexed queries not supported by this engine"));
    }

    /**
     * Execute an indexed query with substrait plan using Lucene indexes to accelerate reads.
     * Default implementation delegates to the basic executeIndexedQuery (ignoring substrait).
     */
    public void executeIndexedQuery(
        org.apache.lucene.index.DirectoryReader luceneReader,
        org.apache.lucene.search.Query query,
        String tableName,
        byte[] substraitBytes,
        int numPartitions,
        int bitsetMode,
        boolean isQueryPlanExplainEnabled,
        ActionListener<Long> listener
    ) {
        // Default: fall back to basic version (no substrait)
        executeIndexedQuery(luceneReader, query, numPartitions, bitsetMode, listener);
    }

    /**
     * Execute the query phase using a pre-obtained stream pointer (e.g. from an indexed query).
     * Consumes the stream and populates results on the context using the same async path as executeQueryPhaseAsync.
     * Default implementation throws UnsupportedOperationException.
     */
    public void executeQueryPhaseWithStreamPointer(C context, long streamPointer, Executor executor, ActionListener<Map<String, Object[]>> listener) {
        listener.onFailure(new UnsupportedOperationException("executeQueryPhaseWithStreamPointer not supported by this engine"));
    }

    /**
     * Execute a boolean tree query. The tree bytes and contextId are produced
     * by the tree query executor. The engine deserializes the tree on the native
     * side and uses the contextId for JNI callbacks through
     * {@link org.opensearch.index.engine.exec.FilterTreeCallbackBridge}.
     *
     * @param treeBytes  serialized {@link IndexFilterTree}
     * @param contextId  the FilterTreeCallbackBridge registration ID
     * @param listener   receives the native stream pointer on success
     */
    public void executeTreeQuery(byte[] treeBytes, long contextId, ActionListener<Long> listener) {
        listener.onFailure(new UnsupportedOperationException("Tree queries not supported by this engine"));
    }
}
